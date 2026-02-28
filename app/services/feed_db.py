from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.db.engine import make_engine
from app.db.models.rules import RawItem, Story, StoryItem
from app.services.event_type_rules import infer_event_type
from app.services.source_meta_index import build_source_meta_index
from app.services.zh_enricher import ZhEnricher
from app.utils.url_norm import url_norm


def _safe_dt(v: str | None) -> dt.datetime | None:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _encode_cursor(ts: str, row_id: str) -> str:
    payload = json.dumps({"ts": ts, "id": row_id}, separators=(",", ":"), ensure_ascii=False)
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str | None) -> tuple[str, str]:
    s = str(cursor or "").strip()
    if not s:
        return "", ""
    try:
        pad = "=" * ((4 - len(s) % 4) % 4)
        raw = base64.urlsafe_b64decode((s + pad).encode("ascii")).decode("utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            return "", ""
        return str(data.get("ts", "")).strip(), str(data.get("id", "")).strip()
    except Exception:
        return "", ""


def _encode_any_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_any_cursor(cursor: str | None) -> dict[str, Any]:
    s = str(cursor or "").strip()
    if not s:
        return {}
    try:
        pad = "=" * ((4 - len(s) % 4) % 4)
        raw = base64.urlsafe_b64decode((s + pad).encode("ascii")).decode("utf-8")
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _normalize_title(title: str) -> str:
    t = str(title or "").lower().strip()
    if not t:
        return ""
    t = re.sub(r"^(stat\+\s*:|comment\s*:|\[comment\]|\[shinsa\])\s*", "", t)
    t = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _contains_cjk(text: str) -> bool:
    s = str(text or "")
    for ch in s:
        if "\u4e00" <= ch <= "\u9fff":
            return True
    return False


def _compact_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _first_sentence(text: str, *, max_chars: int = 180) -> str:
    s = _compact_spaces(text)
    if not s:
        return ""
    parts = re.split(r"(?<=[\.\!\?。！？；;])\s+", s)
    first = parts[0] if parts else s
    return first[: max(30, int(max_chars))]


def _title_zh(title: str, event_type: str = "") -> str:
    t = _compact_spaces(title)
    if not t:
        return ""
    if _contains_cjk(t):
        return t

    low = t.lower()
    # Lightweight localization for high-frequency finance/newswire titles.
    repl = [
        (r"\breports?\s+results?\b", "发布业绩"),
        (r"\bfinancial\s+results?\b", "财务结果"),
        (r"\breceives?\b", "收到"),
        (r"\bnotification\b", "通知"),
        (r"\bannounces?\b", "宣布"),
        (r"\blaunch(es|ed)?\b", "发布"),
        (r"\bacquires?\b|\bacquisition\b", "并购"),
        (r"\bapproval\b|\bapproved\b|\bclearance\b|\bcleared\b", "获批"),
        (r"\bclinical\s+trial\b|\bclinical\s+study\b", "临床研究"),
        (r"\brecall\b", "召回"),
    ]
    out = t
    for pat, zh in repl:
        out = re.sub(pat, zh, out, flags=re.IGNORECASE)
    if out != t:
        return out

    et = str(event_type or "").strip().lower()
    prefix = {
        "regulatory": "监管动态",
        "approval_clearance": "审批进展",
        "procurement": "招采动态",
        "company_update": "企业动态",
        "funding": "融资动态",
        "m_and_a": "并购动态",
        "clinical_trial": "临床进展",
        "product_launch": "产品发布",
        "market_report": "市场报告",
    }.get(et, "行业动态")
    return f"{prefix}：{t}"


def _summary_zh(title: str, snippet: str, source_id: str = "", event_type: str = "") -> str:
    sn = _compact_spaces(snippet)
    if _contains_cjk(sn):
        return _first_sentence(sn, max_chars=180)
    if sn:
        lead = _first_sentence(sn, max_chars=180)
        if lead:
            return f"要点：{lead}（原文英文，建议点开原文核查细节）"
    tz = _title_zh(title, event_type=event_type)
    src = _compact_spaces(source_id)
    if tz and src:
        return f"该条来自 {src}，核心主题为“{tz}”。建议打开原文核对关键数据与时间点。"
    if tz:
        return f"核心主题为“{tz}”。建议打开原文核对关键数据与时间点。"
    return "暂无可用摘要，建议打开原文查看。"


def _story_key_for_row(r: RawItem) -> str:
    title_norm = str(r.title_norm or "").strip()
    if len(title_norm) >= 12:
        return f"title:{title_norm}"
    canonical = str(r.canonical_url or "").strip()
    if canonical:
        return f"url:{canonical}"
    return f"id:{r.id}"


def _trust_rank(v: str | None) -> int:
    vv = str(v or "").strip().upper()
    if vv == "A":
        return 0
    if vv == "B":
        return 1
    if vv == "C":
        return 2
    return 3


def _majority(values: list[str]) -> str:
    clean = [str(v).strip() for v in values if str(v).strip()]
    if not clean:
        return ""
    return Counter(clean).most_common(1)[0][0]


class FeedDBService:
    def __init__(self, database_url: str) -> None:
        self.engine = make_engine(database_url)
        self._Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, expire_on_commit=False)
        self.zh_enricher = ZhEnricher(Path(__file__).resolve().parents[2])

    def _session(self) -> Session:
        return self._Session()

    def _split_csv_values(self, raw: str) -> list[str]:
        vals = [x.strip() for x in str(raw or "").split(",")]
        return [x for x in vals if x]

    def _to_ts(self, published_at: str | None) -> float:
        d = _safe_dt(published_at)
        if d is None:
            return 0.0
        return d.timestamp()

    def _story_score(self, r: Story, primary_priority: int | None = None) -> float:
        trust_weight = {"A": 3.0, "B": 2.0, "C": 1.0}.get(str(r.trust_tier or "").upper(), 0.0)
        priority_weight = max(0.0, min(float(int(primary_priority or 0)), 100.0)) / 100.0
        try:
            import math
            sources_bonus = math.log(1.0 + float(int(r.sources_count or 0)))
        except Exception:
            sources_bonus = 0.0
        recency_bonus = 0.0
        ts = self._to_ts(r.published_at)
        if ts > 0:
            age_hours = (dt.datetime.now(dt.timezone.utc).timestamp() - ts) / 3600.0
            if age_hours <= 24:
                recency_bonus = 0.5
            elif age_hours <= 24 * 7:
                recency_bonus = 0.2
        return trust_weight + priority_weight + sources_bonus + recency_bonus

    def _balanced_order(self, rows: list[Story]) -> list[Story]:
        quotas = {
            "regulatory": 10,
            "procurement": 6,
            "company": 6,
            "evidence": 6,
            "media": 10,
        }
        buckets: dict[str, list[Story]] = defaultdict(list)
        for r in rows:
            buckets[str(r.source_group or "unknown").lower()].append(r)
        selected_ids: set[str] = set()
        out: list[Story] = []
        for g in ("regulatory", "procurement", "company", "evidence", "media"):
            take_n = quotas[g]
            for r in buckets.get(g, [])[:take_n]:
                if r.id in selected_ids:
                    continue
                selected_ids.add(str(r.id))
                out.append(r)
        for r in rows:
            if str(r.id) in selected_ids:
                continue
            out.append(r)
        return out

    def ingest_raw_from_collect(self, project_root: Path, *, scan_artifacts_days: int = 7) -> dict[str, Any]:
        collect_dir = project_root / "artifacts" / "collect"
        if not collect_dir.exists():
            return {"ok": True, "loaded_files": 0, "upserted": 0, "skipped": 0}

        cutoff = dt.date.today() - dt.timedelta(days=max(1, int(scan_artifacts_days)))
        files: list[Path] = []
        for p in sorted(collect_dir.glob("items-*.jsonl")):
            stem = p.stem.replace("items-", "").strip()
            try:
                d = dt.datetime.strptime(stem, "%Y%m%d").date()
            except Exception:
                continue
            if d >= cutoff:
                files.append(p)

        upserted = 0
        skipped = 0
        seen_ids: set[str] = set()
        source_meta = build_source_meta_index(project_root)
        with self._session() as s:
            for p in files:
                with p.open("r", encoding="utf-8") as f:
                    for ln in f:
                        ln = ln.strip()
                        if not ln:
                            continue
                        try:
                            row = json.loads(ln)
                        except Exception:
                            skipped += 1
                            continue
                        if not isinstance(row, dict):
                            skipped += 1
                            continue
                        title_raw = str(row.get("title", "")).strip()
                        url_raw = str(row.get("url", "")).strip()
                        source_id = str(row.get("source_id", "")).strip()
                        if not title_raw or not url_raw or not source_id:
                            skipped += 1
                            continue
                        canonical = str(row.get("url_norm", "")).strip() or url_norm(url_raw)
                        title_norm = _normalize_title(title_raw)
                        published_at = str(row.get("published_at", "")).strip() or ""
                        item_id_seed = "|".join([source_id, canonical, title_norm, published_at])
                        item_id = "ri_" + hashlib.sha1(item_id_seed.encode("utf-8")).hexdigest()[:24]
                        if item_id in seen_ids:
                            skipped += 1
                            continue
                        seen_ids.add(item_id)
                        existing = s.get(RawItem, item_id)
                        if existing is None:
                            existing = RawItem(id=item_id)
                            s.add(existing)
                        existing.source_id = source_id
                        existing.fetched_at = str(row.get("fetched_at", "")).strip() or _iso_now()
                        existing.published_at = published_at or None
                        existing.title_raw = title_raw
                        existing.title_norm = title_norm
                        existing.url_raw = url_raw
                        existing.canonical_url = canonical
                        existing.content_snippet = (
                            str(row.get("evidence_snippet", "")).strip()
                            or str(row.get("summary", "")).strip()
                            or str(row.get("description", "")).strip()
                            or None
                        )
                        existing.raw_payload = row
                        meta = source_meta.get(source_id, {})
                        group = str(meta.get("group", "")).strip() or str(row.get("source_group", "")).strip() or "unknown"
                        region = str(meta.get("region", "")).strip() or "Global"
                        trust_tier = str(meta.get("trust_tier", "")).strip().upper() or str(row.get("trust_tier", "")).strip().upper() or "C"
                        existing.source_group = group
                        existing.region = region
                        existing.trust_tier = trust_tier
                        existing.event_type = infer_event_type(
                            group=group,
                            title=title_raw,
                            snippet=existing.content_snippet,
                            source_id=source_id,
                            url=canonical,
                        )
                        try:
                            existing.priority = int(meta.get("priority", row.get("priority", 10)) or 10)
                        except Exception:
                            existing.priority = 10
                        upserted += 1
            s.commit()
        return {"ok": True, "loaded_files": len(files), "upserted": upserted, "skipped": skipped}

    def rebuild_stories(self, *, window_days: int = 30) -> dict[str, Any]:
        cutoff_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=max(1, int(window_days)))
        cutoff_iso = cutoff_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        with self._session() as s:
            rows = list(
                s.execute(
                    select(RawItem).where(
                        or_(RawItem.published_at.is_(None), RawItem.published_at >= cutoff_iso)
                    )
                ).scalars()
            )
            grouped: dict[str, list[RawItem]] = defaultdict(list)
            for r in rows:
                grouped[_story_key_for_row(r)].append(r)

            s.execute(delete(StoryItem))
            s.execute(delete(Story))

            stories_written = 0
            links_written = 0
            for key, group_rows in grouped.items():
                group_rows.sort(
                    key=lambda r: (
                        _trust_rank(r.trust_tier),
                        -int(r.priority or 0),
                        str(r.published_at or ""),
                        str(r.id),
                    ),
                    reverse=False,
                )
                primary = group_rows[0]
                story_id = "st_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:24]
                published_candidates = [str(r.published_at or "") for r in group_rows if str(r.published_at or "").strip()]
                published_at = max(published_candidates) if published_candidates else None
                story = Story(
                    id=story_id,
                    story_key=key,
                    title_best=str(primary.title_raw),
                    published_at=published_at,
                    source_group=_majority([str(r.source_group or "") for r in group_rows]) or None,
                    region=_majority([str(r.region or "") for r in group_rows]) or None,
                    trust_tier=str(primary.trust_tier or "") or None,
                    event_type=str(primary.event_type or "") or None,
                    primary_raw_item_id=str(primary.id),
                    sources_count=len({str(r.source_id or "") for r in group_rows if str(r.source_id or "").strip()}),
                )
                s.add(story)
                stories_written += 1
                for idx, r in enumerate(group_rows):
                    s.add(
                        StoryItem(
                            story_id=story_id,
                            raw_item_id=str(r.id),
                            is_primary=1 if idx == 0 else 0,
                            rank=idx,
                        )
                    )
                    links_written += 1
            s.commit()
        return {"ok": True, "stories_written": stories_written, "story_items_written": links_written}

    def _apply_common_filters(
        self,
        stmt: Any,
        *,
        model: Any,
        group: str,
        region: str,
        event_type: str,
        trust_tier: str,
        q: str,
        start: str,
        end: str,
        since: str = "",
    ) -> Any:
        groups = self._split_csv_values(group)
        if groups:
            stmt = stmt.where(model.source_group.in_(groups))
        regions = self._split_csv_values(region)
        if regions:
            stmt = stmt.where(model.region.in_(regions))
        event_types = self._split_csv_values(event_type)
        if event_types and hasattr(model, "event_type"):
            stmt = stmt.where(model.event_type.in_(event_types))
        if trust_tier:
            stmt = stmt.where(model.trust_tier == trust_tier)
        if q:
            title_col = model.title_best if model is Story else model.title_raw
            stmt = stmt.where(title_col.ilike(f"%{q}%"))
        if start:
            stmt = stmt.where(model.published_at >= start)
        if end:
            stmt = stmt.where(model.published_at <= end)
        if since:
            stmt = stmt.where(model.published_at > since)
        return stmt

    def list_stories(
        self,
        *,
        cursor: str = "",
        limit: int = 30,
        view_mode: str = "balanced",
        group: str = "",
        region: str = "",
        event_type: str = "",
        trust_tier: str = "",
        source_id: str = "",
        q: str = "",
        start: str = "",
        end: str = "",
        since: str = "",
    ) -> dict[str, Any]:
        self.zh_enricher.begin_request()
        vm = str(view_mode or "balanced").strip().lower()
        if vm not in {"balanced", "signal", "latest"}:
            vm = "balanced"
        lim = max(1, min(100, int(limit or 30)))
        cur_ts, cur_id = _decode_cursor(cursor)
        with self._session() as s:
            base_stmt = select(Story)
            base_stmt = self._apply_common_filters(
                base_stmt,
                model=Story,
                group=group,
                region=region,
                event_type=event_type,
                trust_tier=trust_tier,
                q=q,
                start=start,
                end=end,
                since=since,
            )
            if source_id:
                sid_subq = (
                    select(StoryItem.story_id)
                    .join(RawItem, RawItem.id == StoryItem.raw_item_id)
                    .where(RawItem.source_id == source_id)
                    .distinct()
                )
                base_stmt = base_stmt.where(Story.id.in_(sid_subq))

            has_more = False
            score_map: dict[str, float] = {}
            if vm == "latest":
                stmt = base_stmt
                if cur_ts and cur_id:
                    stmt = stmt.where(or_(Story.published_at < cur_ts, and_(Story.published_at == cur_ts, Story.id < cur_id)))
                stmt = stmt.order_by(Story.published_at.desc().nulls_last(), Story.id.desc()).limit(lim + 1)
                rows = list(s.execute(stmt).scalars())
                has_more = len(rows) > lim
                rows = rows[:lim]
            else:
                all_rows = list(
                    s.execute(
                        base_stmt.order_by(Story.published_at.desc().nulls_last(), Story.id.desc()).limit(10000)
                    ).scalars()
                )
                if vm == "signal":
                    pri_map: dict[str, int] = {}
                    pri_ids = [str(r.primary_raw_item_id or "") for r in all_rows if str(r.primary_raw_item_id or "").strip()]
                    if pri_ids:
                        raw_rows = list(s.execute(select(RawItem).where(RawItem.id.in_(pri_ids))).scalars())
                        pri_map = {str(rr.id): int(rr.priority or 0) for rr in raw_rows}
                    all_rows.sort(
                        key=lambda r: (
                            self._story_score(r, pri_map.get(str(r.primary_raw_item_id or ""), 0)),
                            self._to_ts(r.published_at),
                            str(r.id),
                        ),
                        reverse=True,
                    )
                    score_map = {
                        str(r.id): self._story_score(r, pri_map.get(str(r.primary_raw_item_id or ""), 0))
                        for r in all_rows
                    }
                    cur = _decode_any_cursor(cursor)
                    if cur:
                        c_score = float(cur.get("sc", -1e9))
                        c_ts = float(cur.get("ts", 0.0))
                        c_id = str(cur.get("id", ""))
                        filtered: list[Story] = []
                        for r in all_rows:
                            sc = self._story_score(r, pri_map.get(str(r.primary_raw_item_id or ""), 0))
                            ts = self._to_ts(r.published_at)
                            rid = str(r.id)
                            if (sc < c_score) or (sc == c_score and (ts < c_ts or (ts == c_ts and rid < c_id))):
                                filtered.append(r)
                        all_rows = filtered
                    rows = all_rows[:lim]
                    has_more = len(all_rows) > lim
                else:  # balanced
                    ordered = self._balanced_order(all_rows)
                    cur = _decode_any_cursor(cursor)
                    pos = int(cur.get("pos", 0) or 0)
                    pos = max(0, pos)
                    rows = ordered[pos : pos + lim]
                    has_more = (pos + lim) < len(ordered)

            primary_map: dict[str, dict[str, str]] = {}
            if rows:
                ids = [str(r.primary_raw_item_id or "") for r in rows if str(r.primary_raw_item_id or "").strip()]
                if ids:
                    raw_rows = list(s.execute(select(RawItem).where(RawItem.id.in_(ids))).scalars())
                    primary_map = {
                        str(r.id): {
                            "url": str(r.canonical_url or "") or str(r.url_raw or ""),
                            "source_id": str(r.source_id or ""),
                            "snippet": str(r.content_snippet or ""),
                        }
                        for r in raw_rows
                    }

            items: list[dict[str, Any]] = []
            for r in rows:
                source_id_v = primary_map.get(str(r.primary_raw_item_id or ""), {}).get("source_id", "")
                snippet_v = primary_map.get(str(r.primary_raw_item_id or ""), {}).get("snippet", "")
                primary_url_v = primary_map.get(str(r.primary_raw_item_id or ""), {}).get("url", "")
                zh = self.zh_enricher.enrich(
                    title=str(r.title_best or ""),
                    snippet=snippet_v,
                    source_id=source_id_v,
                    url=primary_url_v,
                    event_type=str(r.event_type or ""),
                )
                items.append(
                    {
                        "id": str(r.id),
                        "title": str(r.title_best or ""),
                        "title_zh": str(zh.get("title_zh", "")).strip(),
                        "published_at": str(r.published_at or ""),
                        "group": str(r.source_group or ""),
                        "region": str(r.region or ""),
                        "event_type": str(r.event_type or ""),
                        "trust_tier": str(r.trust_tier or ""),
                        "source_id": source_id_v,
                        "snippet": snippet_v,
                        "summary_zh": str(zh.get("summary_zh", "")).strip(),
                        "sources_count": int(r.sources_count or 0),
                        "primary_url": primary_url_v,
                    }
                )
            next_cursor = None
            if has_more and rows:
                tail = rows[-1]
                if vm == "latest":
                    next_cursor = _encode_cursor(str(tail.published_at or ""), str(tail.id))
                elif vm == "signal":
                    next_cursor = _encode_any_cursor(
                        {
                            "sc": float(score_map.get(str(tail.id), 0.0)),
                            "ts": self._to_ts(tail.published_at),
                            "id": str(tail.id),
                        }
                    )
                else:
                    cur = _decode_any_cursor(cursor)
                    pos = int(cur.get("pos", 0) or 0)
                    next_cursor = _encode_any_cursor({"pos": pos + lim})
            return {"ok": True, "items": items, "next_cursor": next_cursor, "view_mode": vm}

    def get_story_detail(self, story_id: str) -> dict[str, Any] | None:
        self.zh_enricher.begin_request()
        with self._session() as s:
            story = s.get(Story, story_id)
            if story is None:
                return None
            primary = None
            if story.primary_raw_item_id:
                primary = s.get(RawItem, story.primary_raw_item_id)
            stmt = (
                select(StoryItem, RawItem)
                .join(RawItem, RawItem.id == StoryItem.raw_item_id)
                .where(StoryItem.story_id == story_id)
                .order_by(
                    StoryItem.is_primary.desc(),
                    RawItem.trust_tier.asc(),
                    RawItem.priority.desc(),
                    RawItem.published_at.desc().nulls_last(),
                )
            )
            rows = list(s.execute(stmt).all())
            evidence = []
            for si, r in rows:
                evidence.append(
                    {
                        "raw_item_id": str(r.id),
                        "title": str(r.title_raw or ""),
                        "url": str(r.canonical_url or r.url_raw or ""),
                        "source_id": str(r.source_id or ""),
                        "published_at": str(r.published_at or ""),
                        "trust_tier": str(r.trust_tier or ""),
                        "priority": int(r.priority or 0),
                        "is_primary": bool(int(si.is_primary or 0)),
                        "rank": int(si.rank or 0),
                        "group": str(r.source_group or ""),
                        "region": str(r.region or ""),
                        "event_type": str(r.event_type or ""),
                    }
                )
            zh = self.zh_enricher.enrich(
                title=str(story.title_best or ""),
                snippet=str(primary.content_snippet or "") if primary is not None else "",
                source_id=str(primary.source_id or "") if primary is not None else "",
                url=str(primary.canonical_url or primary.url_raw) if primary is not None else "",
                event_type=str(story.event_type or ""),
            )
            return {
                "ok": True,
                "story": {
                    "id": str(story.id),
                    "title": str(story.title_best or ""),
                    "title_zh": str(zh.get("title_zh", "")).strip(),
                    "published_at": str(story.published_at or ""),
                    "group": str(story.source_group or ""),
                    "region": str(story.region or ""),
                    "event_type": str(story.event_type or ""),
                    "trust_tier": str(story.trust_tier or ""),
                    "source_id": str(primary.source_id or "") if primary is not None else "",
                    "snippet": str(primary.content_snippet or "") if primary is not None else "",
                    "summary_zh": str(zh.get("summary_zh", "")).strip(),
                    "sources_count": int(story.sources_count or 0),
                    "primary_url": str(primary.canonical_url or primary.url_raw) if primary is not None else "",
                },
                "evidence": evidence,
            }

    def list_raw_items(
        self,
        *,
        cursor: str = "",
        limit: int = 30,
        group: str = "",
        region: str = "",
        event_type: str = "",
        trust_tier: str = "",
        source_id: str = "",
        q: str = "",
        start: str = "",
        end: str = "",
        since: str = "",
    ) -> dict[str, Any]:
        self.zh_enricher.begin_request()
        lim = max(1, min(100, int(limit or 30)))
        cur_ts, cur_id = _decode_cursor(cursor)
        with self._session() as s:
            stmt = select(RawItem)
            stmt = self._apply_common_filters(
                stmt,
                model=RawItem,
                group=group,
                region=region,
                event_type=event_type,
                trust_tier=trust_tier,
                q=q,
                start=start,
                end=end,
                since=since,
            )
            if source_id:
                stmt = stmt.where(RawItem.source_id == source_id)
            if cur_ts and cur_id:
                stmt = stmt.where(or_(RawItem.published_at < cur_ts, and_(RawItem.published_at == cur_ts, RawItem.id < cur_id)))
            stmt = stmt.order_by(RawItem.published_at.desc().nulls_last(), RawItem.id.desc()).limit(lim + 1)
            rows = list(s.execute(stmt).scalars())

            has_more = len(rows) > lim
            rows = rows[:lim]
            items: list[dict[str, Any]] = []
            for r in rows:
                url_v = str(r.canonical_url or r.url_raw or "")
                title_v = str(r.title_raw or "")
                snippet_v = str(r.content_snippet or "")
                source_id_v = str(r.source_id or "")
                event_type_v = str(r.event_type or "")
                zh = self.zh_enricher.enrich(
                    title=title_v,
                    snippet=snippet_v,
                    source_id=source_id_v,
                    url=url_v,
                    event_type=event_type_v,
                )
                items.append(
                    {
                        "id": str(r.id),
                        "title": title_v,
                        "title_zh": str(zh.get("title_zh", "")).strip(),
                        "published_at": str(r.published_at or ""),
                        "group": str(r.source_group or ""),
                        "region": str(r.region or ""),
                        "event_type": event_type_v,
                        "trust_tier": str(r.trust_tier or ""),
                        "source_id": source_id_v,
                        "url": url_v,
                        "snippet": snippet_v,
                        "summary_zh": str(zh.get("summary_zh", "")).strip(),
                        "priority": int(r.priority or 0),
                    }
                )
            next_cursor = None
            if has_more and rows:
                tail = rows[-1]
                next_cursor = _encode_cursor(str(tail.published_at or ""), str(tail.id))
            return {"ok": True, "items": items, "next_cursor": next_cursor}

    def get_raw_item_detail(self, raw_item_id: str) -> dict[str, Any] | None:
        self.zh_enricher.begin_request()
        with self._session() as s:
            row = s.get(RawItem, raw_item_id)
            if row is None:
                return None
            story_link = s.execute(
                select(StoryItem.story_id, Story.title_best)
                .join(Story, Story.id == StoryItem.story_id)
                .where(StoryItem.raw_item_id == raw_item_id)
                .limit(1)
            ).first()
            story = None
            if story_link is not None:
                story = {
                    "story_id": str(story_link[0]),
                    "story_title": str(story_link[1] or ""),
                }
            zh = self.zh_enricher.enrich(
                title=str(row.title_raw or ""),
                snippet=str(row.content_snippet or ""),
                source_id=str(row.source_id or ""),
                url=str(row.canonical_url or row.url_raw or ""),
                event_type=str(row.event_type or ""),
            )
            return {
                "ok": True,
                "item": {
                    "id": str(row.id),
                    "title": str(row.title_raw or ""),
                    "title_zh": str(zh.get("title_zh", "")).strip(),
                    "published_at": str(row.published_at or ""),
                    "source_id": str(row.source_id or ""),
                    "group": str(row.source_group or ""),
                    "region": str(row.region or ""),
                    "event_type": str(row.event_type or ""),
                    "trust_tier": str(row.trust_tier or ""),
                    "priority": int(row.priority or 0),
                    "url": str(row.canonical_url or row.url_raw or ""),
                    "snippet": str(row.content_snippet or ""),
                    "summary_zh": str(zh.get("summary_zh", "")).strip(),
                    "raw_payload": row.raw_payload if isinstance(row.raw_payload, dict) else {},
                },
                "story": story,
            }

    def get_feed_summary(self, *, mode: str = "item") -> dict[str, Any]:
        is_story = str(mode or "").strip().lower() == "story"
        model = Story if is_story else RawItem
        with self._session() as s:
            group_rows = list(
                s.execute(
                    select(model.source_group, func.count()).group_by(model.source_group)
                ).all()
            )
            total = int(sum(int(r[1] or 0) for r in group_rows))
            by_group: dict[str, int] = {}
            for g, c in group_rows:
                key = str(g or "unknown").strip().lower() or "unknown"
                by_group[key] = int(c or 0)
            unknown_region = int(
                s.execute(
                    select(func.count()).select_from(model).where(
                        or_(model.region.is_(None), model.region == "", model.region == "__unknown__")
                    )
                ).scalar()
                or 0
            )
            unknown_event = int(
                s.execute(
                    select(func.count()).select_from(model).where(
                        or_(model.event_type.is_(None), model.event_type == "", model.event_type == "unknown")
                    )
                ).scalar()
                or 0
            )
        return {
            "ok": True,
            "mode": "story" if is_story else "item",
            "total": total,
            "by_group": by_group,
            "unknown_region_rate": (float(unknown_region) / float(total)) if total else 0.0,
            "unknown_event_type_rate": (float(unknown_event) / float(total)) if total else 0.0,
        }

    def backfill_raw_meta(
        self,
        project_root: Path,
        *,
        execute: bool,
        batch_size: int = 2000,
    ) -> dict[str, Any]:
        bs = max(100, int(batch_size))
        source_meta = build_source_meta_index(project_root)
        last_id = ""
        scanned = 0
        updated = 0
        by_group: Counter[str] = Counter()
        by_event: Counter[str] = Counter()

        with self._session() as s:
            while True:
                stmt = (
                    select(RawItem)
                    .where(RawItem.id > last_id)
                    .order_by(RawItem.id.asc())
                    .limit(bs)
                )
                rows = list(s.execute(stmt).scalars())
                if not rows:
                    break
                for r in rows:
                    scanned += 1
                    sid = str(r.source_id or "").strip()
                    meta = source_meta.get(sid, {})
                    next_group = str(meta.get("group", "")).strip() or str(r.source_group or "").strip() or "unknown"
                    next_region = str(meta.get("region", "")).strip() or str(r.region or "").strip() or "Global"
                    next_trust = str(meta.get("trust_tier", "")).strip().upper() or str(r.trust_tier or "").strip().upper() or "C"
                    try:
                        next_priority = int(meta.get("priority", r.priority if r.priority is not None else 10) or 10)
                    except Exception:
                        next_priority = 10
                    next_event = infer_event_type(
                        next_group,
                        str(r.title_raw or ""),
                        str(r.content_snippet or ""),
                        source_id=sid,
                        url=str(r.canonical_url or r.url_raw or ""),
                    )
                    changed = (
                        str(r.source_group or "") != next_group
                        or str(r.region or "") != next_region
                        or str(r.trust_tier or "") != next_trust
                        or int(r.priority or 0) != int(next_priority)
                        or str(r.event_type or "") != next_event
                    )
                    by_group[next_group] += 1
                    by_event[next_event] += 1
                    if changed:
                        updated += 1
                        if execute:
                            r.source_group = next_group
                            r.region = next_region
                            r.trust_tier = next_trust
                            r.priority = next_priority
                            r.event_type = next_event
                last_id = str(rows[-1].id)
                if execute:
                    s.commit()
            if not execute:
                s.rollback()

            total = int(s.execute(select(RawItem.id)).fetchall().__len__())
            unknown_region = int(
                s.execute(
                    select(RawItem.id).where(
                        or_(
                            RawItem.region.is_(None),
                            RawItem.region == "",
                            RawItem.region == "__unknown__",
                        )
                    )
                ).fetchall().__len__()
            )
            unknown_event = int(
                s.execute(
                    select(RawItem.id).where(
                        or_(
                            RawItem.event_type.is_(None),
                            RawItem.event_type == "",
                            RawItem.event_type == "unknown",
                        )
                    )
                ).fetchall().__len__()
            )
        return {
            "ok": True,
            "mode": "execute" if execute else "dry-run",
            "total_scanned": scanned,
            "updated": updated,
            "unknown_region_ratio_after": (float(unknown_region) / float(total)) if total else 0.0,
            "unknown_event_type_ratio_after": (float(unknown_event) / float(total)) if total else 0.0,
            "counts_by_group": dict(by_group.most_common(20)),
            "counts_by_event_type": dict(by_event.most_common(20)),
        }

    def backfill_stories_meta(self, *, execute: bool, batch_size: int = 2000) -> dict[str, Any]:
        bs = max(100, int(batch_size))
        last_id = ""
        scanned = 0
        updated = 0
        with self._session() as s:
            while True:
                rows = list(
                    s.execute(select(Story).where(Story.id > last_id).order_by(Story.id.asc()).limit(bs)).scalars()
                )
                if not rows:
                    break
                for st in rows:
                    scanned += 1
                    if not st.primary_raw_item_id:
                        continue
                    pri = s.get(RawItem, st.primary_raw_item_id)
                    if pri is None:
                        continue
                    next_group = str(pri.source_group or "")
                    next_region = str(pri.region or "")
                    next_tt = str(pri.trust_tier or "")
                    next_ev = str(pri.event_type or "")
                    changed = (
                        str(st.source_group or "") != next_group
                        or str(st.region or "") != next_region
                        or str(st.trust_tier or "") != next_tt
                        or str(st.event_type or "") != next_ev
                    )
                    if changed:
                        updated += 1
                        if execute:
                            st.source_group = next_group or None
                            st.region = next_region or None
                            st.trust_tier = next_tt or None
                            st.event_type = next_ev or None
                last_id = str(rows[-1].id)
                if execute:
                    s.commit()
            if not execute:
                s.rollback()
        return {
            "ok": True,
            "mode": "execute" if execute else "dry-run",
            "total_scanned": scanned,
            "updated": updated,
        }
