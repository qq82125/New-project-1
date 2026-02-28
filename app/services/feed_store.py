from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any

from app.utils.url_norm import url_norm


def _safe_dt(v: str | None) -> dt.datetime | None:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _to_iso_utc(v: dt.datetime) -> str:
    if v.tzinfo is None:
        v = v.replace(tzinfo=dt.timezone.utc)
    return v.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_date_or_ts(v: str | None, *, end_of_day: bool = False) -> dt.datetime | None:
    s = str(v or "").strip()
    if not s:
        return None
    d = _safe_dt(s)
    if d is not None:
        return d
    try:
        day = dt.datetime.strptime(s, "%Y-%m-%d")
        if end_of_day:
            day = day + dt.timedelta(days=1) - dt.timedelta(seconds=1)
        return day.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def _make_item_id(row: dict[str, Any]) -> str:
    base = (
        str(row.get("dedupe_key") or "").strip()
        or str(row.get("url_norm") or "").strip()
        or url_norm(str(row.get("url", "")).strip())
        or str(row.get("url", "")).strip()
    )
    material = "|".join(
        [
            str(row.get("published_at", "")).strip(),
            str(row.get("source_id", "")).strip(),
            str(row.get("title", "")).strip(),
            base,
        ]
    )
    return "fi_" + hashlib.sha1(material.encode("utf-8")).hexdigest()[:20]


def _encode_cursor(*, ts: str, item_id: str) -> str:
    payload = json.dumps({"ts": ts, "id": item_id}, ensure_ascii=False, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_cursor(cur: str | None) -> tuple[dt.datetime | None, str]:
    s = str(cur or "").strip()
    if not s:
        return None, ""
    try:
        pad = "=" * ((4 - len(s) % 4) % 4)
        raw = base64.urlsafe_b64decode((s + pad).encode("ascii")).decode("utf-8")
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return None, ""
        ts = _safe_dt(str(obj.get("ts", "")))
        item_id = str(obj.get("id", "")).strip()
        return ts, item_id
    except Exception:
        return None, ""


def _iter_collect_rows(project_root: Path) -> list[dict[str, Any]]:
    base = project_root / "artifacts" / "collect"
    if not base.exists():
        return []
    rows: list[dict[str, Any]] = []
    for p in sorted(base.glob("items-*.jsonl"), reverse=True):
        try:
            with p.open("r", encoding="utf-8") as f:
                for ln in f:
                    ln = ln.strip()
                    if not ln:
                        continue
                    try:
                        r = json.loads(ln)
                    except Exception:
                        continue
                    if isinstance(r, dict):
                        rows.append(r)
        except Exception:
            continue
    return rows


def _project_feed_row(row: dict[str, Any]) -> dict[str, Any]:
    published = str(row.get("published_at", "")).strip()
    source_group = str(row.get("source_group", "")).strip()
    out = {
        "id": _make_item_id(row),
        "title": str(row.get("title", "")).strip(),
        "url": str(row.get("url", "")).strip(),
        "source_id": str(row.get("source_id", "")).strip(),
        "group": source_group,
        "region": str(row.get("region", "")).strip(),
        "lane": str(row.get("lane", "")).strip(),
        "event_type": str(row.get("event_type", "")).strip(),
        "trust_tier": str(row.get("trust_tier", "")).strip(),
        "published_at": published,
        "summary": str(row.get("summary", "")).strip(),
        "score": int(row.get("relevance_level", 0) or 0),
        "track": str(row.get("track", "")).strip(),
        "source": str(row.get("source", "")).strip(),
        "url_norm": str(row.get("url_norm", "")).strip() or url_norm(str(row.get("url", "")).strip()),
        "dedupe_key": str(row.get("dedupe_key", "")).strip(),
    }
    return out


def _matches_filters(
    row: dict[str, Any],
    *,
    group: str,
    region: str,
    event_type: str,
    trust_tier: str,
    source_id: str,
    q: str,
    start_dt: dt.datetime | None,
    end_dt: dt.datetime | None,
    since_dt: dt.datetime | None,
) -> bool:
    if group and str(row.get("group", "")).strip().lower() != group.lower():
        return False
    if region and str(row.get("region", "")).strip().lower() != region.lower():
        return False
    if event_type and str(row.get("event_type", "")).strip().lower() != event_type.lower():
        return False
    if trust_tier and str(row.get("trust_tier", "")).strip().upper() != trust_tier.upper():
        return False
    if source_id and str(row.get("source_id", "")).strip() != source_id:
        return False

    pdt = _safe_dt(str(row.get("published_at", "")))
    if start_dt and (pdt is None or pdt < start_dt):
        return False
    if end_dt and (pdt is None or pdt > end_dt):
        return False
    if since_dt and (pdt is None or pdt <= since_dt):
        return False

    if q:
        blob = " ".join(
            [
                str(row.get("title", "")),
                str(row.get("summary", "")),
                str(row.get("source_id", "")),
                str(row.get("source", "")),
                str(row.get("url", "")),
            ]
        ).lower()
        if q.lower() not in blob:
            return False
    return True


def list_feed_items(
    project_root: Path,
    *,
    cursor: str | None = None,
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
    rows_raw = _iter_collect_rows(project_root)
    rows = [_project_feed_row(r) for r in rows_raw]

    start_dt = _parse_date_or_ts(start)
    end_dt = _parse_date_or_ts(end, end_of_day=True)
    since_dt = _parse_date_or_ts(since)
    cursor_ts, cursor_id = _decode_cursor(cursor)

    filtered: list[dict[str, Any]] = []
    for r in rows:
        if not _matches_filters(
            r,
            group=group,
            region=region,
            event_type=event_type,
            trust_tier=trust_tier,
            source_id=source_id,
            q=q,
            start_dt=start_dt,
            end_dt=end_dt,
            since_dt=since_dt,
        ):
            continue
        filtered.append(r)

    def _k(x: dict[str, Any]) -> tuple[float, str]:
        d = _safe_dt(str(x.get("published_at", "")))
        ts = d.timestamp() if d else 0.0
        return ts, str(x.get("id", ""))

    filtered.sort(key=_k, reverse=True)

    if cursor_ts is not None:
        cts = cursor_ts.timestamp()
        out2: list[dict[str, Any]] = []
        for r in filtered:
            ts, rid = _k(r)
            if (ts < cts) or (ts == cts and rid < cursor_id):
                out2.append(r)
        filtered = out2

    lim = max(1, min(100, int(limit or 30)))
    page = filtered[:lim]
    next_cursor = None
    if len(filtered) > lim and page:
        tail = page[-1]
        pd = _safe_dt(str(tail.get("published_at", ""))) or dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)
        next_cursor = _encode_cursor(ts=_to_iso_utc(pd), item_id=str(tail.get("id", "")))

    return {
        "ok": True,
        "items": page,
        "next_cursor": next_cursor,
    }


def get_feed_item(project_root: Path, item_id: str) -> dict[str, Any] | None:
    rows_raw = _iter_collect_rows(project_root)
    rows = [_project_feed_row(r) for r in rows_raw]
    hit = next((r for r in rows if str(r.get("id", "")) == item_id), None)
    if hit is None:
        return None
    key = str(hit.get("dedupe_key", "")).strip() or str(hit.get("url_norm", "")).strip()
    related: list[dict[str, Any]] = []
    if key:
        for r in rows:
            if str(r.get("id")) == item_id:
                continue
            rk = str(r.get("dedupe_key", "")).strip() or str(r.get("url_norm", "")).strip()
            if rk == key:
                related.append(
                    {
                        "id": str(r.get("id", "")),
                        "title": str(r.get("title", "")),
                        "source_id": str(r.get("source_id", "")),
                        "url": str(r.get("url", "")),
                        "published_at": str(r.get("published_at", "")),
                    }
                )
            if len(related) >= 5:
                break
    out = dict(hit)
    out["related"] = related
    return out
