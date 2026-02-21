from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.core.track_relevance import compute_relevance
from app.services.analysis_cache_store import AnalysisCacheStore
from app.services.analysis_generator import AnalysisGenerator, degraded_analysis


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def url_norm(url: str) -> str:
    try:
        p = urlparse(str(url or ""))
        host = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/")
        query = p.query.strip()
        # Keep query only for common RSS/article identifiers; drop tracking-like noise.
        kept_query = ""
        if query:
            low = query.lower()
            if any(k in low for k in ("id=", "article=", "story=", "p=", "item=")):
                kept_query = "?" + query
        return f"{host}{path}{kept_query}"
    except Exception:
        return str(url or "").strip().lower()


def append_jsonl(file: Path, obj: dict[str, Any]) -> None:
    ensure_dir(file.parent)
    with file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _safe_dt(v: str | None) -> dt.datetime | None:
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _to_iso_utc(d: dt.datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class CollectAssetStore:
    def __init__(self, project_root: Path, asset_dir: str = "artifacts/collect") -> None:
        self.project_root = project_root
        self.base_dir = (project_root / asset_dir).resolve()
        ensure_dir(self.base_dir)

    def _day_file(self, d: dt.date) -> Path:
        return self.base_dir / f"items-{d.strftime('%Y%m%d')}.jsonl"

    def _day_index_file(self, d: dt.date) -> Path:
        return self.base_dir / f"index-{d.strftime('%Y%m%d')}.json"

    def _load_day_index(self, d: dt.date) -> dict[str, str]:
        p = self._day_index_file(d)
        if not p.exists():
            return {}
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return {str(k): str(v) for k, v in raw.items()}
        except Exception:
            return {}
        return {}

    def _save_day_index(self, d: dt.date, idx: dict[str, str]) -> None:
        p = self._day_index_file(d)
        p.write_text(json.dumps(idx, ensure_ascii=False, indent=2), encoding="utf-8")

    def append_items(
        self,
        *,
        run_id: str,
        source_id: str,
        source_name: str,
        source_group: str,
        items: list[dict[str, Any]],
        rules_runtime: dict[str, Any] | None = None,
        now_utc: dt.datetime | None = None,
    ) -> dict[str, int]:
        rules_runtime = rules_runtime or {}
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        day = now_utc.date()
        index = self._load_day_index(day)
        target = self._day_file(day)
        written = 0
        skipped = 0

        rows: list[str] = []
        for it in items:
            title = str(it.get("title", "")).strip()
            url = str(it.get("url", it.get("link", ""))).strip()
            if not title or not url:
                skipped += 1
                continue
            summary = str(it.get("summary", "")).strip()
            published = str(it.get("published_at", "")).strip()
            pdt = _safe_dt(published) or now_utc
            key_seed = url_norm(url) or title.lower()
            dedupe_key = _sha1(key_seed)
            if dedupe_key in index:
                skipped += 1
                continue

            text = f"{title} {summary}".strip()
            track, level, explain = compute_relevance(
                text,
                {
                    "source_group": source_group,
                    "event_type": str(it.get("event_type", "")),
                    "url": url,
                    "title": title,
                },
                rules_runtime,
            )
            row = {
                "run_id": run_id,
                "collected_at": _to_iso_utc(now_utc),
                "source_id": source_id,
                "source": source_name,
                "source_group": source_group,
                "url": url,
                "url_norm": url_norm(url),
                "canonical_url": str(it.get("canonical_url", "")).strip(),
                "dedupe_key": dedupe_key,
                "title": title,
                "published_at": _to_iso_utc(pdt),
                "raw_text": text,
                "normalized_text": re.sub(r"\s+", " ", text).strip().lower(),
                "summary": summary,
                "track": track,
                "relevance_level": int(level),
                "relevance_explain": explain,
                "event_type": str(it.get("event_type", "")).strip(),
                "region": str(it.get("region", "")).strip(),
                "lane": str(it.get("lane", "")).strip(),
                "platform": str(it.get("platform", "")).strip(),
            }
            rows.append(json.dumps(row, ensure_ascii=False))
            index[dedupe_key] = row["collected_at"]
            written += 1

        if rows:
            ensure_dir(target.parent)
            with target.open("a", encoding="utf-8") as f:
                for ln in rows:
                    f.write(ln + "\n")
        self._save_day_index(day, index)
        return {"written": written, "skipped": skipped}

    def append_stub_item(
        self,
        *,
        run_id: str,
        source_id: str,
        source_name: str,
        source_group: str,
        url: str,
        observed_at: dt.datetime | None = None,
        error: str = "",
    ) -> dict[str, int]:
        observed_at = observed_at or dt.datetime.now(dt.timezone.utc)
        day = observed_at.date()
        index = self._load_day_index(day)
        target = self._day_file(day)
        un = url_norm(url)
        key_seed = un or f"{source_id}:{observed_at.isoformat()}"
        dedupe_key = _sha1(key_seed)
        if dedupe_key in index:
            return {"written": 0, "skipped": 1}
        row = {
            "run_id": run_id,
            "collected_at": _to_iso_utc(observed_at),
            "source_id": source_id,
            "source": source_name,
            "source_group": source_group,
            "url": url,
            "url_norm": un,
            "canonical_url": "",
            "dedupe_key": dedupe_key,
            "title": f"{source_name} stub {observed_at.strftime('%Y-%m-%d %H:%M:%S')}",
            "published_at": _to_iso_utc(observed_at),
            "raw_text": f"stub source={source_id}",
            "normalized_text": f"stub source={source_id}",
            "summary": "stub item for non-rss source",
            "track": "frontier",
            "relevance_level": 1,
            "relevance_explain": {
                "anchors_hit": [],
                "negatives_hit": [],
                "rules_applied": ["collect_stub_fallback"],
                "final_reason": "non_rss_stub_item",
            },
            "event_type": "",
            "region": "",
            "lane": "",
            "platform": "",
            "stub": True,
            "stub_error": str(error or ""),
            "observed_at": _to_iso_utc(observed_at),
        }
        append_jsonl(target, row)
        index[dedupe_key] = row["collected_at"]
        self._save_day_index(day, index)
        return {"written": 1, "skipped": 0}

    def load_window_items(
        self,
        *,
        window_hours: int,
        now_utc: dt.datetime | None = None,
    ) -> list[dict[str, Any]]:
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        window_hours = max(1, int(window_hours or 24))
        oldest = now_utc - dt.timedelta(hours=window_hours)

        rows: list[dict[str, Any]] = []
        for p in sorted(self.base_dir.glob("items-*.jsonl")):
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
                        ca = _safe_dt(str(r.get("collected_at", "")))
                        if ca is None:
                            continue
                        if ca < oldest:
                            continue
                        rows.append(r)
            except Exception:
                continue

        # final dedupe by dedupe_key keeping latest collected row
        by_key: dict[str, dict[str, Any]] = {}
        for r in rows:
            k = str(r.get("dedupe_key", "")).strip() or _sha1(str(r.get("url", "")))
            old = by_key.get(k)
            if old is None:
                by_key[k] = r
                continue
            oca = _safe_dt(str(old.get("collected_at", "")))
            nca = _safe_dt(str(r.get("collected_at", "")))
            if oca is None or (nca is not None and nca >= oca):
                by_key[k] = r

        out = list(by_key.values())
        out.sort(key=lambda x: str(x.get("published_at", "")), reverse=True)
        return out

    def cleanup(self, *, keep_days: int = 30, now_utc: dt.datetime | None = None) -> dict[str, int]:
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        keep_days = max(1, int(keep_days or 30))
        cutoff = (now_utc - dt.timedelta(days=keep_days)).date()
        removed_files = 0
        removed_indexes = 0
        for p in sorted(self.base_dir.glob("items-*.jsonl")):
            m = re.match(r"items-(\d{8})\.jsonl$", p.name)
            if not m:
                continue
            try:
                d = dt.datetime.strptime(m.group(1), "%Y%m%d").date()
            except Exception:
                continue
            if d < cutoff:
                try:
                    p.unlink(missing_ok=True)  # type: ignore[arg-type]
                    removed_files += 1
                except Exception:
                    pass
                ip = self.base_dir / f"index-{m.group(1)}.json"
                if ip.exists():
                    try:
                        ip.unlink(missing_ok=True)  # type: ignore[arg-type]
                        removed_indexes += 1
                    except Exception:
                        pass
        return {"removed_files": removed_files, "removed_indexes": removed_indexes}


def render_digest_from_assets(
    *,
    date_str: str,
    items: list[dict[str, Any]],
    subject: str,
    core_min_level_for_A: int = 3,
    frontier_min_level_for_F: int = 2,
    frontier_quota: int = 3,
    analysis_cfg: dict[str, Any] | None = None,
    return_meta: bool = False,
    _cache_store: AnalysisCacheStore | None = None,
    _generator: AnalysisGenerator | None = None,
) -> Any:
    analysis_cfg = analysis_cfg or {}
    enable_analysis_cache = bool(analysis_cfg.get("enable_analysis_cache", True))
    always_generate = bool(analysis_cfg.get("always_generate", False))
    prompt_version = str(analysis_cfg.get("prompt_version", "v1"))
    model_name = str(analysis_cfg.get("model", "local-heuristic-v1"))
    model_primary = str(analysis_cfg.get("model_primary", "")).strip()
    model_fallback = str(analysis_cfg.get("model_fallback", "")).strip()
    model_policy = str(analysis_cfg.get("model_policy", "tiered")).strip().lower()
    core_model = str(analysis_cfg.get("core_model", "primary")).strip().lower()
    frontier_model = str(analysis_cfg.get("frontier_model", "fallback")).strip().lower()
    temperature = float(analysis_cfg.get("temperature", 0.2) or 0.2)
    retries = int(analysis_cfg.get("retries", 1) or 1)
    timeout_seconds = int(analysis_cfg.get("timeout_seconds", 20) or 20)
    backoff_seconds = float(analysis_cfg.get("backoff_seconds", 0.5) or 0.5)
    run_day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    cache_store = _cache_store
    if cache_store is None and enable_analysis_cache:
        cache_store = AnalysisCacheStore(Path("."), asset_dir=str(analysis_cfg.get("asset_dir", "artifacts/analysis")))
    generator = _generator or AnalysisGenerator(
        model=model_name,
        prompt_version=prompt_version,
        primary_model=model_primary,
        fallback_model=model_fallback,
        model_policy=model_policy,
        core_model=core_model,
        frontier_model=frontier_model,
        temperature=temperature,
        retries=retries,
        timeout_seconds=timeout_seconds,
        backoff_seconds=backoff_seconds,
    )

    cache_hit = 0
    cache_miss = 0
    generated_count = 0
    degraded_count = 0
    degraded_reasons: dict[str, int] = {}

    lines: list[str] = [subject, ""]

    core_items = [r for r in items if str(r.get("track", "")) == "core" and int(r.get("relevance_level", 0) or 0) >= int(core_min_level_for_A)]
    frontier_items = [r for r in items if str(r.get("track", "")) == "frontier" and int(r.get("relevance_level", 0) or 0) >= int(frontier_min_level_for_F)]

    lines.append("A. 今日要点（8-15条，按重要性排序）")
    top = core_items[:15]
    if not top:
        lines.append("1) [7天补充] collect资产窗口内无可用 core 条目。")
        lines.append("摘要：请检查 collect 是否正常写入，或放宽窗口/阈值。")
        lines.append(f"发布日期：{date_str}（北京时间）")
        lines.append("来源：collect-assets")
        lines.append("地区：全球")
        lines.append("赛道：其他")
        lines.append("事件类型：政策与市场动态")
        lines.append("技术平台：跨平台/未标注")
        lines.append("")
    else:
        for idx, r in enumerate(top, 1):
            analysis = None
            item_key = ""
            if enable_analysis_cache:
                item_key = AnalysisCacheStore.item_key(r)
                if not always_generate and cache_store is not None:
                    analysis = cache_store.get(item_key, run_day)
                    if analysis:
                        cache_hit += 1
                    else:
                        cache_miss += 1
                else:
                    cache_miss += 1
            if analysis is None:
                try:
                    analysis = generator.generate(r, rules=analysis_cfg)
                    generated_count += 1
                except Exception as e:
                    analysis = degraded_analysis(r, str(e))
                if bool(analysis.get("degraded")):
                    degraded_count += 1
                    rs = str(analysis.get("degraded_reason", "")).strip() or "analysis_generation_failed"
                    degraded_reasons[rs] = degraded_reasons.get(rs, 0) + 1
                if enable_analysis_cache and cache_store is not None and item_key:
                    payload = dict(analysis)
                    payload.update(
                        {
                            "item_key": item_key,
                            "url": str(r.get("url", "")),
                            "story_id": str(r.get("story_id", "")),
                            "source_id": str(r.get("source_id", "")),
                            "title": str(r.get("title", "")),
                        }
                    )
                    cache_store.put(item_key, payload, run_day)

            lines.append(f"{idx}) [24小时内] {str(r.get('title',''))}")
            sm = str((analysis or {}).get("summary", "")).strip() or str(r.get("summary", "")).strip() or "摘要：由 collect 资产生成。"
            lines.append(sm if sm.startswith("摘要") else f"摘要：{sm}")
            lines.append(f"发布日期：{str(r.get('published_at',''))}")
            lines.append(f"来源：{str(r.get('source',''))} | {str(r.get('url',''))}")
            if str(r.get("region", "")).strip():
                lines.append(f"地区：{str(r.get('region',''))}")
            if str(r.get("lane", "")).strip():
                lines.append(f"赛道：{str(r.get('lane',''))}")
            if str(r.get("event_type", "")).strip():
                lines.append(f"事件类型：{str(r.get('event_type',''))}")
            if str(r.get("platform", "")).strip():
                lines.append(f"技术平台：{str(r.get('platform',''))}")
            lines.append("")

    lines.append("B. 分赛道速览（肿瘤/感染/生殖遗传/其他）")
    lanes = {"肿瘤检测": 0, "感染检测": 0, "生殖与遗传检测": 0, "其他": 0}
    for r in top:
        lane = str(r.get("lane", "其他"))
        lanes[lane] = lanes.get(lane, 0) + 1
    for k in ["肿瘤检测", "感染检测", "生殖与遗传检测", "其他"]:
        lines.append(f"- {k}：{lanes.get(k, 0)} 条（以当日抓取为准）")
    lines.append("")

    lines.append("C. 技术平台雷达（按平台汇总当日进展）")
    plats: dict[str, int] = {}
    for r in top:
        p = str(r.get("platform", "未标注"))
        plats[p] = plats.get(p, 0) + 1
    if plats:
        for p, c in sorted(plats.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"- {p}：{c} 条")
    else:
        lines.append("- 今日无有效平台统计。")
    lines.append("")

    lines.append("D. 区域热力图（北美/欧洲/亚太/中国）")
    regs = {"北美": 0, "欧洲": 0, "亚太": 0, "中国": 0}
    for r in top:
        rg = str(r.get("region", ""))
        regs[rg] = regs.get(rg, 0) + 1
    for k in ["北美", "欧洲", "亚太", "中国"]:
        lines.append(f"- {k}：{regs.get(k, 0)}")
    lines.append("")

    lines.append("E. 三条关键趋势判断（产业与技术各至少1条）")
    lines.append("1) 产业：collect/digest 解耦后可提高抓取频率并稳定出报。")
    lines.append("2) 技术：前沿条目与核心条目分轨，降低噪音对主结论的干扰。")
    lines.append("3) 运营：资产化可回放可审计，便于定位信源与规则问题。")
    lines.append("")

    lines.append("F. 信息缺口与次日跟踪清单（3-5条）")
    f_rows = frontier_items[: max(0, int(frontier_quota or 0))]
    if f_rows:
        for idx, r in enumerate(f_rows, 1):
            lines.append(f"{idx}) frontier雷达：L{int(r.get('relevance_level', 0) or 0)} | {str(r.get('title',''))[:90]}")
    else:
        lines.append("1) frontier雷达：当前窗口无满足阈值的 frontier 条目，建议扩充前沿关键词或延长窗口。")
    lines.append("")

    core_count = len([r for r in items if str(r.get("track", "")) == "core"])
    frontier_count = len([r for r in items if str(r.get("track", "")) == "frontier"])
    items_before_dedupe = len(items)
    dedupe_keys: set[str] = set()
    for r in items:
        sid = str(r.get("story_id", "")).strip()
        if sid:
            dedupe_keys.add(f"story:{sid}")
            continue
        u = url_norm(str(r.get("url", "")).strip())
        if u:
            dedupe_keys.add(f"url:{u}")
            continue
        t = str(r.get("title", "")).strip().lower()
        if t:
            dedupe_keys.add(f"title:{t}")
    items_after_dedupe = len(dedupe_keys) if dedupe_keys else items_before_dedupe
    clusters_total = items_after_dedupe
    reduction_ratio = 0.0
    if items_before_dedupe > 0:
        reduction_ratio = max(0.0, (items_before_dedupe - items_after_dedupe) / float(items_before_dedupe))

    primary_source_dist: dict[str, int] = {}
    for r in items:
        src = str(r.get("source", "")).strip() or str(r.get("source_id", "")).strip() or "unknown"
        primary_source_dist[src] = primary_source_dist.get(src, 0) + 1
    source_top = sorted(primary_source_dist.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    source_top_s = "; ".join([f"{k}:{v}" for k, v in source_top]) or "无"

    lines.append("G. 质量指标 (Quality Audit)")
    lines.append(
        f"24H条目数 / 7D补充数：{len(top)} / 0 | 亚太占比：{(regs.get('亚太',0)+regs.get('中国',0))/max(1,len(top)):.0%} | "
        f"商业与监管事件比：待细分 | 必查信源命中清单：待接入 | core/frontier覆盖：{core_count}/{frontier_count}"
    )
    lines.append(
        f"dedupe_cluster_enabled：{str(bool(items_before_dedupe)).lower()} | "
        f"items_before_dedupe：{items_before_dedupe} | items_after_dedupe：{items_after_dedupe} | "
        f"clusters_total：{clusters_total} | reduction_ratio：{reduction_ratio:.0%} | "
        f"primary_source_distribution_top5：{source_top_s}"
    )
    reason_top = "; ".join([f"{k}:{v}" for k, v in sorted(degraded_reasons.items(), key=lambda kv: (-kv[1], kv[0]))[:3]]) or "无"
    lines.append(
        f"analysis_cache_hit/miss：{cache_hit}/{cache_miss} | generated_count：{generated_count} | "
        f"degraded_count：{degraded_count} | degraded_reason_top3：{reason_top}"
    )
    if not items:
        lines.append("分流规则缺口说明：collect 资产窗口内无条目，请检查 collect 调度、信源可达性和资产目录。")
    txt = "\n".join(lines).rstrip() + "\n"
    meta = {
        "analysis_cache_hit": cache_hit,
        "analysis_cache_miss": cache_miss,
        "analysis_generated_count": generated_count,
        "analysis_degraded_count": degraded_count,
        "analysis_degraded_reason_top3": [
            {"reason": k, "count": v}
            for k, v in sorted(degraded_reasons.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        ],
        "analysis_prompt_version": prompt_version,
        "analysis_model": model_name,
        "analysis_cache_enabled": enable_analysis_cache,
        "dedupe_cluster_enabled": bool(items_before_dedupe),
        "items_before_dedupe": items_before_dedupe,
        "items_after_dedupe": items_after_dedupe,
        "clusters_total": clusters_total,
        "reduction_ratio": reduction_ratio,
        "primary_source_distribution_top5": [
            {"source": k, "count": v} for k, v in source_top
        ],
    }
    if return_meta:
        return {"text": txt, "meta": meta}
    return txt
