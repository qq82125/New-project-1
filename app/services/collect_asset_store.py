from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.core.track_relevance import compute_relevance
from app.services.analysis_cache_store import AnalysisCacheStore
from app.services.analysis_generator import AnalysisGenerator, degraded_analysis
from app.services.classification_maps import (
    classify_lane as map_classify_lane,
    classify_region as map_classify_region,
    load_lane_map,
    load_region_map,
    normalize_unknown as map_normalize_unknown,
)
from app.services.page_classifier import is_static_or_listing_url
from app.services.opportunity_index import compute_opportunity_index
from app.services.opportunity_store import EVENT_WEIGHT, OpportunityStore, normalize_event_type
from app.services.source_policy import exclusion_reason, filter_rows_for_digest, normalize_source_policy
from app.utils.url_norm import url_norm


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


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


def _extract_evidence_snippet(item: dict[str, Any], *, max_chars: int = 240) -> tuple[str, str]:
    def _clean_evidence_text(raw: str) -> str:
        s = str(raw or "")
        if not s:
            return ""
        s = html.unescape(s)
        s = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", s)
        s = re.sub(r"(?is)<br\s*/?>", " ", s)
        s = re.sub(r"(?is)</?(p|div|li|ul|ol|span|strong|em|h[1-6])\b[^>]*>", " ", s)
        s = re.sub(r"(?is)<[^>]+>", " ", s)
        s = re.sub(r"https?://\S+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # Prefer RSS summary/description, then body-like fields.
    candidates = [
        ("summary", str(item.get("summary", "")).strip()),
        ("description", str(item.get("description", "")).strip()),
        ("body", str(item.get("body", "")).strip()),
        ("content", str(item.get("content", "")).strip()),
        ("raw_text", str(item.get("raw_text", "")).strip()),
    ]
    for src, txt in candidates:
        snippet = _clean_evidence_text(txt)
        if snippet:
            return snippet[: max(1, int(max_chars))], src
    return "", ""


def _contains_cjk(text: str) -> bool:
    s = str(text or "")
    for ch in s:
        if "\u4e00" <= ch <= "\u9fff":
            return True
    return False


def _zh_summary_for_email(raw_summary: str, *, title: str, source: str) -> str:
    raw = re.sub(r"\s+", " ", str(raw_summary or "")).strip()
    if _contains_cjk(raw):
        return raw
    t = re.sub(r"\s+", " ", str(title or "")).strip()
    s = re.sub(r"\s+", " ", str(source or "")).strip()
    if t and s:
        return f"该条目来自{s}，主题为“{t}”。建议结合原文核查细节并提炼可执行结论。"
    if t:
        return f"该条目主题为“{t}”。建议结合原文核查细节并提炼可执行结论。"
    return "该条目原始摘要为英文，建议结合原文核查细节并提炼可执行结论。"


def _event_weight_key(event_type: str) -> str:
    et = normalize_event_type(event_type).strip().lower()
    if not et:
        return ""
    if et in EVENT_WEIGHT:
        return et
    if ("招采" in et) or ("procure" in et) or ("tender" in et) or ("bid" in et):
        return "procurement"
    if ("监管" in et) or ("指南" in et) or ("regulatory" in et):
        return "regulatory"
    if ("审批" in et) or ("批准" in et) or ("approval" in et) or ("clearance" in et):
        return "approval"
    if ("优先审评" in et) or ("priority review" in et):
        return "priority_review"
    if ("并购" in et) or ("融资" in et) or ("合作" in et) or ("ipo" in et) or ("company" in et):
        return "company_move"
    if ("paper" in et) or ("study" in et) or ("preprint" in et) or ("journal" in et) or ("科研" in et):
        return "paper"
    return "technology_update"


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
        source_trust_tier: str = "C",
        now_utc: dt.datetime | None = None,
    ) -> dict[str, int]:
        rules_runtime = rules_runtime or {}
        source_policy = normalize_source_policy(
            rules_runtime.get("source_policy", {}) if isinstance(rules_runtime.get("source_policy"), dict) else {},
            profile=str(rules_runtime.get("profile", "legacy")),
        )
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        day = now_utc.date()
        index = self._load_day_index(day)
        target = self._day_file(day)
        written = 0
        skipped = 0
        dropped_by_source_policy = 0
        dropped_reasons: dict[str, int] = {}
        dropped_static_or_listing = 0

        rows: list[str] = []
        source_guard = rules_runtime.get("source_guard", {}) if isinstance(rules_runtime.get("source_guard"), dict) else {}
        source_guard_enabled = bool(source_guard.get("enabled", str(rules_runtime.get("profile", "legacy")).strip().lower() == "enhanced"))
        for it in items:
            title = str(it.get("title", "")).strip()
            url = str(it.get("url", it.get("link", ""))).strip()
            if not title or not url:
                skipped += 1
                continue
            if source_guard_enabled and is_static_or_listing_url(url):
                skipped += 1
                dropped_static_or_listing += 1
                continue
            drop_reason = exclusion_reason(source_id, url, source_policy)
            if drop_reason:
                skipped += 1
                dropped_by_source_policy += 1
                dropped_reasons[drop_reason] = dropped_reasons.get(drop_reason, 0) + 1
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
                    "source": source_name,
                    "source_id": source_id,
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
                "trust_tier": str(source_trust_tier or "C").strip().upper() or "C",
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
        return {
            "written": written,
            "skipped": skipped,
            "dropped_by_source_policy": dropped_by_source_policy,
            "dropped_by_source_policy_reasons": dropped_reasons,
            "dropped_static_or_listing_count": dropped_static_or_listing,
        }

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
        window_start_utc: dt.datetime | None = None,
        window_end_utc: dt.datetime | None = None,
        now_utc: dt.datetime | None = None,
    ) -> list[dict[str, Any]]:
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        if window_end_utc is None:
            window_end_utc = now_utc
        if window_start_utc is None:
            window_hours = max(1, int(window_hours or 24))
            window_start_utc = window_end_utc - dt.timedelta(hours=window_hours)

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
                        if ca < window_start_utc:
                            continue
                        if ca > window_end_utc:
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
    profile = str(analysis_cfg.get("profile", "legacy")).strip().lower() or "legacy"
    source_policy = normalize_source_policy(
        analysis_cfg.get("source_policy", {}) if isinstance(analysis_cfg.get("source_policy"), dict) else {},
        profile=profile,
    )
    source_guard_cfg = analysis_cfg.get("source_guard", {}) if isinstance(analysis_cfg.get("source_guard"), dict) else {}
    source_guard_enabled = bool(source_guard_cfg.get("enabled", profile == "enhanced"))
    source_guard_enforce_article_only = bool(source_guard_cfg.get("enforce_article_only", True))
    evidence_policy_cfg = analysis_cfg.get("evidence_policy", {}) if isinstance(analysis_cfg.get("evidence_policy"), dict) else {}
    require_evidence_for_core = bool(evidence_policy_cfg.get("require_evidence_for_core", profile == "enhanced"))
    min_snippet_chars = int(evidence_policy_cfg.get("min_snippet_chars", 80) or 80)
    degrade_if_missing_evidence = bool(evidence_policy_cfg.get("degrade_if_missing", True))
    opportunity_cfg = analysis_cfg.get("opportunity_index", {}) if isinstance(analysis_cfg.get("opportunity_index"), dict) else {}
    opportunity_enabled = bool(opportunity_cfg.get("enabled", profile == "enhanced"))
    opportunity_window_days = max(1, int(opportunity_cfg.get("window_days", 7) or 7))
    opportunity_asset_dir = str(opportunity_cfg.get("asset_dir", "artifacts/opportunity") or "artifacts/opportunity")
    opportunity_dedupe_cfg = opportunity_cfg.get("dedupe", {}) if isinstance(opportunity_cfg.get("dedupe"), dict) else {}
    opportunity_display_cfg = opportunity_cfg.get("display", {}) if isinstance(opportunity_cfg.get("display"), dict) else {}
    opportunity_dedupe_enabled = bool(opportunity_dedupe_cfg.get("enabled", profile == "enhanced"))
    opportunity_tail_lines_scan = max(1, int(opportunity_dedupe_cfg.get("tail_lines_scan", 2000) or 2000))
    opportunity_top_n = max(1, int(opportunity_display_cfg.get("top_n", 5) or 5))
    opportunity_store = OpportunityStore(Path("."), asset_dir=opportunity_asset_dir) if opportunity_enabled else None
    relevance_runtime = {
        "profile": profile,
        "investment_scope_enabled": bool(profile == "enhanced"),
        "anchors_pack": analysis_cfg.get("anchors_pack", {}) if isinstance(analysis_cfg.get("anchors_pack"), dict) else {},
        "negatives_pack": analysis_cfg.get("negatives_pack", []) if isinstance(analysis_cfg.get("negatives_pack"), list) else [],
        "frontier_policy": analysis_cfg.get("frontier_policy", {}) if isinstance(analysis_cfg.get("frontier_policy"), dict) else {},
    }
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
    cache_key_mismatch = 0
    generated_count = 0
    degraded_count = 0
    degraded_reasons: dict[str, int] = {}
    source_policy_dropped_rows: dict[str, int] = {}
    dropped_static_or_listing_count = 0
    dropped_static_or_listing_domains: dict[str, int] = {}
    dropped_bio_general_count = 0
    bio_general_terms_count: dict[str, int] = {}
    evidence_missing_core_count = 0
    evidence_missing_sources: dict[str, int] = {}
    evidence_present_core_count = 0
    opportunity_signals_written = 0
    opportunity_signals_deduped = 0
    opportunity_signals_dropped_probe = 0
    opportunity_index_kpis: dict[str, Any] = {}
    region_maps = load_region_map(Path("."))
    lane_maps = load_lane_map(Path("."))
    style_cfg = analysis_cfg.get("style", {}) if isinstance(analysis_cfg.get("style"), dict) else {}
    style_lang = "en" if str(style_cfg.get("language", "zh")).strip().lower() == "en" else "zh"
    style_tone = str(style_cfg.get("tone", "concise_decision")).strip().lower()
    style_no_fluff = bool(style_cfg.get("no_fluff", True))

    def _sty(decision_zh: str, neutral_zh: str, decision_en: str, neutral_en: str) -> str:
        if style_lang == "en":
            s = neutral_en if style_tone == "neutral" else decision_en
            return s if style_no_fluff else f"Note: {s}"
        s = neutral_zh if style_tone == "neutral" else decision_zh
        return s if style_no_fluff else f"背景：{s}"

    def _sty_trend(decision_zh: str, neutral_zh: str, decision_en: str, neutral_en: str) -> str:
        # E 段固定输出结论句，不添加“背景：/Note:”前缀。
        if style_lang == "en":
            return neutral_en if style_tone == "neutral" else decision_en
        return neutral_zh if style_tone == "neutral" else decision_zh
    sec_a = "A. Key Highlights (8-15 items, ranked by importance)" if style_lang == "en" else "A. 今日要点（8-15条，按重要性排序）"
    sec_b = "B. Lane Snapshot (Oncology/Infectious/Repro-Genetics/Other)" if style_lang == "en" else "B. 分赛道速览（肿瘤/感染/生殖遗传/其他）"
    sec_c = "C. Platform Radar (daily updates by platform)" if style_lang == "en" else "C. 技术平台雷达（按平台汇总当日进展）"
    sec_d = "D. Regional Heatmap (NA/EU/APAC/CN)" if style_lang == "en" else "D. 区域热力图（北美/欧洲/亚太/中国）"
    sec_e = "E. Key Trend Judgments (industry and technology)" if style_lang == "en" else "E. 三条关键趋势判断（产业与技术各至少1条）"
    sec_f = "F. Gaps & Next-day Tracking List (3-5 items)" if style_lang == "en" else "F. 信息缺口与次日跟踪清单（3-5条）"
    sec_g = "G. Quality Metrics (Quality Audit)" if style_lang == "en" else "G. 质量指标 (Quality Audit)"
    lbl_summary = "Summary" if style_lang == "en" else "摘要"
    lbl_evidence = "Evidence Snippet" if style_lang == "en" else "证据摘录"
    lbl_published = "Published" if style_lang == "en" else "发布日期"
    lbl_source = "Source" if style_lang == "en" else "来源"
    lbl_region = "Region" if style_lang == "en" else "地区"
    lbl_lane = "Lane" if style_lang == "en" else "赛道"
    lbl_event = "Event Type" if style_lang == "en" else "事件类型"
    lbl_platform = "Platform" if style_lang == "en" else "技术平台"

    lines: list[str] = [subject, ""]

    items_filtered, source_policy_dropped_count, source_policy_dropped_rows = filter_rows_for_digest(items, policy=source_policy)
    items = []
    for r in items_filtered:
        item_url = str(r.get("url", "")).strip()
        if source_guard_enabled and is_static_or_listing_url(item_url):
            dropped_static_or_listing_count += 1
            dm = str(urlparse(item_url).hostname or "").strip().lower() if item_url else ""
            if dm:
                dropped_static_or_listing_domains[dm] = dropped_static_or_listing_domains.get(dm, 0) + 1
            continue
        if source_guard_enabled and source_guard_enforce_article_only:
            item_type = str(r.get("item_type", "")).strip().lower()
            if item_type == "html_article":
                am = r.get("article_meta", {}) if isinstance(r.get("article_meta"), dict) else {}
                if not am:
                    dropped_static_or_listing_count += 1
                    dm = str(urlparse(item_url).hostname or "").strip().lower() if item_url else ""
                    if dm:
                        dropped_static_or_listing_domains[dm] = dropped_static_or_listing_domains.get(dm, 0) + 1
                    continue
        title = str(r.get("title", "")).strip()
        summary = str(r.get("summary", "")).strip()
        text = f"{title} {summary}".strip()
        track, level, explain = compute_relevance(
            text,
            {
                "source_group": str(r.get("source_group", "")).strip(),
                "source": str(r.get("source", "")).strip(),
                "source_id": str(r.get("source_id", "")).strip(),
                "event_type": str(r.get("event_type", "")).strip(),
                "url": str(r.get("url", "")).strip(),
                "title": title,
            },
            relevance_runtime,
        )
        if str(track).strip().lower() == "drop":
            if str(explain.get("final_reason", "")).strip() == "bio_general_without_diagnostic_anchor":
                dropped_bio_general_count += 1
                for t in explain.get("bio_general_anchors_hit", []) if isinstance(explain.get("bio_general_anchors_hit", []), list) else []:
                    tt = str(t).strip().lower()
                    if tt:
                        bio_general_terms_count[tt] = bio_general_terms_count.get(tt, 0) + 1
            continue
        rr = dict(r)
        rr["track"] = track
        rr["relevance_level"] = int(level)
        rr["relevance_explain"] = explain
        region_in = map_normalize_unknown(rr.get("region", ""))
        lane_in = map_normalize_unknown(rr.get("lane", ""))
        if region_in != "__unknown__":
            rr["region"] = region_in
            rr["region_source"] = "item"
        else:
            rm = map_classify_region(str(rr.get("url", "")).strip(), region_maps)
            rr["region"] = rm
            rr["region_source"] = "domain_map" if rm != "__unknown__" else "unknown"
        if lane_in != "__unknown__":
            rr["lane"] = lane_in
            rr["lane_source"] = "item"
        else:
            lane_text = " ".join(
                [
                    str(rr.get("title", "")).strip(),
                    str(rr.get("summary", "")).strip(),
                    str(rr.get("evidence_snippet", "")).strip(),
                ]
            ).strip()
            lm = map_classify_lane(lane_text, lane_maps)
            rr["lane"] = lm
            rr["lane_source"] = "keyword_map" if lm != "__unknown__" else "unknown"
        items.append(rr)
        if opportunity_store is not None:
            try:
                et = normalize_event_type(
                    str(rr.get("event_type", "")).strip(),
                    text=" ".join(
                        [
                            str(rr.get("title", "")).strip(),
                            str(rr.get("summary", "")).strip(),
                        ]
                    ),
                    url=str(rr.get("url", "")).strip(),
                )
                wk = _event_weight_key(et)
                wres = opportunity_store.append_signal(
                    {
                        "date": date_str,
                        "region": str(rr.get("region", "")).strip() or "__unknown__",
                        "lane": str(rr.get("lane", "")).strip() or "__unknown__",
                        "event_type": et,
                        "weight": int(EVENT_WEIGHT.get(wk, 1)),
                        "source_id": str(rr.get("source_id", "")).strip(),
                        "url_norm": url_norm(str(rr.get("url", "")).strip()),
                    }
                    ,
                    dedupe_enabled=opportunity_dedupe_enabled,
                    tail_lines_scan=opportunity_tail_lines_scan,
                )
                opportunity_signals_written += int((wres or {}).get("written", 0) or 0)
                opportunity_signals_deduped += int((wres or {}).get("deduped", 0) or 0)
                opportunity_signals_dropped_probe += int((wres or {}).get("dropped_probe", 0) or 0)
            except Exception:
                pass

    core_items = [r for r in items if str(r.get("track", "")) == "core" and int(r.get("relevance_level", 0) or 0) >= int(core_min_level_for_A)]
    frontier_items = [r for r in items if str(r.get("track", "")) == "frontier" and int(r.get("relevance_level", 0) or 0) >= int(frontier_min_level_for_F)]

    lines.append(sec_a)
    top = core_items[:15]
    if not top:
        lines.append("1) [7天补充] collect资产窗口内无可用 core 条目。")
        lines.append(
            "摘要："
            + _sty(
                "请检查 collect 是否正常写入，或放宽窗口/阈值。",
                "建议先核对 collect 写入，再视情况放宽窗口或阈值。",
                "Check whether collect assets are being written correctly, or relax window/threshold settings.",
                "Review collect writes first, then adjust window/threshold if needed.",
            )
        )
        lines.append(f"{lbl_published}：{date_str}（北京时间）")
        lines.append(f"{lbl_source}：collect-assets")
        lines.append(f"{lbl_region}：全球")
        lines.append(f"{lbl_lane}：其他")
        lines.append(f"{lbl_event}：政策与市场动态")
        lines.append(f"{lbl_platform}：跨平台/未标注")
        lines.append("")
    else:
        for idx, r in enumerate(top, 1):
            analysis = None
            item_key = ""
            evidence_snippet, evidence_from = _extract_evidence_snippet(r, max_chars=240)
            evidence_ok = len(str(evidence_snippet or "").strip()) >= max(1, min_snippet_chars)
            if require_evidence_for_core and not evidence_ok and degrade_if_missing_evidence:
                evidence_missing_core_count += 1
                src = str(r.get("source", "")).strip() or str(r.get("source_id", "")).strip() or "unknown"
                evidence_missing_sources[src] = evidence_missing_sources.get(src, 0) + 1
                analysis = {
                    "summary": f"[NO_EVIDENCE] {str(r.get('title', '')).strip()}",
                    "impact": "影响：证据片段不足，暂不输出结论性判断。",
                    "action": "建议：需打开原文核查并补充可引用证据后再采用。",
                    "used_model": "",
                    "model": "",
                    "prompt_version": str(prompt_version),
                    "token_usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                    "generated_at": _to_iso_utc(dt.datetime.now(dt.timezone.utc)),
                    "degraded": True,
                    "degraded_reason": "missing_evidence",
                    "ok": False,
                    "evidence_snippet": "",
                    "evidence_from": "",
                }
                degraded_count += 1
                degraded_reasons["missing_evidence"] = degraded_reasons.get("missing_evidence", 0) + 1
            else:
                if evidence_ok:
                    evidence_present_core_count += 1
            if enable_analysis_cache:
                item_key = AnalysisCacheStore.item_key(r)
                if analysis is not None:
                    cache_miss += 1
                elif not always_generate and cache_store is not None:
                    analysis = cache_store.get(item_key, run_day)
                    if analysis:
                        cache_hit += 1
                        computed_un = url_norm(str(r.get("url", "")).strip())
                        payload_un = str((analysis or {}).get("url_norm", "")).strip() or url_norm(str((analysis or {}).get("url", "")).strip())
                        payload_key = str((analysis or {}).get("cache_key", (analysis or {}).get("item_key", ""))).strip()
                        if computed_un and ((payload_un and payload_un != computed_un) or (payload_key and payload_key != computed_un)):
                            cache_key_mismatch += 1
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
                            "cache_key": item_key,
                            "item_key": item_key,
                            "url": str(r.get("url", "")),
                            "url_norm": url_norm(str(r.get("url", ""))),
                            "story_id": str(r.get("story_id", "")),
                            "source_id": str(r.get("source_id", "")),
                            "title": str(r.get("title", "")),
                            "model": str(analysis.get("used_model", analysis.get("model", model_name))),
                            "prompt_version": str(analysis.get("prompt_version", prompt_version)),
                            "generated_at": str(analysis.get("generated_at", "")) or _to_iso_utc(dt.datetime.now(dt.timezone.utc)),
                            "token_usage": analysis.get("token_usage", {}) if isinstance(analysis.get("token_usage", {}), dict) else {},
                            "evidence_snippet": evidence_snippet if evidence_ok else "",
                            "evidence_from": evidence_from if evidence_ok else "",
                        }
                    )
                    cache_store.put(item_key, payload, run_day)
            elif enable_analysis_cache and cache_store is not None and item_key:
                payload = dict(analysis)
                payload.update(
                    {
                        "cache_key": item_key,
                        "item_key": item_key,
                        "url": str(r.get("url", "")),
                        "url_norm": url_norm(str(r.get("url", ""))),
                        "story_id": str(r.get("story_id", "")),
                        "source_id": str(r.get("source_id", "")),
                        "title": str(r.get("title", "")),
                        "model": str(analysis.get("used_model", analysis.get("model", ""))),
                        "prompt_version": str(analysis.get("prompt_version", prompt_version)),
                        "generated_at": str(analysis.get("generated_at", "")) or _to_iso_utc(dt.datetime.now(dt.timezone.utc)),
                        "token_usage": analysis.get("token_usage", {}) if isinstance(analysis.get("token_usage", {}), dict) else {},
                        "evidence_snippet": evidence_snippet if evidence_ok else "",
                        "evidence_from": evidence_from if evidence_ok else "",
                    }
                )
                cache_store.put(item_key, payload, run_day)

            lines.append(f"{idx}) [24小时内] {str(r.get('title',''))}")
            sm = str((analysis or {}).get("summary", "")).strip() or str(r.get("summary", "")).strip() or "摘要：由 collect 资产生成。"
            if style_lang == "zh":
                sm = _zh_summary_for_email(sm, title=str(r.get("title", "")), source=str(r.get("source", "")))
            lines.append(sm if sm.startswith("摘要") else f"{lbl_summary}：{sm}")
            lines.append(f"{lbl_evidence}：{evidence_snippet if evidence_ok else '[缺失]'}")
            lines.append(f"{lbl_published}：{str(r.get('published_at',''))}")
            lines.append(f"{lbl_source}：{str(r.get('source',''))} | {str(r.get('url',''))}")
            if str(r.get("region", "")).strip():
                lines.append(f"{lbl_region}：{str(r.get('region',''))}")
            if str(r.get("lane", "")).strip():
                lines.append(f"{lbl_lane}：{str(r.get('lane',''))}")
            if str(r.get("event_type", "")).strip():
                lines.append(f"{lbl_event}：{str(r.get('event_type',''))}")
            if str(r.get("platform", "")).strip():
                lines.append(f"{lbl_platform}：{str(r.get('platform',''))}")
            lines.append("")

    lines.append(sec_b)
    lanes = {"肿瘤检测": 0, "感染检测": 0, "生殖与遗传检测": 0, "其他": 0}
    for r in top:
        lane = str(r.get("lane", "其他"))
        lanes[lane] = lanes.get(lane, 0) + 1
    for k in ["肿瘤检测", "感染检测", "生殖与遗传检测", "其他"]:
        lines.append(f"- {k}：{lanes.get(k, 0)} 条（以当日抓取为准）")
    lines.append("")

    lines.append(sec_c)
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

    lines.append(sec_d)
    regs = {"北美": 0, "欧洲": 0, "亚太": 0, "中国": 0}
    for r in top:
        rg = str(r.get("region", ""))
        regs[rg] = regs.get(rg, 0) + 1
    for k in ["北美", "欧洲", "亚太", "中国"]:
        lines.append(f"- {k}：{regs.get(k, 0)}")
    lines.append("")

    lines.append(sec_e)
    lines.append(
        "1) "
        + _sty_trend(
            "产业：并购合作与产品注册更聚焦可快速放量场景，商业化节奏正由渠道与准入共同决定。",
            "产业：并购合作与注册节奏仍由渠道和准入共同驱动。",
            "Industry: M&A/cooperation and product registration are concentrating on faster scale-up scenarios.",
            "Industry: M&A/cooperation and registration pace continue to be shaped by channel access and market entry.",
        )
    )
    lines.append(
        "2) "
        + _sty_trend(
            "技术：PCR/NGS/免疫平台继续并行，组合菜单与自动化能力是实验室端的核心竞争变量。",
            "技术：PCR/NGS/免疫平台并行，菜单完整度与自动化能力仍是关键。",
            "Technology: PCR/NGS/immuno platforms continue in parallel; menu breadth and automation remain key moats.",
            "Technology: PCR/NGS/immuno platforms remain parallel, with menu breadth and automation as key factors.",
        )
    )
    lines.append(
        "3) "
        + _sty_trend(
            "监管：亚太监管与中国追溯体系持续强化，跨区域上市正从“单点获批”转向“体系化合规”。",
            "监管：亚太与中国追溯体系持续加强，跨区域上市更依赖体系化合规。",
            "Regulatory: APAC and China traceability systems keep tightening, shifting cross-region launches to system-level compliance.",
            "Regulatory: APAC/China traceability remains stricter, and cross-region launch paths rely more on system-level compliance.",
        )
    )
    lines.append("")

    lines.append(sec_f)
    f_rows = frontier_items[: max(0, int(frontier_quota or 0))]
    if f_rows:
        for idx, r in enumerate(f_rows, 1):
            lines.append(f"{idx}) frontier雷达：L{int(r.get('relevance_level', 0) or 0)} | {str(r.get('title',''))[:90]}")
    else:
        lines.append(
            "1) "
            + _sty(
                "frontier雷达：当前窗口无满足阈值的 frontier 条目，建议扩充前沿关键词或延长窗口。",
                "frontier雷达：当前窗口 frontier 命中不足，可考虑扩充关键词或延长窗口。",
                "Frontier radar: no frontier item met the threshold in the current window; expand frontier keywords or extend the window.",
                "Frontier radar: frontier hits are limited in the current window; consider keyword/window tuning.",
            )
        )
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

    lines.append(sec_g)
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
        f"analysis_cache_hit/miss：{cache_hit}/{cache_miss} | analysis_cache_key_mismatch：{cache_key_mismatch} | generated_count：{generated_count} | "
        f"degraded_count：{degraded_count} | degraded_reason_top3：{reason_top}"
    )
    bio_top = "; ".join([f"{k}:{v}" for k, v in sorted(bio_general_terms_count.items(), key=lambda kv: (-kv[1], kv[0]))[:5]]) or "无"
    lines.append(
        f"dropped_bio_general_count：{dropped_bio_general_count} | top_bio_general_terms：{bio_top}"
    )
    evidence_top = "; ".join([f"{k}:{v}" for k, v in sorted(evidence_missing_sources.items(), key=lambda kv: (-kv[1], kv[0]))[:5]]) or "无"
    lines.append(
        f"evidence_missing_core_count：{evidence_missing_core_count} | evidence_missing_sources_topN：{evidence_top}"
    )
    if source_policy_dropped_count > 0:
        sp_top = "; ".join([f"{k}:{v}" for k, v in sorted(source_policy_dropped_rows.items(), key=lambda kv: (-kv[1], kv[0]))[:3]])
        lines.append(
            f"dropped_by_source_policy_count：{source_policy_dropped_count} | dropped_by_source_policy_top3：{sp_top}"
        )
    if dropped_static_or_listing_count > 0:
        dom_top = "; ".join(
            [f"{k}:{v}" for k, v in sorted(dropped_static_or_listing_domains.items(), key=lambda kv: (-kv[1], kv[0]))[:5]]
        ) or "无"
        lines.append(
            f"dropped_static_or_listing_count：{dropped_static_or_listing_count} | "
            f"dropped_static_or_listing_top_domains：{dom_top}"
        )
    if opportunity_enabled:
        lines.append(
            "opportunity_signals_written/deduped/dropped_probe："
            f"{opportunity_signals_written}/{opportunity_signals_deduped}/{opportunity_signals_dropped_probe}"
        )
    if opportunity_enabled:
        lines.append("")
        lines.append(
            f"H. Opportunity Intensity Index (last {opportunity_window_days} days)"
            if style_lang == "en"
            else f"H. 机会强度指数（近{opportunity_window_days}天）"
        )
        try:
            idx = compute_opportunity_index(
                Path("."),
                window_days=opportunity_window_days,
                asset_dir=opportunity_asset_dir,
                as_of=date_str,
                display=opportunity_display_cfg,
            )
            rows_idx = idx.get("top", []) if isinstance(idx, dict) else []
            if not isinstance(rows_idx, list):
                rows_idx = []
            if not rows_idx:
                lines.append("- 暂无显著机会变化")
            else:
                for r in rows_idx[:opportunity_top_n]:
                    delta = int(r.get("delta_vs_prev_window", 0) or 0)
                    arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "→")
                    region = str(r.get("region", "__unknown__")).strip() or "__unknown__"
                    lane = str(r.get("lane", "__unknown__")).strip() or "__unknown__"
                    score = int(r.get("score", 0) or 0)
                    low_conf = " [LOW_CONF]" if bool(r.get("low_confidence", False)) else ""
                    lines.append(f"- {lane}（{region}）：{arrow} {delta:+d} | score={score}{low_conf}")
                    contrib_rows = r.get("contrib_top2", []) if isinstance(r.get("contrib_top2"), list) else []
                    if contrib_rows:
                        parts: list[str] = []
                        for c in contrib_rows[:2]:
                            if not isinstance(c, dict):
                                continue
                            et = str(c.get("event_type", "__unknown__")).strip() or "__unknown__"
                            ws = int(c.get("weight_sum", 0) or 0)
                            cnt = int(c.get("count", 0) or 0)
                            parts.append(f"{et}={ws} ({cnt})")
                        if parts:
                            lines.append(f"  contrib: {'; '.join(parts)}")
            kpis = idx.get("kpis", {}) if isinstance(idx.get("kpis"), dict) else {}
            opportunity_index_kpis = dict(kpis)
            lines.append(
                "- kpis: "
                + f"unknown_region_rate={float(kpis.get('unknown_region_rate', 0.0) or 0.0):.2f}, "
                + f"unknown_lane_rate={float(kpis.get('unknown_lane_rate', 0.0) or 0.0):.2f}, "
                + f"unknown_event_type_rate={float(kpis.get('unknown_event_type_rate', 0.0) or 0.0):.2f}"
            )
            region_top = kpis.get("unknown_region_top_domains", []) if isinstance(kpis.get("unknown_region_top_domains"), list) else []
            lane_top = kpis.get("unknown_lane_top_terms", []) if isinstance(kpis.get("unknown_lane_top_terms"), list) else []
            if region_top:
                lines.append(
                    "- unknown_region_top_domains: "
                    + "; ".join([f"{str(x.get('host',''))}:{int(x.get('count',0) or 0)}" for x in region_top[:5]])
                )
            if lane_top:
                lines.append(
                    "- unknown_lane_top_terms: "
                    + "; ".join([f"{str(x.get('term',''))}:{int(x.get('count',0) or 0)}" for x in lane_top[:5]])
                )
        except Exception:
            lines.append("- 暂无显著机会变化")
        lines.append(
            f"- opportunity_signals_written/deduped/dropped_probe："
            f"{opportunity_signals_written}/{opportunity_signals_deduped}/{opportunity_signals_dropped_probe}"
        )
    if not items:
        lines.append("分流规则缺口说明：collect 资产窗口内无条目，请检查 collect 调度、信源可达性和资产目录。")
    txt = "\n".join(lines).rstrip() + "\n"
    meta = {
        "analysis_cache_hit": cache_hit,
        "analysis_cache_miss": cache_miss,
        "analysis_cache_key_mismatch": cache_key_mismatch,
        "analysis_generated_count": generated_count,
        "analysis_degraded_count": degraded_count,
        "analysis_degraded_reason_top3": [
            {"reason": k, "count": v}
            for k, v in sorted(degraded_reasons.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        ],
        "analysis_prompt_version": prompt_version,
        "analysis_model": model_name,
        "analysis_cache_enabled": enable_analysis_cache,
        "dropped_by_source_policy_count": int(source_policy_dropped_count),
        "dropped_by_source_policy_reasons": source_policy_dropped_rows,
        "dropped_static_or_listing_count": int(dropped_static_or_listing_count),
        "dropped_static_or_listing_top_domains": [
            {"domain": k, "count": v}
            for k, v in sorted(dropped_static_or_listing_domains.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
        ],
        "dropped_bio_general_count": int(dropped_bio_general_count),
        "top_bio_general_terms": [
            {"term": k, "count": v}
            for k, v in sorted(bio_general_terms_count.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
        ],
        "evidence_missing_core_count": int(evidence_missing_core_count),
        "evidence_present_core_count": int(evidence_present_core_count),
        "evidence_missing_sources_topN": [
            {"source": k, "count": v}
            for k, v in sorted(evidence_missing_sources.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
        ],
        "opportunity_index_enabled": bool(opportunity_enabled),
        "opportunity_window_days": int(opportunity_window_days),
        "opportunity_signals_written": int(opportunity_signals_written),
        "opportunity_signals_deduped": int(opportunity_signals_deduped),
        "opportunity_signals_dropped_probe": int(opportunity_signals_dropped_probe),
        "opportunity_index_kpis": opportunity_index_kpis,
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
