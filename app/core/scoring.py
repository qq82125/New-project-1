from __future__ import annotations

import copy
import hashlib
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import yaml


DEFAULT_SCORING_CONFIG: dict[str, Any] = {
    "base_weight_by_tag": {
        "regulatory": 1.00,
        "journal": 0.95,
        "company": 0.90,
        "market_research": 0.85,
        "thinktank": 0.85,
        "media": 0.70,
        "aggregator": 0.30,
    },
    "trust_tier_adjust": {"A": 0.10, "B": 0.00, "C": -0.10},
    "source_weight": {"min": 0.10, "max": 1.10},
    "evidence_points": {"A": 45, "B": 35, "C": 25, "D": 10},
    "source_points_factor": 30,
    "recency_points": {
        "lt_24h": 15,
        "d1_3": 10,
        "d3_7": 6,
        "d7_14": 2,
        "gt_14": 0,
    },
    "completeness_points": {
        "summary": 3,
        "published_at": 3,
        "source_name": 2,
        "original_source_url": 6,
        "max": 10,
    },
    "penalties": {
        "aggregator_without_original": -12,
        "very_short_duplicate_like": -6,
    },
    "signal_bonus": {"红": 6, "橙": 3, "黄": 1, "灰": 0},
    "signal_rules": {
        "red_keywords": [
            "监管",
            "准入",
            "召回",
            "警示",
            "指南",
            "ivdr",
            "who prequalification",
            "pq",
            "fda",
            "pmda",
            "nmpa",
            "mhra",
            "warning",
            "recall",
            "guidance",
            "field safety",
            "m&a",
            "acquisition",
        ],
        "orange_keywords": [
            "融资",
            "并购",
            "ipo",
            "上市",
            "产品发布",
            "多中心",
            "临床",
            "指南引用",
            "funding",
            "partnership",
            "trial",
            "validation",
            "approval",
        ],
        "yellow_keywords": [
            "趋势",
            "合作",
            "渠道",
            "迭代",
            "industry",
            "trend",
            "launch",
            "update",
            "collaboration",
        ],
    },
    "domain_whitelist": {
        "regulatory": ["fda.gov", "europa.eu", "ema.europa.eu", "imdrf.org", "who.int", "nmpa.gov.cn", "pmda.go.jp", "mhlw.go.jp", "tga.gov.au", "hsa.gov.sg", "mfds.go.kr", "gov.uk"],
        "journal": ["nejm.org", "thelancet.com", "nature.com", "science.org", "pubmed.ncbi.nlm.nih.gov", "europepmc.org"],
        "company": ["roche.com", "abbott.com", "siemens-healthineers.com", "danaher.com", "qiagen.com", "illumina.com", "thermofisher.com", "hologic.com"],
        "preprint": ["medrxiv.org", "biorxiv.org"],
    },
    "quotas": {
        "regulatory": {"min": 4},
        "journal_preprint": {"min": 4},
        "company": {"min": 4},
        "media": {"max": 14},
        "aggregator": {"max": 2},
    },
    "similarity_thresholds": {"title_jaccard": 0.92},
}

EVIDENCE_ORDER = {"A": 4, "B": 3, "C": 2, "D": 1}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def load_scoring_config(project_root: Path) -> dict[str, Any]:
    p = project_root / "rules" / "scoring.yaml"
    if not p.exists():
        return copy.deepcopy(DEFAULT_SCORING_CONFIG)
    try:
        cfg = yaml.safe_load(p.read_text(encoding="utf-8"))
        if not isinstance(cfg, dict):
            return copy.deepcopy(DEFAULT_SCORING_CONFIG)
        return _deep_merge(DEFAULT_SCORING_CONFIG, cfg)
    except Exception:
        return copy.deepcopy(DEFAULT_SCORING_CONFIG)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _norm_text(s: str) -> str:
    t = str(s or "").lower()
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def _match_any(text: str, keywords: list[str]) -> bool:
    t = _norm_text(text)
    for kw in keywords or []:
        k = _norm_text(kw)
        if not k:
            continue
        if k in t:
            return True
    return False


def _first_tag(tags: list[str], candidates: list[str]) -> str | None:
    tset = {str(x).strip().lower() for x in (tags or [])}
    for c in candidates:
        if c in tset:
            return c
    return None


def classify_bucket(tags: list[str], fetcher: str) -> str:
    tset = {str(x).strip().lower() for x in (tags or [])}
    f = str(fetcher or "").strip().lower()
    if "aggregator" in tset or f in {"google_news", "rsshub"}:
        return "aggregator"
    if "regulatory" in tset:
        return "regulatory"
    if "journal" in tset:
        return "journal"
    if "preprint" in tset:
        return "preprint"
    if "company" in tset:
        return "company"
    if "market_research" in tset:
        return "market_research"
    if "thinktank" in tset:
        return "thinktank"
    return "media"


def compute_source_weight(tags: list[str], trust_tier: str, fetcher: str, cfg: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    bucket = classify_bucket(tags, fetcher)
    bmap = cfg.get("base_weight_by_tag", {}) if isinstance(cfg.get("base_weight_by_tag"), dict) else {}
    base = float(bmap.get(bucket, bmap.get("media", 0.70)))
    tadj = cfg.get("trust_tier_adjust", {}) if isinstance(cfg.get("trust_tier_adjust"), dict) else {}
    adj = float(tadj.get(str(trust_tier or "B").upper(), 0.0))
    limits = cfg.get("source_weight", {}) if isinstance(cfg.get("source_weight"), dict) else {}
    lo = float(limits.get("min", 0.10))
    hi = float(limits.get("max", 1.10))
    w = _clamp(base + adj, lo, hi)
    return w, {"bucket": bucket, "base": base, "trust_tier": str(trust_tier or "B").upper(), "adjust": adj}


def _extract_urls(text: str) -> list[str]:
    return re.findall(r"https?://[^\s\]\)\"'>]+", str(text or ""), flags=re.I)


def extract_original_source_url(link: str, summary: str) -> str:
    lk = str(link or "").strip()
    if not lk:
        return ""
    p = urlparse(lk)
    q = parse_qs(p.query or "")
    for key in ("url", "u", "target", "source", "article_url"):
        vals = q.get(key, [])
        if vals:
            u = unquote(vals[0]).strip()
            if u.startswith("http://") or u.startswith("https://"):
                return u
    urls = [u for u in _extract_urls(summary) if u != lk]
    return urls[0] if urls else ""


def _domain(url: str) -> str:
    try:
        return urlparse(str(url or "")).netloc.lower().strip()
    except Exception:
        return ""


def maybe_upgrade_evidence_by_original(evidence: str, original_source_url: str, cfg: dict[str, Any]) -> tuple[str, str]:
    if not original_source_url:
        return evidence, ""
    d = _domain(original_source_url)
    wl = cfg.get("domain_whitelist", {}) if isinstance(cfg.get("domain_whitelist"), dict) else {}

    def _hit(group: str) -> bool:
        vals = wl.get(group, []) if isinstance(wl.get(group), list) else []
        return any(str(x).lower() in d for x in vals)

    target = evidence
    reason = "original_source_url_detected"
    if _hit("regulatory") or _hit("journal"):
        target = "A"
        reason = "original_source_url_whitelist_A"
    elif _hit("company") or _hit("preprint"):
        target = "B"
        reason = "original_source_url_whitelist_B"
    else:
        # At least C when original link is available.
        if EVIDENCE_ORDER.get(evidence, 1) < EVIDENCE_ORDER["C"]:
            target = "C"
            reason = "original_source_url_upgrade_to_C"
    return target, reason


def compute_evidence_grade(bucket: str) -> str:
    if bucket in {"regulatory", "journal"}:
        return "A"
    if bucket in {"company", "preprint"}:
        return "B"
    if bucket in {"media", "market_research", "thinktank"}:
        return "C"
    return "D"


def _recency_points(published_at: datetime | None, now_utc: datetime, cfg: dict[str, Any]) -> float:
    rc = cfg.get("recency_points", {}) if isinstance(cfg.get("recency_points"), dict) else {}
    if not published_at:
        return float(rc.get("gt_14", 0))
    pa = published_at.astimezone(timezone.utc)
    age = now_utc - pa
    if age < timedelta(hours=24):
        return float(rc.get("lt_24h", 15))
    if age < timedelta(days=3):
        return float(rc.get("d1_3", 10))
    if age < timedelta(days=7):
        return float(rc.get("d3_7", 6))
    if age < timedelta(days=14):
        return float(rc.get("d7_14", 2))
    return float(rc.get("gt_14", 0))


def _completeness_points(item: dict[str, Any], cfg: dict[str, Any]) -> float:
    cp = cfg.get("completeness_points", {}) if isinstance(cfg.get("completeness_points"), dict) else {}
    pts = 0.0
    if str(item.get("summary_cn", "")).strip() or str(item.get("summary", "")).strip():
        pts += float(cp.get("summary", 3))
    if item.get("published_at"):
        pts += float(cp.get("published_at", 3))
    if str(item.get("source", "")).strip():
        pts += float(cp.get("source_name", 2))
    if str(item.get("original_source_url", "")).strip():
        pts += float(cp.get("original_source_url", 6))
    return min(pts, float(cp.get("max", 10)))


def _signal_level(event_type: str, evidence: str, text: str, cfg: dict[str, Any]) -> tuple[str, float, str]:
    sr = cfg.get("signal_rules", {}) if isinstance(cfg.get("signal_rules"), dict) else {}
    red = sr.get("red_keywords", []) if isinstance(sr.get("red_keywords"), list) else []
    orange = sr.get("orange_keywords", []) if isinstance(sr.get("orange_keywords"), list) else []
    yellow = sr.get("yellow_keywords", []) if isinstance(sr.get("yellow_keywords"), list) else []
    evid = str(evidence or "D")
    lvl = "灰"
    reason = "default"
    if evid == "A" and _match_any(text, red):
        lvl = "红"
        reason = "A_and_red_keyword"
    elif evid == "B" and _match_any(text, orange):
        lvl = "橙"
        reason = "B_and_orange_keyword"
    elif evid in {"A", "B", "C"} and (_match_any(text, yellow) or _match_any(str(event_type), yellow)):
        lvl = "黄"
        reason = "keyword_yellow"
    bonus_map = cfg.get("signal_bonus", {}) if isinstance(cfg.get("signal_bonus"), dict) else {}
    bonus = float(bonus_map.get(lvl, 0))
    return lvl, bonus, reason


def make_item_id(item: dict[str, Any]) -> str:
    raw = f"{item.get('source_id','')}|{item.get('url') or item.get('link') or ''}|{item.get('title','')}"
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:12]


def score_item(item: dict[str, Any], source_meta: dict[str, Any], now_utc: datetime, cfg: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(item)
    out["item_id"] = out.get("item_id") or make_item_id(out)
    tags = source_meta.get("tags", []) if isinstance(source_meta.get("tags"), list) else []
    fetcher = str(source_meta.get("fetcher") or source_meta.get("connector") or out.get("fetcher") or "rss")
    trust_tier = str(source_meta.get("trust_tier") or "B")

    source_weight, source_meta_brief = compute_source_weight(tags, trust_tier, fetcher, cfg)
    bucket = source_meta_brief.get("bucket", "media")
    evidence = compute_evidence_grade(bucket)

    orig = extract_original_source_url(str(out.get("url") or out.get("link") or ""), str(out.get("summary_cn") or out.get("summary") or ""))
    if orig:
        out["original_source_url"] = orig
    evidence, evidence_reason = maybe_upgrade_evidence_by_original(evidence, str(out.get("original_source_url", "")), cfg)

    ep = cfg.get("evidence_points", {}) if isinstance(cfg.get("evidence_points"), dict) else {}
    evidence_points = float(ep.get(evidence, ep.get("D", 10)))
    source_points = source_weight * float(cfg.get("source_points_factor", 30))
    recency_points = _recency_points(out.get("published_at"), now_utc, cfg)
    completeness_points = _completeness_points(out, cfg)

    penalties = 0.0
    p_cfg = cfg.get("penalties", {}) if isinstance(cfg.get("penalties"), dict) else {}
    if bucket == "aggregator" and not str(out.get("original_source_url", "")).strip():
        penalties += float(p_cfg.get("aggregator_without_original", -12))
    summary_len = len(str(out.get("summary_cn") or out.get("summary") or ""))
    if summary_len < 40:
        penalties += float(p_cfg.get("very_short_duplicate_like", -6))

    full_text = " ".join(
        [
            str(out.get("title", "")),
            str(out.get("summary_cn") or out.get("summary") or ""),
            str(out.get("event_type", "")),
        ]
    )
    signal_level, signal_bonus, signal_reason = _signal_level(str(out.get("event_type", "")), evidence, full_text, cfg)

    quality_score = _clamp(
        evidence_points + source_points + recency_points + completeness_points + penalties + signal_bonus,
        0,
        100,
    )

    out["evidence_grade"] = evidence
    out["source_weight"] = round(source_weight, 4)
    out["signal_level"] = signal_level
    out["quality_score"] = round(float(quality_score), 2)
    out["source_bucket"] = bucket
    out["score_breakdown"] = {
        "evidence_points": evidence_points,
        "source_points": round(source_points, 4),
        "recency_points": recency_points,
        "completeness_points": completeness_points,
        "penalty_points": penalties,
        "signal_bonus": signal_bonus,
        "evidence_reason": evidence_reason or "bucket_based",
        "signal_reason": signal_reason,
        "source_meta": source_meta_brief,
    }
    return out


def evidence_rank(grade: str) -> int:
    return EVIDENCE_ORDER.get(str(grade or "D").upper(), 1)


def diversity_select(items: list[dict[str, Any]], top_n: int, cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    quotas = cfg.get("quotas", {}) if isinstance(cfg.get("quotas"), dict) else {}
    out: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    counts: dict[str, int] = {}

    ranked = sorted(items, key=lambda x: (float(x.get("quality_score", 0) or 0), evidence_rank(str(x.get("evidence_grade", "D"))), float(x.get("source_weight", 0) or 0)), reverse=True)

    def _bucket(it: dict[str, Any]) -> str:
        b = str(it.get("source_bucket", "media"))
        if b == "preprint":
            return "journal_preprint"
        if b == "journal":
            return "journal_preprint"
        return b

    def _max_for(bucket: str) -> int | None:
        q = quotas.get(bucket, {}) if isinstance(quotas.get(bucket), dict) else {}
        if "max" in q:
            return int(q.get("max"))
        return None

    # Hard filter: aggregator must have original_source_url.
    filtered: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for it in ranked:
        if str(it.get("source_bucket", "")) == "aggregator" and not str(it.get("original_source_url", "")).strip():
            dropped.append({"item_id": it.get("item_id"), "title": it.get("title"), "reason": "aggregator_without_original_source_url"})
            continue
        filtered.append(it)

    # Step 1: satisfy minimum quotas.
    for b, q in quotas.items():
        if not isinstance(q, dict) or "min" not in q:
            continue
        need = int(q.get("min", 0))
        if need <= 0:
            continue
        for it in filtered:
            if len(out) >= top_n:
                break
            iid = str(it.get("item_id", ""))
            if iid in selected_ids:
                continue
            if _bucket(it) != b:
                continue
            out.append(it)
            selected_ids.add(iid)
            counts[b] = counts.get(b, 0) + 1
            if counts.get(b, 0) >= need:
                break

    # Step 2: fill remaining by global ranking with max constraints.
    for it in filtered:
        if len(out) >= top_n:
            break
        iid = str(it.get("item_id", ""))
        if iid in selected_ids:
            continue
        b = _bucket(it)
        mx = _max_for(b)
        if mx is not None and counts.get(b, 0) >= mx:
            dropped.append({"item_id": it.get("item_id"), "title": it.get("title"), "reason": f"quota_exceeded:{b}"})
            continue
        out.append(it)
        selected_ids.add(iid)
        counts[b] = counts.get(b, 0) + 1

    summary = {
        "selected_by_bucket": dict(sorted(counts.items())),
        "dropped": dropped,
        "quota_targets": quotas,
        "selected_count": len(out),
    }
    return out, summary

