from __future__ import annotations

import re
from typing import Any


TRACK_VALUES = {"core", "frontier"}  # routing tracks
ITEM_TRACK_VALUES = {"core", "frontier", "drop"}
RELEVANCE_LEVEL_VALUES = {0, 1, 2, 3, 4}

DEFAULT_RELEVANCE_THRESHOLDS = {
    "core_min_level_for_A": 3,
    "frontier_min_level_for_F": 2,
}

DEFAULT_FRONTIER_QUOTA = {
    "max_items_per_day": 3,
}

DEFAULT_ANCHORS_PACK = {
    "core": [
        "ivd",
        "diagnostic",
        "diagnostics",
        "assay",
        "test",
        "testing",
        "pcr",
        "qpcr",
        "rt-pcr",
        "ddpcr",
        "dpcr",
        "ngs",
        "sequencing",
        "poct",
        "immunoassay",
        "chemiluminescence",
        "elisa",
        "ihc",
        "clia",
        "laboratory",
        "pathology",
        "reagent",
        "analyzer",
        "companion diagnostic",
        "cdx",
        "ldt",
    ],
    "frontier": [
        "single-cell",
        "single cell",
        "multi-omics",
        "multiomics",
        "proteomics",
        "metabolomics",
        "spatial",
        "digital pathology",
        "foundation model",
        "ai pathology",
        "microfluidic",
        "lab-on-a-chip",
        "single molecule",
        "simoa",
        "digital immunoassay",
        "autonomous pathology",
        "sample-to-answer",
        "lab automation",
    ],
}

DEFAULT_NEGATIVES_PACK = [
    "earnings",
    "quarterly revenue",
    "revenue",
    "sales",
    "layoff",
    "restructuring",
    "phase 3",
    "phase iii",
    "drug trial",
    "therapy",
    "vaccine",
]

DEFAULT_NEGATIVE_STRONG = [
    "earnings",
    "lawsuit",
    "securities",
    "investor",
    "acquisition",
    "merger",
    "share price",
    "decline",
    "sales",
]

NAV_URL_MARKERS = [
    "/about",
    "/portal",
    "cookie",
    "privacy",
    "newsletter",
    "mission",
    "purpose",
]

NAV_TITLE_EXACT = {
    "about us",
    "learn more",
    "portal",
    "thought leadership",
}

INVESTMENT_PR_MEDIA_SOURCES = ["pr newswire", "globenewswire"]
INVESTMENT_PR_MEDIA_KEYWORDS = [
    "diagnostic",
    "diagnostics",
    "ivd",
    "in vitro",
    "assay",
    "molecular",
    "pcr",
    "biomarker",
    "screening",
    "clinical trial",
    "fda",
    "ce mark",
]
INVESTMENT_ABBOTT_KEEP = [
    "diagnostic",
    "assay",
    "test",
    "molecular",
    "biomarker",
    "launch",
    "approval",
    "regulatory",
    "acquisition",
]
INVESTMENT_ABBOTT_DROP = [
    "recap",
    "blog",
    "interview",
    "sustainability",
    "awareness",
    "story",
]
INVESTMENT_PREPRINT_SOURCES = ["biorxiv", "nature", "medrxiv"]
INVESTMENT_PREPRINT_KEYWORDS = [
    "diagnostic",
    "assay",
    "in vitro",
    "biomarker",
    "pcr",
    "ngs",
    "sequencing",
    "clinical validation",
]


def _has_term(text_lc: str, term_lc: str) -> bool:
    if not term_lc:
        return False
    if " " in term_lc or "-" in term_lc or "/" in term_lc:
        return term_lc in text_lc
    if term_lc.isascii() and term_lc.isalpha() and len(term_lc) <= 5:
        return re.search(rf"\b{re.escape(term_lc)}\b", text_lc) is not None
    return term_lc in text_lc


def _to_kw_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x).strip().lower() for x in v if str(x).strip()]
    return []


def is_navigation_page(url: str, title: str) -> bool:
    u = str(url or "").strip().lower()
    t = str(title or "").strip().lower()
    if any(m in u for m in NAV_URL_MARKERS):
        return True
    if t in NAV_TITLE_EXACT:
        return True
    return False


def compute_relevance(
    text: str,
    source_meta: dict[str, Any] | None = None,
    rules_runtime: dict[str, Any] | None = None,
) -> tuple[str, int, dict[str, Any]]:
    """
    Unified relevance engine (track + level + explain).
    """
    source_meta = source_meta or {}
    rules_runtime = rules_runtime or {}
    text_lc = str(text or "").lower()
    title_lc = str(source_meta.get("title", "") or "").strip().lower()
    url_lc = str(source_meta.get("url", "") or "").strip().lower()

    anchors_cfg = rules_runtime.get("anchors_pack", {})
    if not isinstance(anchors_cfg, dict) or not anchors_cfg:
        anchors_cfg = DEFAULT_ANCHORS_PACK
    negatives_cfg = rules_runtime.get("negatives_pack", [])
    if not isinstance(negatives_cfg, list) or not negatives_cfg:
        negatives_cfg = DEFAULT_NEGATIVES_PACK
    negative_strong_cfg = rules_runtime.get("negatives_strong_pack", [])
    if not isinstance(negative_strong_cfg, list) or not negative_strong_cfg:
        negative_strong_cfg = DEFAULT_NEGATIVE_STRONG

    core_anchors = _to_kw_list(anchors_cfg.get("core"))
    frontier_anchors = _to_kw_list(anchors_cfg.get("frontier"))
    negatives = _to_kw_list(negatives_cfg)
    negatives_strong = _to_kw_list(negative_strong_cfg)

    core_hits = [k for k in core_anchors if _has_term(text_lc, k)]
    frontier_hits = [k for k in frontier_anchors if _has_term(text_lc, k)]
    negative_hits = [k for k in negatives if _has_term(text_lc, k)]
    negative_strong_hits = [k for k in negatives_strong if _has_term(text_lc, k)]

    source_group = str(source_meta.get("source_group", "")).lower()
    event_type = str(source_meta.get("event_type", ""))
    source_name_lc = str(
        source_meta.get("source", "")
        or source_meta.get("source_name", "")
        or source_meta.get("source_id", "")
        or ""
    ).strip().lower()
    investment_scope_enabled = bool(rules_runtime.get("investment_scope_enabled", False))
    has_reg_signal = event_type == "监管审批与指南" or "regulatory" in source_group
    has_journal_signal = "journal" in source_group or "preprint" in source_group

    # hard filter: static/navigation pages.
    if is_navigation_page(url_lc, title_lc):
        explain = {
            "anchors_hit": sorted(set(core_hits + frontier_hits)),
            "negatives_hit": sorted(set(negative_hits + negative_strong_hits)),
            "rule_hits": ["navigation_filter"],
            "rules_applied": ["navigation_filter"],
            "final_reason": "navigation_or_static_page",
            "raw_score": 0,
            "source_group": source_group,
            "event_type": event_type,
        }
        return "drop", 0, explain

    # scoring v1
    raw_score = (len(core_hits) * 2) + (len(frontier_hits) * 2)
    if has_reg_signal:
        raw_score += 2
    if has_journal_signal:
        raw_score += 1
    raw_score -= len(negative_hits)

    strong_diagnostic_anchor_hit = bool(core_hits)
    frontier_anchor_hit = bool(frontier_hits)
    any_anchor_hit = strong_diagnostic_anchor_hit or frontier_anchor_hit

    # hard filter: strong business/legal noise without diagnostic anchors.
    if negative_strong_hits and not any_anchor_hit:
        explain = {
            "anchors_hit": sorted(set(core_hits + frontier_hits)),
            "negatives_hit": sorted(set(negative_hits + negative_strong_hits)),
            "rule_hits": ["negative_strong_gate"],
            "rules_applied": ["negative_strong_gate"],
            "final_reason": "strong_negative_without_diagnostic_anchor",
            "raw_score": raw_score,
            "source_group": source_group,
            "event_type": event_type,
        }
        return "drop", 0, explain

    if investment_scope_enabled:
        # A) PR Newswire / GlobeNewswire gate for media sources.
        if source_group == "media" and any(s in source_name_lc for s in INVESTMENT_PR_MEDIA_SOURCES):
            if not any(_has_term(text_lc, k) for k in INVESTMENT_PR_MEDIA_KEYWORDS):
                explain = {
                    "anchors_hit": sorted(set(core_hits + frontier_hits)),
                    "negatives_hit": sorted(set(negative_hits + negative_strong_hits)),
                    "rule_hits": ["investment_scope_filter_pr_media"],
                    "rules_applied": ["investment_scope_filter_pr_media"],
                    "final_reason": "investment_scope_filter",
                    "raw_score": raw_score,
                    "source_group": source_group,
                    "event_type": event_type,
                }
                return "drop", 0, explain

        # B) Abbott newsroom scope hardening.
        if ("abbott.com" in url_lc) or ("abbottnewsroom.com" in url_lc):
            if any(_has_term(title_lc, k) for k in INVESTMENT_ABBOTT_DROP):
                explain = {
                    "anchors_hit": sorted(set(core_hits + frontier_hits)),
                    "negatives_hit": sorted(set(negative_hits + negative_strong_hits)),
                    "rule_hits": ["investment_scope_filter_abbott_drop"],
                    "rules_applied": ["investment_scope_filter_abbott_drop"],
                    "final_reason": "investment_scope_filter",
                    "raw_score": raw_score,
                    "source_group": source_group,
                    "event_type": event_type,
                }
                return "drop", 0, explain
            if not any(_has_term(title_lc, k) for k in INVESTMENT_ABBOTT_KEEP):
                explain = {
                    "anchors_hit": sorted(set(core_hits + frontier_hits)),
                    "negatives_hit": sorted(set(negative_hits + negative_strong_hits)),
                    "rule_hits": ["investment_scope_filter_abbott_keep"],
                    "rules_applied": ["investment_scope_filter_abbott_keep"],
                    "final_reason": "investment_scope_filter",
                    "raw_score": raw_score,
                    "source_group": source_group,
                    "event_type": event_type,
                }
                return "drop", 0, explain

        # C) bioRxiv / Nature / medRxiv must match at least two diagnostic keywords.
        if any(s in source_name_lc for s in INVESTMENT_PREPRINT_SOURCES):
            kw_hits = [k for k in INVESTMENT_PREPRINT_KEYWORDS if _has_term(text_lc, k)]
            if len(kw_hits) < 2:
                explain = {
                    "anchors_hit": sorted(set(core_hits + frontier_hits)),
                    "negatives_hit": sorted(set(negative_hits + negative_strong_hits)),
                    "rule_hits": ["investment_scope_filter_preprint"],
                    "rules_applied": ["investment_scope_filter_preprint"],
                    "final_reason": "investment_scope_filter",
                    "raw_score": raw_score,
                    "source_group": source_group,
                    "event_type": event_type,
                }
                return "drop", 0, explain

    if raw_score >= 9:
        level = 4
    elif raw_score >= 6:
        level = 3
    elif raw_score >= 3:
        level = 2
    elif raw_score >= 1:
        level = 1
    else:
        level = 0

    # hard filter: raw score non-positive can never enter core/frontier.
    if raw_score <= 0:
        explain = {
            "anchors_hit": sorted(set(core_hits + frontier_hits)),
            "negatives_hit": sorted(set(negative_hits + negative_strong_hits)),
            "rule_hits": ["raw_score_gate"],
            "rules_applied": ["raw_score_gate"],
            "final_reason": "raw_score_non_positive",
            "raw_score": raw_score,
            "source_group": source_group,
            "event_type": event_type,
        }
        return "drop", 0, explain

    # track decision with strict anchor gate.
    if strong_diagnostic_anchor_hit:
        track = "core"
        level = max(3, level)
        final_reason = "core_anchor_hit"
    elif frontier_anchor_hit:
        track = "frontier"
        level = max(2, level)
        final_reason = "frontier_anchor_hit"
    else:
        explain = {
            "anchors_hit": sorted(set(core_hits + frontier_hits)),
            "negatives_hit": sorted(set(negative_hits + negative_strong_hits)),
            "rule_hits": ["anchor_gate"],
            "rules_applied": ["anchor_gate"],
            "final_reason": "no_diagnostic_anchor",
            "raw_score": raw_score,
            "source_group": source_group,
            "event_type": event_type,
        }
        return "drop", 0, explain

    level = max(0, min(4, int(level)))
    explain = {
        "anchors_hit": sorted(set(core_hits + frontier_hits)),
        "negatives_hit": sorted(set(negative_hits + negative_strong_hits)),
        "rule_hits": [
            "compute_relevance_v1",
            "anchor_gate",
            "raw_score_gate",
        ],
        "rules_applied": [
            "compute_relevance_v1",
            "anchor_gate",
            "raw_score_gate",
        ],
        "final_reason": final_reason,
        "raw_score": raw_score,
        "source_group": source_group,
        "event_type": event_type,
    }
    return track, level, explain


def classify_track_relevance(
    title: str,
    summary: str,
    event_type: str,
    source_group: str,
    score: int,
) -> tuple[str, int, str]:
    # Legacy-compatible wrapper.
    track, level, explain = compute_relevance(
        f"{title} {summary}",
        {"source_group": source_group, "event_type": event_type},
        {},
    )
    why = f"{explain.get('final_reason', 'compute_relevance')};score={score};event_type={event_type}"
    return track, level, why


def normalize_item_contract(track: Any, relevance_level: Any) -> tuple[str, int, list[str]]:
    warnings: list[str] = []

    t = str(track or "").strip().lower()
    if t not in ITEM_TRACK_VALUES:
        warnings.append(f"invalid_track:{track!r}->drop")
        t = "drop"

    try:
        lv = int(relevance_level)
    except Exception:
        warnings.append(f"invalid_relevance:{relevance_level!r}->0")
        lv = 0
    if lv < 0 or lv > 4:
        warnings.append(f"out_of_range_relevance:{lv}->clamp")
        lv = max(0, min(4, lv))

    return t, lv, warnings


def default_track_routing_rules() -> dict[str, dict[str, Any]]:
    return {
        "A": {"track": "core", "min_relevance_level": 3},
        "F": {"track": "frontier", "min_relevance_level": 2},
        "G": {"include_track_coverage": True},
    }


def validate_track_routing_rules(raw: Any) -> tuple[dict[str, dict[str, Any]], list[str]]:
    rules = default_track_routing_rules()
    gaps: list[str] = []
    if not isinstance(raw, dict):
        gaps.append("missing_track_routing:use_default")
        return rules, gaps

    for sec in ("A", "F"):
        part = raw.get(sec)
        if not isinstance(part, dict) or not part:
            gaps.append(f"missing_routing_{sec}:use_default")
            continue
        track = str(part.get("track", "")).strip().lower()
        if track not in TRACK_VALUES:
            gaps.append(f"invalid_routing_{sec}_track:{track!r}->default")
        else:
            rules[sec]["track"] = track
        try:
            lv = int(part.get("min_relevance_level", rules[sec]["min_relevance_level"]))
        except Exception:
            lv = int(rules[sec]["min_relevance_level"])
            gaps.append(f"invalid_routing_{sec}_level:use_default")
        lv = max(0, min(4, lv))
        rules[sec]["min_relevance_level"] = lv

    g = raw.get("G")
    if not isinstance(g, dict) or not g:
        gaps.append("missing_routing_G:use_default")
    else:
        rules["G"]["include_track_coverage"] = bool(g.get("include_track_coverage", True))

    return rules, gaps
