from __future__ import annotations

import re
from typing import Any


TRACK_VALUES = {"core", "frontier"}
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

    anchors_cfg = rules_runtime.get("anchors_pack", {})
    if not isinstance(anchors_cfg, dict) or not anchors_cfg:
        anchors_cfg = DEFAULT_ANCHORS_PACK
    negatives_cfg = rules_runtime.get("negatives_pack", [])
    if not isinstance(negatives_cfg, list) or not negatives_cfg:
        negatives_cfg = DEFAULT_NEGATIVES_PACK

    core_anchors = _to_kw_list(anchors_cfg.get("core"))
    frontier_anchors = _to_kw_list(anchors_cfg.get("frontier"))
    negatives = _to_kw_list(negatives_cfg)

    core_hits = [k for k in core_anchors if _has_term(text_lc, k)]
    frontier_hits = [k for k in frontier_anchors if _has_term(text_lc, k)]
    negative_hits = [k for k in negatives if _has_term(text_lc, k)]

    source_group = str(source_meta.get("source_group", "")).lower()
    event_type = str(source_meta.get("event_type", ""))
    has_reg_signal = event_type == "监管审批与指南" or "regulatory" in source_group
    has_journal_signal = "journal" in source_group or "preprint" in source_group

    # scoring v1
    raw_score = (len(core_hits) * 2) + (len(frontier_hits) * 2)
    if has_reg_signal:
        raw_score += 2
    if has_journal_signal:
        raw_score += 1
    raw_score -= len(negative_hits)

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

    # track decision
    frontier_source_bias = ("preprint" in source_group) or ("journal" in source_group) or ("research" in source_group)
    if frontier_hits and (len(frontier_hits) >= len(core_hits)):
        track = "frontier"
        final_reason = "frontier_anchor_dominant"
    elif frontier_hits and frontier_source_bias and not has_reg_signal:
        # Research-heavy sources with frontier signals should not be swallowed by core default.
        track = "frontier"
        final_reason = "frontier_source_bias"
    elif frontier_hits and len(core_hits) <= 1 and not has_reg_signal:
        # Light core overlap + any frontier anchor keeps item in frontier radar.
        track = "frontier"
        final_reason = "frontier_hit_light_core"
    elif negative_hits and not core_hits and not has_reg_signal:
        track = "frontier"
        final_reason = "negative_only_or_business_noise"
    else:
        track = "core"
        final_reason = "core_anchor_or_regulatory_signal"

    # hard drop level when pure negative without diagnostic anchor
    if negative_hits and not core_hits and not frontier_hits and not has_reg_signal:
        level = 0
        final_reason = "negative_without_diagnostic_anchor"

    level = max(0, min(4, int(level)))
    explain = {
        "anchors_hit": sorted(set(core_hits + frontier_hits)),
        "negatives_hit": sorted(set(negative_hits)),
        "rule_hits": [
            "compute_relevance_v1",
            "track_core_frontier",
            "negative_penalty",
        ],
        "rules_applied": [
            "compute_relevance_v1",
            "track_core_frontier",
            "negative_penalty",
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
    if t not in TRACK_VALUES:
        warnings.append(f"invalid_track:{track!r}->core")
        t = "core"

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
