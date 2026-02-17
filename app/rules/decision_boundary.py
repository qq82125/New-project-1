from __future__ import annotations

from copy import deepcopy
from typing import Any

CONTENT_ALLOWED_KEYS = {
    "allow_sources",
    "deny_sources",
    "keyword_sets",
    "categories_map",
    "dedupe_window",
    "item_limit",
    "region_filter",
    "confidence",
    "summary_materials",
}

EMAIL_ALLOWED_KEYS = {
    "subject_template",
    "sections",
    "recipients",
    "schedule",
    "thresholds",
    "retry",
    "dedupe_window_hours",
    "charts",
    "delivery_strategy",
    "content_trim",
}


def _as_dict(v: Any) -> dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _sanitize(d: dict[str, Any], allowed: set[str]) -> dict[str, Any]:
    return {k: deepcopy(v) for k, v in d.items() if k in allowed}


def enforce_decision_boundary(
    decision: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    content_raw = _as_dict(decision.get("content_decision"))
    email_raw = _as_dict(decision.get("email_decision"))

    content_cross = sorted([k for k in content_raw.keys() if k in EMAIL_ALLOWED_KEYS])
    email_cross = sorted([k for k in email_raw.keys() if k in CONTENT_ALLOWED_KEYS])

    if content_cross or email_cross:
        raise ValueError(
            "RULES_BOUNDARY_VIOLATION: "
            f"content_cross={content_cross} email_cross={email_cross}"
        )

    content_clean = _sanitize(content_raw, CONTENT_ALLOWED_KEYS)
    email_clean = _sanitize(email_raw, EMAIL_ALLOWED_KEYS)

    # Contract-level guardrails: each side must not carry cross-domain keys.
    assert not any(k in EMAIL_ALLOWED_KEYS for k in content_clean.keys())
    assert not any(k in CONTENT_ALLOWED_KEYS for k in email_clean.keys())

    return content_clean, email_clean
