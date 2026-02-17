from __future__ import annotations

from typing import Any

from app.rules.models import RuleSelection


def to_content_runtime(rule: RuleSelection) -> dict[str, Any]:
    collection = rule.data.get("collection", {})
    coverage = rule.data.get("coverage", {})
    quality = rule.data.get("quality_gates", {})
    sources = rule.data.get("sources", {})
    output = rule.data.get("output", {})
    return {
        "profile": rule.profile,
        "rules_version": rule.version,
        "date_tz": collection.get("date_tz", "Asia/Shanghai"),
        "windows": {
            "primary_window_hours": collection.get("primary_window_hours", 24),
            "fallback_window_days": collection.get("fallback_window_days", 7),
        },
        "items": {
            "min_items": collection.get("min_items", 8),
            "max_items": collection.get("max_items", 15),
            "topup_if_24h_lt": collection.get("topup_if_24h_lt", 10),
        },
        "coverage": coverage,
        "quality_gates": quality,
        "sources": sources,
        "output": output,
    }

