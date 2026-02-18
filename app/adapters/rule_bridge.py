from __future__ import annotations

import os
import sys
from copy import deepcopy
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.rules.decision_boundary import enforce_decision_boundary
from app.rules.engine import RuleEngine
from app.rules.errors import RuleEngineError


def _warn(msg: str) -> None:
    print(f"[RULES_WARN] {msg}", file=sys.stderr)


def should_use_enhanced(env: dict[str, str] | None = None) -> bool:
    env = env or os.environ
    return str(env.get("ENHANCED_RULES_PROFILE", "")).strip().lower() == "enhanced"


def requested_profile(env: dict[str, str] | None = None) -> str:
    return "enhanced" if should_use_enhanced(env) else "legacy"


def _safe_get(d: dict[str, Any], path: list[str], default: Any) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def _adapt_content(decision: dict[str, Any]) -> dict[str, Any]:
    content, _ = enforce_decision_boundary(decision)
    allow_sources = content.get("allow_sources", []) if isinstance(content, dict) else []
    sources: list[tuple[str, str, str, str]] = []
    for it in allow_sources:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name", "")).strip()
        url = str(it.get("url", "")).strip()
        region = str(it.get("region", "北美")).strip() or "北美"
        group = str(it.get("group", "media")).strip()
        kind = "regulatory" if "regulatory" in group else "media"
        if name and url:
            sources.append((name, url, region, kind))

    dedupe = _safe_get(content, ["dedupe_window"], {})
    item_limit = _safe_get(content, ["item_limit"], {})
    region_filter = _safe_get(content, ["region_filter"], {})
    keyword_sets = _safe_get(content, ["keyword_sets"], {})
    categories = _safe_get(content, ["categories_map"], {})

    include_kw = keyword_sets.get("include_keywords", [])
    if isinstance(include_kw, dict):
        flat: list[str] = []
        for v in include_kw.values():
            if isinstance(v, list):
                flat.extend([str(x) for x in v])
        include_kw = flat

    return {
        "sources": sources,
        "min_items": int(item_limit.get("min", 8)),
        "max_items": int(item_limit.get("max", 15)),
        "topup_if_24h_lt": int(item_limit.get("topup_if_24h_lt", 10)),
        "apac_min_share": float(region_filter.get("apac_min_share", 0.40)),
        "daily_max_repeat_rate": float(dedupe.get("daily_max_repeat_rate", 0.25)),
        "recent_7d_max_repeat_rate": float(dedupe.get("recent_7d_max_repeat_rate", 0.40)),
        "title_similarity_threshold": float(dedupe.get("title_similarity_threshold", 0.78)),
        "include_keywords": [str(x) for x in include_kw if str(x).strip()],
        "exclude_keywords": [str(x) for x in keyword_sets.get("exclude_keywords", []) if str(x).strip()],
        "lane_mapping": deepcopy(categories.get("lane_mapping", {})),
        "platform_mapping": deepcopy(categories.get("platform_mapping", {})),
        "platform_url_hints": deepcopy(content.get("platform_url_hints", {})),
        "event_mapping": deepcopy(categories.get("event_mapping", {})),
        "source_priority": deepcopy(content.get("source_priority", {})),
        "dedupe_cluster": deepcopy(content.get("dedupe_cluster", {})),
        "content_sources": deepcopy(content.get("content_sources", {})),
    }


def _adapt_email(decision: dict[str, Any], date_str: str) -> dict[str, Any]:
    _, email = enforce_decision_boundary(decision)
    template = str(email.get("subject_template", "全球IVD晨报 - {{date}}"))
    subject = template.replace("{{date}}", date_str)
    schedule = email.get("schedule", {}) if isinstance(email, dict) else {}
    return {
        "subject_template": template,
        "subject": subject,
        "sections": email.get("sections", ["A", "B", "C", "D", "E", "F", "G"]),
        "recipients": email.get("recipients", []),
        "schedule": {
            "timezone": str(schedule.get("timezone", "Asia/Shanghai")),
            "hour": int(schedule.get("hour", 8)),
            "minute": int(schedule.get("minute", 30)),
        },
        "thresholds": deepcopy(email.get("thresholds", {})),
    }


def load_runtime_rules(
    date_str: str | None = None,
    env: dict[str, str] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    env = env or os.environ
    profile_req = requested_profile(env)
    engine = RuleEngine()

    if not date_str:
        tz = ZoneInfo("Asia/Shanghai")
        date_str = datetime.now(tz).strftime("%Y-%m-%d")

    build_kwargs: dict[str, Any] = {}
    if run_id:
        build_kwargs["run_id"] = run_id

    try:
        decision = engine.build_decision(profile=profile_req, **build_kwargs)
        active_profile = profile_req
    except (RuleEngineError, Exception) as e:
        _warn(f"profile={profile_req} load failed, fallback to legacy. error={e}")
        try:
            decision = engine.build_decision(profile="legacy", **build_kwargs)
            active_profile = "legacy"
        except (RuleEngineError, Exception) as e2:
            _warn(f"legacy load failed, skip rules sidecar. error={e2}")
            return {
                "enabled": False,
                "requested_profile": profile_req,
                "active_profile": "legacy",
                "run_id": run_id or "",
                "rules_version": {},
                "content": {},
                "email": {},
            }

    try:
        content_cfg = _adapt_content(decision)
        email_cfg = _adapt_email(decision, date_str=date_str)
    except Exception as e:
        _warn(f"profile={active_profile} boundary/adapt failed: {e}")
        if active_profile != "legacy":
            try:
                decision = engine.build_decision(profile="legacy")
                content_cfg = _adapt_content(decision)
                email_cfg = _adapt_email(decision, date_str=date_str)
                active_profile = "legacy"
            except Exception as e2:
                _warn(f"legacy boundary/adapt failed, skip rules sidecar. error={e2}")
                return {
                    "enabled": False,
                    "requested_profile": profile_req,
                    "active_profile": "legacy",
                    "run_id": run_id or "",
                    "rules_version": {},
                    "content": {},
                    "email": {},
                }
        else:
            return {
                "enabled": False,
                "requested_profile": profile_req,
                "active_profile": "legacy",
                "run_id": run_id or "",
                "rules_version": {},
                "content": {},
                "email": {},
            }

    return {
        "enabled": True,
        "requested_profile": profile_req,
        "active_profile": active_profile,
        "run_id": str(decision.get("run_id", run_id or "")),
        "rules_version": decision.get("rules_version", {}),
        "content": content_cfg,
        # Pass-through: used for QC metrics / explainability in offline generators.
        "qc": decision.get("qc_decision", {}) if isinstance(decision.get("qc_decision"), dict) else {},
        "email": email_cfg,
    }
