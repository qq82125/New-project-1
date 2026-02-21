from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse


TRUST_RANK = {"A": 3, "B": 2, "C": 1}


def _as_list(v: Any) -> list[str]:
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for x in v:
        s = str(x or "").strip()
        if s:
            out.append(s)
    return out


def _tier(v: Any, default: str = "C") -> str:
    t = str(v or "").strip().upper()
    if t in TRUST_RANK:
        return t
    return default


def source_policy_default(profile: str = "legacy") -> dict[str, Any]:
    p = str(profile or "legacy").strip().lower()
    return {
        "enabled": True,
        "min_trust_tier": "B" if p == "enhanced" else "C",
        "exclude_domains": [],
        "exclude_source_ids": [],
        "drop_if_url_matches": [],
    }


def normalize_source_policy(raw: dict[str, Any] | None, *, profile: str = "legacy") -> dict[str, Any]:
    base = source_policy_default(profile)
    cfg = raw if isinstance(raw, dict) else {}

    enabled = bool(cfg.get("enabled", base["enabled"]))
    min_tt_raw = cfg.get("min_trust_tier", base["min_trust_tier"])
    min_tt = base["min_trust_tier"]
    if isinstance(min_tt_raw, dict):
        min_tt = _tier(min_tt_raw.get(str(profile).strip().lower()), default=min_tt)
    elif isinstance(min_tt_raw, str):
        min_tt = _tier(min_tt_raw, default=min_tt)

    domains: list[str] = []
    for x in _as_list(cfg.get("exclude_domains", [])):
        h = host_from_url(x) if "://" in x else x.strip().lower()
        h = h.lstrip(".")
        if h:
            domains.append(h)

    return {
        "enabled": enabled,
        "min_trust_tier": min_tt,
        "exclude_domains": domains,
        "exclude_source_ids": _as_list(cfg.get("exclude_source_ids", [])),
        "drop_if_url_matches": _as_list(cfg.get("drop_if_url_matches", [])),
    }


def host_from_url(url: str) -> str:
    try:
        return str(urlparse(str(url or "")).hostname or "").strip().lower()
    except Exception:
        return ""


def is_domain_excluded(url: str, exclude_domains: list[str]) -> bool:
    host = host_from_url(url)
    if not host:
        return False
    for d in exclude_domains:
        dd = str(d or "").strip().lower().lstrip(".")
        if not dd:
            continue
        if host == dd or host.endswith("." + dd):
            return True
    return False


def source_passes_min_trust_tier(trust_tier: str, policy: dict[str, Any]) -> bool:
    min_tt = _tier(policy.get("min_trust_tier", "C"), default="C")
    tt = _tier(trust_tier, default="C")
    return TRUST_RANK.get(tt, 0) >= TRUST_RANK.get(min_tt, 1)


def exclusion_reason(source_id: str, url: str, policy: dict[str, Any]) -> str:
    if not bool(policy.get("enabled", True)):
        return ""
    sid = str(source_id or "").strip()
    if sid and sid in set(_as_list(policy.get("exclude_source_ids", []))):
        return "excluded_source"
    if is_domain_excluded(url, _as_list(policy.get("exclude_domains", []))):
        return "excluded_domain"
    for ptn in _as_list(policy.get("drop_if_url_matches", [])):
        try:
            if re.search(ptn, str(url or ""), flags=re.IGNORECASE):
                return "excluded_url_pattern"
        except Exception:
            continue
    return ""


def filter_entries_for_collect(
    entries: list[dict[str, Any]],
    *,
    source_id: str,
    policy: dict[str, Any],
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    if not bool(policy.get("enabled", True)):
        return list(entries), 0, {}
    kept: list[dict[str, Any]] = []
    dropped = 0
    reasons: dict[str, int] = {}
    for it in entries:
        url = str((it or {}).get("url", (it or {}).get("link", ""))).strip()
        rs = exclusion_reason(source_id, url, policy)
        if rs:
            dropped += 1
            reasons[rs] = reasons.get(rs, 0) + 1
            continue
        kept.append(it)
    return kept, dropped, reasons


def filter_rows_for_digest(
    rows: list[dict[str, Any]],
    *,
    policy: dict[str, Any],
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    if not bool(policy.get("enabled", True)):
        return list(rows), 0, {}
    kept: list[dict[str, Any]] = []
    dropped = 0
    reasons: dict[str, int] = {}
    for r in rows:
        sid = str((r or {}).get("source_id", "")).strip()
        url = str((r or {}).get("url", "")).strip()
        tt_raw = str((r or {}).get("trust_tier", "")).strip().upper()
        if tt_raw:
            if not source_passes_min_trust_tier(tt_raw, policy):
                dropped += 1
                reasons["below_min_trust_tier"] = reasons.get("below_min_trust_tier", 0) + 1
                continue
        rs = exclusion_reason(sid, url, policy)
        if rs:
            dropped += 1
            reasons[rs] = reasons.get(rs, 0) + 1
            continue
        kept.append(r)
    return kept, dropped, reasons
