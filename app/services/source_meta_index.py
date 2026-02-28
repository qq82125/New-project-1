from __future__ import annotations

from pathlib import Path
from typing import Any

from app.services.source_registry import load_sources_registry


_GROUP_PRIORITY = ("procurement", "regulatory", "company", "evidence", "media")


def _canonical_group(value: str) -> str:
    g = str(value or "").strip().lower()
    if not g:
        return ""
    if "procurement" in g:
        return "procurement"
    if "regulatory" in g:
        return "regulatory"
    if "company" in g:
        return "company"
    if "evidence" in g:
        return "evidence"
    if "media" in g:
        return "media"
    return g


def _infer_group(row: dict[str, Any]) -> str:
    explicit = _canonical_group(str(row.get("source_group", "")))
    if explicit:
        return explicit
    tags = {str(x).strip().lower() for x in row.get("tags", []) if str(x).strip()}
    for g in _GROUP_PRIORITY:
        if g in tags:
            return g
    return "unknown"


def _infer_region(row: dict[str, Any]) -> str:
    region = str(row.get("region", "")).strip()
    if region:
        return region
    tags = {str(x).strip().lower() for x in row.get("tags", []) if str(x).strip()}
    if "cn" in tags or "china" in tags:
        return "中国"
    if "us" in tags or "na" in tags or "north_america" in tags:
        return "北美"
    if "eu" in tags or "europe" in tags:
        return "欧洲"
    if "global" in tags:
        return "Global"
    return "Global"


def build_source_meta_index(project_root: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for s in load_sources_registry(project_root):
        sid = str(s.get("id", "")).strip()
        if not sid:
            continue
        group = _infer_group(s)
        region = _infer_region(s)
        trust_tier = str(s.get("trust_tier", "")).strip().upper() or "C"
        try:
            priority = int(s.get("priority", 10) or 10)
        except Exception:
            priority = 10
        out[sid] = {
            "group": group,
            "region": region,
            "trust_tier": trust_tier,
            "priority": priority,
            "tags": s.get("tags", []) if isinstance(s.get("tags", []), list) else [],
        }
    return out
