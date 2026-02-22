from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml


def normalize_unknown(value: Any) -> str:
    s = str(value or "").strip()
    return s or "__unknown__"


def _resolve_rules_root(rules_root: Path | str | None) -> Path:
    if rules_root is None:
        return Path("rules")
    p = Path(rules_root)
    if p.name == "rules":
        return p
    return p / "rules"


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def load_region_map(rules_root: Path | str | None = None) -> dict[str, Any]:
    rr = _resolve_rules_root(rules_root)
    return _read_yaml(rr / "mappings" / "region_map.v1.yaml")


def load_lane_map(rules_root: Path | str | None = None) -> dict[str, Any]:
    rr = _resolve_rules_root(rules_root)
    return _read_yaml(rr / "mappings" / "lane_map.v1.yaml")


def classify_region(url: str, maps: dict[str, Any] | None = None) -> str:
    mm = maps or {}
    host = str(urlparse(str(url or "")).netloc or "").strip().lower()
    if not host:
        return "__unknown__"

    domain_contains = mm.get("domain_contains", {}) if isinstance(mm.get("domain_contains"), dict) else {}
    for k, v in sorted(domain_contains.items(), key=lambda kv: len(str(kv[0])), reverse=True):
        kk = str(k).strip().lower()
        vv = normalize_unknown(v)
        if kk and kk in host and vv != "__unknown__":
            return vv

    domain_suffix = mm.get("domain_suffix", {}) if isinstance(mm.get("domain_suffix"), dict) else {}
    for k, v in domain_suffix.items():
        kk = str(k).strip().lower()
        vv = normalize_unknown(v)
        if not kk or vv == "__unknown__":
            continue
        clean = kk[1:] if kk.startswith(".") else kk
        if host == clean or host.endswith("." + clean):
            return vv
    return "__unknown__"


def classify_lane(text: str, maps: dict[str, Any] | None = None) -> str:
    mm = maps or {}
    lanes = mm.get("lanes", {}) if isinstance(mm.get("lanes"), dict) else {}
    default_lane = normalize_unknown(mm.get("default_lane", "__unknown__"))
    t = str(text or "").lower()
    if not t:
        return default_lane

    for lane, cfg in lanes.items():
        if not isinstance(cfg, dict):
            continue
        any_terms = cfg.get("any", [])
        if not isinstance(any_terms, list):
            continue
        for kw in any_terms:
            k = str(kw or "").strip().lower()
            if k and k in t:
                return str(lane)
    return default_lane
