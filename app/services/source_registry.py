from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import feedparser
import yaml

from app.services.rules_store import RulesStore


class SourceRegistryError(RuntimeError):
    pass


def _is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return bool(p.scheme in ("http", "https") and p.netloc)
    except Exception:
        return False


def _load_yaml(path: Path) -> dict[str, Any]:
    obj = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SourceRegistryError(f"invalid yaml object: {path}")
    return obj


def _normalize_source(raw: dict[str, Any], file_path: Path) -> dict[str, Any]:
    src = dict(raw)
    src["enabled"] = bool(src.get("enabled", True))
    src["source_file"] = str(file_path)
    src["tags"] = src.get("tags", []) if isinstance(src.get("tags", []), list) else []
    src["priority"] = int(src.get("priority", 0))
    return src


def _resolve_rules_root(project_root: Path, rules_root: Path | None = None) -> Path:
    if rules_root is not None:
        return rules_root
    env_root = os.environ.get("RULES_WORKSPACE_DIR", "").strip()
    if env_root:
        return Path(env_root)
    return project_root / "rules"


def load_sources_registry(project_root: Path, rules_root: Path | None = None) -> list[dict[str, Any]]:
    if rules_root is None and not os.environ.get("RULES_WORKSPACE_DIR", "").strip():
        store = RulesStore(project_root)
        db_sources = store.list_sources()
        if db_sources:
            out: list[dict[str, Any]] = []
            for s in db_sources:
                ss = dict(s)
                ss["source_file"] = "db://sources"
                out.append(ss)
            return out

    root = _resolve_rules_root(project_root, rules_root) / "sources"
    if not root.exists():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(root.glob("*.y*ml")):
        doc = _load_yaml(p)
        sources = doc.get("sources", [])
        if not isinstance(sources, list):
            raise SourceRegistryError(f"{p}: sources must be array")
        for s in sources:
            if isinstance(s, dict):
                out.append(_normalize_source(s, p))
    return out


def validate_sources_registry(project_root: Path, rules_root: Path | None = None) -> dict[str, Any]:
    resolved_root = _resolve_rules_root(project_root, rules_root)
    root = resolved_root / "sources"
    schema_path = resolved_root / "schemas" / "sources.schema.json"
    if not schema_path.exists():
        raise SourceRegistryError(f"missing schema: {schema_path}")

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    # Lightweight validation aligned with schema constraints.
    errors: list[str] = []
    ids: dict[str, str] = {}
    connectors = {"rss", "web", "api"}

    for p in sorted(root.glob("*.y*ml")):
        doc = _load_yaml(p)
        if "version" not in doc:
            errors.append(f"{p}: missing version")
        sources = doc.get("sources", [])
        if not isinstance(sources, list):
            errors.append(f"{p}: sources must be array")
            continue
        for idx, s in enumerate(sources):
            if not isinstance(s, dict):
                errors.append(f"{p}[{idx}]: source must be object")
                continue
            sid = str(s.get("id", "")).strip()
            if not sid:
                errors.append(f"{p}[{idx}]: id required")
                continue
            if sid in ids:
                errors.append(f"duplicate id: {sid} in {p} and {ids[sid]}")
            ids[sid] = str(p)

            connector = str(s.get("connector", "")).strip()
            if connector not in connectors:
                errors.append(f"{p}[{idx}] {sid}: invalid connector={connector}")

            if connector in ("rss", "web"):
                url = str(s.get("url", "")).strip()
                if not _is_valid_url(url):
                    errors.append(f"{p}[{idx}] {sid}: invalid url={url}")

            pr = s.get("priority", 0)
            try:
                pr_i = int(pr)
            except Exception:
                errors.append(f"{p}[{idx}] {sid}: priority must be int")
                pr_i = 0
            if pr_i < 0 or pr_i > 1000:
                errors.append(f"{p}[{idx}] {sid}: priority out of range 0..1000")

            tt = str(s.get("trust_tier", ""))
            if tt not in ("A", "B", "C"):
                errors.append(f"{p}[{idx}] {sid}: trust_tier must be A|B|C")

    if errors:
        raise SourceRegistryError("; ".join(errors[:20]))

    return {
        "ok": True,
        "schema": str(schema_path),
        "source_count": len(ids),
        "files": [str(p) for p in sorted(root.glob("*.y*ml"))],
    }


def select_sources(
    sources: list[dict[str, Any]],
    selector: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    selector = selector or {}
    # Trust tier filter: allow only sources with tier >= min_trust_tier (A highest).
    trust_rank = {"A": 3, "B": 2, "C": 1}
    min_tt = str(selector.get("min_trust_tier", "C")).strip().upper() or "C"
    min_rank = trust_rank.get(min_tt, 1)
    include_tags = set(str(x) for x in selector.get("include_tags", []))
    exclude_tags = set(str(x) for x in selector.get("exclude_tags", []))
    include_ids = set(str(x) for x in selector.get("include_source_ids", []))
    exclude_ids = set(str(x) for x in selector.get("exclude_source_ids", []))
    enabled_only = bool(selector.get("default_enabled_only", True))

    out = []
    for s in sources:
        tt = str(s.get("trust_tier", "C")).strip().upper() or "C"
        if trust_rank.get(tt, 0) < min_rank:
            continue
        sid = str(s.get("id", ""))
        if enabled_only and not bool(s.get("enabled", True)):
            continue
        if include_ids and sid not in include_ids:
            continue
        if sid in exclude_ids:
            continue

        tags = set(str(x) for x in s.get("tags", []))
        if include_tags and not (include_tags & tags):
            continue
        if exclude_tags and (exclude_tags & tags):
            continue
        out.append(s)
    return out


def _read_profile_selector(
    project_root: Path,
    profile: str,
    rules_root: Path | None = None,
) -> dict[str, Any]:
    path = _resolve_rules_root(project_root, rules_root) / "content_rules" / f"{profile}.yaml"
    if not path.exists():
        return {}
    doc = _load_yaml(path)
    defaults = doc.get("defaults", {}) if isinstance(doc.get("defaults"), dict) else {}
    sel = defaults.get("content_sources", {})
    return sel if isinstance(sel, dict) else {}


def list_sources_for_profile(
    project_root: Path,
    profile: str,
    rules_root: Path | None = None,
) -> list[dict[str, Any]]:
    sources = load_sources_registry(project_root, rules_root=rules_root)
    selector = _read_profile_selector(project_root, profile, rules_root=rules_root)
    return select_sources(sources, selector)


def diff_sources_for_profiles(
    project_root: Path,
    from_profile: str,
    to_profile: str,
    rules_root: Path | None = None,
) -> dict[str, Any]:
    a = {s.get("id") for s in list_sources_for_profile(project_root, from_profile, rules_root=rules_root)}
    b = {s.get("id") for s in list_sources_for_profile(project_root, to_profile, rules_root=rules_root)}
    return {
        "from": from_profile,
        "to": to_profile,
        "added": sorted(list(b - a)),
        "removed": sorted(list(a - b)),
        "common": sorted(list(a & b)),
    }


def test_source(source: dict[str, Any], limit: int = 3) -> dict[str, Any]:
    connector = str(source.get("connector", ""))
    url = str(source.get("url", ""))
    sid = str(source.get("id", ""))
    out = {
        "source_id": sid,
        "connector": connector,
        "url": url,
        "ok": False,
        "sample": [],
        "http_status": None,
        "error": None,
    }

    try:
        if connector == "rss":
            req = Request(url, headers={"User-Agent": "Mozilla/5.0 CodexIVD/1.0"})
            with urlopen(req, timeout=20) as r:
                data = r.read()
                out["http_status"] = int(getattr(r, "status", 200))
            feed = feedparser.parse(data)
            sample = []
            for e in feed.entries[: max(1, limit)]:
                sample.append({"title": getattr(e, "title", ""), "link": getattr(e, "link", "")})
            out["sample"] = sample
            out["ok"] = True
            return out

        if connector == "web":
            req = Request(url, headers={"User-Agent": "Mozilla/5.0 CodexIVD/1.0"})
            with urlopen(req, timeout=20) as r:
                html = r.read().decode("utf-8", errors="ignore")
                out["http_status"] = int(getattr(r, "status", 200))
            title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
            title = title_match.group(1).strip() if title_match else ""
            out["sample"] = [{"title": title, "link": url}]
            out["ok"] = True
            return out

        if connector == "api":
            out["ok"] = True
            out["sample"] = []
            return out

        out["error"] = f"unsupported connector={connector}"
        return out
    except Exception as e:
        out["error"] = str(e)
        return out


def retire_source(
    project_root: Path,
    source_id: str,
    reason: str = "retired via CLI",
    rules_root: Path | None = None,
) -> dict[str, Any]:
    root = _resolve_rules_root(project_root, rules_root) / "sources"
    now = datetime.now(timezone.utc).isoformat()

    for p in sorted(root.glob("*.y*ml")):
        doc = _load_yaml(p)
        sources = doc.get("sources", [])
        if not isinstance(sources, list):
            continue
        changed = False
        for s in sources:
            if not isinstance(s, dict):
                continue
            if str(s.get("id", "")) != source_id:
                continue
            s["enabled"] = False
            s["retired_reason"] = reason
            s["retired_at"] = now
            changed = True
        if changed:
            p.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")
            return {
                "ok": True,
                "source_id": source_id,
                "file": str(p),
                "reason": reason,
                "retired_at": now,
            }

    raise SourceRegistryError(f"source_id not found: {source_id}")
