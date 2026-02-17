from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from app.services.rules_store import RulesStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def get_console_root(project_root: Path) -> Path:
    return project_root / "rules" / "console"


def get_versions_root(project_root: Path) -> Path:
    return get_console_root(project_root) / "versions"


def get_published_pointer_path(project_root: Path) -> Path:
    return get_console_root(project_root) / "published.json"


def get_workspace_rules_root(project_root: Path) -> Path:
    override = os.environ.get("RULES_WORKSPACE_DIR", "").strip()
    if override:
        return Path(override)
    return get_runtime_rules_root(project_root)


def _load_rule_doc(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        obj = json.loads(text)
    else:
        obj = yaml.safe_load(text)
    return obj if isinstance(obj, dict) else {}


def _iter_rule_profiles(rules_root: Path, ruleset: str) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    base = rules_root / ruleset
    if not base.exists():
        return out
    for p in sorted(base.glob("*.y*ml")) + sorted(base.glob("*.json")):
        doc = _load_rule_doc(p)
        if not isinstance(doc, dict) or not doc:
            continue
        profile = str(doc.get("profile", p.stem))
        out.append((profile, doc))
    return out


def _load_sources_registry_docs(rules_root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    src_root = rules_root / "sources"
    if not src_root.exists():
        return out
    for p in sorted(src_root.glob("*.y*ml")):
        doc = _load_rule_doc(p)
        sources = doc.get("sources", []) if isinstance(doc, dict) else []
        if not isinstance(sources, list):
            continue
        for s in sources:
            if isinstance(s, dict):
                out.append(s)
    return out


def _bootstrap_rules_db(project_root: Path, rules_root: Path) -> None:
    store = RulesStore(project_root)
    for ruleset in ("email_rules", "content_rules"):
        if store.has_any_versions(ruleset):
            continue
        profiles = _iter_rule_profiles(rules_root, ruleset)
        for profile, doc in profiles:
            version = str(doc.get("version", "1.0.0"))
            store.create_version(
                ruleset,
                profile=profile,
                version=version,
                config=doc,
                created_by="system-bootstrap",
                activate=True,
            )
    sources = _load_sources_registry_docs(rules_root)
    if sources:
        store.upsert_sources(sources, replace=True)


def _copy_rules_tree(src_rules_root: Path, dst_rules_root: Path) -> None:
    if dst_rules_root.exists():
        shutil.rmtree(dst_rules_root)
    dst_rules_root.mkdir(parents=True, exist_ok=True)
    for d in ("email_rules", "content_rules", "sources", "schemas"):
        src = src_rules_root / d
        dst = dst_rules_root / d
        if src.exists() and src.is_dir():
            shutil.copytree(src, dst)


def _next_version_id(versions_root: Path) -> str:
    versions_root.mkdir(parents=True, exist_ok=True)
    nums = []
    for p in versions_root.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        if name.startswith("v") and name[1:].isdigit():
            nums.append(int(name[1:]))
    n = (max(nums) + 1) if nums else 1
    return f"v{n:04d}"


def list_versions(project_root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    root = get_versions_root(project_root)
    if not root.exists():
        return out
    for p in sorted(root.iterdir()):
        if not p.is_dir():
            continue
        meta = _read_json(p / "meta.json")
        if meta:
            out.append(meta)
    return out


def get_published_pointer(project_root: Path) -> dict[str, Any]:
    return _read_json(get_published_pointer_path(project_root))


def get_version_rules_root(project_root: Path, version: str) -> Path:
    return get_versions_root(project_root) / version / "rules"


def get_runtime_rules_root(project_root: Path) -> Path:
    ensure_bootstrap_published(project_root)
    pointer = get_published_pointer(project_root)
    active = str(pointer.get("active_version", "")).strip()
    if active:
        p = get_version_rules_root(project_root, active)
        if p.exists():
            return p
    return project_root / "rules"


def ensure_bootstrap_published(project_root: Path) -> None:
    pointer_path = get_published_pointer_path(project_root)
    if pointer_path.exists():
        pointer = _read_json(pointer_path)
        active = str(pointer.get("active_version", "")).strip()
        runtime_root = get_version_rules_root(project_root, active) if active else (project_root / "rules")
        if not runtime_root.exists():
            runtime_root = project_root / "rules"
        _bootstrap_rules_db(project_root, runtime_root)
        return

    versions_root = get_versions_root(project_root)
    version = _next_version_id(versions_root)
    version_root = versions_root / version
    version_rules_root = version_root / "rules"

    src_rules_root = project_root / "rules"
    _copy_rules_tree(src_rules_root, version_rules_root)

    meta = {
        "version": version,
        "created_at": _utc_now(),
        "created_by": "system-bootstrap",
        "note": "bootstrap from existing rules",
    }
    _write_json(version_root / "meta.json", meta)

    pointer = {
        "active_version": version,
        "previous_version": None,
        "history": [version],
        "updated_at": _utc_now(),
    }
    _write_json(pointer_path, pointer)
    _bootstrap_rules_db(project_root, version_rules_root)


def publish_rules_version(
    project_root: Path,
    staged_rules_root: Path,
    *,
    created_by: str,
    note: str = "",
) -> dict[str, Any]:
    ensure_bootstrap_published(project_root)
    versions_root = get_versions_root(project_root)
    version = _next_version_id(versions_root)
    version_root = versions_root / version
    version_rules_root = version_root / "rules"

    _copy_rules_tree(staged_rules_root, version_rules_root)

    meta = {
        "version": version,
        "created_at": _utc_now(),
        "created_by": created_by,
        "note": note,
    }
    _write_json(version_root / "meta.json", meta)

    pointer = get_published_pointer(project_root)
    prev = pointer.get("active_version")
    history = list(pointer.get("history", []))
    history.append(version)
    pointer = {
        "active_version": version,
        "previous_version": prev,
        "history": history,
        "updated_at": _utc_now(),
    }
    _write_json(get_published_pointer_path(project_root), pointer)
    store = RulesStore(project_root)
    for ruleset in ("email_rules", "content_rules"):
        profiles = _iter_rule_profiles(staged_rules_root, ruleset)
        for profile, doc in profiles:
            store.create_version(
                ruleset,
                profile=profile,
                version=version,
                config=doc,
                created_by=created_by,
                activate=True,
            )
    sources = _load_sources_registry_docs(staged_rules_root)
    store.upsert_sources(sources, replace=True)
    return {"ok": True, **meta, "previous_version": prev}


def rollback_to_previous(project_root: Path, created_by: str) -> dict[str, Any]:
    ensure_bootstrap_published(project_root)
    pointer = get_published_pointer(project_root)
    history = list(pointer.get("history", []))
    if len(history) < 2:
        raise RuntimeError("no previous version to rollback")

    current = history[-1]
    previous = history[-2]
    history.append(previous)
    pointer = {
        "active_version": previous,
        "previous_version": current,
        "history": history,
        "updated_at": _utc_now(),
        "rolled_back_by": created_by,
    }
    _write_json(get_published_pointer_path(project_root), pointer)
    store = RulesStore(project_root)
    rollback_rows: list[dict[str, Any]] = []
    for ruleset in ("email_rules", "content_rules"):
        active = store.list_versions(ruleset, active_only=True)
        for row in active:
            profile = str(row.get("profile", ""))
            if not profile:
                continue
            try:
                rollback_rows.append(store.rollback(ruleset, profile=profile))
            except RuntimeError:
                continue
    return {
        "ok": True,
        "active_version": previous,
        "previous_version": current,
        "updated_at": pointer["updated_at"],
        "db_rollback": rollback_rows,
        "rolled_back_by": created_by,
    }


def stage_rules_overlay(project_root: Path, overlays: dict[str, str]) -> Path:
    """
    Build a staged rules workspace from active published rules + overlay files.
    overlays keys: relative to rules root, e.g. 'content_rules/enhanced.yaml'.
    """
    runtime_root = get_runtime_rules_root(project_root)
    tmp_root = get_console_root(project_root) / "_staging"
    if tmp_root.exists():
        shutil.rmtree(tmp_root)
    tmp_root.mkdir(parents=True, exist_ok=True)

    staged_rules_root = tmp_root / "rules"
    _copy_rules_tree(runtime_root, staged_rules_root)

    for rel, content in overlays.items():
        rel_clean = rel.strip().lstrip("/")
        p = staged_rules_root / rel_clean
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")

    return staged_rules_root
