from __future__ import annotations

import hashlib
import json
import sqlite3
from urllib.parse import urlparse, unquote
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app.db.engine import make_engine
from app.db.models.rules import (
    ContentRulesVersion,
    EmailRulesVersion,
    OutputRulesVersion,
    QcRulesVersion,
    RulesDraft,
    SchedulerRulesVersion,
    Source,
)
from app.services.rules_store_sa import SQLAlchemyRulesStore

MVP_TABLES = [
    "email_rules_versions",
    "content_rules_versions",
    "qc_rules_versions",
    "output_rules_versions",
    "scheduler_rules_versions",
    "rules_drafts",
    "sources",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_json_obj(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return None
    if isinstance(value, (str, bytes)):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _json_hash(value: Any) -> str:
    txt = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()


def _stable_hash_text(value: str) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:12], 16)


def _sqlite_path_from_input(from_value: str | Path | None, project_root: Path) -> Path:
    if from_value is None:
        return project_root / "data" / "rules.db"
    if isinstance(from_value, Path):
        return from_value
    raw = str(from_value).strip()
    if raw.startswith("sqlite:///"):
        parsed = urlparse(raw)
        p = Path(unquote(parsed.path))
        if p.is_absolute():
            return p
        return (project_root / p).resolve()
    return (project_root / raw).resolve()


def _load_checkpoint_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"tables": {}, "updated_at": "", "status": "new"}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return {"tables": {}, "updated_at": "", "status": "corrupt"}
        obj.setdefault("tables", {})
        return obj
    except Exception:
        return {"tables": {}, "updated_at": "", "status": "corrupt"}


def _save_checkpoint_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(data)
    payload["updated_at"] = _utc_now()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _ensure_checkpoint_table(session: Session) -> None:
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS _db_migrate_checkpoint (
                table_name TEXT PRIMARY KEY,
                last_key TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
    )
    session.commit()


def _get_checkpoint(session: Session, table_name: str) -> str | None:
    row = session.execute(
        text("SELECT last_key FROM _db_migrate_checkpoint WHERE table_name = :t"),
        {"t": table_name},
    ).first()
    if row is None:
        return None
    return str(row[0]) if row[0] is not None else None


def _set_checkpoint(session: Session, table_name: str, last_key: str | None) -> None:
    session.execute(
        text(
            """
            INSERT INTO _db_migrate_checkpoint(table_name, last_key, updated_at)
            VALUES (:t, :k, :u)
            ON CONFLICT(table_name) DO UPDATE SET
              last_key = excluded.last_key,
              updated_at = excluded.updated_at
            """
        ),
        {"t": table_name, "k": last_key, "u": _utc_now()},
    )
    session.commit()


def _clear_checkpoint(session: Session) -> None:
    _ensure_checkpoint_table(session)
    session.execute(text("DELETE FROM _db_migrate_checkpoint"))
    session.commit()


def _connect_source(source_sqlite_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(source_sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn


def _target_session(target_url: str, project_root: Path) -> Session:
    # Ensure target schema is ready via Alembic-managed path before migration.
    SQLAlchemyRulesStore(project_root=project_root, database_url=target_url, auto_init=True, enable_secondary=False)
    engine = make_engine(target_url)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return SessionLocal()


def _upsert_version_row(session: Session, model: Any, row: sqlite3.Row) -> None:
    profile = str(row["profile"])
    version = str(row["version"])
    current = session.query(model).filter(model.profile == profile, model.version == version).one_or_none()
    payload = _norm_json_obj(row["config_json"]) or {}
    if current is None:
        obj = model(
            profile=profile,
            version=version,
            config_json=payload,
            created_at=str(row["created_at"]),
            created_by=str(row["created_by"]),
            is_active=int(row["is_active"]),
        )
        session.add(obj)
    else:
        current.config_json = payload
        current.created_at = str(row["created_at"])
        current.created_by = str(row["created_by"])
        current.is_active = int(row["is_active"])


def _upsert_draft_row(session: Session, row: sqlite3.Row) -> None:
    rid = int(row["id"])
    current = session.get(RulesDraft, rid)
    cfg = _norm_json_obj(row["config_json"]) or {}
    val = _norm_json_obj(row["validation_json"]) or []
    if current is None:
        session.add(
            RulesDraft(
                ruleset=str(row["ruleset"]),
                profile=str(row["profile"]),
                config_json=cfg,
                validation_json=val,
                created_at=str(row["created_at"]),
                created_by=str(row["created_by"]),
            )
        )
    else:
        current.ruleset = str(row["ruleset"])
        current.profile = str(row["profile"])
        current.config_json = cfg
        current.validation_json = val
        current.created_at = str(row["created_at"])
        current.created_by = str(row["created_by"])


def _upsert_source_row(session: Session, row: sqlite3.Row) -> None:
    sid = str(row["id"])
    current = session.get(Source, sid)
    obj = {
        "name": str(row["name"]),
        "connector": str(row["connector"]),
        "url": str(row["url"] or ""),
        "enabled": int(row["enabled"]),
        "priority": int(row["priority"]),
        "trust_tier": str(row["trust_tier"]),
        "tags_json": _norm_json_obj(row["tags_json"]) or [],
        "rate_limit_json": _norm_json_obj(row["rate_limit_json"]) or {},
        "fetch_json": _norm_json_obj(row["fetch_json"]) or {},
        "parsing_json": _norm_json_obj(row["parsing_json"]) or {},
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
        "last_fetched_at": str(row["last_fetched_at"] or "") or None,
        "last_fetch_status": str(row["last_fetch_status"] or "") or None,
        "last_fetch_http_status": int(row["last_fetch_http_status"]) if row["last_fetch_http_status"] is not None else None,
        "last_fetch_error": str(row["last_fetch_error"] or "") or None,
        "last_success_at": str(row["last_success_at"] or "") or None,
        "last_http_status": int(row["last_http_status"]) if row["last_http_status"] is not None else None,
        "last_error": str(row["last_error"] or "") or None,
    }
    if current is None:
        session.add(Source(id=sid, **obj))
    else:
        for k, v in obj.items():
            setattr(current, k, v)


def _detect_unique_conflicts(src: sqlite3.Connection) -> list[dict[str, Any]]:
    checks = [
        ("email_rules_versions", ["profile", "version"]),
        ("content_rules_versions", ["profile", "version"]),
        ("qc_rules_versions", ["profile", "version"]),
        ("output_rules_versions", ["profile", "version"]),
        ("scheduler_rules_versions", ["profile", "version"]),
        ("sources", ["id"]),
    ]
    conflicts: list[dict[str, Any]] = []
    for table, cols in checks:
        col_sql = ", ".join(cols)
        q = (
            f"SELECT {col_sql}, COUNT(1) AS c "
            f"FROM {table} GROUP BY {col_sql} HAVING COUNT(1) > 1 ORDER BY c DESC LIMIT 20"
        )
        rows = src.execute(q).fetchall()
        if rows:
            conflicts.append(
                {
                    "table": table,
                    "key": cols,
                    "count": len(rows),
                    "examples": [dict(r) for r in rows[:5]],
                }
            )
    return conflicts


def _table_pk_field(table: str) -> str:
    return "id"


def migrate_sqlite_to_target(
    *,
    project_root: Path,
    target_url: str,
    source_sqlite_path: Path | None = None,
    source_sqlite_url_or_path: str | Path | None = None,
    batch_size: int = 500,
    resume: bool = True,
    checkpoint_path: Path | None = None,
    tables: list[str] | None = None,
) -> dict[str, Any]:
    src_path = _sqlite_path_from_input(source_sqlite_url_or_path or source_sqlite_path, project_root)
    if not src_path.exists():
        raise RuntimeError(f"source sqlite not found: {src_path}")

    src = _connect_source(src_path)
    session = _target_session(target_url, project_root)
    target_tables = [t for t in (tables or MVP_TABLES) if t in MVP_TABLES]
    cp_path = checkpoint_path or (project_root / "data" / "db_migrate_checkpoint.json")
    cp = _load_checkpoint_file(cp_path)
    if not resume:
        cp = {"tables": {}, "updated_at": _utc_now(), "status": "reset"}
        _save_checkpoint_file(cp_path, cp)

    conflicts = _detect_unique_conflicts(src)
    if conflicts:
        return {
            "ok": False,
            "error": "unique_conflicts_detected",
            "source": str(src_path),
            "target": target_url,
            "conflicts": conflicts,
        }

    _ensure_checkpoint_table(session)
    if not resume:
        _clear_checkpoint(session)

    version_models = {
        "email_rules_versions": EmailRulesVersion,
        "content_rules_versions": ContentRulesVersion,
        "qc_rules_versions": QcRulesVersion,
        "output_rules_versions": OutputRulesVersion,
        "scheduler_rules_versions": SchedulerRulesVersion,
    }

    moved: dict[str, int] = {t: 0 for t in target_tables}
    checkpoints: dict[str, str | None] = {}

    try:
        for table in target_tables:
            last_key = None
            if resume:
                last_key = str((cp.get("tables") or {}).get(table) or "") or None
                if not last_key:
                    last_key = _get_checkpoint(session, table)
            checkpoints[table] = last_key
            pk = _table_pk_field(table)
            while True:
                if table == "sources":
                    if last_key:
                        rows = src.execute(
                            f"SELECT * FROM sources WHERE {pk} > ? ORDER BY {pk} ASC LIMIT ?",
                            (last_key, batch_size),
                        ).fetchall()
                    else:
                        rows = src.execute(f"SELECT * FROM sources ORDER BY {pk} ASC LIMIT ?", (batch_size,)).fetchall()
                else:
                    if last_key:
                        rows = src.execute(
                            f"SELECT * FROM {table} WHERE {pk} > ? ORDER BY {pk} ASC LIMIT ?",
                            (int(last_key), batch_size),
                        ).fetchall()
                    else:
                        rows = src.execute(f"SELECT * FROM {table} ORDER BY {pk} ASC LIMIT ?", (batch_size,)).fetchall()

                if not rows:
                    break

                for row in rows:
                    if table in version_models:
                        _upsert_version_row(session, version_models[table], row)
                        last_key = str(int(row["id"]))
                    elif table == "rules_drafts":
                        _upsert_draft_row(session, row)
                        last_key = str(int(row["id"]))
                    elif table == "sources":
                        _upsert_source_row(session, row)
                        last_key = str(row["id"])
                    else:
                        continue
                    moved[table] += 1

                session.commit()
                _set_checkpoint(session, table, last_key)
                checkpoints[table] = last_key
                cp_tables = cp.setdefault("tables", {})
                if isinstance(cp_tables, dict):
                    cp_tables[table] = last_key
                _save_checkpoint_file(cp_path, cp)

        return {
            "ok": True,
            "source": str(src_path),
            "target": target_url,
            "batch_size": int(batch_size),
            "resume": bool(resume),
            "moved": moved,
            "checkpoints": checkpoints,
            "checkpoint_file": str(cp_path),
            "tables": target_tables,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        src.close()
        session.close()


def verify_sqlite_vs_target(
    *,
    project_root: Path,
    target_url: str,
    source_sqlite_path: Path | None = None,
    source_sqlite_url_or_path: str | Path | None = None,
    tables: list[str] | None = None,
    sample_rate: float = 0.05,
) -> dict[str, Any]:
    src_path = _sqlite_path_from_input(source_sqlite_url_or_path or source_sqlite_path, project_root)
    src = _connect_source(src_path)
    session = _target_session(target_url, project_root)
    target_tables = [t for t in (tables or MVP_TABLES) if t in MVP_TABLES]
    sample = max(0.0, min(1.0, float(sample_rate)))

    mismatches: list[dict[str, Any]] = []
    counts: dict[str, dict[str, int]] = {}
    version_models = {
        "email_rules_versions": EmailRulesVersion,
        "content_rules_versions": ContentRulesVersion,
        "qc_rules_versions": QcRulesVersion,
        "output_rules_versions": OutputRulesVersion,
        "scheduler_rules_versions": SchedulerRulesVersion,
    }

    try:
        conflicts = _detect_unique_conflicts(src)
        if conflicts:
            mismatches.append({"type": "unique_conflicts_detected", "conflicts": conflicts})

        for table in target_tables:
            src_count = int(src.execute(f"SELECT COUNT(1) AS c FROM {table}").fetchone()["c"])
            if table in version_models:
                dst_count = int(session.query(version_models[table]).count())
            elif table == "rules_drafts":
                dst_count = int(session.query(RulesDraft).count())
            else:
                dst_count = int(session.query(Source).count())
            counts[table] = {"source": src_count, "target": dst_count}
            if src_count != dst_count:
                mismatches.append({"table": table, "type": "count_mismatch", "source": src_count, "target": dst_count})

        for table, model in version_models.items():
            if table not in target_tables:
                continue
            src_rows = src.execute(
                f"SELECT profile, version, is_active, config_json FROM {table} ORDER BY profile, version"
            ).fetchall()
            dst_rows = session.query(model).all()
            src_set = {
                (str(r["profile"]), str(r["version"]), int(r["is_active"]), _json_hash(_norm_json_obj(r["config_json"]) or {}))
                for r in src_rows
            }
            dst_set = {
                (str(r.profile), str(r.version), int(r.is_active), _json_hash(r.config_json if isinstance(r.config_json, dict) else {}))
                for r in dst_rows
            }
            if src_set != dst_set:
                mismatches.append(
                    {
                        "table": table,
                        "type": "content_mismatch",
                        "only_in_source": max(0, len(src_set - dst_set)),
                        "only_in_target": max(0, len(dst_set - src_set)),
                    }
                )

            if sample > 0:
                dst_map = {
                    (str(r.profile), str(r.version)): _json_hash(
                        {
                            "profile": str(r.profile),
                            "version": str(r.version),
                            "is_active": int(r.is_active),
                            "config_json": r.config_json if isinstance(r.config_json, dict) else {},
                        }
                    )
                    for r in dst_rows
                }
                sampled = 0
                sample_miss = 0
                for r in src_rows:
                    key = f"{r['profile']}|{r['version']}"
                    if (_stable_hash_text(key) % 10000) >= int(sample * 10000):
                        continue
                    sampled += 1
                    src_sig = _json_hash(
                        {
                            "profile": str(r["profile"]),
                            "version": str(r["version"]),
                            "is_active": int(r["is_active"]),
                            "config_json": _norm_json_obj(r["config_json"]) or {},
                        }
                    )
                    if dst_map.get((str(r["profile"]), str(r["version"]))) != src_sig:
                        sample_miss += 1
                if sampled > 0 and sample_miss > 0:
                    mismatches.append(
                        {
                            "table": table,
                            "type": "sample_hash_mismatch",
                            "sampled": sampled,
                            "mismatch": sample_miss,
                        }
                    )

        return {
            "ok": len(mismatches) == 0,
            "source": str(src_path),
            "target": target_url,
            "counts": counts,
            "mismatches": mismatches,
            "tables": target_tables,
            "sample_rate": sample,
        }
    finally:
        src.close()
        session.close()


def dual_replay_compare(*, project_root: Path, primary_url: str, secondary_url: str) -> dict[str, Any]:
    primary = SQLAlchemyRulesStore(project_root=project_root, database_url=primary_url, auto_init=False)
    secondary = SQLAlchemyRulesStore(project_root=project_root, database_url=secondary_url, auto_init=False)

    mismatches: list[dict[str, Any]] = []
    rulesets = ["email_rules", "content_rules", "qc_rules", "output_rules", "scheduler_rules"]

    for rs in rulesets:
        p_all = primary.list_versions(rs)
        s_all = secondary.list_versions(rs)
        p_profiles = {str(x.get("profile", "")) for x in p_all if str(x.get("profile", ""))}
        s_profiles = {str(x.get("profile", "")) for x in s_all if str(x.get("profile", ""))}
        profiles = sorted(p_profiles | s_profiles)
        if not profiles:
            profiles = ["legacy", "enhanced"]
        for profile in profiles:
            pv = primary.get_active_rules(rs, profile)
            sv = secondary.get_active_rules(rs, profile)
            if _json_hash(pv or {}) != _json_hash(sv or {}):
                mismatches.append({"ruleset": rs, "profile": profile, "type": "active_rules_diff"})

    ps = primary.list_sources()
    ss = secondary.list_sources()
    if _json_hash(ps) != _json_hash(ss):
        mismatches.append(
            {
                "table": "sources",
                "type": "list_diff",
                "primary_count": len(ps),
                "secondary_count": len(ss),
            }
        )

    return {
        "ok": len(mismatches) == 0,
        "primary": primary_url,
        "secondary": secondary_url,
        "mismatches": mismatches,
    }
