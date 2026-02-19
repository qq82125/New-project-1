from __future__ import annotations

import hashlib
import json
import sqlite3
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


def _target_session(target_url: str) -> Session:
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
            id=int(row["id"]),
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
                id=rid,
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


def migrate_sqlite_to_target(
    *,
    project_root: Path,
    target_url: str,
    source_sqlite_path: Path | None = None,
    batch_size: int = 500,
    resume: bool = True,
) -> dict[str, Any]:
    src_path = source_sqlite_path or (project_root / "data" / "rules.db")
    if not src_path.exists():
        raise RuntimeError(f"source sqlite not found: {src_path}")

    src = _connect_source(src_path)
    session = _target_session(target_url)
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

    moved: dict[str, int] = {t: 0 for t in MVP_TABLES}
    checkpoints: dict[str, str | None] = {}

    try:
        for table in MVP_TABLES:
            last_key = _get_checkpoint(session, table) if resume else None
            checkpoints[table] = last_key
            while True:
                if table == "sources":
                    if last_key:
                        rows = src.execute(
                            "SELECT * FROM sources WHERE id > ? ORDER BY id ASC LIMIT ?",
                            (last_key, batch_size),
                        ).fetchall()
                    else:
                        rows = src.execute("SELECT * FROM sources ORDER BY id ASC LIMIT ?", (batch_size,)).fetchall()
                else:
                    if last_key:
                        rows = src.execute(
                            f"SELECT * FROM {table} WHERE id > ? ORDER BY id ASC LIMIT ?",
                            (int(last_key), batch_size),
                        ).fetchall()
                    else:
                        rows = src.execute(f"SELECT * FROM {table} ORDER BY id ASC LIMIT ?", (batch_size,)).fetchall()

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

        return {
            "ok": True,
            "source": str(src_path),
            "target": target_url,
            "batch_size": int(batch_size),
            "resume": bool(resume),
            "moved": moved,
            "checkpoints": checkpoints,
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
) -> dict[str, Any]:
    src_path = source_sqlite_path or (project_root / "data" / "rules.db")
    src = _connect_source(src_path)
    session = _target_session(target_url)

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
        for table in MVP_TABLES:
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

        return {
            "ok": len(mismatches) == 0,
            "source": str(src_path),
            "target": target_url,
            "counts": counts,
            "mismatches": mismatches,
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
