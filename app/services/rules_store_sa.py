from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Type, Union

from alembic import command
from alembic.config import Config
from sqlalchemy import and_, inspect
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker

from app.db.config import get_db_settings, redact_database_url
from app.db.engine import make_engine
from app.db.models.rules import (
    ContentRulesVersion,
    EmailRulesVersion,
    OutputRulesVersion,
    QcRulesVersion,
    SchedulerRulesVersion,
)
from app.db.repo import RulesRepo, SourcesRepo


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


RulesModel = Union[
    Type[EmailRulesVersion],
    Type[ContentRulesVersion],
    Type[QcRulesVersion],
    Type[OutputRulesVersion],
    Type[SchedulerRulesVersion],
]

REQUIRED_TABLES = (
    "email_rules_versions",
    "content_rules_versions",
    "qc_rules_versions",
    "output_rules_versions",
    "scheduler_rules_versions",
    "rules_drafts",
    "sources",
)


def _sqlite_path_from_url(url: str, fallback_root: Path) -> Path:
    try:
        parsed = make_url(url)
        if parsed.drivername.startswith("sqlite"):
            db_name = parsed.database or ""
            if not db_name:
                return fallback_root / "data" / "rules.db"
            p = Path(db_name)
            if p.is_absolute():
                return p
            return (fallback_root / p).resolve()
    except Exception:
        pass
    return fallback_root / "data" / "rules.db"


class SQLAlchemyRulesStore:
    """
    SQLAlchemy-backed RulesStore implementation.

    Public interface intentionally mirrors app.services.rules_store.RulesStore.
    """

    def __init__(
        self,
        project_root: Path,
        database_url: str,
        auto_init: bool = True,
        *,
        write_mode: str | None = None,
        read_mode: str | None = None,
        secondary_url: str | None = None,
        enable_secondary: bool = True,
    ) -> None:
        self.project_root = project_root
        self.database_url = database_url
        self.db_path = _sqlite_path_from_url(database_url, project_root)
        if self.database_url.lower().startswith("sqlite"):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.engine: Engine = make_engine(database_url)
        self._Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, expire_on_commit=False)
        self.rules_repo = RulesRepo(self._Session)
        self.sources_repo = SourcesRepo(self._Session)
        settings = get_db_settings()
        self.write_mode = (write_mode or settings.db_write_mode or "single").strip().lower()
        self.read_mode = (read_mode or settings.db_read_mode or "primary").strip().lower()
        self.dual_strict = str(os.environ.get("DB_DUAL_STRICT", "false")).strip().lower() in {"1", "true", "yes", "on"}
        self._logger = logging.getLogger("rules_store_sa")
        self._secondary_store: Optional["SQLAlchemyRulesStore"] = None
        sec = (secondary_url or settings.database_url_secondary or "").strip()
        if enable_secondary and sec and sec != database_url and (
            self.write_mode in {"dual"} or self.read_mode in {"shadow_compare"}
        ):
            self._secondary_store = SQLAlchemyRulesStore(
                project_root=project_root,
                database_url=sec,
                auto_init=auto_init,
                write_mode="single",
                read_mode="primary",
                secondary_url=None,
                enable_secondary=False,
            )
        if auto_init:
            self.ensure_schema()

    def _session(self) -> Session:
        return self._Session()

    def _run_alembic_upgrade(self) -> None:
        alembic_ini = self.project_root / "alembic.ini"
        script_location = self.project_root / "alembic"
        if not alembic_ini.exists() or not script_location.exists():
            fallback_root = Path(__file__).resolve().parents[2]
            alembic_ini = fallback_root / "alembic.ini"
            script_location = fallback_root / "alembic"
        if not alembic_ini.exists() or not script_location.exists():
            raise RuntimeError("Alembic configuration not found")
        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(script_location))
        cfg.set_main_option("sqlalchemy.url", self.database_url)
        command.upgrade(cfg, "head")

    def ensure_schema(self) -> None:
        with self.engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        insp = inspect(self.engine)
        missing = [name for name in REQUIRED_TABLES if not insp.has_table(name)]
        if missing:
            try:
                self._run_alembic_upgrade()
                insp = inspect(self.engine)
                still_missing = [name for name in REQUIRED_TABLES if not insp.has_table(name)]
                if still_missing:
                    raise RuntimeError(f"missing tables after migration: {still_missing}")
            except Exception as e:
                raise RuntimeError(
                    "Database schema is not ready; run `alembic upgrade head` "
                    f"(url={redact_database_url(self.database_url)}): {e}"
                ) from e
        if self._secondary_store is not None:
            self._secondary_store.ensure_schema()

    def observability_info(self) -> dict[str, str]:
        backend = "postgresql" if self.database_url.lower().startswith("postgresql") else "sqlite"
        return {
            "db_backend": backend,
            "db_url": redact_database_url(self.database_url),
            "db_path": str(self.db_path),
        }

    def _canonical(self, value: Any) -> str:
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:
            return repr(value)

    def _shadow_compare(self, name: str, primary: Any, secondary: Any) -> None:
        if self.read_mode != "shadow_compare" or self._secondary_store is None:
            return
        if self._canonical(primary) != self._canonical(secondary):
            self._logger.warning(
                "DB shadow mismatch method=%s primary=%s secondary=%s",
                name,
                self._canonical(primary)[:500],
                self._canonical(secondary)[:500],
            )

    def _dual_write(self, method: str, *args: Any, **kwargs: Any) -> None:
        if self.write_mode != "dual" or self._secondary_store is None:
            return
        try:
            fn = getattr(self._secondary_store, method)
            fn(*args, **kwargs)
        except Exception as e:
            self._logger.error("DB dual-write failed method=%s error=%s", method, e)
            if self.dual_strict:
                raise

    def _table_model(self, ruleset: str) -> RulesModel:
        if ruleset == "email_rules":
            return EmailRulesVersion
        if ruleset == "content_rules":
            return ContentRulesVersion
        if ruleset == "qc_rules":
            return QcRulesVersion
        if ruleset == "output_rules":
            return OutputRulesVersion
        if ruleset == "scheduler_rules":
            return SchedulerRulesVersion
        raise ValueError(f"unsupported ruleset={ruleset}")

    def _decode_config_row(self, row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        obj = row.config_json if isinstance(row.config_json, dict) else {}
        if isinstance(obj, dict):
            out = dict(obj)
            out.setdefault("_store_meta", {})
            out["_store_meta"] = {
                "id": int(row.id),
                "profile": str(row.profile),
                "version": str(row.version),
                "created_at": str(row.created_at),
                "created_by": str(row.created_by),
                "is_active": bool(int(row.is_active)),
            }
            return out
        return None

    def has_any_versions(self, ruleset: str) -> bool:
        model = self._table_model(ruleset)
        out = self.rules_repo.has_any_versions(model)
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare("has_any_versions", out, self._secondary_store.has_any_versions(ruleset))
        return out

    def get_active_email_rules(self, profile: str) -> dict[str, Any] | None:
        return self.get_active_rules("email_rules", profile)

    def get_active_content_rules(self, profile: str) -> dict[str, Any] | None:
        return self.get_active_rules("content_rules", profile)

    def get_active_rules(self, ruleset: str, profile: str) -> dict[str, Any] | None:
        return self._get_active(ruleset, profile)

    def _get_active(self, ruleset: str, profile: str) -> dict[str, Any] | None:
        model = self._table_model(ruleset)
        row = self.rules_repo.get_active(model, profile)
        out = self._decode_config_row(row)
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare("get_active_rules", out, self._secondary_store.get_active_rules(ruleset, profile))
        return out

    def list_versions(
        self,
        ruleset: str,
        profile: str | None = None,
        *,
        active_only: bool = False,
    ) -> list[dict[str, Any]]:
        model = self._table_model(ruleset)
        rows = self.rules_repo.list_versions(model, profile, active_only=active_only)
        out = [
            {
                "id": int(r.id),
                "ruleset": ruleset,
                "profile": str(r.profile),
                "version": str(r.version),
                "created_at": str(r.created_at),
                "created_by": str(r.created_by),
                "is_active": bool(int(r.is_active)),
            }
            for r in rows
        ]
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare(
                "list_versions",
                out,
                self._secondary_store.list_versions(ruleset, profile=profile, active_only=active_only),
            )
        return out

    def get_version_config(self, ruleset: str, profile: str, version: str) -> dict[str, Any] | None:
        model = self._table_model(ruleset)
        out = self.rules_repo.get_version_config(model, profile, version)
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare(
                "get_version_config",
                out,
                self._secondary_store.get_version_config(ruleset, profile, version),
            )
        return out

    def create_version(
        self,
        ruleset: str,
        *,
        profile: str,
        version: str,
        config: dict[str, Any],
        created_by: str,
        activate: bool = False,
    ) -> dict[str, Any]:
        model = self._table_model(ruleset)
        now = _utc_now()
        self.rules_repo.upsert_version(
            model,
            profile=profile,
            version=version,
            config_json=config,
            created_at=now,
            created_by=created_by,
            activate=activate,
        )
        self._dual_write(
            "create_version",
            ruleset,
            profile=profile,
            version=version,
            config=config,
            created_by=created_by,
            activate=activate,
        )
        return {
            "ok": True,
            "ruleset": ruleset,
            "profile": profile,
            "version": version,
            "created_at": now,
            "created_by": created_by,
            "is_active": bool(activate),
        }

    def activate_version(self, ruleset: str, *, profile: str, version: str) -> dict[str, Any]:
        model = self._table_model(ruleset)
        ok = self.rules_repo.activate_version(model, profile=profile, version=version)
        if not ok:
            raise RuntimeError(f"version not found: ruleset={ruleset} profile={profile} version={version}")
        self._dual_write("activate_version", ruleset, profile=profile, version=version)
        return {"ok": True, "ruleset": ruleset, "profile": profile, "version": version, "is_active": True}

    def rollback(self, ruleset: str, *, profile: str) -> dict[str, Any]:
        model = self._table_model(ruleset)
        rolled = self.rules_repo.rollback(model, profile=profile)
        if rolled is None:
            active = self.get_active_rules(ruleset, profile)
            if active is None:
                raise RuntimeError(f"no active version: ruleset={ruleset} profile={profile}")
            raise RuntimeError(f"no previous version to rollback: ruleset={ruleset} profile={profile}")
        active_version, previous_version = rolled
        self._dual_write("rollback", ruleset, profile=profile)
        return {
            "ok": True,
            "ruleset": ruleset,
            "profile": profile,
            "active_version": active_version,
            "previous_version": previous_version,
        }

    def upsert_sources(self, sources: list[dict[str, Any]], *, replace: bool = True) -> dict[str, Any]:
        now = _utc_now()
        cnt = self.sources_repo.upsert_many(sources, replace=replace, now=now)
        self._dual_write("upsert_sources", sources, replace=replace)
        return {"ok": True, "source_count": cnt}

    def list_sources(self, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        out = self.sources_repo.list(enabled_only=enabled_only)
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare("list_sources", out, self._secondary_store.list_sources(enabled_only=enabled_only))
        return out

    def get_source(self, source_id: str) -> dict[str, Any] | None:
        out = self.sources_repo.get(source_id)
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare("get_source", out, self._secondary_store.get_source(source_id))
        return out

    def source_url_exists(self, url: str, *, exclude_id: str | None = None) -> bool:
        out = self.sources_repo.url_exists(url, exclude_id=exclude_id)
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare(
                "source_url_exists",
                out,
                self._secondary_store.source_url_exists(url, exclude_id=exclude_id),
            )
        return out

    def upsert_source(self, source: dict[str, Any]) -> dict[str, Any]:
        now = _utc_now()
        out_source = self.sources_repo.upsert_one(source, now=now)
        self._dual_write("upsert_source", source)
        return {"ok": True, "source": out_source}

    def record_source_test(
        self,
        source_id: str,
        *,
        ok: bool,
        http_status: int | None = None,
        error: str | None = None,
    ) -> None:
        now = _utc_now()
        self.sources_repo.record_test(source_id, now=now, ok=ok, http_status=http_status, error=error)
        self._dual_write("record_source_test", source_id, ok=ok, http_status=http_status, error=error)

    def record_source_fetch(
        self,
        source_id: str,
        *,
        status: str,
        http_status: int | None = None,
        error: str | None = None,
        fetched_at: str | None = None,
    ) -> None:
        now = fetched_at or _utc_now()
        keep_last_fetched = str(status).lower() == "skipped" and fetched_at is None
        self.sources_repo.record_fetch(
            source_id,
            now=now,
            status=status,
            http_status=http_status,
            error=error,
            keep_last_fetched=keep_last_fetched,
        )
        self._dual_write(
            "record_source_fetch",
            source_id,
            status=status,
            http_status=http_status,
            error=error,
            fetched_at=fetched_at,
        )

    def toggle_source(self, source_id: str, enabled: bool | None = None) -> dict[str, Any]:
        source = self.get_source(source_id)
        if source is None:
            raise RuntimeError(f"source not found: {source_id}")
        source["enabled"] = (not bool(source["enabled"])) if enabled is None else bool(enabled)
        out = self.upsert_source(source)
        return {"ok": True, "source": out["source"]}

    def create_draft(
        self,
        ruleset: str,
        profile: str,
        config_json: dict[str, Any] | None = None,
        validation_errors: list[dict[str, Any]] | None = None,
        created_by: str = "",
        *,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = _utc_now()
        if config_json is None:
            config_json = config or {}
        did = self.rules_repo.create_draft(
            ruleset=ruleset,
            profile=profile,
            config_json=config_json,
            validation_json=validation_errors or [],
            created_at=now,
            created_by=created_by,
        )
        self._dual_write(
            "create_draft",
            ruleset,
            profile,
            config_json=config_json,
            validation_errors=validation_errors,
            created_by=created_by,
            config=config,
        )
        return {
            "id": did,
            "ruleset": ruleset,
            "profile": profile,
            "created_at": now,
            "created_by": created_by,
            "validation_errors": validation_errors or [],
        }

    def publish_draft(
        self,
        ruleset: str,
        draft_id: int,
        profile: str,
        created_by: str,
    ) -> dict[str, Any]:
        draft = self.get_draft(ruleset=ruleset, profile=profile, draft_id=draft_id)
        if draft is None:
            raise RuntimeError(f"draft not found: ruleset={ruleset} profile={profile} draft_id={draft_id}")
        if str(draft.get("ruleset")) != ruleset or str(draft.get("profile")) != profile:
            raise RuntimeError(
                f"draft mismatch: expected ruleset={ruleset} profile={profile} got ruleset={draft.get('ruleset')} profile={draft.get('profile')}"
            )

        base = datetime.now(timezone.utc).strftime("db-%Y%m%dT%H%M%SZ")
        existing = {str(x["version"]) for x in self.list_versions(ruleset, profile=profile)}
        version = base if base not in existing else f"{base}-{draft_id}"
        if version in existing:
            idx = 2
            while True:
                cand = f"{base}-{idx}"
                if cand not in existing:
                    version = cand
                    break
                idx += 1

        out = self.create_version(
            ruleset,
            profile=profile,
            version=version,
            config=draft["config_json"],
            created_by=created_by,
            activate=True,
        )
        return {
            "ok": True,
            "published": True,
            "ruleset": ruleset,
            "profile": profile,
            "version": out["version"],
            "draft_id": int(draft_id),
        }

    def get_draft(
        self,
        *,
        ruleset: str,
        profile: str,
        draft_id: int | None = None,
    ) -> dict[str, Any] | None:
        row = self.rules_repo.get_draft(ruleset=ruleset, profile=profile, draft_id=draft_id)
        if row is None:
            return None
        out = {
            "id": int(row.id),
            "ruleset": str(row.ruleset),
            "profile": str(row.profile),
            "config_json": row.config_json if isinstance(row.config_json, dict) else {},
            "validation_errors": row.validation_json if isinstance(row.validation_json, list) else [],
            "created_at": str(row.created_at),
            "created_by": str(row.created_by),
        }
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare(
                "get_draft",
                out,
                self._secondary_store.get_draft(ruleset=ruleset, profile=profile, draft_id=draft_id),
            )
        return out

    def list_send_attempts(self, limit: int = 30) -> list[dict[str, Any]]:
        return self.rules_repo.list_send_attempts(limit)
