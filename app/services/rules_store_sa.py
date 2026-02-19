from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Type, Union

from sqlalchemy import and_, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.db.config import get_db_settings
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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


RulesModel = Union[
    Type[EmailRulesVersion],
    Type[ContentRulesVersion],
    Type[QcRulesVersion],
    Type[OutputRulesVersion],
    Type[SchedulerRulesVersion],
]


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
        self.db_path = project_root / "data" / "rules.db"
        self.engine: Engine = make_engine(database_url)
        self._Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, expire_on_commit=False)
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

    def ensure_schema(self) -> None:
        # Runtime path assumes schema is managed by Alembic; only verify connectivity.
        with self.engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        if self._secondary_store is not None:
            self._secondary_store.ensure_schema()

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
        with self._session() as s:
            row = s.execute(select(model.id).limit(1)).first()
            out = row is not None
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
        with self._session() as s:
            row = s.execute(
                select(model)
                .where(and_(model.profile == profile, model.is_active == 1))
                .order_by(model.id.desc())
                .limit(1)
            ).scalar_one_or_none()
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
        with self._session() as s:
            q = select(model).order_by(model.id.desc())
            if profile:
                q = q.where(model.profile == profile)
            if active_only:
                q = q.where(model.is_active == 1)
            rows = list(s.execute(q).scalars().all())
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
        with self._session() as s:
            row = s.execute(
                select(model.config_json)
                .where(and_(model.profile == profile, model.version == version))
                .limit(1)
            ).scalar_one_or_none()
            out = dict(row) if isinstance(row, dict) else None
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
        with self._session() as s:
            try:
                if activate:
                    s.execute(update(model).where(model.profile == profile).values(is_active=0))
                current = s.execute(
                    select(model).where(and_(model.profile == profile, model.version == version)).limit(1)
                ).scalar_one_or_none()
                if current is None:
                    row = model(
                        profile=profile,
                        version=version,
                        config_json=config,
                        created_at=now,
                        created_by=created_by,
                        is_active=1 if activate else 0,
                    )
                    s.add(row)
                else:
                    current.config_json = config
                    current.created_at = now
                    current.created_by = created_by
                    current.is_active = 1 if activate else 0
                s.commit()
            except Exception:
                s.rollback()
                raise
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
        with self._session() as s:
            try:
                row = s.execute(
                    select(model).where(and_(model.profile == profile, model.version == version)).limit(1)
                ).scalar_one_or_none()
                if row is None:
                    raise RuntimeError(f"version not found: ruleset={ruleset} profile={profile} version={version}")
                s.execute(update(model).where(model.profile == profile).values(is_active=0))
                row.is_active = 1
                s.commit()
            except Exception:
                s.rollback()
                raise
        self._dual_write("activate_version", ruleset, profile=profile, version=version)
        return {"ok": True, "ruleset": ruleset, "profile": profile, "version": version, "is_active": True}

    def rollback(self, ruleset: str, *, profile: str) -> dict[str, Any]:
        model = self._table_model(ruleset)
        with self._session() as s:
            current = s.execute(
                select(model).where(and_(model.profile == profile, model.is_active == 1)).order_by(model.id.desc()).limit(1)
            ).scalar_one_or_none()
            if current is None:
                raise RuntimeError(f"no active version: ruleset={ruleset} profile={profile}")
            previous = s.execute(
                select(model)
                .where(and_(model.profile == profile, model.id < current.id))
                .order_by(model.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if previous is None:
                raise RuntimeError(f"no previous version to rollback: ruleset={ruleset} profile={profile}")
            try:
                s.execute(update(model).where(model.profile == profile).values(is_active=0))
                previous.is_active = 1
                s.commit()
            except Exception:
                s.rollback()
                raise
        self._dual_write("rollback", ruleset, profile=profile)
        return {
            "ok": True,
            "ruleset": ruleset,
            "profile": profile,
            "active_version": str(previous.version),
            "previous_version": str(current.version),
        }

    def upsert_sources(self, sources: list[dict[str, Any]], *, replace: bool = True) -> dict[str, Any]:
        now = _utc_now()
        with self._session() as s:
            try:
                if replace:
                    s.query(Source).delete()
                for src in sources:
                    sid = str(src.get("id", "")).strip()
                    if not sid:
                        continue
                    row = s.get(Source, sid)
                    if row is None:
                        row = Source(id=sid, created_at=now, updated_at=now)
                        s.add(row)
                    row.name = str(src.get("name", sid))
                    row.connector = str(src.get("connector", ""))
                    row.url = str(src.get("url", ""))
                    row.enabled = 1 if bool(src.get("enabled", True)) else 0
                    row.priority = int(src.get("priority", 0) or 0)
                    row.trust_tier = str(src.get("trust_tier", "C"))
                    row.tags_json = src.get("tags", [])
                    row.rate_limit_json = src.get("rate_limit", {})
                    row.fetch_json = src.get("fetch", {})
                    row.parsing_json = src.get("parsing", {})
                    row.updated_at = now
                s.commit()
                cnt = int(s.execute(select(Source.id)).all().__len__())
            except Exception:
                s.rollback()
                raise
        self._dual_write("upsert_sources", sources, replace=replace)
        return {"ok": True, "source_count": cnt}

    def _source_to_dict(self, r: Source) -> dict[str, Any]:
        return {
            "id": str(r.id),
            "name": str(r.name),
            "connector": str(r.connector),
            "url": str(r.url or ""),
            "enabled": bool(int(r.enabled)),
            "priority": int(r.priority),
            "trust_tier": str(r.trust_tier),
            "tags": r.tags_json if isinstance(r.tags_json, list) else [],
            "rate_limit": r.rate_limit_json if isinstance(r.rate_limit_json, dict) else {},
            "fetch": r.fetch_json if isinstance(r.fetch_json, dict) else {},
            "parsing": r.parsing_json if isinstance(r.parsing_json, dict) else {},
            "created_at": str(r.created_at),
            "updated_at": str(r.updated_at),
            "last_fetched_at": str(r.last_fetched_at or ""),
            "last_fetch_status": str(r.last_fetch_status or ""),
            "last_fetch_http_status": int(r.last_fetch_http_status) if r.last_fetch_http_status is not None else None,
            "last_fetch_error": str(r.last_fetch_error or ""),
            "last_success_at": str(r.last_success_at or ""),
            "last_http_status": int(r.last_http_status) if r.last_http_status is not None else None,
            "last_error": str(r.last_error or ""),
        }

    def list_sources(self, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        with self._session() as s:
            q = select(Source).order_by(Source.priority.desc(), Source.id.asc())
            if enabled_only:
                q = q.where(Source.enabled == 1)
            rows = list(s.execute(q).scalars().all())
            out = [self._source_to_dict(r) for r in rows]
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare("list_sources", out, self._secondary_store.list_sources(enabled_only=enabled_only))
        return out

    def get_source(self, source_id: str) -> dict[str, Any] | None:
        with self._session() as s:
            row = s.get(Source, source_id)
            out = self._source_to_dict(row) if row is not None else None
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare("get_source", out, self._secondary_store.get_source(source_id))
        return out

    def source_url_exists(self, url: str, *, exclude_id: str | None = None) -> bool:
        with self._session() as s:
            q = select(Source.id).where(Source.url == url)
            if exclude_id:
                q = q.where(Source.id != exclude_id)
            out = s.execute(q.limit(1)).first() is not None
        if self._secondary_store is not None and self.read_mode == "shadow_compare":
            self._shadow_compare(
                "source_url_exists",
                out,
                self._secondary_store.source_url_exists(url, exclude_id=exclude_id),
            )
        return out

    def upsert_source(self, source: dict[str, Any]) -> dict[str, Any]:
        sid = str(source.get("id", "")).strip()
        if not sid:
            raise RuntimeError("source id required")
        now = _utc_now()
        with self._session() as s:
            try:
                row = s.get(Source, sid)
                if row is None:
                    row = Source(id=sid, created_at=now, updated_at=now)
                    s.add(row)
                row.name = str(source.get("name", sid))
                row.connector = str(source.get("connector", ""))
                row.url = str(source.get("url", ""))
                row.enabled = 1 if bool(source.get("enabled", True)) else 0
                row.priority = int(source.get("priority", 0) or 0)
                row.trust_tier = str(source.get("trust_tier", "C"))
                row.tags_json = source.get("tags", [])
                row.rate_limit_json = source.get("rate_limit", {})
                row.fetch_json = source.get("fetch", {})
                row.parsing_json = source.get("parsing", {})
                row.updated_at = now
                s.commit()
            except Exception:
                s.rollback()
                raise
        self._dual_write("upsert_source", source)
        return {"ok": True, "source": self.get_source(sid)}

    def record_source_test(
        self,
        source_id: str,
        *,
        ok: bool,
        http_status: int | None = None,
        error: str | None = None,
    ) -> None:
        now = _utc_now()
        with self._session() as s:
            row = s.get(Source, source_id)
            if row is None:
                return
            if ok:
                row.last_success_at = now
                row.last_http_status = http_status
                row.last_error = None
                row.updated_at = now
            else:
                row.last_http_status = http_status
                row.last_error = str(error or "")
                row.updated_at = now
            s.commit()
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
        with self._session() as s:
            row = s.get(Source, source_id)
            if row is None:
                return
            if str(status).lower() == "skipped" and fetched_at is None:
                row.last_fetch_status = str(status or "")
                row.last_fetch_http_status = int(http_status) if http_status is not None else None
                row.last_fetch_error = str(error or "") if error else None
                row.updated_at = now
            else:
                row.last_fetched_at = now
                row.last_fetch_status = str(status or "")
                row.last_fetch_http_status = int(http_status) if http_status is not None else None
                row.last_fetch_error = str(error or "") if error else None
                row.updated_at = now
            s.commit()
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
        with self._session() as s:
            row = RulesDraft(
                ruleset=ruleset,
                profile=profile,
                config_json=config_json,
                validation_json=validation_errors or [],
                created_at=now,
                created_by=created_by,
            )
            s.add(row)
            s.commit()
            did = int(row.id)
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
        with self._session() as s:
            if draft_id is not None:
                row = s.execute(
                    select(RulesDraft)
                    .where(and_(RulesDraft.id == draft_id, RulesDraft.ruleset == ruleset, RulesDraft.profile == profile))
                    .limit(1)
                ).scalar_one_or_none()
            else:
                row = s.execute(
                    select(RulesDraft)
                    .where(and_(RulesDraft.ruleset == ruleset, RulesDraft.profile == profile))
                    .order_by(RulesDraft.id.desc())
                    .limit(1)
                ).scalar_one_or_none()
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
