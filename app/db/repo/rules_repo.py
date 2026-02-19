from __future__ import annotations

from typing import Any

from sqlalchemy import and_, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from app.db.models.rules import RulesDraft


class RulesRepo:
    def __init__(self, session_factory: sessionmaker[Session]):
        self._Session = session_factory

    def has_any_versions(self, model: Any) -> bool:
        with self._Session() as s:
            return s.execute(select(model.id).limit(1)).first() is not None

    def get_active(self, model: Any, profile: str) -> Any | None:
        with self._Session() as s:
            return s.execute(
                select(model)
                .where(and_(model.profile == profile, model.is_active == 1))
                .order_by(model.id.desc())
                .limit(1)
            ).scalar_one_or_none()

    def list_versions(self, model: Any, profile: str | None = None, *, active_only: bool = False) -> list[Any]:
        with self._Session() as s:
            q = select(model).order_by(model.id.desc())
            if profile:
                q = q.where(model.profile == profile)
            if active_only:
                q = q.where(model.is_active == 1)
            return list(s.execute(q).scalars().all())

    def get_version_config(self, model: Any, profile: str, version: str) -> dict[str, Any] | None:
        with self._Session() as s:
            row = s.execute(
                select(model.config_json)
                .where(and_(model.profile == profile, model.version == version))
                .limit(1)
            ).scalar_one_or_none()
            return dict(row) if isinstance(row, dict) else None

    def upsert_version(
        self,
        model: Any,
        *,
        profile: str,
        version: str,
        config_json: dict[str, Any],
        created_at: str,
        created_by: str,
        activate: bool,
    ) -> None:
        with self._Session() as s:
            try:
                if activate:
                    s.execute(update(model).where(model.profile == profile).values(is_active=0))
                current = s.execute(
                    select(model).where(and_(model.profile == profile, model.version == version)).limit(1)
                ).scalar_one_or_none()
                if current is None:
                    s.add(
                        model(
                            profile=profile,
                            version=version,
                            config_json=config_json,
                            created_at=created_at,
                            created_by=created_by,
                            is_active=1 if activate else 0,
                        )
                    )
                else:
                    current.config_json = config_json
                    current.created_at = created_at
                    current.created_by = created_by
                    current.is_active = 1 if activate else 0
                s.commit()
            except Exception:
                s.rollback()
                raise

    def activate_version(self, model: Any, *, profile: str, version: str) -> bool:
        with self._Session() as s:
            try:
                row = s.execute(
                    select(model).where(and_(model.profile == profile, model.version == version)).limit(1)
                ).scalar_one_or_none()
                if row is None:
                    return False
                s.execute(update(model).where(model.profile == profile).values(is_active=0))
                row.is_active = 1
                s.commit()
                return True
            except Exception:
                s.rollback()
                raise

    def rollback(self, model: Any, *, profile: str) -> tuple[str, str] | None:
        with self._Session() as s:
            current = s.execute(
                select(model)
                .where(and_(model.profile == profile, model.is_active == 1))
                .order_by(model.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if current is None:
                return None
            previous = s.execute(
                select(model).where(and_(model.profile == profile, model.id < current.id)).order_by(model.id.desc()).limit(1)
            ).scalar_one_or_none()
            if previous is None:
                return None
            try:
                s.execute(update(model).where(model.profile == profile).values(is_active=0))
                previous.is_active = 1
                s.commit()
            except Exception:
                s.rollback()
                raise
            return str(previous.version), str(current.version)

    def create_draft(
        self,
        *,
        ruleset: str,
        profile: str,
        config_json: dict[str, Any],
        validation_json: list[dict[str, Any]],
        created_at: str,
        created_by: str,
    ) -> int:
        with self._Session() as s:
            row = RulesDraft(
                ruleset=ruleset,
                profile=profile,
                config_json=config_json,
                validation_json=validation_json,
                created_at=created_at,
                created_by=created_by,
            )
            s.add(row)
            s.commit()
            return int(row.id)

    def get_draft(self, *, ruleset: str, profile: str, draft_id: int | None = None) -> RulesDraft | None:
        with self._Session() as s:
            if draft_id is not None:
                return s.execute(
                    select(RulesDraft)
                    .where(and_(RulesDraft.id == draft_id, RulesDraft.ruleset == ruleset, RulesDraft.profile == profile))
                    .limit(1)
                ).scalar_one_or_none()
            return s.execute(
                select(RulesDraft)
                .where(and_(RulesDraft.ruleset == ruleset, RulesDraft.profile == profile))
                .order_by(RulesDraft.id.desc())
                .limit(1)
            ).scalar_one_or_none()

    def list_send_attempts(self, limit: int) -> list[dict[str, Any]]:
        sql = text(
            "SELECT date, subject, to_email, status, error, created_at, run_id "
            "FROM send_attempts ORDER BY created_at DESC, id DESC LIMIT :lim"
        )
        with self._Session() as s:
            try:
                rows = s.execute(sql, {"lim": int(limit)}).mappings().all()
            except Exception:
                return []
        return [dict(row) for row in rows]
