from __future__ import annotations

from typing import Any

from sqlalchemy import and_, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from app.db.models.rules import (
    DBCompareLog,
    DedupeKey,
    DualWriteFailure,
    ReportArtifact,
    RulesDraft,
    RunExecution,
    SendAttempt,
    SourceFetchEvent,
)


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

    def insert_dual_write_failure(self, *, op_name: str, payload_json: dict[str, Any], error: str, created_at: str) -> int:
        with self._Session() as s:
            row = DualWriteFailure(
                op_name=op_name,
                payload_json=payload_json,
                error=error,
                created_at=created_at,
            )
            s.add(row)
            s.commit()
            return int(row.id)

    def list_dual_write_failures(self, *, limit: int = 100) -> list[DualWriteFailure]:
        with self._Session() as s:
            return list(
                s.execute(
                    select(DualWriteFailure).order_by(DualWriteFailure.id.asc()).limit(max(1, int(limit)))
                ).scalars().all()
            )

    def delete_dual_write_failure(self, failure_id: int) -> None:
        with self._Session() as s:
            row = s.get(DualWriteFailure, int(failure_id))
            if row is None:
                return
            s.delete(row)
            s.commit()

    def count_dual_write_failures(self) -> int:
        with self._Session() as s:
            return int(s.query(DualWriteFailure).count())

    def insert_compare_log(self, *, query_name: str, params_hash: str, diff_summary: str, created_at: str) -> int:
        with self._Session() as s:
            row = DBCompareLog(
                query_name=query_name,
                params_hash=params_hash,
                diff_summary=diff_summary,
                created_at=created_at,
            )
            s.add(row)
            s.commit()
            return int(row.id)

    def count_compare_logs(self) -> int:
        with self._Session() as s:
            return int(s.query(DBCompareLog).count())

    def latest_compare_log_time(self) -> str:
        with self._Session() as s:
            row = s.execute(
                select(DBCompareLog.created_at).order_by(DBCompareLog.id.desc()).limit(1)
            ).scalar_one_or_none()
        return str(row or "")

    def upsert_run_execution(
        self,
        *,
        run_id: str,
        profile: str,
        triggered_by: str,
        window: str,
        status: str,
        started_at: str,
        ended_at: str | None = None,
    ) -> None:
        with self._Session() as s:
            row = s.get(RunExecution, run_id)
            if row is None:
                row = RunExecution(
                    run_id=run_id,
                    run_key=run_id,
                    profile=profile,
                    triggered_by=triggered_by,
                    window=window,
                    status=status,
                    started_at=started_at,
                    ended_at=ended_at,
                )
                s.add(row)
            else:
                row.run_key = run_id
                row.profile = profile
                row.triggered_by = triggered_by
                row.window = window
                row.status = status
                row.started_at = started_at
                row.ended_at = ended_at
            s.commit()

    def finish_run_execution(self, *, run_id: str, status: str, ended_at: str) -> None:
        with self._Session() as s:
            row = s.get(RunExecution, run_id)
            if row is None:
                return
            row.status = status
            row.ended_at = ended_at
            s.commit()

    def insert_source_fetch_event(
        self,
        *,
        run_id: str,
        source_id: str,
        status: str,
        http_status: int | None,
        items_count: int,
        error: str | None,
        duration_ms: int,
    ) -> int:
        with self._Session() as s:
            row = SourceFetchEvent(
                run_id=run_id,
                source_id=source_id,
                status=status,
                http_status=http_status,
                items_count=int(items_count),
                error=error,
                duration_ms=int(duration_ms),
            )
            s.add(row)
            s.commit()
            return int(row.id)

    def insert_report_artifact(
        self,
        *,
        run_id: str,
        artifact_path: str,
        artifact_type: str,
        sha256: str,
        created_at: str,
    ) -> int:
        with self._Session() as s:
            row = ReportArtifact(
                run_id=run_id,
                artifact_path=artifact_path,
                artifact_type=artifact_type,
                sha256=sha256,
                created_at=created_at,
            )
            s.add(row)
            s.commit()
            return int(row.id)

    def recent_run_executions(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._Session() as s:
            rows = list(
                s.execute(
                    select(RunExecution).order_by(RunExecution.started_at.desc(), RunExecution.run_id.desc()).limit(limit)
                ).scalars().all()
            )
        return [
            {
                "run_id": str(r.run_id),
                "profile": str(r.profile),
                "triggered_by": str(r.triggered_by),
                "window": str(r.window or ""),
                "status": str(r.status),
                "started_at": str(r.started_at),
                "ended_at": str(r.ended_at or ""),
            }
            for r in rows
        ]

    def source_fail_top(self, limit: int = 10) -> list[dict[str, Any]]:
        sql = text(
            """
            SELECT source_id, COUNT(1) AS fail_count, MAX(id) AS max_id
            FROM source_fetch_events
            WHERE LOWER(status) IN ('fail','failed','error')
            GROUP BY source_id
            ORDER BY fail_count DESC, max_id DESC
            LIMIT :lim
            """
        )
        with self._Session() as s:
            rows = s.execute(sql, {"lim": int(limit)}).mappings().all()
        return [{"source_id": str(r["source_id"]), "fail_count": int(r["fail_count"])} for r in rows]

    def source_consecutive_failures(self, source_id: str, *, lookback: int = 20) -> int:
        """
        Return consecutive failure count from newest events backward, stopping at first non-fail.
        """
        with self._Session() as s:
            rows = list(
                s.execute(
                    select(SourceFetchEvent.status)
                    .where(SourceFetchEvent.source_id == source_id)
                    .order_by(SourceFetchEvent.id.desc())
                    .limit(max(1, int(lookback)))
                ).scalars().all()
            )
        n = 0
        for st in rows:
            low = str(st or "").strip().lower()
            if low in {"fail", "failed", "error"}:
                n += 1
                continue
            break
        return n

    def upsert_send_attempt(
        self,
        *,
        send_key: str,
        date: str,
        subject: str,
        to_email: str,
        status: str,
        error: str | None,
        created_at: str,
        run_id: str | None,
    ) -> int:
        with self._Session() as s:
            row = s.execute(
                select(SendAttempt).where(SendAttempt.send_key == send_key).limit(1)
            ).scalar_one_or_none()
            if row is None:
                row = SendAttempt(
                    send_key=send_key,
                    date=date,
                    subject=subject,
                    to_email=to_email,
                    status=status,
                    error=error,
                    created_at=created_at,
                    run_id=run_id,
                )
                s.add(row)
            else:
                row.date = date
                row.subject = subject
                row.to_email = to_email
                row.status = status
                row.error = error
                row.created_at = created_at
                row.run_id = run_id
            s.commit()
            return int(row.id)

    def get_send_attempt_success_by_key(self, send_key: str) -> dict[str, Any] | None:
        with self._Session() as s:
            row = s.execute(
                select(SendAttempt)
                .where(and_(SendAttempt.send_key == send_key, SendAttempt.status == "SUCCESS"))
                .order_by(SendAttempt.id.desc())
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            return {
                "id": int(row.id),
                "status": str(row.status),
                "run_id": str(row.run_id or ""),
                "created_at": str(row.created_at),
                "error": str(row.error or ""),
                "send_key": str(row.send_key),
            }

    def insert_dedupe_key(self, *, dedupe_key: str, run_id: str | None, created_at: str) -> bool:
        with self._Session() as s:
            exists = s.get(DedupeKey, dedupe_key)
            if exists is not None:
                return False
            s.add(DedupeKey(dedupe_key=dedupe_key, run_id=run_id, created_at=created_at))
            s.commit()
            return True
