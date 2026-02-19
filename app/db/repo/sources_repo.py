from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.db.models.rules import Source


class SourcesRepo:
    def __init__(self, session_factory: sessionmaker[Session]):
        self._Session = session_factory

    @staticmethod
    def _to_dict(row: Source) -> dict[str, Any]:
        return {
            "id": str(row.id),
            "name": str(row.name),
            "connector": str(row.connector),
            "url": str(row.url or ""),
            "enabled": bool(int(row.enabled)),
            "priority": int(row.priority),
            "trust_tier": str(row.trust_tier),
            "tags": row.tags_json if isinstance(row.tags_json, list) else [],
            "rate_limit": row.rate_limit_json if isinstance(row.rate_limit_json, dict) else {},
            "fetch": row.fetch_json if isinstance(row.fetch_json, dict) else {},
            "parsing": row.parsing_json if isinstance(row.parsing_json, dict) else {},
            "created_at": str(row.created_at),
            "updated_at": str(row.updated_at),
            "last_fetched_at": str(row.last_fetched_at or ""),
            "last_fetch_status": str(row.last_fetch_status or ""),
            "last_fetch_http_status": int(row.last_fetch_http_status) if row.last_fetch_http_status is not None else None,
            "last_fetch_error": str(row.last_fetch_error or ""),
            "last_success_at": str(row.last_success_at or ""),
            "last_http_status": int(row.last_http_status) if row.last_http_status is not None else None,
            "last_error": str(row.last_error or ""),
        }

    def upsert_many(self, sources: list[dict[str, Any]], *, replace: bool, now: str) -> int:
        with self._Session() as s:
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
                return int(s.query(Source).count())
            except Exception:
                s.rollback()
                raise

    def list(self, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        with self._Session() as s:
            q = select(Source).order_by(Source.priority.desc(), Source.id.asc())
            if enabled_only:
                q = q.where(Source.enabled == 1)
            rows = list(s.execute(q).scalars().all())
        return [self._to_dict(r) for r in rows]

    def get(self, source_id: str) -> dict[str, Any] | None:
        with self._Session() as s:
            row = s.get(Source, source_id)
            return self._to_dict(row) if row is not None else None

    def url_exists(self, url: str, *, exclude_id: str | None = None) -> bool:
        with self._Session() as s:
            q = select(Source.id).where(Source.url == url)
            if exclude_id:
                q = q.where(Source.id != exclude_id)
            return s.execute(q.limit(1)).first() is not None

    def upsert_one(self, source: dict[str, Any], *, now: str) -> dict[str, Any]:
        sid = str(source.get("id", "")).strip()
        if not sid:
            raise RuntimeError("source id required")
        with self._Session() as s:
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
                return self._to_dict(row)
            except Exception:
                s.rollback()
                raise

    def record_test(self, source_id: str, *, now: str, ok: bool, http_status: int | None, error: str | None) -> None:
        with self._Session() as s:
            row = s.get(Source, source_id)
            if row is None:
                return
            if ok:
                row.last_success_at = now
                row.last_http_status = http_status
                row.last_error = None
            else:
                row.last_http_status = http_status
                row.last_error = str(error or "")
            row.updated_at = now
            s.commit()

    def record_fetch(
        self,
        source_id: str,
        *,
        now: str,
        status: str,
        http_status: int | None,
        error: str | None,
        keep_last_fetched: bool,
    ) -> None:
        with self._Session() as s:
            row = s.get(Source, source_id)
            if row is None:
                return
            if not keep_last_fetched:
                row.last_fetched_at = now
            row.last_fetch_status = str(status or "")
            row.last_fetch_http_status = int(http_status) if http_status is not None else None
            row.last_fetch_error = str(error or "") if error else None
            row.updated_at = now
            s.commit()
