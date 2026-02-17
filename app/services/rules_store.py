from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RulesStore:
    def __init__(self, project_root: Path, db_path: Path | None = None, auto_init: bool = True) -> None:
        self.project_root = project_root
        self.db_path = db_path or (project_root / "data" / "rules.db")
        if auto_init:
            self.ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS email_rules_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile TEXT NOT NULL,
                    version TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 0
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uq_email_rules_profile_version
                    ON email_rules_versions(profile, version);
                CREATE INDEX IF NOT EXISTS idx_email_rules_profile_active
                    ON email_rules_versions(profile, is_active, id);

                CREATE TABLE IF NOT EXISTS content_rules_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile TEXT NOT NULL,
                    version TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 0
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uq_content_rules_profile_version
                    ON content_rules_versions(profile, version);
                CREATE INDEX IF NOT EXISTS idx_content_rules_profile_active
                    ON content_rules_versions(profile, is_active, id);

                CREATE TABLE IF NOT EXISTS qc_rules_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile TEXT NOT NULL,
                    version TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 0
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uq_qc_rules_profile_version
                    ON qc_rules_versions(profile, version);
                CREATE INDEX IF NOT EXISTS idx_qc_rules_profile_active
                    ON qc_rules_versions(profile, is_active, id);

                CREATE TABLE IF NOT EXISTS output_rules_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile TEXT NOT NULL,
                    version TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 0
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uq_output_rules_profile_version
                    ON output_rules_versions(profile, version);
                CREATE INDEX IF NOT EXISTS idx_output_rules_profile_active
                    ON output_rules_versions(profile, is_active, id);

                CREATE TABLE IF NOT EXISTS sources (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    connector TEXT NOT NULL,
                    url TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    priority INTEGER NOT NULL DEFAULT 0,
                    trust_tier TEXT NOT NULL,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    rate_limit_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_success_at TEXT,
                    last_http_status INTEGER,
                    last_error TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_sources_enabled_priority
                    ON sources(enabled, priority DESC);

                CREATE TABLE IF NOT EXISTS rules_drafts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ruleset TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    validation_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_rules_drafts_lookup
                    ON rules_drafts(ruleset, profile, id DESC);
                """
            )
            # Backward-compatible migrations for pre-existing DBs.
            cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(sources)").fetchall()}
            if "last_success_at" not in cols:
                conn.execute("ALTER TABLE sources ADD COLUMN last_success_at TEXT")
            if "last_http_status" not in cols:
                conn.execute("ALTER TABLE sources ADD COLUMN last_http_status INTEGER")
            if "last_error" not in cols:
                conn.execute("ALTER TABLE sources ADD COLUMN last_error TEXT")
            conn.commit()

    def _table_name(self, ruleset: str) -> str:
        if ruleset == "email_rules":
            return "email_rules_versions"
        if ruleset == "content_rules":
            return "content_rules_versions"
        if ruleset == "qc_rules":
            return "qc_rules_versions"
        if ruleset == "output_rules":
            return "output_rules_versions"
        raise ValueError(f"unsupported ruleset={ruleset}")

    def _decode_config(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        obj = json.loads(str(row["config_json"]))
        if isinstance(obj, dict):
            obj.setdefault("_store_meta", {})
            obj["_store_meta"] = {
                "id": int(row["id"]),
                "profile": str(row["profile"]),
                "version": str(row["version"]),
                "created_at": str(row["created_at"]),
                "created_by": str(row["created_by"]),
                "is_active": bool(int(row["is_active"])),
            }
        return obj

    def has_any_versions(self, ruleset: str) -> bool:
        table = self._table_name(ruleset)
        with self._connect() as conn:
            row = conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            return row is not None

    def get_active_email_rules(self, profile: str) -> dict[str, Any] | None:
        return self.get_active_rules("email_rules", profile)

    def get_active_content_rules(self, profile: str) -> dict[str, Any] | None:
        return self.get_active_rules("content_rules", profile)

    def get_active_rules(self, ruleset: str, profile: str) -> dict[str, Any] | None:
        return self._get_active(ruleset, profile)

    def _get_active(self, ruleset: str, profile: str) -> dict[str, Any] | None:
        table = self._table_name(ruleset)
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT * FROM {table}
                WHERE profile = ? AND is_active = 1
                ORDER BY id DESC LIMIT 1
                """,
                (profile,),
            ).fetchone()
            return self._decode_config(row)

    def list_versions(
        self,
        ruleset: str,
        profile: str | None = None,
        *,
        active_only: bool = False,
    ) -> list[dict[str, Any]]:
        table = self._table_name(ruleset)
        where: list[str] = []
        params: list[Any] = []
        if profile:
            where.append("profile = ?")
            params.append(profile)
        if active_only:
            where.append("is_active = 1")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        sql = f"""
            SELECT id, profile, version, created_at, created_by, is_active
            FROM {table}
            {where_sql}
            ORDER BY id DESC
        """
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
            return [
                {
                    "id": int(r["id"]),
                    "ruleset": ruleset,
                    "profile": str(r["profile"]),
                    "version": str(r["version"]),
                    "created_at": str(r["created_at"]),
                    "created_by": str(r["created_by"]),
                    "is_active": bool(int(r["is_active"])),
                }
                for r in rows
            ]

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
        table = self._table_name(ruleset)
        now = _utc_now()
        payload = json.dumps(config, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute("BEGIN")
            if activate:
                conn.execute(f"UPDATE {table} SET is_active = 0 WHERE profile = ?", (profile,))
            conn.execute(
                f"""
                INSERT INTO {table}(profile, version, config_json, created_at, created_by, is_active)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile, version)
                DO UPDATE SET
                    config_json = excluded.config_json,
                    created_at = excluded.created_at,
                    created_by = excluded.created_by,
                    is_active = excluded.is_active
                """,
                (profile, version, payload, now, created_by, 1 if activate else 0),
            )
            conn.commit()
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
        table = self._table_name(ruleset)
        with self._connect() as conn:
            conn.execute("BEGIN")
            row = conn.execute(
                f"SELECT id FROM {table} WHERE profile = ? AND version = ? LIMIT 1",
                (profile, version),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise RuntimeError(f"version not found: ruleset={ruleset} profile={profile} version={version}")
            conn.execute(f"UPDATE {table} SET is_active = 0 WHERE profile = ?", (profile,))
            conn.execute(
                f"UPDATE {table} SET is_active = 1 WHERE profile = ? AND version = ?",
                (profile, version),
            )
            conn.commit()
        return {"ok": True, "ruleset": ruleset, "profile": profile, "version": version, "is_active": True}

    def rollback(self, ruleset: str, *, profile: str) -> dict[str, Any]:
        table = self._table_name(ruleset)
        with self._connect() as conn:
            current = conn.execute(
                f"SELECT id, version FROM {table} WHERE profile = ? AND is_active = 1 ORDER BY id DESC LIMIT 1",
                (profile,),
            ).fetchone()
            if current is None:
                raise RuntimeError(f"no active version: ruleset={ruleset} profile={profile}")

            previous = conn.execute(
                f"""
                SELECT id, version FROM {table}
                WHERE profile = ? AND id < ?
                ORDER BY id DESC LIMIT 1
                """,
                (profile, int(current["id"])),
            ).fetchone()
            if previous is None:
                raise RuntimeError(f"no previous version to rollback: ruleset={ruleset} profile={profile}")

            conn.execute("BEGIN")
            conn.execute(f"UPDATE {table} SET is_active = 0 WHERE profile = ?", (profile,))
            conn.execute(f"UPDATE {table} SET is_active = 1 WHERE id = ?", (int(previous["id"]),))
            conn.commit()
        return {
            "ok": True,
            "ruleset": ruleset,
            "profile": profile,
            "active_version": str(previous["version"]),
            "previous_version": str(current["version"]),
        }

    def upsert_sources(self, sources: list[dict[str, Any]], *, replace: bool = True) -> dict[str, Any]:
        now = _utc_now()
        with self._connect() as conn:
            conn.execute("BEGIN")
            if replace:
                conn.execute("DELETE FROM sources")
            for src in sources:
                sid = str(src.get("id", "")).strip()
                if not sid:
                    continue
                conn.execute(
                    """
                    INSERT INTO sources(
                        id, name, connector, url, enabled, priority, trust_tier,
                        tags_json, rate_limit_json, created_at, updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name = excluded.name,
                        connector = excluded.connector,
                        url = excluded.url,
                        enabled = excluded.enabled,
                        priority = excluded.priority,
                        trust_tier = excluded.trust_tier,
                        tags_json = excluded.tags_json,
                        rate_limit_json = excluded.rate_limit_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        sid,
                        str(src.get("name", sid)),
                        str(src.get("connector", "")),
                        str(src.get("url", "")),
                        1 if bool(src.get("enabled", True)) else 0,
                        int(src.get("priority", 0) or 0),
                        str(src.get("trust_tier", "C")),
                        json.dumps(src.get("tags", []), ensure_ascii=False),
                        json.dumps(src.get("rate_limit", {}), ensure_ascii=False),
                        now,
                        now,
                    ),
                )
            conn.commit()
            count_row = conn.execute("SELECT COUNT(1) AS c FROM sources").fetchone()
        return {"ok": True, "source_count": int(count_row["c"]) if count_row else 0}

    def list_sources(self, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM sources"
        params: tuple[Any, ...] = ()
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY priority DESC, id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "id": str(r["id"]),
                        "name": str(r["name"]),
                        "connector": str(r["connector"]),
                        "url": str(r["url"] or ""),
                        "enabled": bool(int(r["enabled"])),
                        "priority": int(r["priority"]),
                        "trust_tier": str(r["trust_tier"]),
                        "tags": json.loads(str(r["tags_json"] or "[]")),
                        "rate_limit": json.loads(str(r["rate_limit_json"] or "{}")),
                        "created_at": str(r["created_at"]),
                        "updated_at": str(r["updated_at"]),
                        "last_success_at": str(r["last_success_at"] or ""),
                        "last_http_status": int(r["last_http_status"]) if r["last_http_status"] is not None else None,
                        "last_error": str(r["last_error"] or ""),
                    }
                )
            return out

    def get_source(self, source_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            r = conn.execute("SELECT * FROM sources WHERE id = ? LIMIT 1", (source_id,)).fetchone()
            if r is None:
                return None
            return {
                "id": str(r["id"]),
                "name": str(r["name"]),
                "connector": str(r["connector"]),
                "url": str(r["url"] or ""),
                "enabled": bool(int(r["enabled"])),
                "priority": int(r["priority"]),
                "trust_tier": str(r["trust_tier"]),
                "tags": json.loads(str(r["tags_json"] or "[]")),
                "rate_limit": json.loads(str(r["rate_limit_json"] or "{}")),
                "created_at": str(r["created_at"]),
                "updated_at": str(r["updated_at"]),
                "last_success_at": str(r["last_success_at"] or ""),
                "last_http_status": int(r["last_http_status"]) if r["last_http_status"] is not None else None,
                "last_error": str(r["last_error"] or ""),
            }

    def source_url_exists(self, url: str, *, exclude_id: str | None = None) -> bool:
        with self._connect() as conn:
            if exclude_id:
                r = conn.execute(
                    "SELECT 1 FROM sources WHERE url = ? AND id <> ? LIMIT 1",
                    (url, exclude_id),
                ).fetchone()
            else:
                r = conn.execute("SELECT 1 FROM sources WHERE url = ? LIMIT 1", (url,)).fetchone()
            return r is not None

    def upsert_source(self, source: dict[str, Any]) -> dict[str, Any]:
        sid = str(source.get("id", "")).strip()
        if not sid:
            raise RuntimeError("source id required")
        now = _utc_now()
        existing = self.get_source(sid)
        created_at = str(existing.get("created_at")) if existing else now
        last_success_at = str(source.get("last_success_at") or (existing.get("last_success_at") if existing else "") or "")
        last_http_status = source.get("last_http_status")
        if last_http_status is None and existing is not None:
            last_http_status = existing.get("last_http_status")
        last_error = str(source.get("last_error") or (existing.get("last_error") if existing else "") or "")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sources(
                    id, name, connector, url, enabled, priority, trust_tier,
                    tags_json, rate_limit_json, created_at, updated_at,
                    last_success_at, last_http_status, last_error
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    connector = excluded.connector,
                    url = excluded.url,
                    enabled = excluded.enabled,
                    priority = excluded.priority,
                    trust_tier = excluded.trust_tier,
                    tags_json = excluded.tags_json,
                    rate_limit_json = excluded.rate_limit_json,
                    updated_at = excluded.updated_at
                """,
                (
                    sid,
                    str(source.get("name", sid)),
                    str(source.get("connector", "")),
                    str(source.get("url", "")),
                    1 if bool(source.get("enabled", True)) else 0,
                    int(source.get("priority", 0) or 0),
                    str(source.get("trust_tier", "C")),
                    json.dumps(source.get("tags", []), ensure_ascii=False),
                    json.dumps(source.get("rate_limit", {}), ensure_ascii=False),
                    created_at,
                    now,
                    last_success_at or None,
                    int(last_http_status) if last_http_status is not None else None,
                    last_error or None,
                ),
            )
            conn.commit()
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
        with self._connect() as conn:
            conn.execute("BEGIN")
            if ok:
                conn.execute(
                    "UPDATE sources SET last_success_at = ?, last_http_status = ?, last_error = NULL, updated_at = ? WHERE id = ?",
                    (now, http_status, now, source_id),
                )
            else:
                conn.execute(
                    "UPDATE sources SET last_http_status = ?, last_error = ?, updated_at = ? WHERE id = ?",
                    (http_status, str(error or ""), now, source_id),
                )
            conn.commit()

    def toggle_source(self, source_id: str, enabled: bool | None = None) -> dict[str, Any]:
        source = self.get_source(source_id)
        if source is None:
            raise RuntimeError(f"source not found: {source_id}")
        new_enabled = (not bool(source["enabled"])) if enabled is None else bool(enabled)
        source["enabled"] = new_enabled
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
        # Backward-compatible alias for older call sites.
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = _utc_now()
        if config_json is None:
            config_json = config or {}
        payload = json.dumps(config_json, ensure_ascii=False)
        vjson = json.dumps(validation_errors or [], ensure_ascii=False)
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO rules_drafts(
                    ruleset, profile, config_json, validation_json, created_at, created_by
                )
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (ruleset, profile, payload, vjson, now, created_by),
            )
            conn.commit()
            did = int(cur.lastrowid)
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
        """
        Publish a draft as a new active version.

        Note: This method does not perform schema validation; callers should validate prior to publishing.
        """
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
        # Ensure uniqueness if draft_id also collides (rare, but deterministic).
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
        return {"ok": True, "published": True, "ruleset": ruleset, "profile": profile, "version": out["version"], "draft_id": int(draft_id)}

    def get_draft(
        self,
        *,
        ruleset: str,
        profile: str,
        draft_id: int | None = None,
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            if draft_id is not None:
                row = conn.execute(
                    """
                    SELECT * FROM rules_drafts
                    WHERE id = ? AND ruleset = ? AND profile = ?
                    LIMIT 1
                    """,
                    (draft_id, ruleset, profile),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM rules_drafts
                    WHERE ruleset = ? AND profile = ?
                    ORDER BY id DESC LIMIT 1
                    """,
                    (ruleset, profile),
                ).fetchone()
            if row is None:
                return None
            return {
                "id": int(row["id"]),
                "ruleset": str(row["ruleset"]),
                "profile": str(row["profile"]),
                "config_json": json.loads(str(row["config_json"])),
                "validation_errors": json.loads(str(row["validation_json"] or "[]")),
                "created_at": str(row["created_at"]),
                "created_by": str(row["created_by"]),
            }
