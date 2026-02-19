from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import make_url


DEFAULT_SQLITE_PATH = Path("data") / "rules.db"
DEFAULT_DATABASE_URL = f"sqlite:///{DEFAULT_SQLITE_PATH.as_posix()}"


@dataclass(frozen=True)
class DBSettings:
    database_url: str
    database_url_secondary: str | None
    db_write_mode: str
    db_read_mode: str


ALLOWED_WRITE_MODES = {"single", "dual"}
ALLOWED_READ_MODES = {"primary", "shadow_compare"}


def _normalize_write_mode(v: str) -> str:
    vv = (v or "single").strip().lower()
    return vv if vv in ALLOWED_WRITE_MODES else "single"


def _normalize_read_mode(v: str) -> str:
    vv = (v or "primary").strip().lower()
    return vv if vv in ALLOWED_READ_MODES else "primary"


def get_db_settings() -> DBSettings:
    database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL).strip() or DEFAULT_DATABASE_URL
    database_url_secondary = os.environ.get("DATABASE_URL_SECONDARY", "").strip() or None
    db_write_mode = _normalize_write_mode(os.environ.get("DB_WRITE_MODE", "single"))
    db_read_mode = _normalize_read_mode(os.environ.get("DB_READ_MODE", "primary"))
    return DBSettings(
        database_url=database_url,
        database_url_secondary=database_url_secondary,
        db_write_mode=db_write_mode,
        db_read_mode=db_read_mode,
    )


def redact_database_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    try:
        return make_url(raw).render_as_string(hide_password=True)
    except Exception:
        return raw
