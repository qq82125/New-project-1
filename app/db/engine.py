from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import Engine, create_engine

from app.db.config import get_db_settings


def _engine_options_for_url(url: str) -> dict[str, Any]:
    u = (url or "").strip().lower()
    if u.startswith("postgresql"):
        return {
            "pool_pre_ping": True,
            "pool_size": 5,
            "max_overflow": 10,
            "future": True,
        }
    # SQLite keeps args minimal to avoid compatibility surprises.
    return {"future": True}


def make_engine(url: str, *, extra_options: Mapping[str, Any] | None = None) -> Engine:
    options = _engine_options_for_url(url)
    if extra_options:
        options.update(dict(extra_options))
    return create_engine(url, **options)


def get_primary_engine() -> Engine:
    s = get_db_settings()
    return make_engine(s.database_url)


def get_secondary_engine() -> Engine | None:
    s = get_db_settings()
    if not s.database_url_secondary:
        return None
    return make_engine(s.database_url_secondary)
