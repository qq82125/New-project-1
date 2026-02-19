from __future__ import annotations

import os
from pathlib import Path

from app.db.config import get_db_settings
from app.db.config import redact_database_url
from app.services.rules_store_sa import SQLAlchemyRulesStore


def _sqlite_url_from_path(path: Path) -> str:
    return f"sqlite:///{path.resolve().as_posix()}"


class RulesStore(SQLAlchemyRulesStore):
    """
    Backward-compatible facade: keep constructor and method surface stable while
    always using SQLAlchemy-backed repositories under the hood.
    """

    def __init__(self, project_root: Path, db_path: Path | None = None, auto_init: bool = True) -> None:
        settings = get_db_settings()
        env_database_url = os.environ.get("DATABASE_URL", "").strip()
        if db_path is not None:
            database_url = _sqlite_url_from_path(db_path)
        elif env_database_url:
            database_url = env_database_url
        else:
            database_url = _sqlite_url_from_path(project_root / "data" / "rules.db")

        super().__init__(
            project_root=project_root,
            database_url=database_url,
            auto_init=auto_init,
            write_mode=settings.db_write_mode,
            read_mode=settings.db_read_mode,
            secondary_url=settings.database_url_secondary,
        )

    @property
    def db_url(self) -> str:
        return self.database_url

    @property
    def db_url_redacted(self) -> str:
        return redact_database_url(self.database_url)
