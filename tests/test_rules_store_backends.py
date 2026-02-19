from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from app.services.rules_store import RulesStore


class RulesStoreBackendTests(unittest.TestCase):
    def _env(self, **kwargs: str | None) -> tuple[dict[str, str | None], dict[str, str | None]]:
        before: dict[str, str | None] = {}
        for k, v in kwargs.items():
            before[k] = os.environ.get(k)
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return kwargs, before

    def _restore(self, before: dict[str, str | None]) -> None:
        for k, v in before.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_rules_store_sqlite_url_read_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_url = f"sqlite:///{(root / 'data' / 'rules.db').as_posix()}"
            _, before = self._env(
                DATABASE_URL=db_url,
                DATABASE_URL_SECONDARY=None,
                DB_WRITE_MODE="single",
                DB_READ_MODE="primary",
            )
            try:
                store = RulesStore(root)
                store.create_version(
                    "email_rules",
                    profile="legacy",
                    version="v1",
                    config={"ruleset": "email_rules", "profile": "legacy"},
                    created_by="test",
                    activate=True,
                )
                active = store.get_active_rules("email_rules", "legacy")
                self.assertIsNotNone(active)
                self.assertEqual(active["_store_meta"]["version"], "v1")
            finally:
                self._restore(before)

    @unittest.skipUnless(os.environ.get("TEST_POSTGRES_URL"), "TEST_POSTGRES_URL is not set")
    def test_rules_store_postgres_url_read_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            pg_url = str(os.environ["TEST_POSTGRES_URL"])
            _, before = self._env(
                DATABASE_URL=pg_url,
                DATABASE_URL_SECONDARY=None,
                DB_WRITE_MODE="single",
                DB_READ_MODE="primary",
            )
            try:
                store = RulesStore(root)
                profile = "it_pg"
                store.create_version(
                    "content_rules",
                    profile=profile,
                    version="v1",
                    config={"ruleset": "content_rules", "profile": profile},
                    created_by="test",
                    activate=True,
                )
                active = store.get_active_rules("content_rules", profile)
                self.assertIsNotNone(active)
                self.assertEqual(active["_store_meta"]["version"], "v1")
            finally:
                self._restore(before)


if __name__ == "__main__":
    unittest.main()
