from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from app.services.rules_store import RulesStore


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


class DBDualShadowTests(unittest.TestCase):
    def _set_env(self, **kwargs: str | None) -> dict[str, str | None]:
        old: dict[str, str | None] = {}
        for k, v in kwargs.items():
            old[k] = os.environ.get(k)
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return old

    def _restore(self, old: dict[str, str | None]) -> None:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_dual_secondary_failure_does_not_break_primary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            primary = root / "data" / "primary.db"
            secondary = root / "data" / "secondary.db"
            old = self._set_env(
                DATABASE_URL=_sqlite_url(primary),
                DATABASE_URL_SECONDARY=_sqlite_url(secondary),
                DB_WRITE_MODE="dual",
                DB_READ_MODE="primary",
                DB_DUAL_STRICT="false",
                DB_SHADOW_COMPARE_RATE="1",
            )
            try:
                store = RulesStore(root)
                if getattr(store, "_secondary_store", None) is None:
                    self.fail("expected secondary store in dual mode")

                def _raise(*args, **kwargs):  # type: ignore[no-untyped-def]
                    raise RuntimeError("secondary write failed for test")

                # Inject secondary failure.
                setattr(store._secondary_store, "create_draft", _raise)  # type: ignore[attr-defined]

                out = store.create_draft(
                    "email_rules",
                    "legacy",
                    config_json={"ruleset": "email_rules", "profile": "legacy"},
                    validation_errors=[],
                    created_by="tester",
                )
                self.assertTrue(int(out["id"]) > 0)

                st = store.db_status()
                self.assertGreaterEqual(int(st.get("dual_write_failures", 0)), 1)
            finally:
                self._restore(old)

    def test_shadow_compare_logs_diff(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            primary = root / "data" / "primary.db"
            secondary = root / "data" / "secondary.db"
            old = self._set_env(
                DATABASE_URL=_sqlite_url(primary),
                DATABASE_URL_SECONDARY=_sqlite_url(secondary),
                DB_WRITE_MODE="single",
                DB_READ_MODE="shadow_compare",
                DB_SHADOW_COMPARE_RATE="1",
            )
            try:
                store = RulesStore(root)
                secondary_store = getattr(store, "_secondary_store", None)
                if secondary_store is None:
                    self.fail("expected secondary store in shadow compare mode")

                store.create_version(
                    "email_rules",
                    profile="legacy",
                    version="v1",
                    config={"ruleset": "email_rules", "profile": "legacy", "subject": "A"},
                    created_by="tester",
                    activate=True,
                )
                secondary_store.create_version(
                    "email_rules",
                    profile="legacy",
                    version="v2",
                    config={"ruleset": "email_rules", "profile": "legacy", "subject": "B"},
                    created_by="tester2",
                    activate=True,
                )

                _ = store.get_active_rules("email_rules", "legacy")
                st = store.db_status()
                self.assertGreaterEqual(int(st.get("compare_diff_count", 0)), 1)
                self.assertTrue(str(st.get("last_compare_diff_at", "")))
            finally:
                self._restore(old)


if __name__ == "__main__":
    unittest.main()
