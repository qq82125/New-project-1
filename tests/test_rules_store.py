from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.rules_store import RulesStore


def _email_cfg(profile: str, version: str) -> dict:
    return {
        "ruleset": "email_rules",
        "profile": profile,
        "version": version,
        "defaults": {
            "timezone": "Asia/Shanghai",
            "subject_template": "全球IVD晨报 - {{date}}",
            "recipient": "qq82125@gmail.com",
            "send_window": {"hour": 8, "minute": 30},
            "retry": {"max_retries": 3, "connect_timeout_sec": 10, "max_time_sec": 60},
        },
        "overrides": {"enabled": False},
        "rules": [{"id": "x", "enabled": True, "priority": 1, "type": "subject_template", "params": {"template": "x"}}],
        "output": {"format": "plain_text", "sections": ["A"], "summary_max_chars": 100, "charts_enabled": False},
    }


class RulesStoreTests(unittest.TestCase):
    def test_create_activate_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)

            store.create_version(
                "email_rules",
                profile="legacy",
                version="v0001",
                config=_email_cfg("legacy", "1.0.0"),
                created_by="tester",
                activate=True,
            )
            store.create_version(
                "email_rules",
                profile="legacy",
                version="v0002",
                config=_email_cfg("legacy", "1.0.1"),
                created_by="tester",
                activate=True,
            )

            active = store.get_active_email_rules("legacy")
            self.assertIsNotNone(active)
            self.assertEqual(active["_store_meta"]["version"], "v0002")

            rb = store.rollback("email_rules", profile="legacy")
            self.assertTrue(rb["ok"])
            self.assertEqual(rb["active_version"], "v0001")

            active2 = store.get_active_email_rules("legacy")
            self.assertIsNotNone(active2)
            self.assertEqual(active2["_store_meta"]["version"], "v0001")

    def test_sources_upsert(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)
            out = store.upsert_sources(
                [
                    {
                        "id": "reuters-health",
                        "name": "Reuters Health",
                        "connector": "rss",
                        "url": "https://example.com/rss",
                        "enabled": True,
                        "priority": 100,
                        "trust_tier": "A",
                        "tags": ["global", "news"],
                        "rate_limit": {"rps": 1, "burst": 2},
                    }
                ]
            )
            self.assertTrue(out["ok"])
            rows = store.list_sources()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["id"], "reuters-health")


if __name__ == "__main__":
    unittest.main()
