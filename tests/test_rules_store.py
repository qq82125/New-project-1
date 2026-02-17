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

def _qc_cfg(profile: str, version: str) -> dict:
    return {
        "ruleset": "qc_rules",
        "profile": profile,
        "version": version,
        "defaults": {"timezone": "Asia/Shanghai"},
        "overrides": {"enabled": True},
        "rules": [],
        "output": {},
    }


def _output_cfg(profile: str, version: str) -> dict:
    return {
        "ruleset": "output_rules",
        "profile": profile,
        "version": version,
        "defaults": {"format": "plain_text"},
        "overrides": {"enabled": True},
        "rules": [],
        "output": {"sections": ["A", "G"]},
    }

def _scheduler_cfg(profile: str, version: str) -> dict:
    return {
        "ruleset": "scheduler_rules",
        "profile": profile,
        "version": version,
        "defaults": {
            "enabled": True,
            "timezone": "Asia/Singapore",
            "schedules": [
                {
                    "id": "digest_daily_0900",
                    "type": "cron",
                    "cron": "0 9 * * *",
                    "purpose": "digest",
                    "profile": profile,
                    "jitter_seconds": 0,
                }
            ],
            "concurrency": {"max_instances": 1, "coalesce": True, "misfire_grace_seconds": 600},
            "run_policies": {"allow_manual_trigger": True, "pause_switch": True},
            "artifacts": {"retain_days": 14},
        },
        "overrides": {"enabled": True},
        "rules": [],
        "output": {},
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

    def test_qc_and_output_rules_draft_publish_active_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)

            # QC rules: draft -> publish -> active -> rollback
            d1 = store.create_draft(
                "qc_rules",
                "enhanced",
                _qc_cfg("enhanced", "2.0.0"),
                validation_errors=[],
                created_by="tester",
            )
            pub1 = store.publish_draft("qc_rules", d1["id"], "enhanced", created_by="tester")
            self.assertTrue(pub1["ok"])
            active_qc = store.get_active_rules("qc_rules", "enhanced")
            self.assertIsNotNone(active_qc)
            self.assertEqual(active_qc.get("ruleset"), "qc_rules")
            self.assertEqual(active_qc.get("profile"), "enhanced")
            self.assertTrue(str(active_qc["_store_meta"]["version"]).startswith("db-"))

            d2 = store.create_draft(
                "qc_rules",
                "enhanced",
                _qc_cfg("enhanced", "2.0.1"),
                validation_errors=[],
                created_by="tester",
            )
            pub2 = store.publish_draft("qc_rules", d2["id"], "enhanced", created_by="tester")
            self.assertTrue(pub2["ok"])
            active_qc2 = store.get_active_rules("qc_rules", "enhanced")
            self.assertIsNotNone(active_qc2)
            self.assertEqual(active_qc2["_store_meta"]["version"], pub2["version"])

            rb = store.rollback("qc_rules", profile="enhanced")
            self.assertTrue(rb["ok"])
            active_qc3 = store.get_active_rules("qc_rules", "enhanced")
            self.assertIsNotNone(active_qc3)
            self.assertEqual(active_qc3["_store_meta"]["version"], pub1["version"])

            # Output rules: draft -> publish -> active -> rollback
            od1 = store.create_draft(
                "output_rules",
                "enhanced",
                _output_cfg("enhanced", "2.0.0"),
                validation_errors=[],
                created_by="tester",
            )
            opub1 = store.publish_draft("output_rules", od1["id"], "enhanced", created_by="tester")
            self.assertTrue(opub1["ok"])
            active_out = store.get_active_rules("output_rules", "enhanced")
            self.assertIsNotNone(active_out)
            self.assertEqual(active_out.get("ruleset"), "output_rules")
            self.assertEqual(active_out["_store_meta"]["version"], opub1["version"])

            od2 = store.create_draft(
                "output_rules",
                "enhanced",
                _output_cfg("enhanced", "2.0.1"),
                validation_errors=[],
                created_by="tester",
            )
            opub2 = store.publish_draft("output_rules", od2["id"], "enhanced", created_by="tester")
            self.assertTrue(opub2["ok"])
            rb2 = store.rollback("output_rules", profile="enhanced")
            self.assertTrue(rb2["ok"])
            active_out2 = store.get_active_rules("output_rules", "enhanced")
            self.assertIsNotNone(active_out2)
            self.assertEqual(active_out2["_store_meta"]["version"], opub1["version"])

    def test_scheduler_rules_draft_publish_active_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)

            d1 = store.create_draft(
                "scheduler_rules",
                "enhanced",
                _scheduler_cfg("enhanced", "2.0.0"),
                validation_errors=[],
                created_by="tester",
            )
            pub1 = store.publish_draft("scheduler_rules", d1["id"], "enhanced", created_by="tester")
            self.assertTrue(pub1["ok"])
            active1 = store.get_active_rules("scheduler_rules", "enhanced")
            self.assertIsNotNone(active1)
            self.assertEqual(active1.get("ruleset"), "scheduler_rules")
            self.assertEqual(active1.get("profile"), "enhanced")
            self.assertEqual(active1["_store_meta"]["version"], pub1["version"])

            d2 = store.create_draft(
                "scheduler_rules",
                "enhanced",
                _scheduler_cfg("enhanced", "2.0.1"),
                validation_errors=[],
                created_by="tester",
            )
            pub2 = store.publish_draft("scheduler_rules", d2["id"], "enhanced", created_by="tester")
            self.assertTrue(pub2["ok"])
            active2 = store.get_active_rules("scheduler_rules", "enhanced")
            self.assertIsNotNone(active2)
            self.assertEqual(active2["_store_meta"]["version"], pub2["version"])

            rb = store.rollback("scheduler_rules", profile="enhanced")
            self.assertTrue(rb["ok"])
            active3 = store.get_active_rules("scheduler_rules", "enhanced")
            self.assertIsNotNone(active3)
            self.assertEqual(active3["_store_meta"]["version"], pub1["version"])

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
