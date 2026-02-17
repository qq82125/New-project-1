from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from app.rules.engine import RuleEngine
from app.rules.errors import RuleEngineError


class RuleEngineTests(unittest.TestCase):
    def test_load_pair_defaults_backward_compatible(self) -> None:
        engine = RuleEngine()
        email, content = engine.load_pair()
        self.assertEqual(email.profile, "default.v1")
        self.assertEqual(content.profile, "default.v1")
        self.assertEqual(email.ruleset, "email_rules")
        self.assertEqual(content.ruleset, "content_rules")

    def test_load_pair_fallback_on_missing_profile(self) -> None:
        engine = RuleEngine()
        email, content = engine.load_pair(
            email_profile="does-not-exist",
            content_profile="does-not-exist",
            fallback_on_missing=True,
        )
        self.assertEqual(email.profile, "default.v1")
        self.assertEqual(content.profile, "default.v1")

    def test_load_pair_no_fallback_raises(self) -> None:
        engine = RuleEngine()
        with self.assertRaises(RuleEngineError) as ctx:
            engine.load_pair(
                email_profile="does-not-exist",
                content_profile="default.v1",
                fallback_on_missing=False,
            )
        self.assertEqual(ctx.exception.err.code, "RULES_002_PROFILE_NOT_FOUND")

    def test_ruleset_mismatch_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "rules" / "email_rules").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "content_rules").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "schemas").mkdir(parents=True, exist_ok=True)

            repo_root = Path(__file__).resolve().parents[1]
            shutil.copy(
                repo_root / "rules" / "schemas" / "email_rules.schema.json",
                root / "rules" / "schemas" / "email_rules.schema.json",
            )
            shutil.copy(
                repo_root / "rules" / "schemas" / "content_rules.schema.json",
                root / "rules" / "schemas" / "content_rules.schema.json",
            )

            bad = {
                "ruleset": "content_rules",
                "version": "1.0.0",
                "profile": "bad.v1",
                "feature_flags": {"enable_new_content_rules": False},
                "compatibility": {
                    "backward_compatible": True,
                    "fallback_to_legacy_on_error": True,
                },
                "collection": {
                    "date_tz": "Asia/Shanghai",
                    "primary_window_hours": 24,
                    "fallback_window_days": 7,
                    "min_items": 8,
                    "max_items": 15,
                    "topup_if_24h_lt": 10,
                },
                "coverage": {
                    "tracks": ["x"],
                    "platforms": ["x"],
                    "event_types": ["x"],
                    "region_targets": {"apac_min_share": 0.4},
                },
                "quality_gates": {
                    "dedupe": {
                        "daily_max_repeat_rate": 0.25,
                        "recent_7d_max_repeat_rate": 0.4,
                    },
                    "source_requirements": {"must_check": ["NMPA"]},
                },
                "sources": {"feed_timeout_sec": 10},
                "output": {"sections": ["A"], "quality_section_only_at_end": True},
            }
            (root / "rules" / "email_rules" / "bad.v1.json").write_text(
                json.dumps(bad),
                encoding="utf-8",
            )

            engine = RuleEngine(project_root=root)
            with self.assertRaises(RuleEngineError) as ctx:
                engine.load("email_rules", "bad.v1")
            self.assertEqual(ctx.exception.err.code, "RULES_003_RULESET_MISMATCH")


if __name__ == "__main__":
    unittest.main()
