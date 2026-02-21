from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.adapters.rule_bridge import requested_profile, should_use_enhanced, load_runtime_rules


class RuleBridgeTests(unittest.TestCase):
    def test_default_profile_is_legacy(self) -> None:
        self.assertFalse(should_use_enhanced({}))
        self.assertEqual(requested_profile({}), "legacy")

    def test_enhanced_env_profile(self) -> None:
        env = {"ENHANCED_RULES_PROFILE": "enhanced"}
        self.assertTrue(should_use_enhanced(env))
        self.assertEqual(requested_profile(env), "enhanced")

    @patch("app.adapters.rule_bridge.RuleEngine")
    def test_fallback_to_legacy_when_enhanced_load_fails(self, mock_engine_cls) -> None:
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine

        def side_effect(profile: str):
            if profile == "enhanced":
                raise RuntimeError("boom")
            return {
                "rules_version": {"email": "1.0.0", "content": "1.0.0"},
                "content_decision": {"allow_sources": []},
                "email_decision": {"subject_template": "全球IVD晨报 - {{date}}"},
            }

        mock_engine.build_decision.side_effect = side_effect
        out = load_runtime_rules(date_str="2026-02-16", env={"ENHANCED_RULES_PROFILE": "enhanced"})
        self.assertTrue(out["enabled"])
        self.assertEqual(out["requested_profile"], "enhanced")
        self.assertEqual(out["active_profile"], "legacy")
        self.assertIn("track_routing", out)
        self.assertIn("track_routing_gaps", out)

    @patch("app.adapters.rule_bridge.RuleEngine")
    def test_boundary_violation_fallback_to_legacy(self, mock_engine_cls) -> None:
        mock_engine = MagicMock()
        mock_engine_cls.return_value = mock_engine

        enhanced_bad = {
            "rules_version": {"email": "2.0.0", "content": "2.0.0"},
            "content_decision": {
                "allow_sources": [],
                "subject_template": "BAD-CROSS-FIELD",
            },
            "email_decision": {
                "subject_template": "全球IVD晨报 - {{date}}",
            },
        }
        legacy_ok = {
            "rules_version": {"email": "1.0.0", "content": "1.0.0"},
            "content_decision": {
                "allow_sources": [],
                "dedupe_window": {"primary_hours": 24},
                "item_limit": {"min": 8, "max": 15, "topup_if_24h_lt": 10},
                "region_filter": {"apac_min_share": 0.4},
                "keyword_sets": {"include_keywords": [], "exclude_keywords": []},
                "categories_map": {},
            },
            "email_decision": {
                "subject_template": "全球IVD晨报 - {{date}}",
                "sections": ["A", "B", "C", "D", "E", "F", "G"],
                "recipients": ["qq82125@gmail.com"],
                "schedule": {"timezone": "Asia/Shanghai", "hour": 8, "minute": 30},
                "thresholds": {},
            },
        }

        def side_effect(profile: str):
            return enhanced_bad if profile == "enhanced" else legacy_ok

        mock_engine.build_decision.side_effect = side_effect
        out = load_runtime_rules(date_str="2026-02-16", env={"ENHANCED_RULES_PROFILE": "enhanced"})
        self.assertTrue(out["enabled"])
        self.assertEqual(out["active_profile"], "legacy")
        self.assertEqual(out["email"]["subject"], "全球IVD晨报 - 2026-02-16")
        self.assertIn("track_routing", out)
        self.assertIn("track_routing_gaps", out)


if __name__ == "__main__":
    unittest.main()
