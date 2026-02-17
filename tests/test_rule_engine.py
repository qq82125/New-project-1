from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from app.rules.engine import RuleEngine
from app.rules.errors import RuleEngineError
from app.services.rules_store import RulesStore


def _minimal_email(profile: str = "legacy", rules: list[dict] | None = None) -> dict:
    return {
        "ruleset": "email_rules",
        "version": "1.0.0",
        "profile": profile,
        "defaults": {
            "timezone": "Asia/Shanghai",
            "subject_template": "全球IVD晨报 - {{date}}",
            "recipient": "qq82125@gmail.com",
            "sender_env": "SMTP_FROM",
            "send_window": {"hour": 8, "minute": 30},
            "retry": {"max_retries": 3, "connect_timeout_sec": 10, "max_time_sec": 60},
        },
        "overrides": {
            "enabled": False,
            "on_weekend": "same",
            "subject_prefix": "",
            "dedupe_window_hours": 24,
        },
        "rules": rules
        or [
            {
                "id": "subject-default",
                "enabled": True,
                "priority": 10,
                "type": "subject_template",
                "params": {"template": "全球IVD晨报 - {{date}}"},
            }
        ],
        "output": {
            "format": "plain_text",
            "sections": ["A", "B", "C", "D", "E", "F", "G"],
            "summary_max_chars": 200,
            "charts_enabled": False,
            "chart_types": [],
        },
    }


def _minimal_content(profile: str = "legacy", rules: list[dict] | None = None) -> dict:
    return {
        "ruleset": "content_rules",
        "version": "1.0.0",
        "profile": profile,
        "defaults": {
            "timezone": "Asia/Shanghai",
            "time_window": {"primary_hours": 24, "fallback_days": 7},
            "item_limit": {"min": 8, "max": 15, "topup_if_24h_lt": 10},
            "sources": {
                "media_global": [
                    {
                        "name": "Fierce Biotech",
                        "url": "https://www.fiercebiotech.com/rss/xml",
                        "region": "北美",
                        "trust_tier": "A",
                    }
                ],
                "regulatory_cn": [
                    {
                        "name": "NMPA",
                        "url": "https://www.nmpa.gov.cn/",
                        "region": "中国",
                        "trust_tier": "A",
                    }
                ],
                "regulatory_apac": [
                    {
                        "name": "TGA",
                        "url": "https://www.tga.gov.au/feeds/alert/safety-alerts.xml",
                        "region": "亚太",
                        "trust_tier": "A",
                    }
                ],
            },
            "coverage_tracks": ["肿瘤检测", "感染检测", "生殖与遗传检测", "其他"],
            "region_filter": {
                "apac_min_share": 0.4,
                "china_min_share": 0.0,
                "allowed_regions": ["北美", "欧洲", "亚太", "中国"],
            },
        },
        "overrides": {
            "enabled": True,
            "keywords_pack": ["ivd_core"],
            "exclude_terms": [],
            "min_confidence": 0.0,
        },
        "rules": rules
        or [
            {
                "id": "include-default",
                "enabled": True,
                "priority": 10,
                "type": "include_filter",
                "params": {"include_keywords": ["diagnostic"]},
            }
        ],
        "output": {
            "format": "plain_text",
            "sections": ["A", "B", "C", "D", "E", "F", "G"],
            "quality_audit_at_end": True,
            "per_item_summary_sentences": 2,
            "max_items": 15,
        },
    }


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

            bad = _minimal_content(profile="legacy")
            bad["ruleset"] = "content_rules"
            (root / "rules" / "email_rules" / "legacy.json").write_text(
                json.dumps(bad),
                encoding="utf-8",
            )

            engine = RuleEngine(project_root=root)
            with self.assertRaises(RuleEngineError) as ctx:
                engine.load("email_rules", "legacy")
            self.assertEqual(ctx.exception.err.code, "RULES_003_RULESET_MISMATCH")

    def test_validate_profile_pair(self) -> None:
        engine = RuleEngine()
        out = engine.validate_profile_pair("legacy")
        self.assertEqual(out["profile"], "legacy")
        self.assertEqual(len(out["validated"]), 2)

    def test_build_decision_contains_unified_objects(self) -> None:
        engine = RuleEngine()
        out = engine.build_decision("enhanced")
        self.assertIn("content_decision", out)
        self.assertIn("email_decision", out)
        self.assertIn("explain", out)
        self.assertIn("allow_sources", out["content_decision"])
        self.assertIn("subject_template", out["email_decision"])

    def test_conflict_merge_and_explain(self) -> None:
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

            email_data = _minimal_email(profile="legacy")
            content_data = _minimal_content(
                profile="legacy",
                rules=[
                    {
                        "id": "include-1",
                        "enabled": True,
                        "priority": 10,
                        "type": "include_filter",
                        "params": {"include_keywords": ["diagnostic"]},
                    },
                    {
                        "id": "include-2",
                        "enabled": True,
                        "priority": 20,
                        "type": "include_filter",
                        "merge_strategy": "append",
                        "params": {"include_keywords": ["pcr"]},
                    },
                    {
                        "id": "exclude-1",
                        "enabled": True,
                        "priority": 30,
                        "type": "exclude_filter",
                        "params": {"exclude_keywords": ["earnings"]},
                    },
                ],
            )

            (root / "rules" / "email_rules" / "legacy.json").write_text(
                json.dumps(email_data),
                encoding="utf-8",
            )
            (root / "rules" / "content_rules" / "legacy.json").write_text(
                json.dumps(content_data),
                encoding="utf-8",
            )

            engine = RuleEngine(project_root=root)
            out = engine.build_decision("legacy")

            self.assertIn("diagnostic", out["content_decision"]["keyword_sets"]["include_keywords"])
            self.assertIn("pcr", out["content_decision"]["keyword_sets"]["include_keywords"])
            self.assertGreaterEqual(out["explain"]["summary"]["conflict_count"], 1)
            self.assertIn("exclude-1", out["explain"]["summary"]["why_excluded"])

    def test_db_active_rules_preferred_over_files(self) -> None:
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

            # Write a file profile that should not be selected when DB active exists.
            file_email = _minimal_email(profile="legacy")
            file_email["version"] = "file-1.0.0"
            (root / "rules" / "email_rules" / "legacy.json").write_text(
                json.dumps(file_email),
                encoding="utf-8",
            )

            store = RulesStore(root)
            db_email = _minimal_email(profile="legacy")
            db_email["version"] = "db-1.0.0"
            store.create_version(
                "email_rules",
                profile="legacy",
                version="v0100",
                config=db_email,
                created_by="tester",
                activate=True,
            )

            # content must still be loadable from file fallback for this test.
            content = _minimal_content(profile="legacy")
            (root / "rules" / "content_rules" / "legacy.json").write_text(
                json.dumps(content),
                encoding="utf-8",
            )

            engine = RuleEngine(project_root=root)
            email = engine.load("email_rules", "legacy")
            self.assertEqual(email.version, "v0100")
            self.assertEqual(email.data.get("version"), "db-1.0.0")

    def test_file_fallback_when_db_missing_profile(self) -> None:
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

            email = _minimal_email(profile="legacy")
            email["version"] = "file-only"
            (root / "rules" / "email_rules" / "legacy.json").write_text(json.dumps(email), encoding="utf-8")

            engine = RuleEngine(project_root=root)
            out = engine.load("email_rules", "legacy")
            self.assertEqual(out.version, "file-only")


if __name__ == "__main__":
    unittest.main()
