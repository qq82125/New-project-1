from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.services.rules_store import RulesStore
from app.web.rules_admin_api import create_app


def _email_cfg(profile: str, version: str) -> dict:
    return {
        "ruleset": "email_rules",
        "version": version,
        "profile": profile,
        "defaults": {
            "timezone": "Asia/Shanghai",
            "subject_template": "全球IVD晨报 - {{date}}",
            "recipient": "qq82125@gmail.com",
            "send_window": {"hour": 8, "minute": 30},
            "retry": {"max_retries": 3, "connect_timeout_sec": 10, "max_time_sec": 60},
        },
        "overrides": {"enabled": True},
        "rules": [
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
        },
    }


def _content_cfg(profile: str, version: str) -> dict:
    return {
        "ruleset": "content_rules",
        "version": version,
        "profile": profile,
        "defaults": {
            "timezone": "Asia/Shanghai",
            "time_window": {"primary_hours": 24, "fallback_days": 7},
            "item_limit": {"min": 8, "max": 15, "topup_if_24h_lt": 10},
            "sources": {"media_global": [], "regulatory_cn": [], "regulatory_apac": []},
            "coverage_tracks": ["肿瘤检测", "感染检测", "生殖与遗传检测", "其他"],
            "region_filter": {"apac_min_share": 0.4},
        },
        "overrides": {"enabled": True},
        "rules": [
            {
                "id": "include-default",
                "enabled": True,
                "priority": 10,
                "type": "include_filter",
                "params": {"include_keywords": ["diagnostic"]},
            }
        ],
        "output": {"format": "plain_text", "sections": ["A", "B", "C", "D", "E", "F", "G"], "quality_audit_at_end": True},
    }


class RulesAdminApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        (self.root / "rules" / "schemas").mkdir(parents=True, exist_ok=True)
        repo_root = Path(__file__).resolve().parents[1]
        shutil.copy(
            repo_root / "rules" / "schemas" / "email_rules.schema.json",
            self.root / "rules" / "schemas" / "email_rules.schema.json",
        )
        shutil.copy(
            repo_root / "rules" / "schemas" / "content_rules.schema.json",
            self.root / "rules" / "schemas" / "content_rules.schema.json",
        )

        self.store = RulesStore(self.root)
        self.store.create_version(
            "email_rules",
            profile="enhanced",
            version="v0001",
            config=_email_cfg("enhanced", "2.0.0"),
            created_by="seed",
            activate=True,
        )
        self.store.create_version(
            "content_rules",
            profile="enhanced",
            version="v0001",
            config=_content_cfg("enhanced", "2.0.0"),
            created_by="seed",
            activate=True,
        )
        os.environ["ADMIN_USER"] = "admin"
        os.environ["ADMIN_PASS"] = "pass123"
        os.environ.pop("ADMIN_TOKEN", None)
        self.client = TestClient(create_app(project_root=self.root))

    def tearDown(self) -> None:
        os.environ.pop("ADMIN_USER", None)
        os.environ.pop("ADMIN_PASS", None)
        os.environ.pop("ADMIN_TOKEN", None)
        self._td.cleanup()

    def test_active_requires_auth(self) -> None:
        r = self.client.get("/admin/api/email_rules/active?profile=enhanced")
        self.assertEqual(r.status_code, 401)

        r2 = self.client.get("/admin/api/email_rules/active?profile=enhanced", auth=("admin", "pass123"))
        self.assertEqual(r2.status_code, 200)
        self.assertTrue(r2.json()["ok"])

    def test_draft_publish_and_rollback(self) -> None:
        draft_payload = {
            "profile": "enhanced",
            "created_by": "tester",
            "config_json": _email_cfg("enhanced", "2.0.1"),
        }
        dr = self.client.post("/admin/api/email_rules/draft", auth=("admin", "pass123"), json=draft_payload)
        self.assertEqual(dr.status_code, 200)
        body = dr.json()
        self.assertTrue(body["ok"])
        did = int(body["draft"]["id"])

        pr = self.client.post(
            "/admin/api/email_rules/publish",
            auth=("admin", "pass123"),
            json={"profile": "enhanced", "draft_id": did, "created_by": "tester"},
        )
        self.assertEqual(pr.status_code, 200)
        self.assertTrue(pr.json()["ok"])

        rb = self.client.post(
            "/admin/api/email_rules/rollback",
            auth=("admin", "pass123"),
            json={"profile": "enhanced"},
        )
        self.assertEqual(rb.status_code, 200)
        self.assertTrue(rb.json()["ok"])

    def test_sources_upsert_toggle_test(self) -> None:
        payload = {
            "id": "demo-api-source",
            "name": "Demo API",
            "connector": "api",
            "url": "",
            "enabled": True,
            "priority": 50,
            "trust_tier": "B",
            "tags": ["demo"],
            "rate_limit": {"rps": 1, "burst": 1},
        }
        up = self.client.post("/admin/api/sources", auth=("admin", "pass123"), json=payload)
        self.assertEqual(up.status_code, 200)
        self.assertTrue(up.json()["ok"])

        tg = self.client.post(
            "/admin/api/sources/demo-api-source/toggle",
            auth=("admin", "pass123"),
            json={"enabled": False},
        )
        self.assertEqual(tg.status_code, 200)
        self.assertFalse(tg.json()["source"]["enabled"])

        ts = self.client.post("/admin/api/sources/demo-api-source/test", auth=("admin", "pass123"))
        self.assertEqual(ts.status_code, 200)
        self.assertTrue(ts.json()["ok"])
        self.assertTrue(ts.json()["result"]["ok"])

    def test_draft_validation_errors_structured(self) -> None:
        bad = _email_cfg("enhanced", "2.0.1")
        bad.pop("defaults", None)
        dr = self.client.post(
            "/admin/api/email_rules/draft",
            auth=("admin", "pass123"),
            json={"profile": "enhanced", "created_by": "tester", "config_json": bad},
        )
        self.assertEqual(dr.status_code, 200)
        body = dr.json()
        self.assertFalse(body["ok"])
        self.assertGreater(len(body["draft"]["validation_errors"]), 0)
        self.assertIn("path", body["draft"]["validation_errors"][0])
        self.assertIn("message", body["draft"]["validation_errors"][0])

    def test_unified_dryrun_endpoint_shape(self) -> None:
        run_id = "dryrun-demo"
        artifacts_dir = self.root / "artifacts" / run_id
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        (artifacts_dir / "newsletter_preview.md").write_text("PREVIEW", encoding="utf-8")
        (artifacts_dir / "items.json").write_text(json.dumps([{"title": "t"}]), encoding="utf-8")
        (artifacts_dir / "clustered_items.json").write_text(json.dumps([{"title": "t", "story_id": "s1"}]), encoding="utf-8")
        (artifacts_dir / "cluster_explain.json").write_text(json.dumps({"items_before_count": 3, "items_after_count": 1}), encoding="utf-8")
        (artifacts_dir / "run_id.json").write_text(json.dumps({"run_id": run_id, "decision_explain": {}}), encoding="utf-8")

        fake = {
            "run_id": run_id,
            "profile": "enhanced",
            "date": "2026-02-16",
            "items_before_count": 3,
            "items_after_count": 1,
            "artifacts": {
                "preview": str(artifacts_dir / "newsletter_preview.md"),
                "items": str(artifacts_dir / "items.json"),
                "clustered_items": str(artifacts_dir / "clustered_items.json"),
                "cluster_explain": str(artifacts_dir / "cluster_explain.json"),
                "explain": str(artifacts_dir / "run_id.json"),
            },
        }

        with patch("app.web.rules_admin_api.run_dryrun", return_value=fake):
            r = self.client.post("/admin/api/dryrun?profile=enhanced&date=2026-02-16", auth=("admin", "pass123"))
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["run_id"], run_id)
        self.assertEqual(body["items_before"], 3)
        self.assertEqual(body["items_after"], 1)
        self.assertIn("preview_text", body)
        self.assertIn("preview_html", body)
        self.assertIsInstance(body["items"], list)
        self.assertIsInstance(body["clustered_items"], list)
        self.assertIsInstance(body["explain"], dict)


if __name__ == "__main__":
    unittest.main()
