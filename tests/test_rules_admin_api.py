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

def _qc_cfg(profile: str, version: str) -> dict:
    return {
        "ruleset": "qc_rules",
        "version": version,
        "profile": profile,
        "defaults": {
            "timezone": "Asia/Shanghai",
            "min_24h_items": 10,
            "fallback_days": 7,
            "7d_topup_limit": 20,
            "apac_min_share": 0.4,
            "china_min_share": 0.2,
            "daily_repeat_rate_max": 0.25,
            "recent_7d_repeat_rate_max": 0.4,
            "required_sources_checklist": ["NMPA"],
            "rumor_policy": {"enabled": True, "trigger_terms": ["rumor"], "label": "传闻（未确认）"},
            "fail_policy": {"mode": "only_warn"},
        },
        "overrides": {"enabled": True},
        "rules": [],
        "output": {"format": "json", "panel_enabled": True},
    }


def _output_cfg(profile: str, version: str) -> dict:
    return {
        "ruleset": "output_rules",
        "version": version,
        "profile": profile,
        "defaults": {
            "format": "plain_text",
            "sections": [{"id": "A", "enabled": True}, {"id": "G", "enabled": True}],
            "A": {
                "items_range": {"min": 8, "max": 15},
                "sort_by": "importance",
                "summary_sentences": {"min": 2, "max": 3},
                "summary_max_chars": 260,
                "show_tags": True,
                "show_other_sources": True,
                "show_source_link": True,
            },
            "D": {"heatmap_regions": ["北美", "欧洲", "亚太", "中国"]},
            "E": {"trends_count": 3},
            "F": {"gaps_count": {"min": 3, "max": 5}},
            "constraints": {"g_must_be_last": True, "a_to_f_must_not_include_quality_metrics": True},
        },
        "overrides": {"enabled": True},
        "rules": [],
        "output": {"sections_order": ["A", "G"]},
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
        shutil.copy(
            repo_root / "rules" / "schemas" / "qc_rules.schema.json",
            self.root / "rules" / "schemas" / "qc_rules.schema.json",
        )
        shutil.copy(
            repo_root / "rules" / "schemas" / "output_rules.schema.json",
            self.root / "rules" / "schemas" / "output_rules.schema.json",
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
        self.store.create_version(
            "qc_rules",
            profile="enhanced",
            version="v0001",
            config=_qc_cfg("enhanced", "2.0.0"),
            created_by="seed",
            activate=True,
        )
        self.store.create_version(
            "output_rules",
            profile="enhanced",
            version="v0001",
            config=_output_cfg("enhanced", "2.0.0"),
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

        r3 = self.client.get("/admin/api/qc_rules/active?profile=enhanced", auth=("admin", "pass123"))
        self.assertEqual(r3.status_code, 200)
        self.assertTrue(r3.json()["ok"])

        r4 = self.client.get("/admin/api/output_rules/active?profile=enhanced", auth=("admin", "pass123"))
        self.assertEqual(r4.status_code, 200)
        self.assertTrue(r4.json()["ok"])

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
            "url": "https://api.example.com",
            "enabled": True,
            "priority": 50,
            "trust_tier": "B",
            "tags": ["demo"],
            "rate_limit": {"rps": 1.0, "burst": 2},
            "fetch": {
                "endpoint": "/v1/items",
                "interval_minutes": 60,
                "timeout_seconds": 10,
                "headers_json": {},
                "auth_ref": "DEMO_API_TOKEN",
            },
            "parsing": {"parse_profile": "demo_v1"},
        }
        with patch.dict(os.environ, {"DEMO_API_TOKEN": "abc123"}, clear=False):
            up = self.client.post("/admin/api/sources", auth=("admin", "pass123"), json=payload)
        self.assertEqual(up.status_code, 200)
        self.assertTrue(up.json()["ok"])

        with patch.dict(os.environ, {"DEMO_API_TOKEN": "abc123"}, clear=False):
            ls = self.client.get("/admin/api/sources", auth=("admin", "pass123"))
        self.assertEqual(ls.status_code, 200)
        body = ls.json()
        self.assertTrue(body["ok"])
        row = [x for x in body["sources"] if x["id"] == "demo-api-source"][0]
        self.assertTrue(row.get("auth_configured"))

        tg = self.client.post(
            "/admin/api/sources/demo-api-source/toggle",
            auth=("admin", "pass123"),
            json={"enabled": False},
        )
        self.assertEqual(tg.status_code, 200)
        self.assertFalse(tg.json()["source"]["enabled"])

        with patch(
            "app.web.rules_admin_api.test_source",
            return_value={"ok": True, "connector": "api", "url": "https://api.example.com/v1/items", "sample": []},
        ):
            ts = self.client.post("/admin/api/sources/demo-api-source/test", auth=("admin", "pass123"))
            self.assertEqual(ts.status_code, 200)
            self.assertTrue(ts.json()["ok"])
            self.assertTrue(ts.json()["result"]["ok"])

    def test_sources_auth_ref_validation(self) -> None:
        payload = {
            "id": "bad-auth-ref",
            "name": "Bad",
            "connector": "api",
            "url": "https://api.example.com",
            "enabled": True,
            "priority": 1,
            "trust_tier": "B",
            "tags": [],
            "rate_limit": {"rps": 1.0, "burst": 2},
            "fetch": {"endpoint": "/v1/items", "interval_minutes": 60, "timeout_seconds": 10, "auth_ref": "bad-token"},
            "parsing": {"parse_profile": "demo_v1"},
        }
        up = self.client.post("/admin/api/sources", auth=("admin", "pass123"), json=payload)
        self.assertEqual(up.status_code, 200)
        body = up.json()
        self.assertFalse(body["ok"])
        details = body.get("error", {}).get("details", [])
        self.assertTrue(any(d.get("path") == "$.fetch.auth_ref" for d in details))

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
        (artifacts_dir / "qc_report.json").write_text(json.dumps({"pass": True}), encoding="utf-8")
        (artifacts_dir / "output_render.json").write_text(json.dumps({"sections_order": ["A","G"]}), encoding="utf-8")
        (artifacts_dir / "run_meta.json").write_text(json.dumps({"rules_version": {"email":"x"}}), encoding="utf-8")

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
                "qc_report": str(artifacts_dir / "qc_report.json"),
                "output_render": str(artifacts_dir / "output_render.json"),
                "run_meta": str(artifacts_dir / "run_meta.json"),
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
        self.assertIsInstance(body["qc_report"], dict)
        self.assertIsInstance(body["output_render"], dict)
        self.assertIsInstance(body["run_meta"], dict)
        self.assertIsInstance(body["items"], list)
        self.assertIsInstance(body["clustered_items"], list)
        self.assertIsInstance(body["explain"], dict)


if __name__ == "__main__":
    unittest.main()
