from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from app.web.rules_admin_api import create_app


class OpsDashboardPr17Tests(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        (self.root / "artifacts").mkdir(parents=True, exist_ok=True)
        os.environ["ADMIN_USER"] = "admin"
        os.environ["ADMIN_PASS"] = "pass123"
        os.environ.pop("ADMIN_TOKEN", None)
        self.client = TestClient(create_app(project_root=self.root))
        self.fixture_dir = Path(__file__).parent / "fixtures"

    def tearDown(self) -> None:
        os.environ.pop("ADMIN_USER", None)
        os.environ.pop("ADMIN_PASS", None)
        os.environ.pop("ADMIN_TOKEN", None)
        self._td.cleanup()

    def _auth_get(self, url: str) -> dict:
        r = self.client.get(url, auth=("admin", "pass123"))
        self.assertEqual(r.status_code, 200)
        return r.json()

    def _write_fixture(self, fixture_name: str, out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        src = self.fixture_dir / fixture_name
        out_path.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    def test_summary_missing_files_degrades_gracefully(self) -> None:
        body = self._auth_get("/admin/api/ops/summary?limit=20")
        self.assertIn("digest", body)
        self.assertIn("collect", body)
        self.assertIn("acceptance", body)
        self.assertIn("procurement_probe", body)
        self.assertIsNone(body["digest"])
        self.assertIsNone(body["collect"])
        self.assertIsNone(body["acceptance"])
        self.assertIsNone(body["procurement_probe"])
        self.assertIsInstance(body.get("errors"), list)
        self.assertGreaterEqual(len(body["errors"]), 1)

    def test_acceptance_fixture_parsed(self) -> None:
        self._write_fixture("acceptance_report.json", self.root / "artifacts" / "acceptance" / "acceptance_report.json")
        body = self._auth_get("/admin/api/ops/summary")
        self.assertIsInstance(body["acceptance"], dict)
        self.assertEqual(body["acceptance"]["ok"], True)
        self.assertEqual(body["acceptance"]["checks_passed"], 11)
        self.assertEqual(body["acceptance"]["quality_pack_selected_total"], 27)

    def test_probe_fixture_parsed(self) -> None:
        self._write_fixture("probe_report.json", self.root / "artifacts" / "procurement" / "probe_report-20260222-0100.json")
        body = self._auth_get("/admin/api/ops/summary")
        probe = body["procurement_probe"]
        self.assertIsInstance(probe, dict)
        self.assertEqual(probe["totals"]["error"], 3)
        self.assertIsInstance(probe["by_error_kind"], list)
        self.assertEqual(probe["by_error_kind"][0]["key"], "parse_error")

    def test_digest_fixture_extracts_core_metrics(self) -> None:
        self._write_fixture("run_meta_digest.json", self.root / "artifacts" / "run_meta.json")
        body = self._auth_get("/admin/api/ops/summary")
        digest = body["digest"]
        self.assertIsInstance(digest, dict)
        self.assertEqual(digest["items_before_dedupe"], 120)
        self.assertEqual(digest["items_after_dedupe"], 85)
        self.assertEqual(digest["analysis_cache_hit"], 44)
        self.assertEqual(digest["analysis_cache_miss"], 9)
        self.assertIn("unknown_metrics", digest)

    def test_collect_fixture_extracts_assets_written(self) -> None:
        self._write_fixture("run_meta_collect.json", self.root / "artifacts" / "collect-1771737600" / "run_meta.json")
        body = self._auth_get("/admin/api/ops/summary")
        collect = body["collect"]
        self.assertIsInstance(collect, dict)
        self.assertEqual(collect["assets_written_count"], 222)
        self.assertEqual(collect["sources_failed_count"], 5)
        self.assertEqual(collect["dropped_static_or_listing_count"], 9)

    def test_api_schema_shape(self) -> None:
        self._write_fixture("run_meta_digest.json", self.root / "artifacts" / "run_meta.json")
        self._write_fixture("run_meta_collect.json", self.root / "artifacts" / "collect-1771737600" / "run_meta.json")
        self._write_fixture("acceptance_report.json", self.root / "artifacts" / "acceptance" / "acceptance_report.json")
        self._write_fixture("probe_report.json", self.root / "artifacts" / "procurement" / "probe_report-20260222-0100.json")
        body = self._auth_get("/admin/api/ops/summary?limit=3")
        self.assertIn("files", body)
        self.assertIn("errors", body)
        self.assertIsInstance(body["files"], dict)
        self.assertIsInstance(body["errors"], list)
        self.assertTrue(set(["digest_meta_path", "collect_meta_path", "acceptance_path", "probe_path"]).issubset(body["files"].keys()))
        # files path should be relative by default
        self.assertFalse(str(body["files"]["digest_meta_path"]).startswith(str(self.root)))

        # optional smoke check for read-only page route
        page = self.client.get("/admin/ops", auth=("admin", "pass123"))
        self.assertEqual(page.status_code, 200)
        self.assertIn("Ops Dashboard Lite", page.text)


if __name__ == "__main__":
    unittest.main()
