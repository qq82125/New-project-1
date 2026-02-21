from __future__ import annotations

import datetime as dt
import os
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from app.services.rules_store import RulesStore
from app.services.source_registry import load_sources_registry_bundle
from app.web.rules_admin_api import create_app
from app.workers.scheduler_worker import _is_due


def _write_sources_yaml(root: Path) -> None:
    p = root / "rules" / "sources"
    p.mkdir(parents=True, exist_ok=True)
    (p / "rss.yaml").write_text(
        """
version: "1.0.0"
sources:
  - id: demo-rss
    enabled: true
    connector: rss
    url: https://example.com/feed
    name: Demo RSS
    region: 全球
    trust_tier: B
    priority: 50
    source_group: media
    tags: [media]
    fetch:
      interval_minutes: 30
""".strip()
        + "\n",
        encoding="utf-8",
    )


class DynamicSourceManagementPR11Tests(unittest.TestCase):
    def test_effective_interval_source_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_sources_yaml(root)
            store = RulesStore(root)
            store.upsert_source_group({"group_key": "media", "display_name": "Media", "default_interval_minutes": 60, "enabled": True})
            store.upsert_source(
                {
                    "id": "demo-rss",
                    "name": "Demo RSS",
                    "connector": "rss",
                    "url": "https://example.com/feed",
                    "enabled": True,
                    "priority": 50,
                    "source_group": "media",
                    "trust_tier": "B",
                    "tags": ["media"],
                    "fetch_interval_minutes": 15,
                    "fetch": {"interval_minutes": 30},
                }
            )
            rows = load_sources_registry_bundle(root).get("sources", [])
            row = next(x for x in rows if x.get("id") == "demo-rss")
            self.assertEqual(int(row.get("effective_interval_minutes", 0) or 0), 15)
            self.assertEqual(str(row.get("interval_source", "")), "source")

    def test_effective_interval_group_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_sources_yaml(root)
            store = RulesStore(root)
            store.upsert_source_group({"group_key": "media", "display_name": "Media", "default_interval_minutes": 45, "enabled": True})
            store.upsert_source(
                {
                    "id": "demo-rss",
                    "name": "Demo RSS",
                    "connector": "rss",
                    "url": "https://example.com/feed",
                    "enabled": True,
                    "priority": 50,
                    "source_group": "media",
                    "trust_tier": "B",
                    "tags": ["media"],
                    "fetch_interval_minutes": None,
                    "fetch": {},
                }
            )
            rows = load_sources_registry_bundle(root).get("sources", [])
            row = next(x for x in rows if x.get("id") == "demo-rss")
            self.assertEqual(int(row.get("effective_interval_minutes", 0) or 0), 45)
            self.assertEqual(str(row.get("interval_source", "")), "group")

    def test_effective_interval_yaml_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_sources_yaml(root)
            store = RulesStore(root)
            store.upsert_source_group({"group_key": "media", "display_name": "Media", "default_interval_minutes": None, "enabled": True})
            rows = load_sources_registry_bundle(root).get("sources", [])
            row = next(x for x in rows if x.get("id") == "demo-rss")
            self.assertEqual(int(row.get("effective_interval_minutes", 0) or 0), 30)
            self.assertEqual(str(row.get("interval_source", "")), "yaml")

    def test_deleted_filtered_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_sources_yaml(root)
            store = RulesStore(root)
            store.upsert_source(
                {
                    "id": "demo-rss",
                    "name": "Demo RSS",
                    "connector": "rss",
                    "url": "https://example.com/feed",
                    "enabled": True,
                    "priority": 50,
                    "source_group": "media",
                    "trust_tier": "B",
                    "tags": ["media"],
                }
            )
            store.soft_delete_source("demo-rss")
            rows0 = load_sources_registry_bundle(root, include_deleted=False).get("sources", [])
            rows1 = load_sources_registry_bundle(root, include_deleted=True).get("sources", [])
            self.assertFalse(any(x.get("id") == "demo-rss" for x in rows0))
            self.assertTrue(any(x.get("id") == "demo-rss" for x in rows1))

    def test_soft_delete_sets_deleted_and_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)
            store.upsert_source(
                {
                    "id": "s1",
                    "name": "S1",
                    "connector": "rss",
                    "url": "https://example.com/a",
                    "enabled": True,
                    "priority": 1,
                    "source_group": "media",
                    "trust_tier": "B",
                    "tags": ["media"],
                }
            )
            out = store.soft_delete_source("s1")
            s = out.get("source", {})
            self.assertFalse(bool(s.get("enabled")))
            self.assertTrue(str(s.get("deleted_at", "")).strip())

    def test_restore_clears_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)
            store.upsert_source(
                {
                    "id": "s1",
                    "name": "S1",
                    "connector": "rss",
                    "url": "https://example.com/a",
                    "enabled": True,
                    "priority": 1,
                    "source_group": "media",
                    "trust_tier": "B",
                    "tags": ["media"],
                }
            )
            store.soft_delete_source("s1")
            out = store.restore_source("s1")
            s = out.get("source", {})
            self.assertEqual(str(s.get("deleted_at", "")).strip(), "")

    def test_api_create_patch_delete_restore_list(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_sources_yaml(root)
            os.environ["ADMIN_USER"] = "admin"
            os.environ["ADMIN_PASS"] = "pass123"
            os.environ.pop("ADMIN_TOKEN", None)
            client = TestClient(create_app(project_root=root))
            try:
                payload = {
                    "id": "api-src-1",
                    "name": "API Source 1",
                    "connector": "rss",
                    "fetcher": "rss",
                    "url": "https://example.com/api-src-1",
                    "enabled": True,
                    "priority": 20,
                    "source_group": "company",
                    "fetch_interval_minutes": 120,
                    "trust_tier": "B",
                    "tags": ["company"],
                }
                up = client.post("/admin/api/sources", auth=("admin", "pass123"), json=payload).json()
                self.assertTrue(up.get("ok"))
                p = client.patch(
                    "/admin/api/sources/api-src-1",
                    auth=("admin", "pass123"),
                    json={"source_group": "media", "fetch_interval_minutes": 60},
                ).json()
                self.assertTrue(p.get("ok"))
                d = client.delete("/admin/api/sources/api-src-1", auth=("admin", "pass123")).json()
                self.assertTrue(d.get("ok"))
                ls0 = client.get("/admin/api/sources?include_deleted=0", auth=("admin", "pass123")).json()
                self.assertFalse(any(x.get("id") == "api-src-1" for x in ls0.get("sources", [])))
                r = client.post("/admin/api/sources/api-src-1/restore", auth=("admin", "pass123")).json()
                self.assertTrue(r.get("ok"))
                ls1 = client.get("/admin/api/sources?include_deleted=0", auth=("admin", "pass123")).json()
                self.assertTrue(any(x.get("id") == "api-src-1" for x in ls1.get("sources", [])))
            finally:
                os.environ.pop("ADMIN_USER", None)
                os.environ.pop("ADMIN_PASS", None)
                os.environ.pop("ADMIN_TOKEN", None)

    def test_due_gating_uses_effective_interval(self) -> None:
        now_ts = dt.datetime.fromisoformat("2025-01-01T00:00:00+00:00").timestamp()
        self.assertFalse(
            _is_due(
                last_fetched_at="2024-12-31T23:50:00+00:00",
                interval_min=20,
                now_ts=now_ts,
            )
        )
        self.assertTrue(
            _is_due(
                last_fetched_at="2024-12-31T23:30:00+00:00",
                interval_min=20,
                now_ts=now_ts,
            )
        )


if __name__ == "__main__":
    unittest.main()
