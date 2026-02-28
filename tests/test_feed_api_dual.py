from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from app.services.feed_db import FeedDBService
from app.services.rules_store import RulesStore
from app.web.rules_admin_api import create_app


class FeedApiDualTests(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.root = Path(self._td.name)
        (self.root / "artifacts" / "collect").mkdir(parents=True, exist_ok=True)
        self.store = RulesStore(self.root)
        self.feed_db = FeedDBService(self.store.database_url)
        self.client = TestClient(create_app(project_root=self.root))

    def tearDown(self) -> None:
        self._td.cleanup()

    def _write_collect(self, rows: list[dict]) -> None:
        p = self.root / "artifacts" / "collect" / "items-20990101.jsonl"
        with p.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    def test_feed_items_cursor_pagination(self) -> None:
        self._write_collect(
            [
                {
                    "source_id": "s1",
                    "title": "T3",
                    "url": "https://a.example.com/3",
                    "published_at": "2026-02-27T03:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                },
                {
                    "source_id": "s1",
                    "title": "T2",
                    "url": "https://a.example.com/2",
                    "published_at": "2026-02-27T02:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                },
                {
                    "source_id": "s1",
                    "title": "T1",
                    "url": "https://a.example.com/1",
                    "published_at": "2026-02-27T01:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                },
            ]
        )
        self.feed_db.ingest_raw_from_collect(self.root, scan_artifacts_days=7)
        r1 = self.client.get("/api/feed-items", params={"limit": 2})
        self.assertEqual(r1.status_code, 200)
        b1 = r1.json()
        self.assertEqual(len(b1["items"]), 2)
        self.assertTrue(b1.get("next_cursor"))

        r2 = self.client.get("/api/feed-items", params={"limit": 2, "cursor": b1["next_cursor"]})
        self.assertEqual(r2.status_code, 200)
        b2 = r2.json()
        self.assertEqual(len(b2["items"]), 1)
        ids = {x["id"] for x in b1["items"]} | {x["id"] for x in b2["items"]}
        self.assertEqual(len(ids), 3)

    def test_feed_story_group_filter(self) -> None:
        self._write_collect(
            [
                {
                    "source_id": "reg1",
                    "title": "FDA approves test",
                    "url": "https://fda.gov/x1",
                    "published_at": "2026-02-27T01:00:00Z",
                    "source_group": "regulatory",
                    "trust_tier": "A",
                },
                {
                    "source_id": "med1",
                    "title": "Media report IVD",
                    "url": "https://m.example.com/x2",
                    "published_at": "2026-02-27T02:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                },
            ]
        )
        self.feed_db.ingest_raw_from_collect(self.root, scan_artifacts_days=7)
        self.feed_db.rebuild_stories(window_days=30)
        r = self.client.get("/api/feed", params={"group": "regulatory"})
        self.assertEqual(r.status_code, 200)
        items = r.json()["items"]
        self.assertGreaterEqual(len(items), 1)
        self.assertTrue(all(str(x.get("group", "")) == "regulatory" for x in items))
        self.assertTrue(all("region" in x for x in items))
        self.assertTrue(all("event_type" in x for x in items))

    def test_feed_story_detail_evidence_primary_first(self) -> None:
        self._write_collect(
            [
                {
                    "source_id": "src_b",
                    "title": "Same Story Title",
                    "url": "https://b.example.com/story",
                    "published_at": "2026-02-27T01:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                    "priority": 40,
                },
                {
                    "source_id": "src_a",
                    "title": "Same Story Title",
                    "url": "https://a.example.com/story",
                    "published_at": "2026-02-27T02:00:00Z",
                    "source_group": "media",
                    "trust_tier": "A",
                    "priority": 80,
                },
            ]
        )
        self.feed_db.ingest_raw_from_collect(self.root, scan_artifacts_days=7)
        self.feed_db.rebuild_stories(window_days=30)

        r_list = self.client.get("/api/feed")
        self.assertEqual(r_list.status_code, 200)
        story_id = r_list.json()["items"][0]["id"]

        r_detail = self.client.get(f"/api/feed/{story_id}")
        self.assertEqual(r_detail.status_code, 200)
        ev = r_detail.json()["evidence"]
        self.assertGreaterEqual(len(ev), 2)
        self.assertTrue(ev[0]["is_primary"])
        self.assertEqual(ev[0]["trust_tier"], "A")

    def test_feed_story_latest_cursor_pagination_stable(self) -> None:
        self._write_collect(
            [
                {
                    "source_id": "s1",
                    "title": "Story T3",
                    "url": "https://a.example.com/s3",
                    "published_at": "2026-02-27T03:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                },
                {
                    "source_id": "s1",
                    "title": "Story T2",
                    "url": "https://a.example.com/s2",
                    "published_at": "2026-02-27T02:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                },
                {
                    "source_id": "s1",
                    "title": "Story T1",
                    "url": "https://a.example.com/s1",
                    "published_at": "2026-02-27T01:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                },
            ]
        )
        self.feed_db.ingest_raw_from_collect(self.root, scan_artifacts_days=7)
        self.feed_db.rebuild_stories(window_days=30)

        r1 = self.client.get("/api/feed", params={"limit": 2, "view_mode": "latest"})
        self.assertEqual(r1.status_code, 200)
        b1 = r1.json()
        self.assertEqual(len(b1["items"]), 2)
        self.assertTrue(b1.get("next_cursor"))

        r2 = self.client.get("/api/feed", params={"limit": 2, "view_mode": "latest", "cursor": b1["next_cursor"]})
        self.assertEqual(r2.status_code, 200)
        b2 = r2.json()
        self.assertEqual(len(b2["items"]), 1)
        ids = {x["id"] for x in b1["items"]} | {x["id"] for x in b2["items"]}
        self.assertEqual(len(ids), 3)

    def test_feed_story_balanced_not_all_media_when_mixed_groups(self) -> None:
        self._write_collect(
            [
                {
                    "source_id": "m1",
                    "title": "Media story 1",
                    "url": "https://m.example.com/1",
                    "published_at": "2026-02-27T03:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                },
                {
                    "source_id": "m2",
                    "title": "Media story 2",
                    "url": "https://m.example.com/2",
                    "published_at": "2026-02-27T02:00:00Z",
                    "source_group": "media",
                    "trust_tier": "B",
                },
                {
                    "source_id": "r1",
                    "title": "Regulatory story",
                    "url": "https://r.example.com/1",
                    "published_at": "2026-02-27T01:30:00Z",
                    "source_group": "regulatory",
                    "trust_tier": "A",
                },
                {
                    "source_id": "p1",
                    "title": "Procurement story",
                    "url": "https://p.example.com/1",
                    "published_at": "2026-02-27T01:00:00Z",
                    "source_group": "procurement",
                    "trust_tier": "A",
                },
            ]
        )
        self.feed_db.ingest_raw_from_collect(self.root, scan_artifacts_days=7)
        self.feed_db.rebuild_stories(window_days=30)

        r = self.client.get("/api/feed", params={"view_mode": "balanced", "limit": 30})
        self.assertEqual(r.status_code, 200)
        items = r.json()["items"]
        self.assertGreaterEqual(len(items), 3)
        groups = {str(x.get("group", "")) for x in items}
        self.assertIn("media", groups)
        self.assertTrue(any(g in groups for g in ("regulatory", "procurement")))


if __name__ == "__main__":
    unittest.main()
