from __future__ import annotations

import unittest

from app.services.story_clusterer import StoryClusterer
from scripts.generate_ivd_report import _resolve_dedupe_cluster_config


class PR2EnhancedDedupeTests(unittest.TestCase):
    def test_enhanced_default_enables_dedupe_cluster(self) -> None:
        cfg = _resolve_dedupe_cluster_config(
            use_enhanced=True,
            content_cfg={},
            base_cfg={},
        )
        self.assertTrue(bool(cfg.get("enabled")))

        legacy_cfg = _resolve_dedupe_cluster_config(
            use_enhanced=False,
            content_cfg={},
            base_cfg={},
        )
        self.assertFalse(bool(legacy_cfg.get("enabled")))

    def test_story_clustering_reduces_duplicates(self) -> None:
        clusterer = StoryClusterer(
            config={
                "enabled": True,
                "window_hours": 72,
                "key_strategies": ["title_fingerprint_v1"],
                "primary_select": ["source_priority", "published_at_earliest"],
                "max_other_sources": 3,
            },
            source_priority={
                "Reuters": 100,
                "MediaA": 70,
                "MediaB": 60,
            },
        )
        raw = [
            {
                "title": "FDA clears new molecular diagnostic assay for sepsis",
                "url": "https://a.example.com/news/1",
                "published_at": "2026-02-20T08:00:00Z",
                "source_name": "MediaA",
            },
            {
                "title": "Update: FDA clears new molecular diagnostic assay for sepsis",
                "url": "https://b.example.com/news/123",
                "published_at": "2026-02-20T09:00:00Z",
                "source_name": "MediaB",
            },
            {
                "title": "FDA clears new molecular diagnostic assay for sepsis",
                "url": "https://reuters.example.com/health/abc",
                "published_at": "2026-02-20T10:00:00Z",
                "source_name": "Reuters",
            },
        ]
        out, explain = clusterer.cluster(raw)
        self.assertLess(len(out), len(raw))
        self.assertEqual(len(out), 1)
        self.assertGreaterEqual(len(explain.get("clusters", [])), 1)


if __name__ == "__main__":
    unittest.main()

