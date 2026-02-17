from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from app.services.story_clusterer import StoryClusterer


def _item(title, url, source, published_at):
    return {
        "title": title,
        "url": url,
        "source": source,
        "source_key": source,
        "published_at": published_at,
        "first_seen_at": published_at,
        "evidence_grade": 0,
    }


class StoryClustererTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = {
            "enabled": True,
            "window_hours": 72,
            "key_strategies": [
                "canonical_url",
                "normalized_url_host_path",
                "title_fingerprint_v1",
            ],
            "primary_select": [
                "source_priority",
                "evidence_grade",
                "published_at_earliest",
            ],
            "max_other_sources": 5,
        }
        self.pri = {"reuters": 100, "fiercebiotech": 75, "generic_rss": 50}

    def test_similar_titles_cluster_into_one_primary(self) -> None:
        now = datetime.now(timezone.utc)
        items = [
            _item("Company launches new IVD panel", "https://a.com/news/1", "generic_rss", now),
            _item("Company launches new IVD panel", "https://b.com/story/abc", "fiercebiotech", now + timedelta(hours=1)),
            _item("Update: Company launches new IVD panel", "https://c.com/item/777", "reuters", now + timedelta(hours=2)),
        ]
        primaries, explain = StoryClusterer(self.cfg, self.pri).cluster(items)
        self.assertEqual(len(primaries), 1)
        self.assertEqual(primaries[0]["cluster_size"], 3)
        self.assertEqual(len(primaries[0]["other_sources"]), 2)
        self.assertEqual(explain["clusters"][0]["key_strategy"], "title_fingerprint_v1")
        self.assertEqual(primaries[0]["source"], "reuters")

    def test_similar_but_different_news_not_clustered(self) -> None:
        now = datetime.now(timezone.utc)
        items = [
            _item("ACME launches PCR kit for flu", "https://a.com/n1", "generic_rss", now),
            _item("ACME launches PCR kit for sepsis", "https://b.com/n2", "generic_rss", now + timedelta(hours=2)),
        ]
        primaries, _ = StoryClusterer(self.cfg, self.pri).cluster(items)
        self.assertEqual(len(primaries), 2)

    def test_same_title_outside_window_not_clustered(self) -> None:
        now = datetime.now(timezone.utc)
        items = [
            _item("New oncology assay released", "https://a.com/1", "generic_rss", now),
            _item("Update: New oncology assay released", "https://b.com/2", "fiercebiotech", now + timedelta(days=10)),
        ]
        primaries, _ = StoryClusterer(self.cfg, self.pri).cluster(items)
        self.assertEqual(len(primaries), 2)

    def test_primary_selection_prefers_source_priority(self) -> None:
        now = datetime.now(timezone.utc)
        items = [
            _item("Novel test gets approval", "https://x.com/r1", "fiercebiotech", now),
            _item("Update: Novel test gets approval", "https://y.com/r2", "reuters", now + timedelta(hours=1)),
        ]
        primaries, _ = StoryClusterer(self.cfg, self.pri).cluster(items)
        self.assertEqual(len(primaries), 1)
        self.assertEqual(primaries[0]["source"], "reuters")


if __name__ == "__main__":
    unittest.main()
