from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from app.services.story_clusterer import StoryClusterer


def _mk(title: str, url: str, source: str, dt: datetime) -> dict:
    return {
        "title": title,
        "url": url,
        "source": source,
        "source_key": source,
        "published_at": dt,
        "first_seen_at": dt,
        "evidence_grade": 1,
    }


def _repeat_rate(items: list[dict]) -> float:
    if not items:
        return 0.0
    uniq = set()
    for it in items:
        t = str(it.get("title", "")).strip().lower()
        t = t.replace("update:", "").strip()
        uniq.add(t)
    return max(0.0, 1.0 - (len(uniq) / len(items)))


class PR3ClusteringRegressionTests(unittest.TestCase):
    def test_enhanced_clustering_reduces_repeat_rate(self) -> None:
        now = datetime.now(timezone.utc)
        raw = [
            _mk("Update: FDA clears new molecular diagnostic panel", "https://a.com/n1", "generic_rss", now),
            _mk("FDA clears new molecular diagnostic panel", "https://b.com/n2", "reuters", now + timedelta(hours=1)),
            _mk("FDA clears new molecular diagnostic panel", "https://c.com/n3", "fiercebiotech", now + timedelta(hours=2)),
            _mk("Company launches assay automation system", "https://d.com/x1", "generic_rss", now),
            _mk("Company launches assay automation system", "https://e.com/x2", "reuters", now + timedelta(hours=3)),
            _mk("Independent different story", "https://f.com/y1", "generic_rss", now),
        ]
        before = _repeat_rate(raw)
        clusterer = StoryClusterer(
            {
                "enabled": True,
                "window_hours": 72,
                "key_strategies": ["canonical_url", "normalized_url_host_path", "title_fingerprint_v1"],
                "primary_select": ["source_priority", "evidence_grade", "published_at_earliest"],
                "max_other_sources": 5,
            },
            {"reuters": 100, "fiercebiotech": 75, "generic_rss": 50},
        )
        out, explain = clusterer.cluster(raw)
        after = _repeat_rate(out)
        self.assertTrue(explain.get("enabled"))
        self.assertLess(len(out), len(raw))
        self.assertLess(after, before)


if __name__ == "__main__":
    unittest.main()
