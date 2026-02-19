from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from app.core.dedupe import strong_dedupe
from app.core.scoring import DEFAULT_SCORING_CONFIG, compute_source_weight, diversity_select, score_item


class ScoringDedupeTests(unittest.TestCase):
    def test_source_weight_by_tags_and_tier(self) -> None:
        cfg = DEFAULT_SCORING_CONFIG
        w1, m1 = compute_source_weight(["regulatory"], "A", "rss", cfg)
        w2, m2 = compute_source_weight(["aggregator"], "C", "google_news", cfg)
        self.assertGreater(w1, w2)
        self.assertEqual(m1["bucket"], "regulatory")
        self.assertEqual(m2["bucket"], "aggregator")

    def test_quality_score_breakdown_stable(self) -> None:
        cfg = DEFAULT_SCORING_CONFIG
        now = datetime(2026, 2, 19, 0, 0, tzinfo=timezone.utc)
        item = {
            "title": "FDA issues new IVD guidance",
            "url": "https://www.fda.gov/devices/test",
            "published_at": now - timedelta(hours=4),
            "source": "FDA",
            "summary_cn": "摘要：监管发布体外诊断指南更新。",
            "event_type": "监管审批与指南",
        }
        out = score_item(item, {"tags": ["regulatory"], "trust_tier": "A", "fetcher": "rss"}, now, cfg)
        self.assertIn("score_breakdown", out)
        self.assertGreaterEqual(float(out.get("quality_score", 0)), 60)
        self.assertEqual(out.get("evidence_grade"), "A")

    def test_dedupe_prefers_higher_evidence_and_score(self) -> None:
        cfg = DEFAULT_SCORING_CONFIG
        now = datetime(2026, 2, 19, 0, 0, tzinfo=timezone.utc)
        a = score_item(
            {
                "title": "Update: New molecular diagnostic panel launched",
                "url": "https://agg.example.com/x",
                "published_at": now - timedelta(hours=2),
                "source": "Aggregator",
                "summary_cn": "摘要：转载消息。",
                "event_type": "注册上市/产品发布",
            },
            {"tags": ["aggregator"], "trust_tier": "C", "fetcher": "google_news"},
            now,
            cfg,
        )
        b = score_item(
            {
                "title": "New molecular diagnostic panel launched",
                "url": "https://company.example.com/news/panel",
                "published_at": now - timedelta(hours=3),
                "source": "Company",
                "summary_cn": "摘要：公司官网发布产品上市。",
                "event_type": "注册上市/产品发布",
            },
            {"tags": ["company"], "trust_tier": "A", "fetcher": "html"},
            now,
            cfg,
        )
        out, report = strong_dedupe([a, b], cfg)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].get("source"), "Company")
        self.assertEqual(int(report.get("deduped_count", 0)), 1)

    def test_diversity_quota_enforced(self) -> None:
        cfg = DEFAULT_SCORING_CONFIG
        rows = []
        for i in range(10):
            rows.append(
                {
                    "item_id": f"media-{i}",
                    "title": f"media {i}",
                    "quality_score": 80 - i,
                    "evidence_grade": "C",
                    "source_weight": 0.7,
                    "source_bucket": "media",
                    "original_source_url": "",
                }
            )
        for i in range(5):
            rows.append(
                {
                    "item_id": f"reg-{i}",
                    "title": f"reg {i}",
                    "quality_score": 75 - i,
                    "evidence_grade": "A",
                    "source_weight": 1.0,
                    "source_bucket": "regulatory",
                    "original_source_url": "",
                }
            )
        selected, summary = diversity_select(rows, top_n=8, cfg=cfg)
        by_bucket = summary.get("selected_by_bucket", {})
        self.assertGreaterEqual(int(by_bucket.get("regulatory", 0)), 4)
        self.assertLessEqual(len(selected), 8)


if __name__ == "__main__":
    unittest.main()
