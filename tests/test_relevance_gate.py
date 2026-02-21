from __future__ import annotations

import unittest

from app.core.track_relevance import compute_relevance


class RelevanceGateTests(unittest.TestCase):
    def test_prnewswire_lawsuit_drops(self) -> None:
        track, level, explain = compute_relevance(
            "PR Newswire lawsuit securities investor update and share price decline",
            {"source_group": "media", "event_type": "政策与市场动态", "title": "lawsuit update", "url": "https://example.com/news/lawsuit"},
            {},
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertEqual(str(explain.get("final_reason", "")), "strong_negative_without_diagnostic_anchor")

    def test_about_page_drops(self) -> None:
        track, level, explain = compute_relevance(
            "About us and mission details",
            {"source_group": "media", "event_type": "政策与市场动态", "title": "About us", "url": "https://example.com/about"},
            {},
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertEqual(str(explain.get("final_reason", "")), "navigation_or_static_page")

    def test_earnings_news_drops(self) -> None:
        track, level, explain = compute_relevance(
            "quarterly earnings revenue and investor call details",
            {"source_group": "company", "event_type": "政策与市场动态", "title": "Q4 earnings", "url": "https://example.com/earnings"},
            {},
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertIn(str(explain.get("final_reason", "")), {"strong_negative_without_diagnostic_anchor", "raw_score_non_positive"})

    def test_ivd_test_news_is_core(self) -> None:
        track, level, explain = compute_relevance(
            "FDA approves new IVD diagnostic assay for laboratory testing",
            {"source_group": "regulatory", "event_type": "监管审批与指南", "title": "FDA approves IVD assay", "url": "https://example.com/fda/ivd"},
            {},
        )
        self.assertEqual(track, "core")
        self.assertGreaterEqual(level, 3)
        self.assertEqual(str(explain.get("final_reason", "")), "core_anchor_hit")

    def test_biorxiv_proteomics_is_frontier(self) -> None:
        track, level, explain = compute_relevance(
            "bioRxiv reports single-cell proteomics workflow for lab automation",
            {"source_group": "preprint", "event_type": "临床与科研证据", "title": "single-cell proteomics", "url": "https://example.com/biorxiv"},
            {},
        )
        self.assertEqual(track, "frontier")
        self.assertGreaterEqual(level, 2)
        self.assertEqual(str(explain.get("final_reason", "")), "frontier_anchor_hit")

    def test_raw_score_zero_drops(self) -> None:
        track, level, explain = compute_relevance(
            "general corporate update with no clinical context",
            {"source_group": "media", "event_type": "政策与市场动态", "title": "update", "url": "https://example.com/update"},
            {},
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertEqual(str(explain.get("final_reason", "")), "raw_score_non_positive")


if __name__ == "__main__":
    unittest.main()
