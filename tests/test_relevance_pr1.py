from __future__ import annotations

import unittest

from app.core.track_relevance import compute_relevance
from app.workers.dryrun import _render_section_a
from scripts.generate_ivd_report import _resolve_track_split_enabled


class RelevancePR1Tests(unittest.TestCase):
    def test_ivd_regulatory_news_core_high(self) -> None:
        text = "FDA cleared a new IVD molecular diagnostic assay for sepsis testing in clinical laboratories"
        track, level, explain = compute_relevance(
            text,
            {"source_group": "regulatory_us_eu", "event_type": "监管审批与指南"},
            {},
        )
        self.assertEqual(track, "core")
        self.assertGreaterEqual(level, 3)
        self.assertIn("final_reason", explain)

    def test_ruo_breakthrough_frontier(self) -> None:
        text = "Single-cell microfluidic RUO workflow enables digital immunoassay and proteomics biomarker profiling"
        track, level, explain = compute_relevance(
            text,
            {"source_group": "journal", "event_type": "临床与科研证据"},
            {},
        )
        self.assertEqual(track, "frontier")
        self.assertGreaterEqual(level, 2)
        self.assertIn("frontier", str(explain.get("final_reason", "")))

    def test_finance_layoff_low_or_filtered(self) -> None:
        text = "Company reports quarterly revenue and announces layoff and restructuring"
        track, level, explain = compute_relevance(
            text,
            {"source_group": "media", "event_type": "政策与市场动态"},
            {},
        )
        self.assertEqual(level, 0)
        self.assertIn("negative", str(explain.get("final_reason", "")))
        self.assertIn("layoff", " ".join(explain.get("negatives_hit", [])))

    def test_drug_trial_with_diagnostic_not_hard_filtered(self) -> None:
        text = "Phase 3 drug trial includes companion diagnostic assay and PCR biomarker test"
        track, level, explain = compute_relevance(
            text,
            {"source_group": "media", "event_type": "临床与科研证据"},
            {},
        )
        self.assertGreaterEqual(level, 1)
        self.assertIn("assay", " ".join(explain.get("anchors_hit", [])))

    def test_explain_fields_integrity(self) -> None:
        text = "NGS assay for oncology diagnostics in pathology lab"
        _track, _level, explain = compute_relevance(
            text,
            {"source_group": "journal", "event_type": "临床与科研证据"},
            {},
        )
        self.assertIn("anchors_hit", explain)
        self.assertIn("negatives_hit", explain)
        self.assertIn("rule_hits", explain)
        self.assertIn("rules_applied", explain)
        self.assertIn("final_reason", explain)

    def test_upstream_tools_level_one(self) -> None:
        text = "Lab automation workflow update for sample preparation platform"
        _track, level, _explain = compute_relevance(
            text,
            {"source_group": "media", "event_type": "政策与市场动态"},
            {},
        )
        self.assertEqual(level, 1)

    def test_legacy_default_no_track_split(self) -> None:
        self.assertFalse(_resolve_track_split_enabled(use_enhanced=False, content_cfg={}))
        self.assertTrue(_resolve_track_split_enabled(use_enhanced=True, content_cfg={}))

    def test_legacy_profile_output_stable(self) -> None:
        items = [
            {
                "window_tag": "24小时内",
                "title": "legacy stable title",
                "summary": "摘要：legacy stable summary",
                "published": "2026-02-16 08:30 CST",
                "source": "Legacy Source",
                "link": "https://example.com/legacy",
                "region": "北美",
                "lane": "其他",
                "event_type": "政策与市场动态",
                "platform": "跨平台/未标注",
            }
        ]
        section_a = _render_section_a(items)
        self.assertIn("1) [24小时内] legacy stable title", section_a)


if __name__ == "__main__":
    unittest.main()
