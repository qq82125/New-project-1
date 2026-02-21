from __future__ import annotations

import unittest

from app.core.track_relevance import compute_relevance


ENHANCED_RULES = {
    "profile": "enhanced",
    "frontier_policy": {
        "require_diagnostic_anchor": True,
        "drop_bio_general_without_diagnostic": True,
    },
}


class FrontierNarrowingPR9Tests(unittest.TestCase):
    def test_single_cell_without_diagnostic_drops(self) -> None:
        track, level, explain = compute_relevance(
            "single-cell spatial transcriptomics workflow for tissue atlas",
            {"source_group": "journal", "event_type": "临床与科研证据"},
            ENHANCED_RULES,
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertEqual(str(explain.get("final_reason", "")), "bio_general_without_diagnostic_anchor")

    def test_proteomics_without_diagnostic_drops(self) -> None:
        track, level, explain = compute_relevance(
            "proteomics multi-omics method benchmark for discovery biology",
            {"source_group": "preprint", "event_type": "临床与科研证据"},
            ENHANCED_RULES,
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertEqual(str(explain.get("final_reason", "")), "bio_general_without_diagnostic_anchor")

    def test_mced_detection_test_is_not_dropped(self) -> None:
        track, level, explain = compute_relevance(
            "MCED blood detection test assay workflow improves cancer screening sensitivity",
            {"source_group": "journal", "event_type": "临床与科研证据"},
            ENHANCED_RULES,
        )
        self.assertIn(track, {"core", "frontier"})
        self.assertGreaterEqual(level, 2)
        self.assertNotEqual(str(explain.get("final_reason", "")), "bio_general_without_diagnostic_anchor")

    def test_ldt_clia_lab_innovation_is_not_dropped(self) -> None:
        track, level, explain = compute_relevance(
            "CLIA laboratory LDT assay automation innovation for multiplex diagnostic testing",
            {"source_group": "company", "event_type": "政策与市场动态"},
            ENHANCED_RULES,
        )
        self.assertIn(track, {"core", "frontier"})
        self.assertGreaterEqual(level, 2)
        self.assertNotEqual(str(explain.get("final_reason", "")), "bio_general_without_diagnostic_anchor")


if __name__ == "__main__":
    unittest.main()
