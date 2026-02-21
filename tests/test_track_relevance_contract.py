from __future__ import annotations

import unittest

from app.core.track_relevance import (
    classify_track_relevance,
    normalize_item_contract,
    validate_track_routing_rules,
)


class TrackRelevanceContractTests(unittest.TestCase):
    def test_normalize_item_contract_clamps_invalid_values(self) -> None:
        track, level, warns = normalize_item_contract("bad", 9)
        self.assertEqual(track, "core")
        self.assertEqual(level, 4)
        self.assertTrue(any("invalid_track" in w for w in warns))
        self.assertTrue(any("out_of_range_relevance" in w for w in warns))

    def test_validate_track_routing_rules_fills_defaults(self) -> None:
        rules, gaps = validate_track_routing_rules({})
        self.assertIn("A", rules)
        self.assertIn("F", rules)
        self.assertIn("G", rules)
        self.assertGreaterEqual(len(gaps), 1)

    def test_classify_track_relevance_frontier_keyword(self) -> None:
        track, level, why = classify_track_relevance(
            title="New single-cell microfluidic diagnostics workflow",
            summary="Lab-on-a-chip platform for infection testing.",
            event_type="临床与科研证据",
            source_group="preprint",
            score=3,
        )
        self.assertEqual(track, "frontier")
        self.assertGreaterEqual(level, 1)
        self.assertIn("frontier", why)


if __name__ == "__main__":
    unittest.main()

