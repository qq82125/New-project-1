from __future__ import annotations

import unittest

from app.workers.dryrun import _render_section_g


class DryrunTrackContractTests(unittest.TestCase):
    def test_render_section_g_includes_track_coverage_when_present(self) -> None:
        panel = {
            "n24": 2,
            "n7": 1,
            "apac_share_pct": "40%",
            "required_sources_hits": "NMPA:命中",
            "event_mix": {"regulatory": 2, "commercial": 1},
        }
        track_contract = {
            "coverage": {"core": 3, "frontier": 2, "a_pool_count": 2, "f_pool_count": 1},
            "routing_gaps": ["missing_routing_F:use_default"],
        }
        txt = _render_section_g(panel, track_contract=track_contract)
        self.assertIn("core/frontier覆盖", txt)
        self.assertIn("A候选=2", txt)
        self.assertIn("分流缺口", txt)

    def test_render_section_g_without_track_contract_keeps_base(self) -> None:
        panel = {
            "n24": 1,
            "n7": 0,
            "apac_share_pct": "0%",
            "required_sources_hits": "NMPA:未命中",
            "event_mix": {"regulatory": 0, "commercial": 1},
        }
        txt = _render_section_g(panel, track_contract=None)
        self.assertIn("G. 质量指标", txt)
        self.assertNotIn("core/frontier覆盖", txt)


if __name__ == "__main__":
    unittest.main()
