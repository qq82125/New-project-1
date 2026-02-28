from __future__ import annotations

import unittest

from app.services.ops_metrics import evaluate_health


class OpsHealthPr171Tests(unittest.TestCase):
    def _base_metrics(self) -> dict:
        return {
            "digest": {
                "analysis_degraded_count": 1,
                "items_after_dedupe": 20,
                "analysis_cache_hit": 8,
                "analysis_cache_miss": 2,
                "unknown_metrics": {"unknown_lane_rate": 0.1},
            },
            "collect": {"dropped_static_or_listing_count": 1},
            "acceptance": {"ok": True},
            "procurement_probe": {
                "totals": {"ok": 3, "error": 0},
                "by_error_kind": [{"key": "timeout", "count": 1}],
                "per_source": [],
            },
        }

    def test_unknown_lane_rate_0_8_is_red(self) -> None:
        m = self._base_metrics()
        m["digest"]["unknown_metrics"]["unknown_lane_rate"] = 0.8
        h = evaluate_health(m)
        self.assertEqual(h["overall"], "red")
        self.assertTrue(any(x["metric"] == "unknown_lane_rate" and x["level"] == "red" for x in h["rules_triggered"]))

    def test_degraded_ratio_0_5_is_yellow(self) -> None:
        m = self._base_metrics()
        m["digest"]["analysis_degraded_count"] = 5
        m["digest"]["items_after_dedupe"] = 10
        h = evaluate_health(m)
        self.assertEqual(h["overall"], "yellow")
        self.assertTrue(any(x["metric"] == "degraded_ratio" and x["level"] == "yellow" for x in h["rules_triggered"]))

    def test_cache_hit_ratio_zero_is_red(self) -> None:
        m = self._base_metrics()
        m["digest"]["analysis_cache_hit"] = 0
        m["digest"]["analysis_cache_miss"] = 10
        h = evaluate_health(m)
        self.assertEqual(h["overall"], "red")
        self.assertTrue(any(x["metric"] == "analysis_cache_hit_ratio" and x["level"] == "red" for x in h["rules_triggered"]))
        row = next(x for x in h["rules_triggered"] if x["metric"] == "analysis_cache_hit_ratio")
        self.assertEqual(row["threshold"], "hit>0")

    def test_acceptance_false_is_red(self) -> None:
        m = self._base_metrics()
        m["acceptance"]["ok"] = False
        h = evaluate_health(m)
        self.assertEqual(h["overall"], "red")
        self.assertTrue(any(x["metric"] == "acceptance.ok" and x["level"] == "red" for x in h["rules_triggered"]))

    def test_probe_ok_zero_and_error_positive_is_yellow(self) -> None:
        m = self._base_metrics()
        m["procurement_probe"]["totals"] = {"ok": 0, "error": 2}
        h = evaluate_health(m)
        self.assertEqual(h["overall"], "yellow")
        self.assertTrue(any(x["metric"] == "procurement_probe_totals" and x["level"] == "yellow" for x in h["rules_triggered"]))

    def test_all_good_is_green(self) -> None:
        m = self._base_metrics()
        h = evaluate_health(m)
        self.assertEqual(h["overall"], "green")
        self.assertEqual(h["rules_triggered"], [])

    def test_cache_hit_ratio_skipped_when_no_cache_events(self) -> None:
        m = self._base_metrics()
        m["digest"]["analysis_cache_hit"] = 0
        m["digest"]["analysis_cache_miss"] = 0
        h = evaluate_health(m)
        self.assertFalse(any(x["metric"] == "analysis_cache_hit_ratio" for x in h["rules_triggered"]))


if __name__ == "__main__":
    unittest.main()
