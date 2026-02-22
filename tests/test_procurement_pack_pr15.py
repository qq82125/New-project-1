from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.collect_asset_store import _event_weight_key
from app.services.opportunity_index import compute_opportunity_index
from app.services.opportunity_store import EVENT_WEIGHT, OpportunityStore, normalize_event_type
from app.services.source_registry import load_sources_registry


class ProcurementPackPR15Tests(unittest.TestCase):
    def test_procurement_sources_loaded(self) -> None:
        root = Path(__file__).resolve().parents[1]
        rows = load_sources_registry(root)
        ids = {str(x.get("id", "")) for x in rows}
        self.assertIn("procurement_ccgp", ids)
        self.assertIn("procurement_china_province", ids)
        self.assertIn("procurement_hospital", ids)
        self.assertIn("procurement_global_tender", ids)

    def test_event_type_detection_procurement(self) -> None:
        self.assertEqual(normalize_event_type("technology_update", text="hospital tender award announced"), "procurement")
        self.assertEqual(normalize_event_type("政策与市场动态", text="省级集采中标结果公示"), "procurement")

    def test_procurement_weight_is_highest(self) -> None:
        self.assertEqual(int(EVENT_WEIGHT.get("procurement", 0) or 0), 6)
        self.assertEqual(_event_weight_key("procurement"), "procurement")

    def test_opportunity_score_promoted_by_procurement(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = OpportunityStore(root, asset_dir="artifacts/opportunity")
            # two procurement signals => 12
            for i in range(2):
                store.append_signal(
                    {
                        "date": "2026-02-22",
                        "region": "中国",
                        "lane": "分子诊断",
                        "event_type": "procurement",
                        "weight": EVENT_WEIGHT.get("procurement", 1),
                        "source_id": f"p{i}",
                        "url_norm": f"https://example.com/p{i}",
                    },
                    dedupe_enabled=False,
                )
            # two paper signals => 2
            for i in range(2):
                store.append_signal(
                    {
                        "date": "2026-02-22",
                        "region": "中国",
                        "lane": "分子诊断",
                        "event_type": "paper",
                        "weight": EVENT_WEIGHT.get("paper", 1),
                        "source_id": f"r{i}",
                        "url_norm": f"https://example.com/r{i}",
                    },
                    dedupe_enabled=False,
                )
            out = compute_opportunity_index(root, window_days=7, as_of="2026-02-22")
            rl = out.get("region_lane", {}) if isinstance(out, dict) else {}
            row = rl.get("中国|分子诊断", {}) if isinstance(rl, dict) else {}
            self.assertGreaterEqual(int(row.get("score", 0) or 0), 14)


if __name__ == "__main__":
    unittest.main()
