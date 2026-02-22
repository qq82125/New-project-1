from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.classification_maps import (
    classify_lane,
    classify_region,
    load_lane_map,
    load_region_map,
)
from app.services.opportunity_index import compute_opportunity_index
from app.services.opportunity_store import OpportunityStore


class RegionLaneMappingPR13Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.region_map = load_region_map(self.repo_root / "rules")
        self.lane_map = load_lane_map(self.repo_root / "rules")

    def test_region_medtechdive_to_na(self) -> None:
        self.assertEqual(classify_region("https://www.medtechdive.com/news/x", self.region_map), "北美")

    def test_region_pmda_to_apac(self) -> None:
        self.assertEqual(classify_region("https://www.pmda.go.jp/english/safety/", self.region_map), "亚太")

    def test_region_iivd_to_china(self) -> None:
        self.assertEqual(classify_region("https://www.iivd.net/article/123", self.region_map), "中国")

    def test_region_eu_suffix_to_europe(self) -> None:
        self.assertEqual(classify_region("https://news.example.eu/ivd", self.region_map), "欧洲")

    def test_lane_multi_cancer_to_oncology(self) -> None:
        t = "multi-cancer early detection test for high-risk populations"
        self.assertEqual(classify_lane(t, self.lane_map), "肿瘤检测")

    def test_lane_ldt_clia_to_molecular(self) -> None:
        t = "LDT CLIA laboratory innovation with PCR workflow"
        self.assertEqual(classify_lane(t, self.lane_map), "分子诊断")

    def test_lane_transplant_hla(self) -> None:
        t = "new transplant HLA donor matching workflow"
        self.assertEqual(classify_lane(t, self.lane_map), "输血与移植")

    def test_integration_unknown_rates_drop(self) -> None:
        raw = [
            {
                "url": "https://www.medtechdive.com/news/ivd-mced-test/",
                "title": "multi-cancer early detection test update",
                "event_type": "regulatory",
            },
            {
                "url": "https://www.pmda.go.jp/english/medical-devices/ivd/",
                "title": "LDT CLIA molecular diagnostics policy",
                "event_type": "regulatory",
            },
            {
                "url": "https://www.iivd.net/news/transplant-hla-program",
                "title": "transplant HLA donor screening program",
                "event_type": "technology_update",
            },
        ]

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = OpportunityStore(root, asset_dir="artifacts/opportunity")
            for i, r in enumerate(raw, 1):
                region = classify_region(str(r.get("url", "")), self.region_map)
                lane = classify_lane(str(r.get("title", "")), self.lane_map)
                store.append_signal(
                    {
                        "date": "2026-02-22",
                        "region": region,
                        "lane": lane,
                        "event_type": str(r.get("event_type", "")),
                        "weight": 2,
                        "source_id": f"s{i}",
                        "url_norm": str(r.get("url", "")),
                    }
                )

            out = compute_opportunity_index(root, window_days=7, as_of="2026-02-22")
            kpis = out.get("kpis", {}) if isinstance(out, dict) else {}
            self.assertLess(float(kpis.get("unknown_region_rate", 1.0)), 0.30)
            self.assertLess(float(kpis.get("unknown_lane_rate", 1.0)), 0.30)


if __name__ == "__main__":
    unittest.main()
