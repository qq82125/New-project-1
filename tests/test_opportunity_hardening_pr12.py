from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

import yaml

from app.services.collect_asset_store import render_digest_from_assets
from app.services.opportunity_index import compute_opportunity_index
from app.services.opportunity_store import OpportunityStore


class OpportunityHardeningPR12Tests(unittest.TestCase):
    def test_signal_dedup_same_day_same_url(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = OpportunityStore(root, asset_dir="artifacts/opportunity")
            base = {
                "date": "2026-02-21",
                "region": "中国",
                "lane": "肿瘤检测",
                "event_type": "regulatory",
                "weight": 4,
                "source_id": "fda",
                "url_norm": "https://example.com/a",
            }
            r1 = store.append_signal(base)
            r2 = store.append_signal(base)
            rows = store.load_signals(7, now_utc=dt.datetime(2026, 2, 21, tzinfo=dt.timezone.utc))
            self.assertEqual(int(r1.get("written", 0)), 1)
            self.assertEqual(int(r2.get("deduped", 0)), 1)
            self.assertEqual(len(rows), 1)

    def test_signal_probe_filtered(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = OpportunityStore(root, asset_dir="artifacts/opportunity")
            r = store.append_signal(
                {
                    "date": "2026-02-21",
                    "region": "__window_probe__region",
                    "lane": "感染检测",
                    "event_type": "regulatory",
                    "weight": 4,
                    "source_id": "probe",
                    "url_norm": "https://example.com/probe",
                }
            )
            rows = store.load_signals(7, now_utc=dt.datetime(2026, 2, 21, tzinfo=dt.timezone.utc))
            self.assertEqual(int(r.get("dropped_probe", 0)), 1)
            self.assertEqual(len(rows), 0)

    def test_unknown_kpi_rates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = OpportunityStore(root, asset_dir="artifacts/opportunity")
            day = "2026-02-21"
            store.append_signal(
                {
                    "date": day,
                    "region": "",
                    "lane": "肿瘤检测",
                    "event_type": "regulatory",
                    "weight": 4,
                    "source_id": "a",
                    "url_norm": "https://example.com/1",
                }
            )
            store.append_signal(
                {
                    "date": day,
                    "region": "中国",
                    "lane": "",
                    "event_type": "approval",
                    "weight": 4,
                    "source_id": "b",
                    "url_norm": "https://example.com/2",
                }
            )
            store.append_signal(
                {
                    "date": day,
                    "region": "中国",
                    "lane": "感染检测",
                    "event_type": "",
                    "weight": 1,
                    "source_id": "c",
                    "url_norm": "https://example.com/3",
                }
            )
            out = compute_opportunity_index(root, window_days=7, as_of=day)
            kpis = out.get("kpis", {}) if isinstance(out, dict) else {}
            self.assertAlmostEqual(float(kpis.get("unknown_region_rate", 0.0)), 1 / 3, places=6)
            self.assertAlmostEqual(float(kpis.get("unknown_lane_rate", 0.0)), 1 / 3, places=6)
            self.assertAlmostEqual(float(kpis.get("unknown_event_type_rate", 0.0)), 1 / 3, places=6)

    def test_top5_suppresses_unknown_both(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = OpportunityStore(root, asset_dir="artifacts/opportunity")
            day = "2026-02-21"
            for i in range(5):
                store.append_signal(
                    {
                        "date": day,
                        "region": "中国",
                        "lane": f"lane-{i}",
                        "event_type": "regulatory",
                        "weight": 4,
                        "source_id": "src",
                        "url_norm": f"https://example.com/{i}",
                    }
                )
            store.append_signal(
                {
                    "date": day,
                    "region": "",
                    "lane": "",
                    "event_type": "regulatory",
                    "weight": 999,
                    "source_id": "unknown",
                    "url_norm": "https://example.com/unknown",
                }
            )
            out = compute_opportunity_index(root, window_days=7, as_of=day, display={"top_n": 5, "suppress_unknown_both": True})
            top = out.get("top", []) if isinstance(out, dict) else []
            pairs = {(str(r.get("region", "")), str(r.get("lane", ""))) for r in top if isinstance(r, dict)}
            self.assertNotIn(("__unknown__", "__unknown__"), pairs)

    def test_h_section_has_contrib(self) -> None:
        item = {
            "title": "FDA approves diagnostic IVD assay",
            "url": "https://example.com/fda/ivd-1",
            "summary": "diagnostic assay update with sufficient context for report rendering.",
            "source": "FDA",
            "source_id": "fda-medwatch-rss",
            "source_group": "regulatory",
            "event_type": "监管审批与指南",
            "region": "中国",
            "lane": "肿瘤检测",
            "track": "core",
            "relevance_level": 4,
            "published_at": "2026-02-21T08:00:00Z",
        }
        out = render_digest_from_assets(
            date_str="2026-02-21",
            items=[item],
            subject="全球IVD晨报 - 2026-02-21",
            analysis_cfg={"profile": "enhanced", "opportunity_index": {"enabled": True, "window_days": 7}},
            return_meta=True,
        )
        txt = str((out or {}).get("text", ""))
        self.assertIn("contrib:", txt)

    def test_schema_defaults(self) -> None:
        enhanced = yaml.safe_load(Path("rules/content_rules/enhanced.yaml").read_text(encoding="utf-8"))
        legacy = yaml.safe_load(Path("rules/content_rules/legacy.yaml").read_text(encoding="utf-8"))
        e_opp = (((enhanced or {}).get("defaults", {}) or {}).get("opportunity_index", {}) or {})
        l_opp = (((legacy or {}).get("defaults", {}) or {}).get("opportunity_index", {}) or {})
        self.assertTrue(bool(e_opp.get("enabled", False)))
        self.assertFalse(bool(l_opp.get("enabled", True)))
        self.assertEqual(int(((e_opp.get("dedupe", {}) or {}).get("tail_lines_scan", 0) or 0)), 2000)


if __name__ == "__main__":
    unittest.main()
