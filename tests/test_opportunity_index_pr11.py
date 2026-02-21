from __future__ import annotations

import datetime as dt
import os
import tempfile
import unittest
from pathlib import Path

from app.services.collect_asset_store import render_digest_from_assets
from app.services.opportunity_index import compute_opportunity_index
from app.services.opportunity_store import OpportunityStore


class OpportunityIndexPR11Tests(unittest.TestCase):
    def test_append_signal_unknown_fields_normalized(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = OpportunityStore(root, asset_dir="artifacts/opportunity")
            store.append_signal(
                {
                    "date": "2026-02-21",
                    "region": "",
                    "lane": "",
                    "event_type": "regulatory",
                    "weight": 4,
                    "source_id": "fda",
                    "url_norm": "https://example.com/a",
                }
            )
            rows = store.load_signals(7, now_utc=dt.datetime(2026, 2, 21, 0, 0, tzinfo=dt.timezone.utc))
            self.assertEqual(len(rows), 1)
            self.assertEqual(str(rows[0].get("region", "")), "__unknown__")
            self.assertEqual(str(rows[0].get("lane", "")), "__unknown__")

    def test_compute_opportunity_index_delta(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = OpportunityStore(root, asset_dir="artifacts/opportunity")
            for _ in range(3):
                store.append_signal(
                    {
                        "date": "2026-02-21",
                        "region": "中国",
                        "lane": "肿瘤检测",
                        "event_type": "regulatory",
                        "weight": 4,
                        "source_id": "fda",
                        "url_norm": "https://example.com/today",
                    }
                    ,
                    dedupe_enabled=False,
                )
            store.append_signal(
                {
                    "date": "2026-02-14",
                    "region": "中国",
                    "lane": "肿瘤检测",
                    "event_type": "paper",
                    "weight": 1,
                    "source_id": "paper",
                    "url_norm": "https://example.com/prev",
                }
            )
            out = compute_opportunity_index(root, window_days=7, as_of="2026-02-21", asset_dir="artifacts/opportunity")
            rl = out.get("region_lane", {}) if isinstance(out, dict) else {}
            row = rl.get("中国|肿瘤检测", {}) if isinstance(rl, dict) else {}
            self.assertEqual(int(row.get("score", 0) or 0), 12)
            self.assertGreater(int(row.get("delta_vs_prev_window", 0) or 0), 0)

    def test_render_digest_writes_signals_and_h_section(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = os.getcwd()
            try:
                os.chdir(td)
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
                    analysis_cfg={
                        "profile": "enhanced",
                        "opportunity_index": {"enabled": True, "window_days": 7, "asset_dir": "artifacts/opportunity"},
                    },
                    return_meta=True,
                )
                txt = str((out or {}).get("text", ""))
                meta = (out or {}).get("meta", {})
                self.assertIn("H. 机会强度指数", txt)
                self.assertGreaterEqual(int((meta or {}).get("opportunity_signals_written", 0) or 0), 1)
                p = Path("artifacts/opportunity/opportunity_signals-20260221.jsonl")
                self.assertTrue(p.exists())
            finally:
                os.chdir(cwd)

    def test_legacy_profile_does_not_emit_h_by_default(self) -> None:
        item = {
            "title": "Legacy diagnostic IVD assay",
            "url": "https://example.com/legacy/ivd",
            "summary": "diagnostic assay update",
            "source": "FDA",
            "source_group": "regulatory",
            "event_type": "监管审批与指南",
            "track": "core",
            "relevance_level": 3,
            "published_at": "2026-02-21T08:00:00Z",
        }
        out = render_digest_from_assets(
            date_str="2026-02-21",
            items=[item],
            subject="全球IVD晨报 - 2026-02-21",
            analysis_cfg={"profile": "legacy"},
            return_meta=True,
        )
        txt = str((out or {}).get("text", ""))
        self.assertNotIn("H. 机会强度指数", txt)


if __name__ == "__main__":
    unittest.main()
