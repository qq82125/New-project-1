from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.collect_asset_store import CollectAssetStore
from app.services.opportunity_store import normalize_event_type
from app.services.source_registry import fetch_source_entries


class ProcurementOnboardingPR151Tests(unittest.TestCase):
    def test_file_rss_fixture_parsing(self) -> None:
        root = Path(__file__).resolve().parents[1]
        fixture = root / "tests" / "fixtures" / "procurement_sample_rss.xml"
        out = fetch_source_entries(
            {
                "id": "procurement_fixture",
                "connector": "rss",
                "url": f"file://{fixture}",
                "source_group": "procurement",
                "fetch": {"mode": "rss"},
            },
            limit=10,
            timeout_seconds=3,
            retries=0,
            source_guard={"enabled": True},
        )
        self.assertTrue(out.get("ok"))
        self.assertGreaterEqual(int(out.get("items_count", 0) or 0), 2)

    def test_collect_writes_fixture_rows(self) -> None:
        root = Path(__file__).resolve().parents[1]
        fixture = root / "tests" / "fixtures" / "procurement_sample_rss.xml"
        fetched = fetch_source_entries(
            {
                "id": "procurement_fixture",
                "connector": "rss",
                "url": f"file://{fixture}",
                "source_group": "procurement",
                "fetch": {"mode": "rss"},
            },
            limit=10,
            timeout_seconds=3,
            retries=0,
            source_guard={"enabled": True},
        )
        entries = list(fetched.get("entries", [])) if isinstance(fetched.get("entries"), list) else []
        with tempfile.TemporaryDirectory() as td:
            td_root = Path(td)
            store = CollectAssetStore(td_root, asset_dir="artifacts/collect")
            wr = store.append_items(
                run_id="pr15-1",
                source_id="procurement_fixture",
                source_name="Procurement Fixture RSS",
                source_group="procurement",
                source_trust_tier="A",
                items=entries,
            )
            self.assertGreaterEqual(int(wr.get("written", 0) or 0), 1)
            rows = store.load_window_items(window_hours=72)
            self.assertTrue(any(str(x.get("source_id", "")) == "procurement_fixture" for x in rows))

    def test_procurement_event_type_detection(self) -> None:
        self.assertEqual(
            normalize_event_type("technology_update", text="省级采购结果公示，PCR试剂中标"),
            "procurement",
        )
        self.assertEqual(
            normalize_event_type("__unknown__", text="hospital tender award for diagnostic kits"),
            "procurement",
        )


if __name__ == "__main__":
    unittest.main()
