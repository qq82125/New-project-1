from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

from app.services.collect_asset_store import CollectAssetStore
from app.workers.live_run import _compute_segment_window, _parse_send_times


class MailScheduleSegmentedTests(unittest.TestCase):
    def test_parse_send_times_prefers_list(self) -> None:
        schedule = {"send_times": ["09:00", "21:00", "bad"], "hour": 8, "minute": 30}
        self.assertEqual(_parse_send_times(schedule), [(9, 0), (21, 0)])

    def test_parse_send_times_fallback_to_hour_minute(self) -> None:
        schedule = {"hour": 9, "minute": 0}
        self.assertEqual(_parse_send_times(schedule), [(9, 0)])

    def test_compute_segment_window_by_adjacent_slots(self) -> None:
        tz = ZoneInfo("Asia/Shanghai")
        send_times = [(9, 0), (21, 0)]

        now_morning = dt.datetime(2026, 2, 24, 10, 15, tzinfo=tz)
        start_m, end_m = _compute_segment_window(now_local=now_morning, send_times=send_times)
        self.assertEqual((start_m.hour, start_m.minute), (21, 0))
        self.assertEqual((end_m.hour, end_m.minute), (9, 0))
        self.assertEqual((end_m.date() - start_m.date()).days, 1)

        now_evening = dt.datetime(2026, 2, 24, 22, 0, tzinfo=tz)
        start_e, end_e = _compute_segment_window(now_local=now_evening, send_times=send_times)
        self.assertEqual((start_e.hour, start_e.minute), (9, 0))
        self.assertEqual((end_e.hour, end_e.minute), (21, 0))
        self.assertEqual(start_e.date(), end_e.date())

    def test_collect_window_items_supports_explicit_range(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = CollectAssetStore(root, asset_dir="artifacts/collect")
            day = dt.date(2026, 2, 24)
            f = root / "artifacts" / "collect" / f"items-{day.strftime('%Y%m%d')}.jsonl"
            rows = [
                {"dedupe_key": "k1", "url": "https://a", "collected_at": "2026-02-24T00:00:00Z", "published_at": "2026-02-24T00:00:00Z"},
                {"dedupe_key": "k2", "url": "https://b", "collected_at": "2026-02-24T10:00:00Z", "published_at": "2026-02-24T10:00:00Z"},
            ]
            f.parent.mkdir(parents=True, exist_ok=True)
            with f.open("w", encoding="utf-8") as fp:
                for r in rows:
                    fp.write(json.dumps(r, ensure_ascii=False) + "\n")

            out = store.load_window_items(
                window_hours=24,
                window_start_utc=dt.datetime(2026, 2, 24, 9, 0, tzinfo=dt.timezone.utc),
                window_end_utc=dt.datetime(2026, 2, 24, 21, 0, tzinfo=dt.timezone.utc),
            )
            self.assertEqual(len(out), 1)
            self.assertEqual(out[0]["dedupe_key"], "k2")


if __name__ == "__main__":
    unittest.main()
