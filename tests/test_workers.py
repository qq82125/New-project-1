from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path

from app.workers.dryrun import run_dryrun
from app.workers.replay import run_replay


class WorkerTests(unittest.TestCase):
    def test_dryrun_contains_expected_structure(self) -> None:
        result = run_dryrun()
        self.assertEqual(result["mode"], "dryrun")
        self.assertEqual(result["profile"]["email"], "default.v1")
        self.assertEqual(result["profile"]["content"], "default.v1")
        self.assertIn("would_collect", result)
        self.assertIn("would_send", result)
        self.assertIn("rules_version", result)

    def test_replay_uses_snapshot_without_send(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        reports_dir = project_root / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        replay_date = datetime.now().strftime("%Y-%m-%d")
        source_file = reports_dir / f"ivd_morning_{replay_date}.txt"
        source_file.write_text("snapshot-content\n", encoding="utf-8")

        result = run_replay(replay_date=replay_date, send=False)
        self.assertEqual(result["mode"], "replay")
        self.assertEqual(result["replay_source"], "existing_report_snapshot")
        self.assertFalse(result["sent"])

        out_file = Path(result["output_file"])
        self.assertTrue(out_file.exists())
        self.assertEqual(out_file.read_text(encoding="utf-8"), "snapshot-content\n")

        out_file.unlink(missing_ok=True)
        source_file.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
