from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.acceptance_run import run_acceptance


class AcceptanceRunTests(unittest.TestCase):
    def test_acceptance_smoke_writes_reports(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            collect_file = root / "artifacts" / "collect" / "items-20260221.jsonl"

            def _fake_run(cmd, cwd):  # noqa: ANN001
                joined = " ".join(cmd)
                if "collect-now" in joined:
                    collect_file.parent.mkdir(parents=True, exist_ok=True)
                    collect_file.write_text('{"url":"https://x/a"}\n', encoding="utf-8")
                    return 0, {"ok": True, "deduped_count": 1}, '{"ok": true}'
                if "digest-now" in joined:
                    out_file = root / "reports" / "ivd_morning_2026-02-21.txt"
                    out_file.parent.mkdir(parents=True, exist_ok=True)
                    out_file.write_text(
                        "\n".join(
                            [
                                "A. x",
                                "B. x",
                                "C. x",
                                "D. x",
                                "E. x",
                                "F. x",
                                "G. 质量指标 (Quality Audit)",
                                "core/frontier覆盖：1/1",
                                "items_before_dedupe：3 | items_after_dedupe：2 | reduction_ratio：33%",
                            ]
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                    return 0, {"ok": True, "output_file": str(out_file), "analysis_meta": {"analysis_cache_hit": 1, "analysis_cache_miss": 0}}, '{"ok": true}'
                return 1, {}, "unknown"

            with patch("scripts.acceptance_run._run_cmd_json", side_effect=_fake_run):
                out = run_acceptance(project_root=root, mode="smoke", as_of="2026-02-21", keep_artifacts=False)
            self.assertIn("summary", out)
            self.assertEqual(out.get("mode"), "smoke")
            md = root / "artifacts" / "acceptance" / "acceptance_report.md"
            js = root / "artifacts" / "acceptance" / "acceptance_report.json"
            self.assertTrue(md.exists())
            self.assertTrue(js.exists())

    def test_acceptance_has_minimum_six_assertions_in_full(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out = run_acceptance(project_root=root, mode="full", as_of="2026-02-21", keep_artifacts=False)
            checks = out.get("checks", [])
            self.assertGreaterEqual(len(checks), 6)
            qmd = root / "artifacts" / "acceptance" / "quality_pack.md"
            qjs = root / "artifacts" / "acceptance" / "quality_pack.json"
            self.assertTrue(qmd.exists())
            self.assertTrue(qjs.exists())


if __name__ == "__main__":
    unittest.main()
