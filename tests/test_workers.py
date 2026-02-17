from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.workers.dryrun import run_dryrun
from app.workers.replay import run_replay


class WorkerTests(unittest.TestCase):
    @patch("app.workers.dryrun.RuleEngine")
    @patch("app.workers.dryrun.subprocess.run")
    def test_dryrun_generates_artifacts_and_no_send(self, mock_run, mock_engine_cls) -> None:
        with tempfile.TemporaryDirectory() as td:
            project_root = Path(td)
            mock_engine = mock_engine_cls.return_value
            mock_engine.project_root = project_root
            mock_engine.build_decision.return_value = {
                "rules_version": {"email": "2.0.0", "content": "2.0.0"},
                "content_decision": {},
                "qc_decision": {"min_24h_items": 1, "apac_min_share": 0.0, "china_min_share": 0.0, "required_sources_checklist": []},
                "output_decision": {"A": {"items_range": {"min": 1, "max": 15}}},
                "email_decision": {"schedule": {"timezone": "Asia/Shanghai"}},
                "explain": {},
            }

            mock_run.return_value.stdout = (
                "全球IVD晨报 - 2026-02-16\n\n"
                "A. 今日要点（8-15条，按重要性排序）\n"
                "1) [24小时内] test title\n"
                "摘要：test summary\n"
                "发布日期：2026-02-16 08:30 CST\n"
                "来源：Test Source | https://example.com/item\n"
                "地区：北美\n"
                "赛道：其他\n"
                "事件类型：政策与市场动态\n"
                "技术平台：跨平台/未标注\n\n"
                "B. 分赛道速览（肿瘤/感染/生殖遗传/其他）\n"
            )

            result = run_dryrun(profile="enhanced", report_date="2026-02-16")
            artifacts_dir = Path(result["artifacts_dir"])

            self.assertEqual(result["mode"], "dryrun")
            self.assertFalse(result["sent"])
            self.assertTrue((artifacts_dir / "run_id.json").exists())
            self.assertTrue((artifacts_dir / "newsletter_preview.md").exists())
            self.assertTrue((artifacts_dir / "items.json").exists())
            self.assertTrue((artifacts_dir / "qc_report.json").exists())
            self.assertTrue((artifacts_dir / "output_render.json").exists())
            self.assertTrue((artifacts_dir / "run_meta.json").exists())

            items = json.loads((artifacts_dir / "items.json").read_text(encoding="utf-8"))
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["title"], "test title")
            self.assertIn("items_before_count", result)
            self.assertIn("items_after_count", result)
            self.assertIn("top_clusters", result)

            preview = (artifacts_dir / "newsletter_preview.md").read_text(encoding="utf-8")
            # G must be at the end and A-F must not contain quality markers.
            self.assertIn("G. 质量指标", preview)
            self.assertIn("24H条目数 / 7D补充数：", preview)
            a_to_f = preview.split("G. 质量指标", 1)[0]
            self.assertNotIn("24H条目数", a_to_f)
            self.assertNotIn("Quality Audit", a_to_f)

            cmd = mock_run.call_args[0][0]
            self.assertEqual(cmd, ["python3", "scripts/generate_ivd_report.py"])
            self.assertNotIn("send_mail_icloud.sh", " ".join(cmd))

    @patch("app.workers.replay.RuleEngine")
    @patch("app.workers.replay.subprocess.run")
    def test_replay_uses_artifacts_only_and_no_send(self, mock_run, mock_engine_cls) -> None:
        with tempfile.TemporaryDirectory() as td:
            project_root = Path(td)
            mock_engine = mock_engine_cls.return_value
            mock_engine.project_root = project_root
            mock_engine.build_decision.return_value = {
                "email_decision": {"schedule": {"timezone": "Asia/Shanghai"}},
            }

            artifacts_dir = project_root / "artifacts" / "dryrun-abc"
            artifacts_dir.mkdir(parents=True, exist_ok=True)

            (artifacts_dir / "run_id.json").write_text(
                json.dumps(
                    {
                        "run_id": "dryrun-abc",
                        "mode": "dryrun",
                        "profile": "legacy",
                        "date": "2026-02-16",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (artifacts_dir / "newsletter_preview.md").write_text("preview\n", encoding="utf-8")
            (artifacts_dir / "items.json").write_text("[]", encoding="utf-8")

            result = run_replay(run_id="dryrun-abc", send=False, profile="legacy")

            self.assertEqual(result["mode"], "replay")
            self.assertEqual(result["replay_source"], "artifacts_only")
            self.assertFalse(result["network_fetch"])
            self.assertFalse(result["sent"])
            mock_run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
