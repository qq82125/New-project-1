from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class CloudBackupSendTests(unittest.TestCase):
    def test_missing_env_produces_diagnostic_report_and_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            wd = Path(td)
            (wd / "reports").mkdir(parents=True, exist_ok=True)

            env = {
                # Minimal required vars: keep one missing on purpose (SMTP_PASS).
                "SMTP_HOST": "smtp.example.com",
                "SMTP_USER": "u@example.com",
                "SMTP_PASS": "",
                "TO_EMAIL": "to@example.com",
                "REPORT_TZ": "Asia/Shanghai",
                "GITHUB_RUN_ID": "12345",
                "GITHUB_SHA": "deadbeef",
            }
            with patch.dict(os.environ, env, clear=True):
                with patch("scripts.cloud_backup_send.load_runtime_rules", return_value={"enabled": False}):
                    from scripts.cloud_backup_send import main

                    cwd = os.getcwd()
                    try:
                        os.chdir(wd)
                        rc = main(["--date", "2026-02-16", "--dry-run"])
                    finally:
                        os.chdir(cwd)

            self.assertEqual(rc, 2)
            diag = wd / "reports" / "ivd_backup_2026-02-16.txt"
            self.assertTrue(diag.exists())
            content = diag.read_text(encoding="utf-8")
            self.assertIn("run_id:", content)
            self.assertIn("subject_final:", content)
            self.assertIn("missing_env=", content)
            self.assertIn("SMTP_PASS", content)

    def test_smtp_failure_is_recorded_in_diagnostic_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            wd = Path(td)
            env = {
                "SMTP_HOST": "smtp.example.com",
                "SMTP_USER": "u@example.com",
                "SMTP_PASS": "secret",
                "SMTP_FROM": "u@example.com",
                "TO_EMAIL": "to@example.com",
                "REPORT_TZ": "Asia/Shanghai",
                "GITHUB_RUN_ID": "12345",
                "GITHUB_SHA": "deadbeef",
            }
            with patch.dict(os.environ, env, clear=True):
                with patch("scripts.cloud_backup_send.load_runtime_rules", return_value={"enabled": False}):
                    with patch("scripts.cloud_backup_send.imap_check_sent", return_value={"ok": True, "already_sent": False, "hits": 0, "mailbox": "Sent"}):
                        with patch("scripts.cloud_backup_send.send_email", side_effect=RuntimeError("smtp boom")):
                            from scripts.cloud_backup_send import main

                            cwd = os.getcwd()
                            try:
                                os.chdir(wd)
                                rc = main(["--date", "2026-02-16"])
                            finally:
                                os.chdir(cwd)

            self.assertEqual(rc, 3)
            diag = wd / "reports" / "ivd_backup_2026-02-16.txt"
            self.assertTrue(diag.exists())
            content = diag.read_text(encoding="utf-8")
            self.assertIn("## SMTP Send", content)
            self.assertIn("status=FAIL", content)
            self.assertIn("error=", content)


if __name__ == "__main__":
    unittest.main()
