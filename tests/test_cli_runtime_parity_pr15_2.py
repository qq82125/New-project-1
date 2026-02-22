from __future__ import annotations

import builtins
import importlib
import unittest
from unittest.mock import patch


class CliRuntimeParityPR152Tests(unittest.TestCase):
    def test_import_cli_without_apscheduler_side_effect(self) -> None:
        original_import = builtins.__import__

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name.startswith("apscheduler"):
                raise AssertionError("apscheduler should not be imported when importing cli module")
            return original_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=guarded_import):
            cli = importlib.import_module("app.workers.cli")
            importlib.reload(cli)

    @patch("app.workers.scheduler_worker.SchedulerWorker._run_collect")
    def test_collect_now_entrypoint_available(self, mock_run_collect) -> None:
        mock_run_collect.return_value = {"ok": True, "run_id": "x"}
        cli = importlib.import_module("app.workers.cli")
        rc = cli.cmd_collect_now(["--force", "true", "--max-sources", "1"])
        self.assertEqual(rc, 0)
        self.assertTrue(mock_run_collect.called)


if __name__ == "__main__":
    unittest.main()
