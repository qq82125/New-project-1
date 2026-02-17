from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.services.run_lock import RunLockError, acquire_run_lock


class RunLockTests(unittest.TestCase):
    def test_lock_exclusive_and_meta_written(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_path = Path(td) / "x.lock"
            with acquire_run_lock(lock_path, run_id="r1", purpose="digest") as meta:
                self.assertEqual(meta["run_id"], "r1")
                meta_file = Path(str(lock_path) + ".meta.json")
                self.assertTrue(meta_file.exists())
                obj = json.loads(meta_file.read_text(encoding="utf-8"))
                self.assertEqual(obj["run_id"], "r1")

                with self.assertRaises(RunLockError):
                    with acquire_run_lock(lock_path, run_id="r2", purpose="digest"):
                        pass


if __name__ == "__main__":
    unittest.main()

