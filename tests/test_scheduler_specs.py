from __future__ import annotations

import unittest

from app.workers.scheduler_worker import build_job_specs


class SchedulerSpecsTests(unittest.TestCase):
    def test_build_job_specs_filters_invalid(self) -> None:
        defaults = {
            "schedules": [
                {"id": "a", "type": "cron", "cron": "0 9 * * *", "purpose": "digest", "profile": "enhanced", "jitter_seconds": 0},
                {"id": "b", "type": "interval", "interval_minutes": 60, "purpose": "collect", "profile": "enhanced", "jitter_seconds": 10},
                {"id": "", "type": "cron", "cron": "0 9 * * *", "purpose": "digest", "profile": "enhanced", "jitter_seconds": 0},
                {"id": "x", "type": "cron", "interval_minutes": 1, "purpose": "digest", "profile": "enhanced", "jitter_seconds": 0},
                {"id": "y", "type": "noop", "purpose": "digest", "profile": "enhanced", "jitter_seconds": 0},
            ]
        }
        specs = build_job_specs(defaults)
        ids = [s.id for s in specs]
        self.assertIn("a", ids)
        self.assertIn("b", ids)
        self.assertNotIn("", ids)
        self.assertNotIn("y", ids)


if __name__ == "__main__":
    unittest.main()

