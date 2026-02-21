from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sqlalchemy import select

from app.db.models import ReportArtifact
from app.services.rules_store import RulesStore


class RunLedgerTests(unittest.TestCase):
    def test_run_execution_write_and_query(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)
            store.upsert_run_execution(
                run_id="run-001",
                profile="enhanced",
                triggered_by="scheduler",
                window="digest",
                status="running",
                started_at="2026-02-19T10:00:00+00:00",
            )
            store.finish_run_execution(
                run_id="run-001",
                status="success",
                ended_at="2026-02-19T10:05:00+00:00",
            )
            rows = store.recent_run_executions(limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "run-001")
            self.assertEqual(rows[0]["status"], "success")
            self.assertEqual(rows[0]["profile"], "enhanced")

    def test_recent_run_query_order(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)
            store.upsert_run_execution(
                run_id="run-old",
                profile="enhanced",
                triggered_by="dryrun",
                window="daily",
                status="success",
                started_at="2026-02-19T00:00:00+00:00",
                ended_at="2026-02-19T00:01:00+00:00",
            )
            store.upsert_run_execution(
                run_id="run-new",
                profile="enhanced",
                triggered_by="scheduler",
                window="daily",
                status="failed",
                started_at="2026-02-19T01:00:00+00:00",
                ended_at="2026-02-19T01:01:00+00:00",
            )
            rows = store.recent_run_executions(limit=1)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "run-new")

    def test_source_fail_top_records_failures(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)
            store.record_source_fetch_event(
                run_id="run-001",
                source_id="src-a",
                status="fail",
                http_status=503,
                items_count=0,
                error="service unavailable",
                duration_ms=1200,
            )
            store.record_source_fetch_event(
                run_id="run-001",
                source_id="src-a",
                status="failed",
                http_status=500,
                items_count=0,
                error="server error",
                duration_ms=1100,
            )
            store.record_source_fetch_event(
                run_id="run-001",
                source_id="src-b",
                status="success",
                http_status=200,
                items_count=3,
                error=None,
                duration_ms=300,
            )
            top = store.source_fail_top(limit=5)
            self.assertGreaterEqual(len(top), 1)
            self.assertEqual(top[0]["source_id"], "src-a")
            self.assertEqual(top[0]["fail_count"], 2)

    def test_report_artifact_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RulesStore(root)
            store.record_report_artifact(
                run_id="run-001",
                artifact_path=str(root / "artifacts" / "run-001" / "newsletter_preview.md"),
                artifact_type="preview_markdown",
                sha256="abc123",
                created_at="2026-02-19T10:06:00+00:00",
            )
            with store.rules_repo._Session() as s:  # type: ignore[attr-defined]
                row = s.execute(
                    select(ReportArtifact).where(ReportArtifact.run_id == "run-001").limit(1)
                ).scalar_one_or_none()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(str(row.artifact_type), "preview_markdown")
            self.assertEqual(str(row.sha256), "abc123")


if __name__ == "__main__":
    unittest.main()
