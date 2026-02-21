from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from app.services.collect_asset_store import (
    CollectAssetStore,
    append_jsonl,
    render_digest_from_assets,
    url_norm,
)
from app.services.source_registry import fetch_source_entries


class CollectDigestPR2Tests(unittest.TestCase):
    def test_url_norm_dedupe_equivalent_urls(self) -> None:
        a = "https://Example.com/path/item/?utm_source=x#frag"
        b = "https://example.com/path/item"
        self.assertEqual(url_norm(a), url_norm(b))

    def test_append_jsonl_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "items.jsonl"
            append_jsonl(p, {"a": 1, "b": "x"})
            append_jsonl(p, {"a": 2, "b": "y"})
            rows = [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["a"], 1)
            self.assertEqual(rows[1]["b"], "y")

    @patch("app.services.source_registry.urlopen")
    def test_collect_with_rss_fixture_writes_rows(self, mock_urlopen) -> None:
        class _Resp:
            status = 200

            def read(self):
                return (
                    b'<?xml version="1.0" encoding="UTF-8"?>'
                    b"<rss><channel>"
                    b"<item><title>IVD assay update</title><link>https://example.com/a</link><pubDate>Thu, 20 Feb 2026 08:00:00 GMT</pubDate></item>"
                    b"<item><title>PCR panel launch</title><link>https://example.com/b</link><pubDate>Thu, 20 Feb 2026 09:00:00 GMT</pubDate></item>"
                    b"</channel></rss>"
                )

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        mock_urlopen.return_value = _Resp()
        res = fetch_source_entries(
            {"id": "rss-1", "connector": "rss", "url": "https://example.com/feed.xml"},
            limit=20,
            timeout_seconds=5,
            retries=0,
        )
        self.assertTrue(res.get("ok"))
        self.assertGreater(int(res.get("items_count") or 0), 0)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = CollectAssetStore(root, asset_dir="artifacts/collect")
            out = store.append_items(
                run_id="collect-rss",
                source_id="rss-1",
                source_name="RSS 1",
                source_group="media",
                items=list(res.get("entries", [])),
            )
            self.assertGreater(out.get("written", 0), 0)

    def test_collect_append_jsonl_and_dedupe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = CollectAssetStore(root, asset_dir="artifacts/collect")
            out = store.append_items(
                run_id="collect-1",
                source_id="s1",
                source_name="Source 1",
                source_group="media",
                items=[
                    {"title": "FDA approves IVD diagnostic assay", "url": "https://example.com/a", "summary": "good"},
                    {"title": "FDA approves IVD diagnostic assay update", "url": "https://example.com/a", "summary": "dup"},
                ],
            )
            self.assertEqual(out["written"], 1)
            self.assertEqual(out["skipped"], 1)

            files = sorted((root / "artifacts" / "collect").glob("items-*.jsonl"))
            self.assertEqual(len(files), 1)
            lines = [x for x in files[0].read_text(encoding="utf-8").splitlines() if x.strip()]
            self.assertEqual(len(lines), 1)
            row = json.loads(lines[0])
            self.assertEqual(row["source_id"], "s1")
            self.assertTrue(row["dedupe_key"])

    def test_digest_reads_assets_and_renders_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = CollectAssetStore(root, asset_dir="artifacts/collect")
            store.append_items(
                run_id="collect-2",
                source_id="fda",
                source_name="FDA",
                source_group="regulatory",
                items=[{"title": "FDA approves IVD diagnostic assay", "url": "https://example.com/fda-1", "summary": "regulatory approval"}],
            )
            rows = store.load_window_items(window_hours=24)
            txt = render_digest_from_assets(
                date_str="2026-02-20",
                items=rows,
                subject="全球IVD晨报 - 2026-02-20",
            )
            self.assertIn("A. 今日要点", txt)
            self.assertIn("G. 质量指标", txt)
            self.assertIn("FDA approves IVD diagnostic assay", txt)

    def test_digest_without_assets_has_gap_explain_in_g(self) -> None:
        txt = render_digest_from_assets(
            date_str="2026-02-20",
            items=[],
            subject="全球IVD晨报 - 2026-02-20",
        )
        self.assertIn("G. 质量指标", txt)
        self.assertIn("分流规则缺口说明", txt)

    def test_scheduler_purpose_collect_routes_without_digest(self) -> None:
        try:
            from app.workers.scheduler_worker import SchedulerWorker
        except Exception as e:  # pragma: no cover
            self.skipTest(f"scheduler dependency missing: {e}")
            return

        @contextmanager
        def _noop_lock(*args, **kwargs):
            yield

        try:
            worker = SchedulerWorker()
        except SystemExit as e:  # pragma: no cover
            self.skipTest(str(e))
            return
        with patch("app.workers.scheduler_worker.acquire_run_lock", _noop_lock):
            with patch.object(worker, "_run_collect", return_value={"ok": True}) as mock_collect:
                with patch("app.workers.scheduler_worker.run_digest") as mock_digest:
                    worker._run_job(
                        schedule_id="manual",
                        purpose="collect",
                        profile="enhanced",
                        jitter=0,
                        misfire_grace=600,
                        trigger="manual",
                    )
                    mock_collect.assert_called_once()
                    mock_digest.assert_not_called()


if __name__ == "__main__":
    unittest.main()
