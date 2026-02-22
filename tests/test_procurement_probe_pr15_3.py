from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.fetch_probe import _classify_error_kind, run_procurement_probe
from app.services.source_registry import fetch_source_entries


def _fixture(name: str) -> str:
    p = Path(__file__).parent / "fixtures" / name
    return p.read_text(encoding="utf-8")


class ProcurementProbePR153Tests(unittest.TestCase):
    def test_probe_rss_empty_feed_classification(self) -> None:
        kind, _ = _classify_error_kind(
            {"url": "https://example.com/feed.xml", "fetch": {"mode": "rss"}},
            {"error_type": "parse_empty", "error_message": "no entries", "http_status": 200},
        )
        self.assertEqual(kind, "empty_feed")

    def test_probe_api_needs_api_key_classification(self) -> None:
        kind, _ = _classify_error_kind(
            {"url": "https://api.sam.gov/x?apikey=YOUR_KEY", "fetch": {"mode": "api_json"}},
            {"error_type": "needs_api_key", "error_message": "missing key"},
        )
        self.assertEqual(kind, "needs_api_key")

    def test_probe_http_403_classification(self) -> None:
        kind, _ = _classify_error_kind(
            {"url": "https://example.com/x", "fetch": {"mode": "html_list"}},
            {"error_type": "http_error", "error_message": "HTTPError: 403", "http_status": 403},
        )
        self.assertEqual(kind, "http_403")

    def test_probe_dns_and_timeout_classification(self) -> None:
        kind_dns, _ = _classify_error_kind(
            {"url": "https://example.com/x", "fetch": {"mode": "rss"}},
            {"error_type": "dns_error", "error_message": "nodename nor servname provided"},
        )
        kind_timeout, _ = _classify_error_kind(
            {"url": "https://example.com/x", "fetch": {"mode": "rss"}},
            {"error_type": "timeout", "error_message": "timed out"},
        )
        self.assertEqual(kind_dns, "dns")
        self.assertEqual(kind_timeout, "timeout")

    @patch("app.services.source_registry._fetch_url_with_retry")
    def test_html_list_extracts_items(self, mfetch) -> None:  # noqa: ANN001
        list_html = _fixture("procurement_list_page.html").encode("utf-8")
        detail_html = _fixture("procurement_detail_page.html").encode("utf-8")

        def _side(url, headers, timeout, retries):  # noqa: ANN001
            if str(url).endswith("/chotatu/"):
                return {"ok": True, "data": list_html, "http_status": 200, "content_type": "text/html"}
            return {"ok": True, "data": detail_html, "http_status": 200, "content_type": "text/html"}

        mfetch.side_effect = _side
        out = fetch_source_entries(
            {
                "id": "procurement_mhlw",
                "name": "MHLW",
                "connector": "html",
                "url": "https://www.mhlw.go.jp/sinsei/chotatu/chotatu/",
                "source_group": "procurement",
                "fetch": {
                    "mode": "html_list",
                    "list_link_regex": "notice|detail|\\.html|\\.htm|id=\\d+",
                    "article_min_paragraphs": 1,
                    "article_min_text_chars": 80,
                },
            },
            limit=5,
            source_guard={"enabled": True, "article_min_paragraphs": 1, "article_min_text_chars": 80},
        )
        self.assertTrue(out.get("ok"))
        self.assertGreaterEqual(int(out.get("items_count", 0) or 0), 1)

    @patch("app.services.source_registry._fetch_url_with_retry")
    def test_html_list_page_itself_not_item(self, mfetch) -> None:  # noqa: ANN001
        list_html = _fixture("procurement_list_page.html").encode("utf-8")
        detail_html = _fixture("procurement_detail_page.html").encode("utf-8")

        def _side(url, headers, timeout, retries):  # noqa: ANN001
            if str(url).endswith("/chotatu/"):
                return {"ok": True, "data": list_html, "http_status": 200, "content_type": "text/html"}
            return {"ok": True, "data": detail_html, "http_status": 200, "content_type": "text/html"}

        mfetch.side_effect = _side
        src_url = "https://www.mhlw.go.jp/sinsei/chotatu/chotatu/"
        out = fetch_source_entries(
            {
                "id": "procurement_mhlw",
                "name": "MHLW",
                "connector": "html",
                "url": src_url,
                "source_group": "procurement",
                "fetch": {"mode": "html_list", "list_link_regex": "notice|detail|\\.html|\\.htm|id=\\d+"},
            },
            limit=5,
            source_guard={"enabled": True, "article_min_paragraphs": 1, "article_min_text_chars": 80},
        )
        entries = out.get("entries", []) if isinstance(out.get("entries"), list) else []
        self.assertTrue(all(str(x.get("url", "")) != src_url for x in entries))

    @patch("app.services.source_registry._fetch_url_with_retry")
    def test_detail_page_short_threshold_80_passes(self, mfetch) -> None:  # noqa: ANN001
        list_html = "<html><body><a href='/procurement/notice-1.html'>n1</a></body></html>".encode("utf-8")
        detail_html = (
            "<html><body><article><h1>Award</h1>"
            "<p>short procurement text one with enough additional wording for the threshold.</p>"
            "<p>second text expands context for laboratory reagent tender processing.</p>"
            "</article></body></html>"
        ).encode("utf-8")

        def _side(url, headers, timeout, retries):  # noqa: ANN001
            if str(url).endswith("/list"):
                return {"ok": True, "data": list_html, "http_status": 200, "content_type": "text/html"}
            return {"ok": True, "data": detail_html, "http_status": 200, "content_type": "text/html"}

        mfetch.side_effect = _side
        out = fetch_source_entries(
            {
                "id": "p",
                "connector": "html",
                "url": "https://example.com/list",
                "source_group": "procurement",
                "fetch": {"mode": "html_list", "article_min_paragraphs": 1, "article_min_text_chars": 80},
            },
            source_guard={"enabled": True, "article_min_paragraphs": 1, "article_min_text_chars": 80},
        )
        self.assertTrue(bool(out.get("ok")))

    @patch("app.services.fetch_probe.fetch_source_entries")
    def test_probe_report_files_written(self, mfetch) -> None:  # noqa: ANN001
        mfetch.return_value = {"ok": False, "error_type": "parse_empty", "error_message": "empty", "items_count": 0, "duration_ms": 5}
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "rules" / "sources").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "sources" / "p.yaml").write_text(
                "version: '1.0.0'\nsources:\n  - id: p1\n    enabled: true\n    connector: rss\n    url: https://example.com/rss\n    source_group: procurement\n",
                encoding="utf-8",
            )
            out = run_procurement_probe(project_root=root, output_dir="artifacts/procurement", force=True)
            self.assertTrue(out.get("ok"))
            js = Path(out["artifacts"]["json"])
            md = Path(out["artifacts"]["md"])
            self.assertTrue(js.exists())
            self.assertTrue(md.exists())
            payload = json.loads(js.read_text(encoding="utf-8"))
            self.assertIn("totals", payload)
            self.assertIn("per_source", payload)

    @patch("app.workers.cli.run_procurement_probe")
    def test_cli_procurement_probe_route_runs(self, mrun) -> None:  # noqa: ANN001
        mrun.return_value = {"ok": True, "totals": {}, "per_source": []}
        from app.workers import cli

        rc = cli.cmd_procurement_probe(["--force", "true", "--max-sources", "1"])
        self.assertEqual(rc, 0)
        self.assertTrue(mrun.called)

    @patch("app.services.fetch_probe.fetch_source_entries")
    def test_probe_write_assets_writes_collect_jsonl(self, mfetch) -> None:  # noqa: ANN001
        mfetch.return_value = {
            "ok": True,
            "items_count": 1,
            "entries": [{"title": "bid award", "url": "https://example.com/a", "summary": "procurement notice"}],
            "duration_ms": 5,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "rules" / "sources").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "sources" / "p.yaml").write_text(
                "version: '1.0.0'\nsources:\n  - id: p1\n    enabled: true\n    connector: rss\n    url: https://example.com/rss\n    source_group: procurement\n    trust_tier: A\n",
                encoding="utf-8",
            )
            out = run_procurement_probe(project_root=root, write_assets=True, output_dir="artifacts/procurement", force=True)
            self.assertTrue(out.get("ok"))
            collect_files = sorted((root / "artifacts" / "collect").glob("items-*.jsonl"))
            self.assertTrue(bool(collect_files))


if __name__ == "__main__":
    unittest.main()
