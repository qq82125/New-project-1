from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.source_registry import run_sources_test_harness, test_source


class _Resp:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self._body = body
        self.status = status
        self.headers = {"Content-Type": "text/html; charset=utf-8"}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Feed:
    def __init__(self, entries: list[object]) -> None:
        self.entries = entries


def _entry(title: str, link: str) -> object:
    return type("E", (), {"title": title, "link": link, "published": "2026-02-19"})()


class SourcesHarnessTests(unittest.TestCase):
    def _fixture(self, name: str) -> str:
        p = Path(__file__).parent / "fixtures" / name
        return p.read_text(encoding="utf-8")

    @patch("app.services.source_registry.feedparser.parse")
    @patch("app.services.source_registry.urlopen")
    def test_rss_discovery_extracts_feed_link_from_html(self, mock_urlopen, mock_parse) -> None:
        html = self._fixture("rss_discovery_page.html")
        mock_urlopen.side_effect = [_Resp(html.encode("utf-8")), _Resp(b"<rss></rss>")]
        mock_parse.side_effect = [_Feed([]), _Feed([_entry("A", "https://example.com/a")])]

        out = test_source(
            {"id": "s1", "fetcher": "rss", "url": "https://example.com/news"},
            limit=3,
        )
        self.assertTrue(out["ok"])
        self.assertEqual(out["discovered_feed_url"], "https://example.com/feed.xml")
        self.assertGreater(out["items_count"], 0)

    @patch("app.services.source_registry.feedparser.parse")
    @patch("app.services.source_registry.urlopen")
    def test_index_discovery_extracts_child_feeds(self, mock_urlopen, mock_parse) -> None:
        html = self._fixture("index_fda_like.html")
        mock_urlopen.side_effect = [_Resp(html.encode("utf-8")), _Resp(b"<rss></rss>")]
        mock_parse.return_value = _Feed([_entry("Device update", "https://example.com/dev")])

        out = test_source(
            {
                "id": "fda-index",
                "fetcher": "html",
                "url": "https://example.com/get-email-updates",
                "tags": ["regulatory"],
                "discovery_policy": "pick_by_keywords",
            },
            limit=3,
        )
        self.assertTrue(out["ok"])
        self.assertTrue(len(out.get("discovered_child_feeds", [])) >= 1)
        self.assertIn("medical-devices", out.get("discovered_feed_url", ""))

    @patch("app.services.source_registry.urlopen")
    def test_html_list_parser_extracts_items(self, mock_urlopen) -> None:
        html = self._fixture("html_who_like.html")
        mock_urlopen.return_value = _Resp(html.encode("utf-8"))

        out = test_source(
            {"id": "who", "fetcher": "html", "url": "https://example.com/prequal"},
            limit=3,
        )
        self.assertTrue(out["ok"])
        self.assertEqual(out["items_count"], 3)
        self.assertIn("title", out["samples"][0])
        self.assertIn("url", out["samples"][0])

    @patch("app.services.source_registry.test_source")
    def test_sources_test_report_schema(self, mock_test_source) -> None:
        mock_test_source.side_effect = [
            {
                "id": "a",
                "source_id": "a",
                "name": "A",
                "fetcher": "rss",
                "enabled": True,
                "status": "success",
                "ok": True,
                "http_status": 200,
                "items_count": 2,
                "samples": [{"title": "t1", "url": "https://x/1"}],
                "duration_ms": 12,
                "error_type": "",
                "error_message": "",
            },
            {
                "id": "b",
                "source_id": "b",
                "name": "B",
                "fetcher": "html",
                "enabled": True,
                "status": "failed",
                "ok": False,
                "http_status": 403,
                "items_count": 0,
                "samples": [],
                "duration_ms": 8,
                "error_type": "http_error",
                "error_message": "HTTPError: 403",
            },
        ]

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "rules").mkdir(parents=True, exist_ok=True)
            (root / "data").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "sources_registry.v1.yaml").write_text(
                """
version: "1.0.0"
sources:
  - id: a
    name: A
    url: https://example.com/a.xml
    region: Global
    trust_tier: A
    enabled: true
    fetcher: rss
  - id: b
    name: B
    url: https://example.com/b
    region: Global
    trust_tier: B
    enabled: true
    fetcher: html
groups:
  media_global: [a, b]
""".strip()
                + "\n",
                encoding="utf-8",
            )

            out = run_sources_test_harness(root, enabled_only=True, limit=3, max_workers=2)
            self.assertTrue(out["ok"])
            self.assertIn("summary", out)
            self.assertIn("results", out)
            self.assertEqual(out["summary"]["total"], 2)
            self.assertIn("markdown", out)
            self.assertIn("by_fetcher", out["summary"])
            self.assertIn("top_failure_reasons", out["summary"])


if __name__ == "__main__":
    unittest.main()

