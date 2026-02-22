from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.collect_asset_store import CollectAssetStore
from app.services.page_classifier import is_article_html, is_static_or_listing_url
from app.services.source_registry import fetch_source_entries


ARTICLE_HTML = """
<html><head>
<meta property="og:type" content="article" />
<meta property="og:title" content="IVD Article" />
<meta property="article:published_time" content="2026-02-22T08:00:00Z" />
</head><body>
<article>
<h1>IVD Article</h1>
<p>Paragraph one with enough content to be meaningful for extraction.</p>
<p>Paragraph two includes diagnostics context and workflow details for labs.</p>
<p>Paragraph three adds additional supporting text for minimum length.</p>
</article>
</body></html>
"""

LISTING_HTML = """
<html><body>
<nav>menu menu menu</nav>
<a href="/category/a">A</a><a href="/category/b">B</a><a href="/category/c">C</a>
<a href="/category/d">D</a><a href="/category/e">E</a><a href="/category/f">F</a>
<h1>Category</h1>
<p>Short intro.</p>
</body></html>
"""

RSS_NO_SUMMARY = b"""
<rss version='2.0'><channel><title>x</title>
<item><title>Entry A</title><link>https://example.com/a</link><pubDate>Mon, 01 Jan 2026 00:00:00 GMT</pubDate></item>
</channel></rss>
"""


class SourceItemHardeningPR14Tests(unittest.TestCase):
    def test_static_or_listing_url_keywords(self) -> None:
        self.assertTrue(is_static_or_listing_url("https://x.com/about"))
        self.assertTrue(is_static_or_listing_url("https://x.com/privacy"))
        self.assertTrue(is_static_or_listing_url("https://x.com/portal"))
        self.assertTrue(is_static_or_listing_url("https://x.com/newsletter"))

    def test_static_or_listing_laboratory_diagnostics(self) -> None:
        self.assertTrue(is_static_or_listing_url("https://www.siemens-healthineers.com/laboratory-diagnostics"))

    def test_is_article_html_ok(self) -> None:
        ok, reason, meta = is_article_html(ARTICLE_HTML, "https://example.com/article/123", article_min_paragraphs=2, article_min_text_chars=120)
        self.assertTrue(ok)
        self.assertEqual(reason, "article_ok")
        self.assertGreaterEqual(int(meta.get("paragraph_count", 0) or 0), 2)

    def test_is_article_html_listing_false(self) -> None:
        ok, reason, _ = is_article_html(LISTING_HTML, "https://example.com/category/news")
        self.assertFalse(ok)
        self.assertIn(reason, {"static_or_listing_page", "too_short", "too_few_paragraphs"})

    def test_is_article_html_too_short_false(self) -> None:
        short = "<html><body><h1>A</h1><p>tiny</p><p>tiny</p></body></html>"
        ok, reason, _ = is_article_html(short, "https://example.com/article/1", article_min_paragraphs=2, article_min_text_chars=200)
        self.assertFalse(ok)
        self.assertEqual(reason, "too_short")

    @patch("app.services.source_registry._fetch_url_with_retry")
    def test_rss_missing_summary_allow_body_fetch_false(self, mfetch) -> None:  # noqa: ANN001
        mfetch.return_value = {"ok": True, "data": RSS_NO_SUMMARY, "http_status": 200, "content_type": "application/rss+xml"}
        out = fetch_source_entries(
            {
                "id": "s1",
                "name": "S1",
                "connector": "rss",
                "url": "https://example.com/feed.xml",
                "fetch": {"mode": "rss", "allow_body_fetch_for_rss": False},
            },
            limit=5,
            source_guard={"enabled": True, "enforce_article_only": True, "allow_body_fetch_for_rss": False},
        )
        self.assertTrue(bool(out.get("ok")))
        entries = out.get("entries", []) if isinstance(out.get("entries"), list) else []
        self.assertEqual(len(entries), 1)
        self.assertEqual(str(entries[0].get("summary", "")), "")
        self.assertEqual(mfetch.call_count, 1)

    @patch("app.services.source_registry._fetch_url_with_retry")
    def test_rss_missing_summary_allow_body_fetch_true(self, mfetch) -> None:  # noqa: ANN001
        def _side(url, headers, timeout, retries):  # noqa: ANN001
            if str(url).endswith("feed.xml"):
                return {"ok": True, "data": RSS_NO_SUMMARY, "http_status": 200, "content_type": "application/rss+xml"}
            return {"ok": True, "data": ARTICLE_HTML.encode("utf-8"), "http_status": 200, "content_type": "text/html"}

        mfetch.side_effect = _side
        out = fetch_source_entries(
            {
                "id": "s1",
                "name": "S1",
                "connector": "rss",
                "url": "https://example.com/feed.xml",
                "fetch": {"mode": "rss", "allow_body_fetch_for_rss": True},
            },
            limit=5,
            source_guard={"enabled": True, "enforce_article_only": True, "allow_body_fetch_for_rss": True},
        )
        self.assertTrue(bool(out.get("ok")))
        entries = out.get("entries", []) if isinstance(out.get("entries"), list) else []
        self.assertEqual(len(entries), 1)
        self.assertTrue(len(str(entries[0].get("summary", ""))) > 20)

    @patch("app.services.source_registry._fetch_url_with_retry")
    def test_html_article_mode_listing_dropped(self, mfetch) -> None:  # noqa: ANN001
        mfetch.return_value = {"ok": True, "data": LISTING_HTML.encode("utf-8"), "http_status": 200, "content_type": "text/html"}
        out = fetch_source_entries(
            {
                "id": "s2",
                "name": "S2",
                "connector": "html",
                "url": "https://example.com/category/news",
                "fetch": {"mode": "html_article"},
            },
            limit=5,
            source_guard={"enabled": True, "enforce_article_only": True},
        )
        self.assertFalse(bool(out.get("ok")))
        self.assertGreaterEqual(int(out.get("dropped_static_or_listing_count", 0) or 0), 1)

    @patch("app.services.source_registry._fetch_url_with_retry")
    def test_missing_mode_defaults_to_rss_with_warning(self, mfetch) -> None:  # noqa: ANN001
        mfetch.return_value = {"ok": True, "data": RSS_NO_SUMMARY, "http_status": 200, "content_type": "application/rss+xml"}
        out = fetch_source_entries(
            {
                "id": "s3",
                "name": "S3",
                "connector": "html",
                "url": "https://example.com/feed.xml",
                "fetch": {},
            },
            source_guard={"enabled": True},
        )
        warns = out.get("warnings", []) if isinstance(out.get("warnings"), list) else []
        self.assertIn("fetch.mode_missing_defaulted_to_rss", warns)

    def test_collect_append_filters_static_listing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = CollectAssetStore(root, asset_dir="artifacts/collect")
            out = store.append_items(
                run_id="pr14-collect",
                source_id="src-a",
                source_name="Src A",
                source_group="media",
                items=[
                    {"title": "Listing", "url": "https://example.com/about", "summary": ""},
                    {"title": "Article", "url": "https://example.com/article/1", "summary": "diag content"},
                ],
                rules_runtime={"profile": "enhanced", "source_guard": {"enabled": True}},
            )
            self.assertEqual(int(out.get("written", 0) or 0), 1)
            self.assertGreaterEqual(int(out.get("dropped_static_or_listing_count", 0) or 0), 1)


if __name__ == "__main__":
    unittest.main()
