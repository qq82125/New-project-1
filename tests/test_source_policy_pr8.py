from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.collect_asset_store import CollectAssetStore, render_digest_from_assets
from app.services.source_registry import select_sources


class SourcePolicyPR8Tests(unittest.TestCase):
    def test_excluded_domain_dropped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = CollectAssetStore(root, asset_dir="artifacts/collect")
            out = store.append_items(
                run_id="collect-pr8-domain",
                source_id="media-1",
                source_name="Media 1",
                source_group="media",
                source_trust_tier="B",
                items=[
                    {
                        "title": "PR release",
                        "url": "https://www.prnewswire.com/news-releases/a.html",
                        "summary": "press release",
                    }
                ],
                rules_runtime={
                    "profile": "enhanced",
                    "source_policy": {"enabled": True, "exclude_domains": ["prnewswire.com"]},
                },
            )
            self.assertEqual(int(out.get("written", 0) or 0), 0)
            self.assertGreaterEqual(int(out.get("dropped_by_source_policy", 0) or 0), 1)

    def test_excluded_source_id_dropped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = CollectAssetStore(root, asset_dir="artifacts/collect")
            out = store.append_items(
                run_id="collect-pr8-source",
                source_id="blocked-source",
                source_name="Blocked",
                source_group="media",
                source_trust_tier="B",
                items=[{"title": "Some news", "url": "https://example.com/a", "summary": "x"}],
                rules_runtime={
                    "profile": "enhanced",
                    "source_policy": {"enabled": True, "exclude_source_ids": ["blocked-source"]},
                },
            )
            self.assertEqual(int(out.get("written", 0) or 0), 0)
            self.assertGreaterEqual(int(out.get("dropped_by_source_policy", 0) or 0), 1)

    def test_min_trust_tier_filtering(self) -> None:
        rows = [
            {"id": "a", "enabled": True, "trust_tier": "A", "tags": ["media"]},
            {"id": "b", "enabled": True, "trust_tier": "B", "tags": ["media"]},
            {"id": "c", "enabled": True, "trust_tier": "C", "tags": ["media"]},
        ]
        out = select_sources(rows, {"min_trust_tier": "B", "default_enabled_only": True})
        self.assertEqual([x["id"] for x in out], ["a", "b"])

    def test_policy_applied_in_collect_and_digest(self) -> None:
        item = {
            "title": "Low signal PR release",
            "url": "https://www.globenewswire.com/news-release/abc",
            "source": "Example",
            "source_id": "media-2",
            "track": "core",
            "relevance_level": 3,
            "trust_tier": "B",
            "published_at": "2026-02-21T08:00:00Z",
        }
        rendered = render_digest_from_assets(
            date_str="2026-02-21",
            items=[item],
            subject="全球IVD晨报 - 2026-02-21",
            analysis_cfg={
                "profile": "enhanced",
                "source_policy": {"enabled": True, "exclude_domains": ["globenewswire.com"]},
                "enable_analysis_cache": False,
            },
            return_meta=True,
        )
        self.assertIsInstance(rendered, dict)
        txt = str(rendered.get("text", ""))
        meta = rendered.get("meta", {}) if isinstance(rendered.get("meta"), dict) else {}
        self.assertIn("dropped_by_source_policy_count", txt)
        self.assertGreaterEqual(int(meta.get("dropped_by_source_policy_count", 0) or 0), 1)


if __name__ == "__main__":
    unittest.main()
