from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from app.services.analysis_cache_store import AnalysisCacheStore
from app.services.collect_asset_store import CollectAssetStore
from app.utils.url_norm import url_norm
from scripts.acceptance_run import _build_quality_pack


class CacheKeyAuditPR7Tests(unittest.TestCase):
    def test_url_norm_stable(self) -> None:
        a = "https://Example.com/path/item/?utm_source=x#frag"
        b = "https://example.com/path/item"
        self.assertEqual(url_norm(a), url_norm(b))

    def test_cache_put_get_by_url_norm(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = AnalysisCacheStore(root, asset_dir="artifacts/analysis")
            day = dt.date(2026, 2, 21)
            key = url_norm("https://example.com/news/abc?utm_source=x")
            store.put(
                key,
                {
                    "url": "https://example.com/news/abc?utm_source=x",
                    "summary": "摘要：cache by url_norm",
                    "model": "local-heuristic-v1",
                    "prompt_version": "v2",
                },
                day,
            )
            got = store.get(url_norm("https://example.com/news/abc"), day)
            self.assertIsInstance(got, dict)
            self.assertEqual(str((got or {}).get("summary", "")), "摘要：cache by url_norm")
            self.assertEqual(str((got or {}).get("cache_key", "")), key)

    def test_acceptance_cache_hit_counting(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            collect = CollectAssetStore(root, asset_dir="artifacts/collect")
            collect.append_items(
                run_id="collect-pr7",
                source_id="fda",
                source_name="FDA",
                source_group="regulatory",
                source_trust_tier="B",
                items=[
                    {
                        "title": "FDA approves IVD assay for hospital labs",
                        "url": "https://example.com/fda/ivd-1?utm_source=rss",
                        "summary": "regulatory approval update",
                        "published_at": "2026-02-21T08:00:00Z",
                    }
                ],
            )

            cache = AnalysisCacheStore(root, asset_dir="artifacts/analysis")
            day = dt.date.today()
            key = url_norm("https://example.com/fda/ivd-1")
            cache.put(
                key,
                {
                    "url": "https://example.com/fda/ivd-1?utm_source=rss",
                    "summary": "摘要：命中缓存",
                    "impact": "影响：示例",
                    "action": "建议：示例",
                    "model": "local-heuristic-v1",
                    "prompt_version": "v2",
                },
                day,
            )

            (root / "artifacts" / "acceptance").mkdir(parents=True, exist_ok=True)
            out = _build_quality_pack(root, as_of=day.isoformat(), window_hours=48)
            analysis_cache = out.get("analysis_cache", {}) if isinstance(out, dict) else {}
            self.assertGreaterEqual(int(analysis_cache.get("hit", 0) or 0), 1)
            self.assertEqual(int(analysis_cache.get("mismatch", 0) or 0), 0)


if __name__ == "__main__":
    unittest.main()
