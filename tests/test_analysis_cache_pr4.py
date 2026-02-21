from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from app.services.analysis_cache_store import AnalysisCacheStore
from app.services.collect_asset_store import render_digest_from_assets


class _NeverGenerator:
    def __init__(self) -> None:
        self.called = 0

    def generate(self, item, rules=None):  # noqa: ANN001
        self.called += 1
        return {
            "summary": "摘要：should not be used",
            "impact": "",
            "action": "",
            "model": "x",
            "prompt_version": "v1",
            "token_usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            "generated_at": "2026-02-20T00:00:00Z",
            "degraded": False,
            "degraded_reason": "",
            "ok": True,
        }


class _FailGenerator:
    def generate(self, item, rules=None):  # noqa: ANN001
        raise RuntimeError("upstream_api_down")


class AnalysisCachePR4Tests(unittest.TestCase):
    def test_cache_put_get(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = AnalysisCacheStore(root, asset_dir="artifacts/analysis")
            day = dt.date(2026, 2, 20)
            store.put("k1", {"summary": "摘要：cache"}, day)
            got = store.get("k1", day)
            self.assertIsInstance(got, dict)
            self.assertEqual(str(got.get("summary", "")), "摘要：cache")

    def test_digest_uses_cache_hit_without_generate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache = AnalysisCacheStore(root, asset_dir="artifacts/analysis")
            day = dt.date(2026, 2, 20)
            item = {
                "title": "FDA clears new IVD assay",
                "url": "https://example.com/a",
                "source": "FDA",
                "track": "core",
                "relevance_level": 4,
                "published_at": "2026-02-20T08:00:00Z",
            }
            key = AnalysisCacheStore.item_key(item)
            cache.put(
                key,
                {
                    "summary": "摘要：命中缓存摘要。",
                    "impact": "影响：缓存。",
                    "action": "建议：缓存。",
                    "model": "local-heuristic-v1",
                    "prompt_version": "v1",
                    "token_usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                    "generated_at": "2026-02-20T08:30:00Z",
                    "degraded": False,
                    "degraded_reason": "",
                    "ok": True,
                },
                day,
            )
            gen = _NeverGenerator()
            rendered = render_digest_from_assets(
                date_str="2026-02-20",
                items=[item],
                subject="全球IVD晨报 - 2026-02-20",
                analysis_cfg={
                    "enable_analysis_cache": True,
                    "always_generate": False,
                    "prompt_version": "v1",
                    "model": "local-heuristic-v1",
                    "asset_dir": "artifacts/analysis",
                },
                return_meta=True,
                _cache_store=cache,
                _generator=gen,
            )
            self.assertIsInstance(rendered, dict)
            txt = str(rendered.get("text", ""))
            self.assertIn("摘要：命中缓存摘要。", txt)
            self.assertEqual(gen.called, 0)
            meta = rendered.get("meta", {})
            self.assertEqual(int(meta.get("analysis_cache_hit", 0) or 0), 1)

    def test_degraded_generation_keeps_digest_and_marks_g(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache = AnalysisCacheStore(root, asset_dir="artifacts/analysis")
            item = {
                "title": "IVD market note",
                "url": "https://example.com/b",
                "source": "Media",
                "track": "core",
                "relevance_level": 3,
                "published_at": "2026-02-20T08:00:00Z",
            }
            rendered = render_digest_from_assets(
                date_str="2026-02-20",
                items=[item],
                subject="全球IVD晨报 - 2026-02-20",
                analysis_cfg={"enable_analysis_cache": True, "always_generate": False},
                return_meta=True,
                _cache_store=cache,
                _generator=_FailGenerator(),
            )
            txt = str(rendered.get("text", ""))
            self.assertIn("G. 质量指标", txt)
            self.assertIn("degraded_count：1", txt)
            meta = rendered.get("meta", {})
            self.assertEqual(int(meta.get("analysis_degraded_count", 0) or 0), 1)


if __name__ == "__main__":
    unittest.main()

