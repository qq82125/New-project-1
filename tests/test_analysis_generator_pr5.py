from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.analysis_cache_store import AnalysisCacheStore
from app.services.analysis_generator import AnalysisGenerator
from app.services.collect_asset_store import render_digest_from_assets


class AnalysisGeneratorPR5Tests(unittest.TestCase):
    def test_primary_failure_fallback(self) -> None:
        item = {
            "title": "FDA clears core IVD assay",
            "track": "core",
            "relevance_level": 4,
            "source": "FDA",
            "event_type": "regulatory",
        }
        with patch.dict("os.environ", {"ANALYSIS_FAIL_MODELS": "model-primary"}, clear=False):
            gen = AnalysisGenerator(
                primary_model="model-primary",
                fallback_model="model-fallback",
                model_policy="tiered",
                core_model="primary",
                frontier_model="fallback",
                prompt_version="v2",
            )
            out = gen.generate(item, rules={})
        self.assertEqual(str(out.get("used_model", "")), "model-fallback")
        self.assertEqual(str(out.get("fallback_from", "")), "model-primary")
        self.assertEqual(str(out.get("prompt_version", "")), "v2")

    def test_tiered_policy_core_uses_primary(self) -> None:
        gen = AnalysisGenerator(
            primary_model="model-primary",
            fallback_model="model-fallback",
            model_policy="tiered",
            core_model="primary",
            frontier_model="fallback",
        )
        core_item = {"title": "Core item", "track": "core", "relevance_level": 3}
        frontier_item = {"title": "Frontier item", "track": "frontier", "relevance_level": 2}
        core_out = gen.generate(core_item, rules={})
        frontier_out = gen.generate(frontier_item, rules={})
        self.assertEqual(str(core_out.get("used_model", "")), "model-primary")
        self.assertEqual(str(frontier_out.get("used_model", "")), "model-fallback")

    def test_cache_records_used_model_and_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache = AnalysisCacheStore(root, asset_dir="artifacts/analysis")
            item = {
                "title": "Core item with cache",
                "url": "https://example.com/core-cache",
                "source": "FDA",
                "track": "core",
                "relevance_level": 4,
                "published_at": "2026-02-21T00:00:00Z",
            }
            rendered = render_digest_from_assets(
                date_str="2026-02-21",
                items=[item],
                subject="全球IVD晨报 - 2026-02-21",
                analysis_cfg={
                    "enable_analysis_cache": True,
                    "always_generate": False,
                    "prompt_version": "v2",
                    "model_primary": "model-primary",
                    "model_fallback": "model-fallback",
                    "model_policy": "tiered",
                    "core_model": "primary",
                    "frontier_model": "fallback",
                    "asset_dir": "artifacts/analysis",
                },
                return_meta=True,
                _cache_store=cache,
            )
            self.assertIsInstance(rendered, dict)
            day = dt.date(2026, 2, 21)
            key = AnalysisCacheStore.item_key(item)
            got = cache.get(key, day)
            self.assertIsInstance(got, dict)
            self.assertEqual(str((got or {}).get("prompt_version", "")), "v2")
            self.assertTrue(str((got or {}).get("used_model", "")).strip())
            token_usage = (got or {}).get("token_usage", {})
            self.assertIsInstance(token_usage, dict)
            self.assertIn("total_tokens", token_usage)


if __name__ == "__main__":
    unittest.main()

