from __future__ import annotations

import datetime as dt
import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.collect_asset_store import render_digest_from_assets
from scripts.generate_ivd_report import Item, _dedupe_metrics_summary, _metrics_scope_summary


def _mk_item(title: str, source: str = "s1", cluster_size: int = 1) -> Item:
    return Item(
        title=title,
        link=f"https://example.com/{title}",
        published=dt.datetime.now(dt.timezone.utc),
        source=source,
        region="北美",
        lane="其他",
        platform="分子诊断",
        event_type="技术进展",
        window_tag="24小时内",
        summary_cn="summary",
        cluster_size=cluster_size,
    )


class MetricsScopePR231Tests(unittest.TestCase):
    @staticmethod
    def _collect_item(
        title: str,
        *,
        track: str,
        relevance_level: int,
        story_id: str = "",
        source_id: str = "src",
        final_reason: str = "",
    ) -> dict:
        return {
            "title": title,
            "url": f"https://example.com/{title}",
            "summary": "summary",
            "published_at": "2026-02-23T00:00:00Z",
            "source": "Source",
            "source_id": source_id,
            "track": track,
            "relevance_level": relevance_level,
            "story_id": story_id,
            "region": "北美",
            "lane": "其他",
            "event_type": "技术进展",
            "platform": "分子诊断",
            "relevance_explain": {"final_reason": final_reason} if final_reason else {},
        }

    def test_raw_and_filtered_pool_scope(self) -> None:
        dedupe = _dedupe_metrics_summary(
            dedupe_enabled=True,
            items_before=3,
            items_after=3,
            items=[_mk_item("a"), _mk_item("b"), _mk_item("c")],
            cluster_explain={"clusters": []},
        )
        scope = _metrics_scope_summary(
            raw_items_loaded=10,
            raw_items_after_basic_parsing=10,
            dropped_investment_scope_count=7,
            dedupe_metrics=dedupe,
        )
        self.assertEqual(scope["raw_items_loaded"], 10)
        self.assertEqual(scope["dropped_investment_scope_count"], 7)
        self.assertEqual(scope["items_before_dedupe"], 3)

    def test_filtered_pool_clustering_applies_only_filtered_items(self) -> None:
        dedupe = _dedupe_metrics_summary(
            dedupe_enabled=True,
            items_before=3,
            items_after=2,
            items=[_mk_item("a", cluster_size=2), _mk_item("b", cluster_size=1)],
            cluster_explain={"clusters": [{"story_id": "x"}, {"story_id": "y"}]},
        )
        scope = _metrics_scope_summary(
            raw_items_loaded=10,
            raw_items_after_basic_parsing=6,
            dropped_investment_scope_count=7,
            dedupe_metrics=dedupe,
        )
        self.assertEqual(scope["items_before_dedupe"], 3)
        self.assertEqual(scope["items_after_dedupe"], 2)
        self.assertEqual(scope["clusters_total"], 2)

    def test_reduction_ratio_uses_filtered_pool(self) -> None:
        dedupe = _dedupe_metrics_summary(
            dedupe_enabled=True,
            items_before=3,
            items_after=2,
            items=[_mk_item("a"), _mk_item("b")],
            cluster_explain={"clusters": [{"story_id": "x"}]},
        )
        scope = _metrics_scope_summary(
            raw_items_loaded=10,
            raw_items_after_basic_parsing=5,
            dropped_investment_scope_count=7,
            dedupe_metrics=dedupe,
        )
        self.assertAlmostEqual(scope["reduction_ratio"], 1.0 / 3.0, places=6)

    def test_scope_fields_exist_and_consistent(self) -> None:
        dedupe = _dedupe_metrics_summary(
            dedupe_enabled=False,
            items_before=4,
            items_after=4,
            items=[_mk_item("a"), _mk_item("b"), _mk_item("c"), _mk_item("d")],
            cluster_explain=None,
        )
        scope = _metrics_scope_summary(
            raw_items_loaded=10,
            raw_items_after_basic_parsing=8,
            dropped_investment_scope_count=2,
            dedupe_metrics=dedupe,
        )
        required = {
            "raw_items_loaded",
            "raw_items_after_basic_parsing",
            "dropped_investment_scope_count",
            "dropped_investment_scope_ratio",
            "items_before_dedupe",
            "items_after_dedupe",
            "clusters_total",
            "reduction_ratio",
        }
        self.assertTrue(required.issubset(set(scope.keys())))
        self.assertAlmostEqual(scope["dropped_investment_scope_ratio"], 0.2, places=6)

    def test_render_meta_scope_consistent(self) -> None:
        items = []
        for idx in range(7):
            items.append(
                self._collect_item(
                    f"drop-{idx}",
                    track="drop",
                    relevance_level=0,
                    source_id="prnewswire",
                    final_reason="investment_scope_filter",
                )
            )
        items.append(self._collect_item("keep-a", track="core", relevance_level=4, story_id="s1", source_id="a"))
        items.append(self._collect_item("keep-b", track="core", relevance_level=4, story_id="s1", source_id="b"))
        items.append(self._collect_item("keep-c", track="core", relevance_level=4, story_id="s2", source_id="c"))

        out = render_digest_from_assets(
            date_str="2026-02-23",
            items=items,
            subject="test",
            analysis_cfg={"enable_analysis_cache": False},
            return_meta=True,
        )
        meta = out["meta"]
        self.assertEqual(meta["raw_items_loaded"], 10)
        self.assertEqual(meta["dropped_investment_scope_count"], 7)
        self.assertEqual(meta["items_before_dedupe"], 3)
        self.assertEqual(meta["items_after_dedupe"], 2)
        self.assertAlmostEqual(float(meta["reduction_ratio"]), 1.0 / 3.0, places=6)

    def test_render_g_section_contains_raw_and_filtered(self) -> None:
        items = [
            self._collect_item("drop-1", track="drop", relevance_level=0, final_reason="investment_scope_filter"),
            self._collect_item("keep-1", track="core", relevance_level=4, story_id="k1"),
            self._collect_item("keep-2", track="core", relevance_level=4, story_id="k2"),
        ]
        out = render_digest_from_assets(
            date_str="2026-02-23",
            items=items,
            subject="test",
            analysis_cfg={"enable_analysis_cache": False},
            return_meta=True,
        )
        txt = out["text"]
        self.assertIn("raw_pool：raw_items_loaded=", txt)
        self.assertIn("filtered_pool：items_before_dedupe=", txt)


if __name__ == "__main__":
    unittest.main()
