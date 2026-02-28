from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.collect_asset_store import render_digest_from_assets


class HTemplateLockPR232Tests(unittest.TestCase):
    @staticmethod
    def _item() -> dict:
        return {
            "title": "FDA approves diagnostic IVD assay",
            "url": "https://example.com/fda/ivd-1",
            "summary": "diagnostic assay update with sufficient context for report rendering.",
            "source": "FDA",
            "source_id": "fda-medwatch-rss",
            "source_group": "regulatory",
            "event_type": "监管审批与指南",
            "region": "中国",
            "lane": "肿瘤检测",
            "track": "core",
            "relevance_level": 4,
            "published_at": "2026-02-21T08:00:00Z",
        }

    def test_h_section_template_order_locked(self) -> None:
        fake_idx = {
            "top": [
                {
                    "region": "北美",
                    "lane": "分子诊断",
                    "score": 53,
                    "delta_vs_prev_window": 53,
                    "contrib_top2": [
                        {"event_type": "__unknown__", "weight_sum": 14, "count": 7},
                        {"event_type": "approval", "weight_sum": 8, "count": 2},
                    ],
                },
                {
                    "region": "北美",
                    "lane": "肿瘤检测",
                    "score": 22,
                    "delta_vs_prev_window": 22,
                    "contrib_top2": [
                        {"event_type": "research", "weight_sum": 8, "count": 4},
                        {"event_type": "market_report", "weight_sum": 4, "count": 2},
                    ],
                },
            ],
            "kpis": {
                "unknown_region_rate": 0.27,
                "unknown_lane_rate": 0.09,
                "unknown_event_type_rate": 0.28,
                "unknown_lane_top_terms": [
                    {"term": "__unknown_source__", "count": 3},
                    {"term": "kalorama_blog", "count": 2},
                    {"term": "nature-rss", "count": 1},
                ],
            },
        }
        with patch("app.services.collect_asset_store.compute_opportunity_index", return_value=fake_idx):
            out = render_digest_from_assets(
                date_str="2026-02-21",
                items=[self._item()],
                subject="全球IVD晨报 - 2026-02-21",
                analysis_cfg={"profile": "enhanced", "opportunity_index": {"enabled": True, "window_days": 7}},
                return_meta=True,
            )
        txt = str((out or {}).get("text", ""))
        lines = txt.splitlines()
        h_idx = lines.index("H. 机会强度指数（近7天）")
        expected = [
            "- 分子诊断（北美）：▲ +53 | score=53",
            "  contrib: __unknown__=14 (7); approval=8 (2)",
            "- 肿瘤检测（北美）：▲ +22 | score=22",
            "  contrib: research=8 (4); market_report=4 (2)",
            "- kpis: unknown_region_rate=0.27, unknown_lane_rate=0.09, unknown_event_type_rate=0.28",
            "- unknown_lane_top_terms: __unknown_source__:3; kalorama_blog:2; nature-rss:1",
            "- opportunity_signals_written/deduped/dropped_probe：",
        ]
        actual = lines[h_idx + 1 : h_idx + 1 + len(expected)]
        self.assertEqual(actual[:-1], expected[:-1])
        self.assertTrue(actual[-1].startswith(expected[-1]))

    def test_a_to_h_sections_present_and_order_locked(self) -> None:
        fake_idx = {
            "top": [
                {
                    "region": "北美",
                    "lane": "分子诊断",
                    "score": 12,
                    "delta_vs_prev_window": 3,
                    "contrib_top2": [{"event_type": "approval", "weight_sum": 8, "count": 2}],
                }
            ],
            "kpis": {
                "unknown_region_rate": 0.2,
                "unknown_lane_rate": 0.1,
                "unknown_event_type_rate": 0.3,
                "unknown_lane_top_terms": [{"term": "src", "count": 1}],
            },
        }
        with patch("app.services.collect_asset_store.compute_opportunity_index", return_value=fake_idx):
            out = render_digest_from_assets(
                date_str="2026-02-21",
                items=[self._item()],
                subject="全球IVD晨报 - 2026-02-21",
                analysis_cfg={"profile": "enhanced", "opportunity_index": {"enabled": True, "window_days": 7}},
                return_meta=True,
            )
        txt = str((out or {}).get("text", ""))
        lines = txt.splitlines()
        expected_sections = [
            "A. 今日要点（8-15条，按重要性排序）",
            "B. 分赛道速览（肿瘤/感染/生殖遗传/其他）",
            "C. 技术平台雷达（按平台汇总当日进展）",
            "D. 区域热力图（北美/欧洲/亚太/中国）",
            "E. 三条关键趋势判断（产业与技术各至少1条）",
            "F. 信息缺口与次日跟踪清单（3-5条）",
            "G. 质量指标 (Quality Audit)",
            "H. 机会强度指数（近7天）",
        ]
        positions = []
        for sec in expected_sections:
            self.assertIn(sec, lines)
            positions.append(lines.index(sec))
        self.assertEqual(positions, sorted(positions))

    def test_h_section_contrib_kpi_suffix_order(self) -> None:
        fake_idx = {
            "top": [
                {
                    "region": "欧洲",
                    "lane": "分子诊断",
                    "score": 10,
                    "delta_vs_prev_window": 10,
                    "contrib_top2": [{"event_type": "regulatory", "weight_sum": 4, "count": 1}],
                }
            ],
            "kpis": {
                "unknown_region_rate": 0.1,
                "unknown_lane_rate": 0.2,
                "unknown_event_type_rate": 0.3,
                "unknown_lane_top_terms": [{"term": "src", "count": 1}],
            },
        }
        with patch("app.services.collect_asset_store.compute_opportunity_index", return_value=fake_idx):
            out = render_digest_from_assets(
                date_str="2026-02-21",
                items=[self._item()],
                subject="全球IVD晨报 - 2026-02-21",
                analysis_cfg={"profile": "enhanced", "opportunity_index": {"enabled": True, "window_days": 7}},
                return_meta=True,
            )
        txt = str((out or {}).get("text", ""))
        self.assertIn("  contrib: regulatory=4 (1)", txt)
        p_kpi = txt.find("- kpis:")
        p_lane = txt.find("- unknown_lane_top_terms:")
        p_sig = txt.find("- opportunity_signals_written/deduped/dropped_probe：")
        self.assertTrue(p_kpi != -1 and p_lane != -1 and p_sig != -1)
        self.assertTrue(p_kpi < p_lane < p_sig)


if __name__ == "__main__":
    unittest.main()
