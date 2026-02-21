from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from app.services.analysis_cache_store import AnalysisCacheStore
from app.services.collect_asset_store import render_digest_from_assets


class EvidenceEnforcementPR10Tests(unittest.TestCase):
    def test_core_with_summary_passes_evidence_requirement(self) -> None:
        item = {
            "title": "FDA approves new IVD assay",
            "url": "https://example.com/fda/a1",
            "summary": "This diagnostic test summary provides enough evidence text for verification and quoting in reports.",
            "source": "FDA",
            "track": "core",
            "relevance_level": 4,
            "published_at": "2026-02-21T08:00:00Z",
        }
        rendered = render_digest_from_assets(
            date_str="2026-02-21",
            items=[item],
            subject="全球IVD晨报 - 2026-02-21",
            analysis_cfg={
                "profile": "enhanced",
                "evidence_policy": {
                    "require_evidence_for_core": True,
                    "min_snippet_chars": 80,
                    "degrade_if_missing": True,
                },
            },
            return_meta=True,
        )
        meta = rendered.get("meta", {}) if isinstance(rendered, dict) else {}
        self.assertEqual(int(meta.get("evidence_missing_core_count", 0) or 0), 0)
        self.assertGreaterEqual(int(meta.get("evidence_present_core_count", 0) or 0), 1)

    def test_core_without_evidence_degrades_and_counted(self) -> None:
        item = {
            "title": "IVD diagnostic assay update missing evidence",
            "url": "https://example.com/core/no-evidence",
            "summary": "",
            "source": "Media",
            "source_group": "regulatory",
            "event_type": "监管审批与指南",
            "track": "core",
            "relevance_level": 3,
            "published_at": "2026-02-21T08:00:00Z",
        }
        rendered = render_digest_from_assets(
            date_str="2026-02-21",
            items=[item],
            subject="全球IVD晨报 - 2026-02-21",
            analysis_cfg={
                "profile": "enhanced",
                "evidence_policy": {
                    "require_evidence_for_core": True,
                    "min_snippet_chars": 80,
                    "degrade_if_missing": True,
                },
            },
            return_meta=True,
        )
        txt = str(rendered.get("text", "")) if isinstance(rendered, dict) else ""
        meta = rendered.get("meta", {}) if isinstance(rendered, dict) else {}
        self.assertIn("[NO_EVIDENCE]", txt)
        self.assertGreaterEqual(int(meta.get("evidence_missing_core_count", 0) or 0), 1)

    def test_legacy_profile_evidence_policy_off_no_degradation(self) -> None:
        item = {
            "title": "Legacy IVD diagnostic assay item missing summary",
            "url": "https://example.com/legacy/no-evidence",
            "summary": "",
            "source": "Legacy Source",
            "source_group": "regulatory",
            "event_type": "监管审批与指南",
            "track": "core",
            "relevance_level": 3,
            "published_at": "2026-02-21T08:00:00Z",
        }
        rendered = render_digest_from_assets(
            date_str="2026-02-21",
            items=[item],
            subject="全球IVD晨报 - 2026-02-21",
            analysis_cfg={
                "profile": "legacy",
                "evidence_policy": {
                    "require_evidence_for_core": False,
                    "min_snippet_chars": 80,
                    "degrade_if_missing": True,
                },
            },
            return_meta=True,
        )
        txt = str(rendered.get("text", "")) if isinstance(rendered, dict) else ""
        self.assertNotIn("[NO_EVIDENCE]", txt)

    def test_cache_record_includes_missing_evidence_degraded_reason(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cache = AnalysisCacheStore(root, asset_dir="artifacts/analysis")
            item = {
                "title": "IVD diagnostic test item for cache missing evidence",
                "url": "https://example.com/core/cache-no-evidence",
                "summary": "",
                "source": "Media",
                "source_group": "regulatory",
                "event_type": "监管审批与指南",
                "track": "core",
                "relevance_level": 3,
                "published_at": "2026-02-21T08:00:00Z",
            }
            _ = render_digest_from_assets(
                date_str="2026-02-21",
                items=[item],
                subject="全球IVD晨报 - 2026-02-21",
                analysis_cfg={
                    "profile": "enhanced",
                    "enable_analysis_cache": True,
                    "asset_dir": "artifacts/analysis",
                    "evidence_policy": {
                        "require_evidence_for_core": True,
                        "min_snippet_chars": 80,
                        "degrade_if_missing": True,
                    },
                },
                _cache_store=cache,
                return_meta=True,
            )
            got = cache.get(AnalysisCacheStore.item_key(item), dt.date(2026, 2, 21))
            self.assertIsInstance(got, dict)
            self.assertEqual(str((got or {}).get("degraded_reason", "")), "missing_evidence")


if __name__ == "__main__":
    unittest.main()
