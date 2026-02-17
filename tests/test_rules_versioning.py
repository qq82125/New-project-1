from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.rules_store import RulesStore
from app.services.rules_versioning import (
    ensure_bootstrap_published,
    get_published_pointer,
    get_runtime_rules_root,
    list_versions,
    publish_rules_version,
    rollback_to_previous,
    stage_rules_overlay,
)


def _seed_rules(root: Path) -> None:
    (root / "rules" / "email_rules").mkdir(parents=True, exist_ok=True)
    (root / "rules" / "content_rules").mkdir(parents=True, exist_ok=True)
    (root / "rules" / "sources").mkdir(parents=True, exist_ok=True)
    (root / "rules" / "schemas").mkdir(parents=True, exist_ok=True)

    (root / "rules" / "email_rules" / "legacy.yaml").write_text("ruleset: email_rules\nversion: '1'\nprofile: legacy\ndefaults: {}\noverrides: {enabled: false}\nrules: [{id: r1, enabled: true, priority: 1, type: subject_template, params: {template: x}}]\noutput: {format: plain_text, sections: [A], summary_max_chars: 100, charts_enabled: false}\n", encoding="utf-8")
    (root / "rules" / "content_rules" / "legacy.yaml").write_text("ruleset: content_rules\nversion: '1'\nprofile: legacy\ndefaults: {timezone: Asia/Shanghai, time_window: {primary_hours: 24, fallback_days: 7}, item_limit: {min: 1, max: 2, topup_if_24h_lt: 1}, sources: {media_global: [], regulatory_cn: [], regulatory_apac: []}, coverage_tracks: [其他], region_filter: {apac_min_share: 0.1}}\noverrides: {enabled: false}\nrules: [{id: r1, enabled: true, priority: 1, type: source_priority, params: {}}]\noutput: {format: plain_text, sections: [A], quality_audit_at_end: true}\n", encoding="utf-8")
    (root / "rules" / "sources" / "rss.yaml").write_text("version: '1'\nsources: []\n", encoding="utf-8")
    (root / "rules" / "schemas" / "email_rules.schema.json").write_text("{}\n", encoding="utf-8")
    (root / "rules" / "schemas" / "content_rules.schema.json").write_text("{}\n", encoding="utf-8")
    (root / "rules" / "schemas" / "sources.schema.json").write_text("{}\n", encoding="utf-8")


class RulesVersioningTests(unittest.TestCase):
    def test_bootstrap_publish_and_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _seed_rules(root)

            ensure_bootstrap_published(root)
            ptr = get_published_pointer(root)
            self.assertTrue(ptr.get("active_version"))
            self.assertEqual(len(list_versions(root)), 1)
            store = RulesStore(root)
            self.assertIsNotNone(store.get_active_email_rules("legacy"))
            self.assertIsNotNone(store.get_active_content_rules("legacy"))

            staged = stage_rules_overlay(root, {"sources/rss.yaml": "version: '1'\nsources:\n  - id: x\n    connector: rss\n    name: X\n    region: 北美\n    trust_tier: B\n    priority: 1\n"})
            out = publish_rules_version(root, staged, created_by="tester", note="update")
            self.assertTrue(out["ok"])

            ptr2 = get_published_pointer(root)
            self.assertNotEqual(ptr["active_version"], ptr2["active_version"])

            rb = rollback_to_previous(root, created_by="tester")
            self.assertTrue(rb["ok"])
            self.assertEqual(get_published_pointer(root)["active_version"], ptr["active_version"])
            self.assertIn("db_rollback", rb)

            runtime = get_runtime_rules_root(root)
            self.assertTrue(runtime.exists())


if __name__ == "__main__":
    unittest.main()
