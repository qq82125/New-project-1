from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.source_registry import (
    list_sources_for_profile,
    retire_source,
    select_sources,
    validate_sources_registry,
)


class SourceRegistryTests(unittest.TestCase):
    def test_select_sources_by_tags(self) -> None:
        sources = [
            {"id": "a", "enabled": True, "tags": ["media", "en"]},
            {"id": "b", "enabled": True, "tags": ["regulatory", "cn"]},
            {"id": "c", "enabled": False, "tags": ["media"]},
        ]
        out = select_sources(
            sources,
            {
                "include_tags": ["media"],
                "exclude_tags": ["cn"],
                "default_enabled_only": True,
            },
        )
        self.assertEqual([x["id"] for x in out], ["a"])

    def test_validate_and_retire(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "rules" / "sources").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "schemas").mkdir(parents=True, exist_ok=True)

            (root / "rules" / "sources" / "rss.yaml").write_text(
                """
version: "1.0.0"
sources:
  - id: test-rss
    enabled: true
    connector: rss
    url: https://example.com/feed.xml
    name: Test RSS
    region: 北美
    trust_tier: B
    priority: 60
    tags:
      - media
      - en
""".strip()
                + "\n",
                encoding="utf-8",
            )
            (root / "rules" / "schemas" / "sources.schema.json").write_text("{}\n", encoding="utf-8")

            out = validate_sources_registry(root)
            self.assertTrue(out["ok"])
            retired = retire_source(root, "test-rss", reason="test")
            self.assertTrue(retired["ok"])

    def test_list_sources_for_profile(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "rules" / "sources").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "content_rules").mkdir(parents=True, exist_ok=True)

            (root / "rules" / "sources" / "rss.yaml").write_text(
                """
version: "1.0.0"
sources:
  - id: s1
    enabled: true
    connector: rss
    url: https://example.com/a.xml
    name: A
    region: 北美
    trust_tier: B
    priority: 50
    tags: [media, en]
  - id: s2
    enabled: true
    connector: rss
    url: https://example.com/b.xml
    name: B
    region: 中国
    trust_tier: A
    priority: 90
    tags: [regulatory, cn]
""".strip()
                + "\n",
                encoding="utf-8",
            )
            (root / "rules" / "content_rules" / "enhanced.yaml").write_text(
                """
ruleset: content_rules
version: "2.0.0"
profile: enhanced
defaults:
  content_sources:
    include_tags: [regulatory]
    exclude_tags: []
    include_source_ids: []
    exclude_source_ids: []
    default_enabled_only: true
overrides:
  enabled: true
rules:
  - id: x1
    enabled: true
    priority: 1
    type: source_priority
    params: {}
output:
  format: plain_text
  sections: [A]
  quality_audit_at_end: true
""".strip()
                + "\n",
                encoding="utf-8",
            )

            out = list_sources_for_profile(root, "enhanced")
            self.assertEqual(len(out), 1)
            self.assertEqual(out[0]["id"], "s2")


if __name__ == "__main__":
    unittest.main()
