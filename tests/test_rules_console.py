from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from app.web.rules_console import _read_rules_bundle
from app.services.rules_versioning import ensure_bootstrap_published


class RulesConsoleTests(unittest.TestCase):
    def test_read_rules_bundle_from_published(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "rules" / "email_rules").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "content_rules").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "sources").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "schemas").mkdir(parents=True, exist_ok=True)
            (root / "rules" / "email_rules" / "enhanced.yaml").write_text("x: 1\n", encoding="utf-8")
            (root / "rules" / "content_rules" / "enhanced.yaml").write_text("y: 1\n", encoding="utf-8")
            (root / "rules" / "email_rules" / "legacy.yaml").write_text("x: 1\n", encoding="utf-8")
            (root / "rules" / "content_rules" / "legacy.yaml").write_text("y: 1\n", encoding="utf-8")
            (root / "rules" / "sources" / "rss.yaml").write_text("version: '1'\nsources: []\n", encoding="utf-8")
            (root / "rules" / "sources" / "web.yaml").write_text("version: '1'\nsources: []\n", encoding="utf-8")
            (root / "rules" / "sources" / "api.yaml").write_text("version: '1'\nsources: []\n", encoding="utf-8")
            (root / "rules" / "schemas" / "email_rules.schema.json").write_text("{}\n", encoding="utf-8")
            (root / "rules" / "schemas" / "content_rules.schema.json").write_text("{}\n", encoding="utf-8")
            (root / "rules" / "schemas" / "sources.schema.json").write_text("{}\n", encoding="utf-8")

            ensure_bootstrap_published(root)
            bundle = _read_rules_bundle(root)
            self.assertIn("email_rules/enhanced.yaml", bundle)
            self.assertIn("content_rules/enhanced.yaml", bundle)


if __name__ == "__main__":
    unittest.main()
