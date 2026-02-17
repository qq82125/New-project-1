from __future__ import annotations

import unittest

from app.rules.decision_boundary import enforce_decision_boundary


class DecisionBoundaryTests(unittest.TestCase):
    def test_reject_cross_domain_keys(self) -> None:
        decision = {
            "content_decision": {
                "allow_sources": [],
                "subject_template": "not-allowed-in-content",
            },
            "email_decision": {
                "subject_template": "全球IVD晨报 - {{date}}",
                "allow_sources": [],
            },
        }
        with self.assertRaises(ValueError) as ctx:
            enforce_decision_boundary(decision)
        self.assertIn("RULES_BOUNDARY_VIOLATION", str(ctx.exception))

    def test_accept_clean_layered_decision(self) -> None:
        decision = {
            "content_decision": {
                "allow_sources": [],
                "dedupe_window": {"primary_hours": 24},
            },
            "email_decision": {
                "subject_template": "全球IVD晨报 - {{date}}",
                "sections": ["A", "B"],
            },
        }
        content, email = enforce_decision_boundary(decision)
        self.assertIn("allow_sources", content)
        self.assertNotIn("subject_template", content)
        self.assertIn("subject_template", email)
        self.assertNotIn("allow_sources", email)


if __name__ == "__main__":
    unittest.main()
