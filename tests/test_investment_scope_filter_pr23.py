from __future__ import annotations

import unittest

from app.core.track_relevance import compute_relevance


ENHANCED_INVESTMENT_RULES = {
    "investment_scope_enabled": True,
}


class InvestmentScopeFilterPR23Tests(unittest.TestCase):
    def test_crypto_news_drops(self) -> None:
        track, level, explain = compute_relevance(
            "Crypto exchange expands treasury operations in global market",
            {
                "source": "PR Newswire",
                "source_group": "media",
                "title": "Crypto market expansion update",
                "url": "https://www.prnewswire.com/news/crypto-market-expansion.html",
            },
            ENHANCED_INVESTMENT_RULES,
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertEqual(str(explain.get("final_reason", "")), "investment_scope_filter")

    def test_abbott_csr_drops(self) -> None:
        track, level, explain = compute_relevance(
            "Abbott sustainability story and awareness campaign recap",
            {
                "source": "Abbott Newsroom",
                "source_group": "company",
                "title": "Sustainability awareness story recap",
                "url": "https://www.abbott.com/newsroom/sustainability-awareness.html",
            },
            ENHANCED_INVESTMENT_RULES,
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertEqual(str(explain.get("final_reason", "")), "investment_scope_filter")

    def test_abbott_product_launch_kept(self) -> None:
        track, level, explain = compute_relevance(
            "Abbott launches molecular diagnostic assay with biomarker validation",
            {
                "source": "Abbott Newsroom",
                "source_group": "company",
                "title": "Abbott launch molecular diagnostic assay",
                "url": "https://www.abbott.com/newsroom/diagnostics/new-assay-launch.html",
            },
            ENHANCED_INVESTMENT_RULES,
        )
        self.assertNotEqual(track, "drop")
        self.assertGreaterEqual(level, 1)
        self.assertNotEqual(str(explain.get("final_reason", "")), "investment_scope_filter")

    def test_pr_newswire_non_ivd_drops(self) -> None:
        track, level, explain = compute_relevance(
            "PR Newswire announces fintech expansion and cloud partnership",
            {
                "source": "PR Newswire",
                "source_group": "media",
                "title": "Fintech expansion partnership update",
                "url": "https://www.prnewswire.com/news/fintech-cloud-partnership.html",
            },
            ENHANCED_INVESTMENT_RULES,
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertEqual(str(explain.get("final_reason", "")), "investment_scope_filter")

    def test_biorxiv_non_diagnostic_drops(self) -> None:
        track, level, explain = compute_relevance(
            "bioRxiv preprint on spatial transcriptomics atlas of embryo development",
            {
                "source": "bioRxiv",
                "source_group": "evidence",
                "title": "Spatial transcriptomics atlas preprint",
                "url": "https://www.biorxiv.org/content/10.1101/example",
            },
            ENHANCED_INVESTMENT_RULES,
        )
        self.assertEqual(track, "drop")
        self.assertEqual(level, 0)
        self.assertEqual(str(explain.get("final_reason", "")), "investment_scope_filter")

    def test_biorxiv_diagnostic_assay_kept(self) -> None:
        track, level, explain = compute_relevance(
            "bioRxiv diagnostic assay with PCR biomarker sequencing clinical validation",
            {
                "source": "bioRxiv",
                "source_group": "evidence",
                "title": "Diagnostic assay PCR clinical validation",
                "url": "https://www.biorxiv.org/content/10.1101/diagnostic-assay",
            },
            ENHANCED_INVESTMENT_RULES,
        )
        self.assertNotEqual(track, "drop")
        self.assertGreaterEqual(level, 1)
        self.assertNotEqual(str(explain.get("final_reason", "")), "investment_scope_filter")


if __name__ == "__main__":
    unittest.main()
