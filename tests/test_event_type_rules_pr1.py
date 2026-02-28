from __future__ import annotations

from app.services.event_type_rules import infer_event_type


def test_infer_event_type_earnings_noise() -> None:
    assert infer_event_type("media", "Company reports quarterly earnings", "") == "earnings_noise"


def test_infer_event_type_funding() -> None:
    assert infer_event_type("media", "Startup raises Series B financing", "") == "funding"


def test_infer_event_type_m_and_a() -> None:
    assert infer_event_type("media", "BigCo to acquire LabTech", "") == "m_and_a"


def test_infer_event_type_unknown() -> None:
    assert infer_event_type("media", "General industry commentary", "") == "unknown"


def test_infer_event_type_source_fallback_market_report() -> None:
    assert infer_event_type("media", "General update", "", source_id="kalorama_blog") == "market_report"


def test_infer_event_type_source_fallback_industry_commentary() -> None:
    assert infer_event_type("media", "General update", "", source_id="prnewswire-press-rss") == "industry_commentary"


def test_infer_event_type_procurement_keywords() -> None:
    assert infer_event_type("media", "Hospital tender award announced", "") == "procurement"


def test_infer_event_type_media_subgroup_alias() -> None:
    assert infer_event_type("media_global", "General update", "", source_id="prnewswire-press-rss") == "industry_commentary"


def test_infer_event_type_regulatory_subgroup_alias() -> None:
    assert infer_event_type("regulatory_cn", "Any title", "") == "regulatory"
