from app.services.opportunity_store import normalize_event_type


def test_regulatory_keyword_match() -> None:
    et = normalize_event_type("__unknown__", text="Implementing act under IVDR updated")
    assert et == "regulatory"


def test_approval_keyword_match() -> None:
    et = normalize_event_type("__unknown__", text="FDA approves a new molecular assay")
    assert et == "approval"


def test_company_move_keyword_match() -> None:
    et = normalize_event_type("__unknown__", text="Company to acquire diagnostics unit")
    assert et == "company_move"


def test_product_launch_keyword_match() -> None:
    et = normalize_event_type("__unknown__", text="New solution launched for IVD labs")
    assert et == "product_launch"


def test_research_keyword_match() -> None:
    et = normalize_event_type("__unknown__", text="Key clinical study results announced")
    assert et == "research"


def test_market_report_keyword_match() -> None:
    et = normalize_event_type("__unknown__", text="IVD market size 2025 forecast analysis")
    assert et == "market_report"


def test_industry_commentary_keyword_match() -> None:
    et = normalize_event_type("__unknown__", text="Interview and commentary on diagnostics trends")
    assert et == "industry_commentary"


def test_procurement_keyword_priority_match() -> None:
    et = normalize_event_type("__unknown__", text="FDA tender award for diagnostic kits")
    assert et == "procurement"


def test_domain_fallback_match() -> None:
    et = normalize_event_type("__unknown__", text="", url="https://www.fda.gov/medical-devices")
    assert et == "regulatory"


def test_unknown_preserved_when_no_signals() -> None:
    et = normalize_event_type("__unknown__", text="General ecosystem update", url="https://example.org/news")
    assert et == "__unknown__"
