from __future__ import annotations

from typing import Final
from urllib.parse import urlparse


EVENT_TYPE_KEYWORDS: Final[list[tuple[str, list[str]]]] = [
    (
        "procurement",
        [
            "tender",
            "bid",
            "award",
            "procurement",
            "中标",
            "招标",
            "集采",
            "采购结果",
        ],
    ),
    (
        "regulatory",
        [
            "recall",
            "voluntary recall",
            "safety alert",
            "warning letter",
            "field safety",
            "implementing act",
            "guideline",
            "guidance",
            "监管",
            "召回",
            "警示",
        ],
    ),
    (
        "earnings_noise",
        [
            "reports results",
            "quarter",
            "quarterly",
            "earnings",
            "revenue",
            "fiscal",
            "q1",
            "q2",
            "q3",
            "q4",
            "year ended",
            "financial results",
        ],
    ),
    (
        "funding",
        [
            "raises",
            "financing",
            "series a",
            "series b",
            "series c",
            "seed round",
            "funding round",
            "raised",
            "investment round",
        ],
    ),
    (
        "m_and_a",
        [
            "acquires",
            "acquisition",
            "to acquire",
            "merger",
            "merges with",
            "buyout",
        ],
    ),
    (
        "approval_clearance",
        [
            "approved",
            "approval",
            "cleared",
            "clearance",
            "authorized",
            "authorization",
            "ce-ivd",
            "ce ivd",
            "fda",
            "510(k)",
            "pma",
            "de novo",
        ],
    ),
    (
        "clinical_trial",
        [
            "clinical trial",
            "phase i",
            "phase ii",
            "phase iii",
            "enrollment",
            "randomized",
            "nct",
            "clinical study",
        ],
    ),
    (
        "product_launch",
        [
            "launches",
            "launch",
            "unveils",
            "introduces",
            "new assay",
            "platform",
            "announces new",
            "commercial launch",
        ],
    ),
    (
        "market_report",
        [
            "market size",
            "market report",
            "forecast",
            "industry report",
            "analysis report",
            "市场规模",
            "市场预测",
        ],
    ),
]


def _canonical_group(value: str) -> str:
    g = str(value or "").strip().lower()
    if not g:
        return ""
    if "procurement" in g:
        return "procurement"
    if "regulatory" in g:
        return "regulatory"
    if "evidence" in g:
        return "evidence"
    if "company" in g:
        return "company"
    if "media" in g:
        return "media"
    return g


def infer_event_type(group: str, title: str, snippet: str | None, source_id: str = "", url: str = "") -> str:
    g = _canonical_group(group)
    if g == "regulatory":
        return "regulatory"
    if g == "procurement":
        return "procurement"
    if g == "evidence":
        return "evidence"
    if g == "company":
        return "company_update"

    if g in {"media", "unknown", ""}:
        text = f"{title or ''} {snippet or ''}".lower()
        for et, kws in EVENT_TYPE_KEYWORDS:
            if any(k in text for k in kws):
                return et
        sid = str(source_id or "").strip().lower()
        host = ""
        try:
            host = str(urlparse(str(url or "")).netloc or "").lower()
        except Exception:
            host = ""
        if "kalorama" in sid or "kalorama" in host:
            return "market_report"
        if "medtech_europe" in sid or "medtecheurope.org" in host:
            return "regulatory"
        if any(
            k in sid
            for k in (
                "prnewswire",
                "globenewswire",
                "statnews",
                "fierce-biotech",
                "medtech-dive",
                "biopharma-dive",
                "todays_clinical_lab",
                "360dx",
                "iivd",
            )
        ):
            return "industry_commentary"
        if any(k in host for k in ("prnewswire.com", "globenewswire.com", "statnews.com", "clinicallab.com")):
            return "industry_commentary"
        return "unknown"
    return "unknown"
