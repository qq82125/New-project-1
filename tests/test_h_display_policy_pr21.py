from __future__ import annotations

import tempfile
from pathlib import Path

from app.services.opportunity_index import compute_opportunity_index
from app.services.opportunity_store import OpportunityStore


def _append(store: OpportunityStore, *, date: str, region: str, lane: str, event_type: str, weight: int, url: str) -> None:
    store.append_signal(
        {
            "date": date,
            "region": region,
            "lane": lane,
            "event_type": event_type,
            "weight": weight,
            "source_id": "s",
            "url_norm": url,
        },
        dedupe_enabled=False,
    )


def test_top_prefers_non_unknown_when_sufficient() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        store = OpportunityStore(root)
        day = "2026-02-23"
        for i in range(5):
            _append(
                store,
                date=day,
                region="北美",
                lane=f"lane-{i}",
                event_type="regulatory",
                weight=20,
                url=f"https://example.com/a{i}",
            )
        _append(
            store,
            date=day,
            region="",
            lane="分子诊断",
            event_type="regulatory",
            weight=30,
            url="https://example.com/u1",
        )
        out = compute_opportunity_index(root, as_of=day, display={"top_n": 5, "unknown_min_score": 15, "mark_low_conf": True})
        top = out.get("top", []) if isinstance(out, dict) else []
        assert all((str(x.get("region", "")) != "__unknown__") for x in top if isinstance(x, dict))


def test_unknown_allowed_with_low_conf_when_insufficient() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        store = OpportunityStore(root)
        day = "2026-02-23"
        _append(store, date=day, region="", lane="分子诊断", event_type="regulatory", weight=20, url="https://x.local/1")
        _append(store, date=day, region="", lane="肿瘤检测", event_type="regulatory", weight=18, url="https://x.local/2")
        out = compute_opportunity_index(root, as_of=day, display={"top_n": 5, "unknown_min_score": 15, "mark_low_conf": True})
        top = out.get("top", []) if isinstance(out, dict) else []
        assert any(bool(x.get("low_confidence", False)) for x in top if isinstance(x, dict))
        kpis = out.get("kpis", {}) if isinstance(out, dict) else {}
        assert str(kpis.get("top_fill_note", "")).strip() != ""


def test_unknown_min_score_enforced_for_unknown_region_lane_known() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        store = OpportunityStore(root)
        day = "2026-02-23"
        _append(store, date=day, region="", lane="分子诊断", event_type="regulatory", weight=10, url="https://x.local/low")
        _append(store, date=day, region="北美", lane="感染检测", event_type="regulatory", weight=20, url="https://x.local/high")
        _append(store, date=day, region="北美", lane="肿瘤检测", event_type="regulatory", weight=19, url="https://x.local/h2")
        _append(store, date=day, region="北美", lane="分子诊断", event_type="regulatory", weight=18, url="https://x.local/h3")
        out = compute_opportunity_index(root, as_of=day, display={"top_n": 3, "unknown_min_score": 15, "mark_low_conf": True})
        top = out.get("top", []) if isinstance(out, dict) else []
        pairs = {(str(x.get("region", "")), str(x.get("lane", ""))) for x in top if isinstance(x, dict)}
        assert ("__unknown__", "分子诊断") not in pairs
