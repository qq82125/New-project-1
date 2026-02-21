from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from app.services.opportunity_store import OpportunityStore


def _normalize_unknown(value: Any) -> str:
    return OpportunityStore.normalize_unknown(value)


def _is_probe_value(value: str) -> bool:
    s = str(value or "").strip().lower()
    return s.startswith("__window_probe__")


def _should_skip_unknown_both(region: str, lane: str, display_cfg: dict[str, Any]) -> bool:
    if bool(display_cfg.get("suppress_unknown_both", True)):
        return region == "__unknown__" and lane == "__unknown__"
    return False


def _format_contrib_top2(breakdown: dict[str, dict[str, int]]) -> list[dict[str, int | str]]:
    rows: list[dict[str, int | str]] = []
    for event_type, item in breakdown.items():
        weight_sum = int(item.get("weight_sum", 0) or 0)
        count = int(item.get("count", 0) or 0)
        rows.append(
            {
                "event_type": str(event_type),
                "weight_sum": weight_sum,
                "count": count,
            }
        )
    rows.sort(key=lambda x: (int(x.get("weight_sum", 0) or 0), int(x.get("count", 0) or 0)), reverse=True)
    return rows[:2]


def compute_opportunity_index(
    project_root: Path,
    *,
    window_days: int = 7,
    asset_dir: str = "artifacts/opportunity",
    as_of: str | None = None,
    display: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wd = max(1, int(window_days or 7))
    display_cfg = dict(display or {})
    top_n = max(1, int(display_cfg.get("top_n", 5) or 5))
    unknown_min_score = max(0, int(display_cfg.get("unknown_min_score", 5) or 5))
    mark_low_conf = bool(display_cfg.get("mark_low_conf", True))

    if as_of:
        try:
            now_utc = dt.datetime.fromisoformat(str(as_of).replace("Z", "+00:00"))
            if now_utc.tzinfo is None:
                now_utc = now_utc.replace(tzinfo=dt.timezone.utc)
        except Exception:
            now_utc = dt.datetime.now(dt.timezone.utc)
    else:
        now_utc = dt.datetime.now(dt.timezone.utc)

    store = OpportunityStore(project_root, asset_dir=asset_dir)
    cur_rows = store.load_signals(wd, now_utc=now_utc)
    prev_rows = store.load_signals(wd * 2, now_utc=now_utc - dt.timedelta(days=wd))

    cur_scores: dict[tuple[str, str], int] = {}
    prev_scores: dict[tuple[str, str], int] = {}
    breakdown: dict[str, dict[str, dict[str, int]]] = {}

    cur_total = 0
    unknown_region = 0
    unknown_lane = 0
    unknown_event_type = 0

    for r in cur_rows:
        region = _normalize_unknown((r or {}).get("region", ""))
        lane = _normalize_unknown((r or {}).get("lane", ""))
        event_type = _normalize_unknown((r or {}).get("event_type", ""))
        if _is_probe_value(region) or _is_probe_value(lane):
            continue
        try:
            w = int((r or {}).get("weight", 1) or 1)
        except Exception:
            w = 1
        k = (region, lane)
        w1 = max(1, w)
        cur_scores[k] = cur_scores.get(k, 0) + w1
        cur_total += 1
        if region == "__unknown__":
            unknown_region += 1
        if lane == "__unknown__":
            unknown_lane += 1
        if event_type == "__unknown__":
            unknown_event_type += 1
        bk = breakdown.setdefault(f"{region}|{lane}", {})
        by_et = bk.setdefault(event_type, {"weight_sum": 0, "count": 0})
        by_et["weight_sum"] = int(by_et.get("weight_sum", 0) or 0) + w1
        by_et["count"] = int(by_et.get("count", 0) or 0) + 1

    for r in prev_rows:
        region = _normalize_unknown((r or {}).get("region", ""))
        lane = _normalize_unknown((r or {}).get("lane", ""))
        if _is_probe_value(region) or _is_probe_value(lane):
            continue
        try:
            w = int((r or {}).get("weight", 1) or 1)
        except Exception:
            w = 1
        k = (region, lane)
        prev_scores[k] = prev_scores.get(k, 0) + max(1, w)

    all_keys = set(cur_scores.keys()) | set(prev_scores.keys())
    region_lane: dict[str, dict[str, Any]] = {}
    for k in sorted(all_keys):
        cur = int(cur_scores.get(k, 0))
        prev_avg = float(prev_scores.get(k, 0)) / float(wd) if wd > 0 else 0.0
        delta = int(round(cur - prev_avg))
        key = f"{k[0]}|{k[1]}"
        region_lane[key] = {
            "score": cur,
            "delta_vs_prev_window": delta,
            "region": k[0],
            "lane": k[1],
        }

    scored_rows = list(region_lane.values())
    scored_rows = [r for r in scored_rows if isinstance(r, dict)]
    scored_rows.sort(key=lambda x: int(x.get("score", 0) or 0), reverse=True)

    top_rows: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    for row in scored_rows:
        if len(top_rows) >= top_n:
            break
        region = _normalize_unknown(row.get("region", ""))
        lane = _normalize_unknown(row.get("lane", ""))
        score = int(row.get("score", 0) or 0)
        if _should_skip_unknown_both(region, lane, display_cfg):
            suppressed.append(row)
            continue
        if (region == "__unknown__" or lane == "__unknown__") and score < unknown_min_score:
            suppressed.append(row)
            continue
        row_key = f"{region}|{lane}"
        row["contrib_top2"] = _format_contrib_top2(breakdown.get(row_key, {}))
        row["low_confidence"] = False
        top_rows.append(row)

    if len(top_rows) < top_n and suppressed:
        for row in suppressed:
            if len(top_rows) >= top_n:
                break
            region = _normalize_unknown(row.get("region", ""))
            lane = _normalize_unknown(row.get("lane", ""))
            row_key = f"{region}|{lane}"
            row["contrib_top2"] = _format_contrib_top2(breakdown.get(row_key, {}))
            row["low_confidence"] = bool(mark_low_conf)
            top_rows.append(row)

    signals_total = max(0, int(cur_total))
    unknown_region_rate = (float(unknown_region) / float(signals_total)) if signals_total > 0 else 0.0
    unknown_lane_rate = (float(unknown_lane) / float(signals_total)) if signals_total > 0 else 0.0
    unknown_event_rate = (float(unknown_event_type) / float(signals_total)) if signals_total > 0 else 0.0

    return {
        "window_days": wd,
        "as_of": now_utc.date().isoformat(),
        "region_lane": region_lane,
        "breakdown": breakdown,
        "kpis": {
            "signals_total": signals_total,
            "unknown_region_rate": unknown_region_rate,
            "unknown_lane_rate": unknown_lane_rate,
            "unknown_event_type_rate": unknown_event_rate,
        },
        "top": top_rows,
    }
