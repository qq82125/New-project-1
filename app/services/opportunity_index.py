from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from app.services.opportunity_store import OpportunityStore


def compute_opportunity_index(
    project_root: Path,
    *,
    window_days: int = 7,
    asset_dir: str = "artifacts/opportunity",
    as_of: str | None = None,
) -> dict[str, Any]:
    wd = max(1, int(window_days or 7))
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

    for r in cur_rows:
        region = str((r or {}).get("region", "")).strip() or "__unknown__"
        lane = str((r or {}).get("lane", "")).strip() or "__unknown__"
        try:
            w = int((r or {}).get("weight", 1) or 1)
        except Exception:
            w = 1
        k = (region, lane)
        cur_scores[k] = cur_scores.get(k, 0) + max(1, w)

    for r in prev_rows:
        region = str((r or {}).get("region", "")).strip() or "__unknown__"
        lane = str((r or {}).get("lane", "")).strip() or "__unknown__"
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

    return {
        "window_days": wd,
        "as_of": now_utc.date().isoformat(),
        "region_lane": region_lane,
    }

