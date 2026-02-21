from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

EVENT_WEIGHT = {
    "procurement": 5,
    "regulatory": 4,
    "approval": 4,
    "priority_review": 3,
    "company_move": 3,
    "technology_update": 2,
    "paper": 1,
}


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        pass
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return None


class OpportunityStore:
    def __init__(self, project_root: Path, asset_dir: str = "artifacts/opportunity") -> None:
        self.project_root = project_root
        self.base_dir = (project_root / asset_dir).resolve()
        _ensure_dir(self.base_dir)

    def _day_file(self, day: dt.date) -> Path:
        return self.base_dir / f"opportunity_signals-{day.strftime('%Y%m%d')}.jsonl"

    def append_signal(self, signal: dict[str, Any]) -> None:
        raw_date = str(signal.get("date", "")).strip()
        day = _safe_date(raw_date) or dt.date.today()
        region = str(signal.get("region", "")).strip() or "__unknown__"
        lane = str(signal.get("lane", "")).strip() or "__unknown__"
        event_type = str(signal.get("event_type", "")).strip() or "__unknown__"
        weight_raw = signal.get("weight", 1)
        try:
            weight = int(weight_raw)
        except Exception:
            weight = 1
        row = {
            "date": day.isoformat(),
            "region": region,
            "lane": lane,
            "event_type": event_type,
            "weight": max(1, weight),
            "source_id": str(signal.get("source_id", "")).strip(),
            "url_norm": str(signal.get("url_norm", "")).strip(),
        }
        p = self._day_file(day)
        _ensure_dir(p.parent)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def load_signals(self, window_days: int, *, now_utc: dt.datetime | None = None) -> list[dict[str, Any]]:
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        wd = max(1, int(window_days or 7))
        latest = now_utc.date()
        oldest = (now_utc - dt.timedelta(days=wd - 1)).date()
        out: list[dict[str, Any]] = []
        for p in sorted(self.base_dir.glob("opportunity_signals-*.jsonl")):
            m = re.match(r"opportunity_signals-(\d{8})\.jsonl$", p.name)
            if not m:
                continue
            try:
                d = dt.datetime.strptime(m.group(1), "%Y%m%d").date()
            except Exception:
                continue
            if d < oldest:
                continue
            if d > latest:
                continue
            try:
                with p.open("r", encoding="utf-8") as f:
                    for ln in f:
                        ln = ln.strip()
                        if not ln:
                            continue
                        try:
                            row = json.loads(ln)
                        except Exception:
                            continue
                        if not isinstance(row, dict):
                            continue
                        out.append(row)
            except Exception:
                continue
        return out

    def cleanup(self, *, keep_days: int = 90, now_utc: dt.datetime | None = None) -> dict[str, int]:
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        kd = max(1, int(keep_days or 90))
        cutoff = (now_utc - dt.timedelta(days=kd)).date()
        removed = 0
        for p in sorted(self.base_dir.glob("opportunity_signals-*.jsonl")):
            m = re.match(r"opportunity_signals-(\d{8})\.jsonl$", p.name)
            if not m:
                continue
            try:
                d = dt.datetime.strptime(m.group(1), "%Y%m%d").date()
            except Exception:
                continue
            if d < cutoff:
                try:
                    p.unlink(missing_ok=True)  # type: ignore[arg-type]
                    removed += 1
                except Exception:
                    pass
        return {"removed_files": removed}
