from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from collections import deque
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
        self._seen_by_day: dict[str, set[str]] = {}

    def _day_file(self, day: dt.date) -> Path:
        return self.base_dir / f"opportunity_signals-{day.strftime('%Y%m%d')}.jsonl"

    @staticmethod
    def normalize_unknown(value: Any) -> str:
        s = str(value or "").strip()
        return s or "__unknown__"

    @staticmethod
    def _is_probe_value(value: str) -> bool:
        s = str(value or "").strip().lower()
        return s.startswith("__window_probe__")

    @classmethod
    def _signal_key(cls, *, date_s: str, url_norm_v: str, event_type: str, region: str, lane: str) -> str:
        seed = "|".join(
            [
                str(date_s).strip(),
                str(url_norm_v).strip(),
                cls.normalize_unknown(event_type),
                cls.normalize_unknown(region),
                cls.normalize_unknown(lane),
            ]
        )
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()

    def _build_seen_for_day(self, day: dt.date, *, tail_lines_scan: int) -> set[str]:
        day_iso = day.isoformat()
        if day_iso in self._seen_by_day:
            return self._seen_by_day[day_iso]

        seen: set[str] = set()
        p = self._day_file(day)
        if p.exists():
            tail_n = max(1, int(tail_lines_scan or 2000))
            try:
                with p.open("r", encoding="utf-8") as f:
                    tail = deque(f, maxlen=tail_n)
                for ln in tail:
                    ln = str(ln or "").strip()
                    if not ln:
                        continue
                    try:
                        row = json.loads(ln)
                    except Exception:
                        continue
                    if not isinstance(row, dict):
                        continue
                    k = str(row.get("signal_key", "")).strip()
                    if not k:
                        k = self._signal_key(
                            date_s=str(row.get("date", day_iso)).strip() or day_iso,
                            url_norm_v=str(row.get("url_norm", "")).strip(),
                            event_type=str(row.get("event_type", "")).strip(),
                            region=str(row.get("region", "")).strip(),
                            lane=str(row.get("lane", "")).strip(),
                        )
                    if k:
                        seen.add(k)
            except Exception:
                pass
        self._seen_by_day[day_iso] = seen
        return seen

    def append_signal(
        self,
        signal: dict[str, Any],
        *,
        dedupe_enabled: bool = True,
        tail_lines_scan: int = 2000,
    ) -> dict[str, int]:
        raw_date = str(signal.get("date", "")).strip()
        day = _safe_date(raw_date) or dt.date.today()
        day_iso = day.isoformat()
        region = self.normalize_unknown(signal.get("region", ""))
        lane = self.normalize_unknown(signal.get("lane", ""))
        event_type = self.normalize_unknown(signal.get("event_type", ""))
        if self._is_probe_value(region) or self._is_probe_value(lane):
            return {"written": 0, "deduped": 0, "dropped_probe": 1}
        weight_raw = signal.get("weight", 1)
        try:
            weight = int(weight_raw)
        except Exception:
            weight = 1
        url_norm_v = str(signal.get("url_norm", "")).strip()
        signal_key = self._signal_key(
            date_s=day_iso,
            url_norm_v=url_norm_v,
            event_type=event_type,
            region=region,
            lane=lane,
        )
        seen = self._build_seen_for_day(day, tail_lines_scan=tail_lines_scan)
        if dedupe_enabled and signal_key in seen:
            return {"written": 0, "deduped": 1, "dropped_probe": 0}

        row = {
            "date": day_iso,
            "region": region,
            "lane": lane,
            "event_type": event_type,
            "weight": max(1, weight),
            "source_id": str(signal.get("source_id", "")).strip(),
            "url_norm": url_norm_v,
            "signal_key": signal_key,
            "observed_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "run_id": str(signal.get("run_id", "")).strip(),
            "interval_source": str(signal.get("interval_source", "")).strip(),
        }
        p = self._day_file(day)
        _ensure_dir(p.parent)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        seen.add(signal_key)
        return {"written": 1, "deduped": 0, "dropped_probe": 0}

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
