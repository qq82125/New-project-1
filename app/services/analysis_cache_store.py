from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

from app.utils.url_norm import url_norm


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_dt(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _to_iso_utc(value: dt.datetime | None = None) -> str:
    d = value or dt.datetime.now(dt.timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class AnalysisCacheStore:
    def __init__(self, project_root: Path, asset_dir: str = "artifacts/analysis") -> None:
        self.project_root = project_root
        self.base_dir = (project_root / asset_dir).resolve()
        ensure_dir(self.base_dir)

    @staticmethod
    def item_key(item: dict[str, Any]) -> str:
        u = url_norm(str(item.get("url", item.get("link", ""))).strip())
        if u:
            return u
        story_id = str(item.get("story_id", "")).strip()
        if story_id:
            return story_id
        return str(item.get("title", "")).strip().lower()

    def _day_file(self, day: dt.date) -> Path:
        return self.base_dir / f"items-{day.strftime('%Y%m%d')}.jsonl"

    @staticmethod
    def _row_alias_keys(row: dict[str, Any]) -> set[str]:
        keys: set[str] = set()

        cache_key = str(row.get("cache_key", "")).strip()
        if cache_key:
            keys.add(cache_key)

        item_key = str(row.get("item_key", "")).strip()
        if item_key:
            keys.add(item_key)

        item_id = str(row.get("item_id", "")).strip()
        if item_id:
            keys.add(item_id)

        un = str(row.get("url_norm", "")).strip() or url_norm(str(row.get("url", "")).strip())
        if un:
            keys.add(un)

        story_id = str(row.get("story_id", "")).strip()
        if story_id and un:
            keys.add(f"{story_id}|{un}")
        if story_id:
            keys.add(story_id)
        return {k for k in keys if k}

    def _load_day_map(self, day: dt.date) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        p = self._day_file(day)
        if not p.exists():
            return out
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
                    for k in self._row_alias_keys(row):
                        out[k] = row
        except Exception:
            return out
        return out

    def get(self, item_key: str, day: dt.date) -> dict[str, Any] | None:
        row = self._load_day_map(day).get(str(item_key).strip())
        if not isinstance(row, dict):
            return None
        return row

    def put(self, item_key: str, payload: dict[str, Any], day: dt.date) -> None:
        p = self._day_file(day)
        ensure_dir(p.parent)
        row = dict(payload or {})
        key = str(item_key).strip() or str(row.get("cache_key", "")).strip()
        if not key:
            key = url_norm(str(row.get("url", "")).strip())
        url_raw = str(row.get("url", "")).strip()
        un = str(row.get("url_norm", "")).strip() or url_norm(url_raw)
        row["cache_key"] = key
        row["item_key"] = key  # backward compatibility for old readers
        row["url"] = url_raw
        row["url_norm"] = un
        row.setdefault("model", str(row.get("used_model", row.get("model", ""))).strip())
        row.setdefault("prompt_version", "v1")
        row.setdefault("token_usage", {})
        row.setdefault("generated_at", _to_iso_utc())
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def cleanup(self, *, keep_days: int = 30, now_utc: dt.datetime | None = None) -> dict[str, int]:
        now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
        keep_days = max(1, int(keep_days or 30))
        cutoff = (now_utc - dt.timedelta(days=keep_days)).date()
        removed = 0
        for p in sorted(self.base_dir.glob("items-*.jsonl")):
            m = re.match(r"items-(\d{8})\.jsonl$", p.name)
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
