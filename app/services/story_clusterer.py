from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse


NOISE_WORDS = {
    "breaking",
    "update",
    "exclusive",
    "live",
    "analysis",
}

TITLE_PREFIXES = [
    "stat+:",
    "comment:",
    "[comment]",
    "[shinsa]",
]


def _to_dt(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    return None


def canonical_url_key(item):
    c = str(item.get("canonical_url") or "").strip()
    if not c:
        return None
    return "canonical_url", hashlib.sha1(c.encode("utf-8")).hexdigest()


def normalized_url_host_path_key(item):
    url = str(item.get("url") or "").strip()
    if not url:
        return None
    p = urlparse(url)
    host = (p.netloc or "").lower()
    path = (p.path or "").rstrip("/")
    if not host:
        return None
    material = f"{host}{path}"
    return "normalized_url_host_path", hashlib.sha1(material.encode("utf-8")).hexdigest()


def title_fingerprint(text):
    t = (text or "").lower()
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    words = [w for w in t.split(" ") if w and w not in NOISE_WORDS]
    return " ".join(words)


def title_fingerprint_key(item):
    title = str(item.get("title") or "").strip()
    if not title:
        return None
    fp = title_fingerprint(title)
    if not fp:
        return None
    return "title_fingerprint_v1", hashlib.sha1(fp.encode("utf-8")).hexdigest()


def normalize_title(title: str) -> str:
    t = str(title or "").strip().lower()
    if not t:
        return ""
    changed = True
    while changed:
        changed = False
        for p in TITLE_PREFIXES:
            if t.startswith(p):
                t = t[len(p) :].strip()
                changed = True
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) < 12:
        return ""
    return t


def normalized_title_key(item):
    title = str(item.get("title") or "").strip()
    nt = normalize_title(title)
    if not nt:
        return None
    return "normalized_title_v1", hashlib.sha1(nt.encode("utf-8")).hexdigest()


def host_published_day_key(item):
    url = str(item.get("url") or "").strip()
    if not url:
        return None
    p = urlparse(url)
    host = (p.netloc or "").strip().lower()
    if not host:
        return None
    pub = _to_dt(item.get("published_at")) or _to_dt(item.get("first_seen_at"))
    if pub is None:
        return None
    day = pub.astimezone(timezone.utc).date().isoformat()
    material = f"{host}|{day}"
    return "host_published_day_v1", hashlib.sha1(material.encode("utf-8")).hexdigest()


def _source_priority(item, source_priority):
    raw = item.get("source_priority")
    if raw is not None:
        try:
            return int(raw)
        except Exception:
            pass
    key = str(item.get("source_key") or "").strip().lower()
    if key in source_priority:
        return int(source_priority.get(key, 0))
    source_name = str(item.get("source") or "").strip().lower()
    if source_name in source_priority:
        return int(source_priority.get(source_name, 0))
    return int(source_priority.get("generic_rss", 0))


class StoryClusterer:
    def __init__(self, config, source_priority):
        self.enabled = bool((config or {}).get("enabled", False))
        self.window_hours = int((config or {}).get("window_hours", 72))
        self.key_strategies = list((config or {}).get("key_strategies", []))
        self.primary_select = list((config or {}).get("primary_select", []))
        self.max_other_sources = int((config or {}).get("max_other_sources", 5))
        self.source_priority = source_priority or {}

    def _key_by_strategy(self, item, strategy):
        if strategy == "normalized_title_v1":
            return normalized_title_key(item)
        if strategy == "host_published_day_v1":
            return host_published_day_key(item)
        if strategy == "canonical_url":
            return canonical_url_key(item)
        if strategy == "normalized_url_host_path":
            return normalized_url_host_path_key(item)
        if strategy == "title_fingerprint_v1":
            return title_fingerprint_key(item)
        return None

    def _within_window(self, dt_values):
        if not dt_values:
            return True
        mn = min(dt_values)
        mx = max(dt_values)
        return (mx - mn) <= timedelta(hours=self.window_hours)

    def _choose_primary(self, cluster_items):
        def k(it):
            parts = []
            pub = _to_dt(it.get("published_at")) or _to_dt(it.get("first_seen_at")) or datetime.max.replace(tzinfo=timezone.utc)
            first_seen = _to_dt(it.get("first_seen_at")) or pub
            for s in self.primary_select:
                if s == "source_priority":
                    parts.append(-_source_priority(it, self.source_priority))
                elif s == "evidence_grade":
                    parts.append(-int(it.get("evidence_grade", 0) or 0))
                elif s == "published_at_latest":
                    parts.append(-int(pub.timestamp()))
                elif s == "first_seen_earliest":
                    parts.append(int(first_seen.timestamp()))
                else:
                    parts.append(int(pub.timestamp()))
            # Stable fallback ordering.
            parts.append(str(it.get("title", "")))
            return tuple(parts)

        ordered = sorted(cluster_items, key=k)
        return ordered[0], ordered[1:]

    def cluster(self, items):
        if not self.enabled:
            explain = {"enabled": False, "clusters": []}
            return items, explain

        clusters = {}
        cluster_meta = {}
        key_to_story = {}
        ungrouped = []

        for it in items:
            candidate_keys = []
            for st in self.key_strategies:
                out = self._key_by_strategy(it, st)
                if out is None:
                    continue
                st_name, key_hash = out
                candidate_keys.append((st_name, key_hash))

            picked_story = None
            picked_strategy = None
            for st_name, key_hash in candidate_keys:
                story_id = key_to_story.get(f"{st_name}:{key_hash}")
                if not story_id:
                    continue
                dt_values = []
                for ex in clusters[story_id]:
                    d = _to_dt(ex.get("published_at")) or _to_dt(ex.get("first_seen_at"))
                    if d is not None:
                        dt_values.append(d)
                d_cur = _to_dt(it.get("published_at")) or _to_dt(it.get("first_seen_at"))
                if d_cur is not None:
                    dt_values.append(d_cur)
                if self._within_window(dt_values):
                    picked_story = story_id
                    picked_strategy = st_name
                    if picked_story in cluster_meta:
                        cluster_meta[picked_story]["strategy"] = st_name
                    break

            if picked_story is None and candidate_keys:
                # Create a new story and register all keys as aliases.
                picked_strategy, first_hash = candidate_keys[0]
                picked_story = hashlib.sha1(f"story::{picked_strategy}:{first_hash}".encode("utf-8")).hexdigest()
                clusters[picked_story] = []
                cluster_meta[picked_story] = {"strategy": picked_strategy}
                for st_name, key_hash in candidate_keys:
                    key_to_story[f"{st_name}:{key_hash}"] = picked_story

            if picked_story is None:
                ungrouped.append(it)
                continue
            if picked_story not in clusters:
                clusters[picked_story] = []
                cluster_meta[picked_story] = {"strategy": picked_strategy or "unknown"}
            clusters[picked_story].append(it)

        primaries = []
        explain_clusters = []
        for sid, c_items in clusters.items():
            if not c_items:
                continue
            primary, others = self._choose_primary(c_items)
            other_sources = []
            others_sorted = sorted(
                others,
                key=lambda x: -_source_priority(x, self.source_priority),
            )
            for o in others_sorted[: self.max_other_sources]:
                other_sources.append(
                    {
                        "source": o.get("source"),
                        "url": o.get("url"),
                        "title": o.get("title"),
                        "published_at": str(o.get("published_at")),
                    }
                )
            p = dict(primary)
            p["story_id"] = sid
            p["is_primary"] = True
            p["other_sources"] = other_sources
            p["cluster_size"] = 1 + len(others)
            p["dedupe_reason"] = f"{cluster_meta[sid]['strategy']} within {self.window_hours}h"
            primaries.append(p)

            explain_clusters.append(
                {
                    "story_id": sid,
                    "key_strategy": cluster_meta[sid]["strategy"],
                    "window_hours": self.window_hours,
                    "candidate_count": len(c_items),
                    "candidate_titles": [str(x.get("title", "")) for x in c_items],
                    "primary_title": str(p.get("title", "")),
                    "primary_reason": "source_priority > evidence_grade > published_at_earliest",
                }
            )

        for it in ungrouped:
            p = dict(it)
            sid = hashlib.sha1(f"single::{it.get('title')}::{it.get('url')}".encode("utf-8")).hexdigest()
            p["story_id"] = sid
            p["is_primary"] = True
            p["other_sources"] = []
            p["cluster_size"] = 1
            p["dedupe_reason"] = "single_item"
            primaries.append(p)
            explain_clusters.append(
                {
                    "story_id": sid,
                    "key_strategy": "none",
                    "window_hours": self.window_hours,
                    "candidate_count": 1,
                    "candidate_titles": [str(it.get("title", ""))],
                    "primary_title": str(it.get("title", "")),
                    "primary_reason": "single_item",
                }
            )

        explain = {
            "enabled": True,
            "window_hours": self.window_hours,
            "key_strategies": self.key_strategies,
            "primary_select": self.primary_select,
            "clusters": explain_clusters,
        }
        return primaries, explain
