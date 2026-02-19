from __future__ import annotations

import copy
import hashlib
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from app.core.scoring import evidence_rank


NOISE_PREFIX = re.compile(r"^(press release|breaking|update|exclusive)\s*[:\-]\s*", flags=re.I)


def normalize_title(title: str) -> str:
    t = str(title or "").lower().strip()
    t = NOISE_PREFIX.sub("", t)
    t = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def tokenize_title(title: str) -> set[str]:
    n = normalize_title(title)
    toks = {x for x in n.split(" ") if len(x) >= 2}
    for zh in re.findall(r"[\u4e00-\u9fff]{2,}", str(title or "")):
        toks.add(zh)
    return toks


def jaccard_title(a: str, b: str) -> float:
    ta = tokenize_title(a)
    tb = tokenize_title(b)
    if not ta or not tb:
        return 0.0
    u = len(ta | tb)
    if u <= 0:
        return 0.0
    return len(ta & tb) / u


def _domain(url: str) -> str:
    try:
        return urlparse(str(url or "")).netloc.lower().strip()
    except Exception:
        return ""


def _published_date(item: dict[str, Any]) -> str:
    dtv = item.get("published_at")
    if isinstance(dtv, datetime):
        return dtv.astimezone(timezone.utc).strftime("%Y-%m-%d")
    return ""


def build_dedupe_key(item: dict[str, Any]) -> str:
    canonical_url = str(item.get("canonical_url", "")).strip()
    if canonical_url:
        return hashlib.sha1(canonical_url.encode("utf-8", errors="ignore")).hexdigest()

    u = str(item.get("url") or item.get("link") or "").strip()
    if u:
        p = urlparse(u)
        if p.netloc and p.path:
            host_path = f"{p.netloc.lower().strip()}{p.path.rstrip('/')}"
            if host_path:
                return hashlib.sha1(host_path.encode("utf-8", errors="ignore")).hexdigest()

    title = normalize_title(str(item.get("title", "")))
    dom = _domain(u)
    d = _published_date(item)
    raw = f"{title}|{dom}|{d}"
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def _completeness_rank(it: dict[str, Any]) -> int:
    score = 0
    if str(it.get("title", "")).strip():
        score += 1
    if str(it.get("summary_cn") or it.get("summary") or "").strip():
        score += 1
    if it.get("published_at") is not None:
        score += 1
    if str(it.get("source", "")).strip():
        score += 1
    if str(it.get("original_source_url", "")).strip():
        score += 2
    return score


def _choose_canonical(cluster: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ranked = sorted(
        cluster,
        key=lambda x: (
            float(x.get("quality_score", 0) or 0),
            evidence_rank(str(x.get("evidence_grade", "D"))),
            float(x.get("source_weight", 0) or 0),
            _completeness_rank(x),
        ),
        reverse=True,
    )
    primary = copy.deepcopy(ranked[0])
    others = [copy.deepcopy(x) for x in ranked[1:]]
    return primary, others


def strong_dedupe(items: list[dict[str, Any]], cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    threshold = float(((cfg.get("similarity_thresholds") or {}).get("title_jaccard") or 0.92))
    report = {
        "items_before": len(items),
        "items_after": 0,
        "deduped_count": 0,
        "clusters": [],
    }
    if not items:
        return [], report

    work = [copy.deepcopy(i) for i in items]
    for it in work:
        it["dedupe_key"] = build_dedupe_key(it)

    clusters: list[list[dict[str, Any]]] = []
    for it in work:
        placed = False
        for c in clusters:
            head = c[0]
            if it.get("dedupe_key") == head.get("dedupe_key"):
                c.append(it)
                placed = True
                break
            sim = jaccard_title(str(it.get("title", "")), str(head.get("title", "")))
            if sim >= threshold:
                c.append(it)
                placed = True
                break
        if not placed:
            clusters.append([it])

    out: list[dict[str, Any]] = []
    deduped_total = 0
    for idx, c in enumerate(clusters, 1):
        primary, others = _choose_canonical(c)
        sid = f"story-{idx:04d}-{str(primary.get('dedupe_key', ''))[:8]}"
        primary["story_id"] = sid
        primary["is_primary"] = True
        primary["cluster_size"] = len(c)
        primary["deduped_from_ids"] = [str(x.get("item_id", "")) for x in others]
        primary["other_sources"] = [
            {
                "source_name": str(x.get("source", "")),
                "url": str(x.get("url") or x.get("link") or ""),
                "published_at": str(x.get("published_at", "")),
                "title": str(x.get("title", "")),
            }
            for x in others
        ]
        primary["dedupe_reason"] = "quality_score>evidence>source_weight>completeness"
        out.append(primary)

        deduped_total += max(0, len(c) - 1)
        report["clusters"].append(
            {
                "story_id": sid,
                "cluster_size": len(c),
                "canonical_item_id": str(primary.get("item_id", "")),
                "canonical_title": str(primary.get("title", "")),
                "key": str(primary.get("dedupe_key", "")),
                "dropped_item_ids": [str(x.get("item_id", "")) for x in others],
            }
        )

    out.sort(
        key=lambda x: (
            float(x.get("quality_score", 0) or 0),
            evidence_rank(str(x.get("evidence_grade", "D"))),
            float(x.get("source_weight", 0) or 0),
        ),
        reverse=True,
    )
    report["items_after"] = len(out)
    report["deduped_count"] = deduped_total
    return out, report

