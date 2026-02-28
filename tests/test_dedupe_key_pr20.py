from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.services.story_clusterer import StoryClusterer
from scripts.generate_ivd_report import _resolve_dedupe_cluster_config


def _item(title: str, url: str, published: datetime) -> dict:
    return {
        "title": title,
        "url": url,
        "published_at": published,
        "first_seen_at": published,
        "source": "generic",
        "source_key": "generic_rss",
    }


def test_same_title_different_urls_clustered_by_normalized_title() -> None:
    now = datetime.now(timezone.utc)
    rows = [
        _item("STAT+: New IVD Panel Launches in US", "https://a.example.com/news/1", now),
        _item("[COMMENT] New IVD panel launches in US", "https://b.example.com/news/2", now + timedelta(hours=1)),
        _item("new ivd panel launches in us", "https://c.example.com/news/3", now + timedelta(hours=2)),
    ]
    c = StoryClusterer(
        {"enabled": True, "window_hours": 72, "key_strategies": ["normalized_title_v1", "host_published_day_v1"]},
        {"generic_rss": 0},
    )
    out, _ = c.cluster(rows)
    assert len(out) < len(rows)


def test_different_titles_not_clustered() -> None:
    now = datetime.now(timezone.utc)
    rows = [
        _item("Company A announces assay approval", "https://a.example.com/news/1", now),
        _item("Company B opens new manufacturing site", "https://b.example.com/news/2", now + timedelta(hours=1)),
    ]
    c = StoryClusterer(
        {"enabled": True, "window_hours": 72, "key_strategies": ["normalized_title_v1", "host_published_day_v1"]},
        {"generic_rss": 0},
    )
    out, _ = c.cluster(rows)
    assert len(out) == 2


def test_enhanced_injects_new_key_strategies_legacy_unchanged() -> None:
    base_cfg = {
        "enabled": True,
        "window_hours": 72,
        "key_strategies": ["canonical_url", "normalized_url_host_path", "title_fingerprint_v1"],
    }
    enhanced = _resolve_dedupe_cluster_config(use_enhanced=True, content_cfg={}, base_cfg=base_cfg)
    legacy = _resolve_dedupe_cluster_config(use_enhanced=False, content_cfg={}, base_cfg=base_cfg)
    assert "normalized_title_v1" in list(enhanced.get("key_strategies") or [])
    assert "host_published_day_v1" in list(enhanced.get("key_strategies") or [])
    assert "normalized_title_v1" not in list(legacy.get("key_strategies") or [])
