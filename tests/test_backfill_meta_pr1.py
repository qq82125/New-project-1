from __future__ import annotations

import tempfile
from pathlib import Path

from app.db.models.rules import RawItem
from app.services.feed_db import FeedDBService
from app.services.rules_store import RulesStore


def _write_registry(root: Path) -> None:
    p = root / "rules" / "sources_registry.v1.yaml"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        """
version: "1.0.0"
sources:
  - id: test_source
    name: Test
    fetcher: rss
    enabled: true
    region: 北美
    trust_tier: A
    priority: 80
    tags: [media, us]
groups: {}
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_backfill_meta_dry_run_does_not_write() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        _write_registry(root)
        store = RulesStore(root)
        svc = FeedDBService(store.database_url)
        with svc._session() as s:  # noqa: SLF001
            s.add(
                RawItem(
                    id="ri_test",
                    source_id="test_source",
                    fetched_at="2026-02-27T00:00:00Z",
                    published_at="2026-02-27T00:00:00Z",
                    title_raw="Company reports quarterly earnings",
                    title_norm="company reports quarterly earnings",
                    url_raw="https://example.com/a",
                    canonical_url="example.com/a",
                    content_snippet="",
                    raw_payload={},
                    source_group=None,
                    region=None,
                    trust_tier=None,
                    event_type=None,
                    priority=0,
                )
            )
            s.commit()
        out = svc.backfill_raw_meta(root, execute=False, batch_size=100)
        assert out["ok"] is True
        with svc._session() as s:  # noqa: SLF001
            row = s.get(RawItem, "ri_test")
            assert row is not None
            assert row.region is None
            assert row.event_type is None


def test_backfill_meta_execute_writes_region_and_event_type() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        _write_registry(root)
        store = RulesStore(root)
        svc = FeedDBService(store.database_url)
        with svc._session() as s:  # noqa: SLF001
            s.add(
                RawItem(
                    id="ri_test2",
                    source_id="test_source",
                    fetched_at="2026-02-27T00:00:00Z",
                    published_at="2026-02-27T00:00:00Z",
                    title_raw="Company reports quarterly earnings",
                    title_norm="company reports quarterly earnings",
                    url_raw="https://example.com/b",
                    canonical_url="example.com/b",
                    content_snippet="",
                    raw_payload={},
                    source_group=None,
                    region=None,
                    trust_tier=None,
                    event_type=None,
                    priority=0,
                )
            )
            s.commit()
        out = svc.backfill_raw_meta(root, execute=True, batch_size=100)
        assert out["ok"] is True
        with svc._session() as s:  # noqa: SLF001
            row = s.get(RawItem, "ri_test2")
            assert row is not None
            assert row.region == "北美"
            assert row.trust_tier == "A"
            assert row.source_group == "media"
            assert row.event_type == "earnings_noise"

