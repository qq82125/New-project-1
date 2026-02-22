# Procurement Onboarding

This guide focuses on getting procurement sources into collect assets reliably.

## Checklist

1. Prefer RSS sources for procurement notices.
2. Use announcement-level URLs:
   - RSS feed for procurement notices, or
   - concrete article/announcement URL for `html_article`.
3. For short procurement announcements, keep thresholds relaxed:
   - `article_min_paragraphs: 1`
   - `article_min_text_chars: 120`
4. Validate with collect diagnostics in `artifacts/<run_id>/run_meta.json`:
   - `sources_attempted`
   - `sources_written_counts`
   - `sources_dropped_counts`
   - `sources_fetch_errors`

## Recommended Source Patterns

- `fetch.mode: rss` with `interval_minutes: 60` for stable feeds.
- If RSS is unavailable, use:
  - `fetch.mode: html_article`
  - point to a concrete notice page, not list/index/search pages.

## Troubleshooting

- `written=0` with `drop_reason=static_or_listing_page`:
  source URL is likely a list/index/navigation page.
- `fetch_errors` shows network or parse errors:
  verify URL reachability and feed format.
- `not_due` appears in `sources_dropped_counts`:
  force collect (`--force`) or lower interval for testing.
