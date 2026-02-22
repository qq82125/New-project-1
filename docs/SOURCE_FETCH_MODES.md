# Source Fetch Modes

## Modes
- `rss`: default mode. Parse feed entries.
- `html_article`: fetch a single article page URL and extract one article item.
- `html_list`: list-page parser (debug/testing only).

## Recommended Usage
- Prefer `rss` whenever source provides feed.
- Use `html_article` only for concrete article URLs.
- Avoid home/category/nav URLs in `html_article`.

## WHO/Siemens-like sites
- If feed exists, use RSS URL instead of section/home pages.
- If no feed, use direct article URLs and keep `article_required` checks enabled.

## Guard & Drop Reasons
Common `drop_reason` values:
- `static_or_listing_page`: URL/page looks like nav/category/static page.
- `too_short`: content too short to be a valid article.
- `no_h1` / `too_few_paragraphs`: article structure missing.

## Minimal Fetch Config Example
```yaml
fetch:
  mode: rss
  article_required: true
  article_min_paragraphs: 2
  article_min_text_chars: 200
  allow_body_fetch_for_rss: false
```
