from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlparse


STATIC_PATH_KEYWORDS = [
    "/about",
    "/privacy",
    "/cookie",
    "/newsletter",
    "/portal",
    "/mission",
    "/purpose",
]

LISTING_PATH_KEYWORDS = [
    "/tag/",
    "/type/",
    "/category/",
    "/topics/",
    "/page/",
    "/laboratory-diagnostics",
]


def is_static_or_listing_url(url: str, *, source_group: str | None = None) -> bool:
    u = str(url or "").strip()
    if not u:
        return False
    try:
        p = urlparse(u)
    except Exception:
        return False
    path = str(p.path or "").strip().lower()
    query = parse_qs(str(p.query or ""))
    sg = str(source_group or "").strip().lower()

    if any(k in path for k in STATIC_PATH_KEYWORDS):
        return True
    if sg == "procurement":
        if any(k in path for k in ("/tag/", "/type/", "/category/", "/topics/", "/page/")):
            return True
        if ("page" in query) or re.search(r"/(index|list|search)(/|$)", path):
            return True
        if any(k in path for k in ("/notice", "/bulletin", "/announcement")):
            return False
    if any(k in path for k in LISTING_PATH_KEYWORDS):
        return True
    if "page" in query:
        return True
    if not path or path == "/":
        return True
    return False


def is_article_html(
    html: str,
    url: str,
    *,
    source_group: str | None = None,
    article_min_paragraphs: int = 2,
    article_min_text_chars: int = 200,
) -> tuple[bool, str, dict[str, Any]]:
    raw = str(html or "")
    if not raw.strip():
        return False, "empty_html", {"h1": 0, "paragraph_count": 0, "text_chars": 0, "og_type": ""}
    if is_static_or_listing_url(url, source_group=source_group):
        return False, "static_or_listing_page", {"h1": 0, "paragraph_count": 0, "text_chars": 0, "og_type": ""}

    og_type = ""
    m_og = re.search(
        r'<meta[^>]+property=["\']og:type["\'][^>]+content=["\']([^"\']+)["\']',
        raw,
        flags=re.I,
    )
    if m_og:
        og_type = str(m_og.group(1) or "").strip().lower()
    has_article_tag = bool(re.search(r"<article\b", raw, flags=re.I))
    has_published_meta = bool(re.search(r"article:published_time", raw, flags=re.I))
    h1_count = len(re.findall(r"<h1\b", raw, flags=re.I))
    paragraph_count = len(re.findall(r"<p\b[^>]*>.*?</p>", raw, flags=re.I | re.S))
    cleaned = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", raw)
    text = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    text = re.sub(r"\s+", " ", text).strip()
    text_chars = len(text)
    links_count = len(re.findall(r"<a\b", raw, flags=re.I))
    nav_count = len(re.findall(r"<nav\b|class=[\"'][^\"']*(menu|nav|footer)[^\"']*[\"']", raw, flags=re.I))

    meta = {
        "h1": h1_count,
        "paragraph_count": paragraph_count,
        "text_chars": text_chars,
        "og_type": og_type,
        "has_article_tag": has_article_tag,
        "has_published_meta": has_published_meta,
        "links_count": links_count,
        "nav_count": nav_count,
    }

    if text_chars < max(20, int(article_min_text_chars or 200) // 2):
        return False, "too_short", meta
    if links_count >= 30 and paragraph_count <= 2:
        return False, "static_or_listing_page", meta
    if nav_count >= 5 and paragraph_count <= 2:
        return False, "static_or_listing_page", meta

    if has_article_tag or og_type == "article" or has_published_meta:
        if paragraph_count >= max(1, int(article_min_paragraphs or 2)) and text_chars >= int(article_min_text_chars or 200):
            return True, "article_ok", meta
        return False, "too_short", meta

    if h1_count < 1:
        return False, "no_h1", meta
    if paragraph_count < max(1, int(article_min_paragraphs or 2)):
        return False, "too_few_paragraphs", meta
    if text_chars < int(article_min_text_chars or 200):
        return False, "too_short", meta
    return True, "article_ok", meta
