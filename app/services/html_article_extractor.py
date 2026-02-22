from __future__ import annotations

import re
from typing import Any

from app.services.page_classifier import is_article_html


def _clean_text(raw: str) -> str:
    t = str(raw or "")
    t = re.sub(r"(?is)<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_article(url: str, html: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = dict(config or {})
    min_p = int(cfg.get("article_min_paragraphs", 2) or 2)
    min_chars = int(cfg.get("article_min_text_chars", 200) or 200)
    ok, reason, meta = is_article_html(
        html,
        url,
        source_group=str(cfg.get("source_group", "")).strip() or None,
        article_min_paragraphs=min_p,
        article_min_text_chars=min_chars,
    )
    if not ok:
        return {"ok": False, "dropped": True, "reason": reason, "article_meta": meta}

    h1 = ""
    m_h1 = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", html)
    if m_h1:
        h1 = _clean_text(m_h1.group(1))
    og_title = ""
    m_og = re.search(
        r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        html,
    )
    if m_og:
        og_title = _clean_text(m_og.group(1))
    title = h1 or og_title

    published_at = ""
    m_pub = re.search(
        r'(?is)<meta[^>]+(?:property|name)=["\'](?:article:published_time|pubdate|datePublished)["\'][^>]+content=["\']([^"\']+)["\']',
        html,
    )
    if m_pub:
        published_at = str(m_pub.group(1) or "").strip()

    paragraphs = re.findall(r"(?is)<p[^>]*>(.*?)</p>", html)
    para_txt = [_clean_text(x) for x in paragraphs if _clean_text(x)]
    snippet_raw = " ".join(para_txt[:2]).strip()
    if not snippet_raw:
        snippet_raw = _clean_text(html)
    max_chars = int(cfg.get("snippet_max_chars", 480) or 480)
    evidence_snippet = snippet_raw[: max(120, max_chars)].strip()

    return {
        "ok": True,
        "dropped": False,
        "reason": "article_ok",
        "title": title,
        "published_at": published_at,
        "evidence_snippet": evidence_snippet,
        "article_meta": meta,
    }
