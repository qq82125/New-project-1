from __future__ import annotations

from urllib.parse import urlparse


def url_norm(url: str) -> str:
    """Normalize URL for stable cache/dedupe keys."""
    try:
        p = urlparse(str(url or ""))
        host = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/")
        query = p.query.strip()
        kept_query = ""
        if query:
            low = query.lower()
            if any(k in low for k in ("id=", "article=", "story=", "p=", "item=")):
                kept_query = "?" + query
        return f"{host}{path}{kept_query}"
    except Exception:
        return str(url or "").strip().lower()
