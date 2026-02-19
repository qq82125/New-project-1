from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import yaml

from app.services.rules_store import RulesStore


class SourceRegistryError(RuntimeError):
    pass


REGISTRY_FILE_NAME = "sources_registry.v1.yaml"
OVERRIDES_FILE_NAME = "sources_overrides.json"
ALLOWED_FETCHERS = {"rss", "html", "rsshub", "google_news", "web", "api"}


def _canonical_fetcher(value: str) -> str:
    raw = str(value or "").strip().lower()
    if raw == "web":
        return "html"
    if raw in ALLOWED_FETCHERS:
        return raw
    return raw


def _is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return bool(p.scheme in ("http", "https") and p.netloc)
    except Exception:
        return False


def _env_bool(name: str, default: bool = True) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "y", "on"}


def _safe_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _same_host(a: str, b: str) -> bool:
    try:
        pa = urlparse(str(a))
        pb = urlparse(str(b))
        return pa.hostname and pb.hostname and pa.hostname.lower() == pb.hostname.lower()
    except Exception:
        return False


def _load_yaml(path: Path) -> dict[str, Any]:
    obj = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SourceRegistryError(f"invalid yaml object: {path}")
    return obj


def _normalize_source(raw: dict[str, Any], file_path: Path) -> dict[str, Any]:
    src = dict(raw)
    fetcher = _canonical_fetcher(str(src.get("fetcher") or src.get("connector") or ""))
    if fetcher:
        src["fetcher"] = fetcher
        src["connector"] = "web" if fetcher == "html" else fetcher  # backward-compatible field used by runtime
    src["enabled"] = bool(src.get("enabled", True))
    src["source_file"] = str(file_path)
    src["tags"] = src.get("tags", []) if isinstance(src.get("tags", []), list) else []
    src["priority"] = int(src.get("priority", 0))
    return src


def _extract_web_sample_entries(html: str, base_url: str, limit: int = 3) -> list[dict[str, str]]:
    if not html:
        return []
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for m in re.finditer(r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", html, flags=re.I | re.S):
        href = (m.group(1) or "").strip()
        raw_title = re.sub(r"<[^>]+>", " ", m.group(2) or "")
        title = re.sub(r"\s+", " ", raw_title).strip()
        if not href or not title or len(title) < 8:
            continue
        low = href.lower()
        if any(x in low for x in ("javascript:", "mailto:", "#", "login", "signin", "register")):
            continue
        if not re.search(r"\.?(portal\.php\?mod=(view|show|thread|detail)|/thread|/article|/news|aid=|itemid=)", low):
            continue
        if low.startswith("http://") or low.startswith("https://"):
            link = href
        elif low.startswith("//"):
            link = f"https:{href}"
        elif href.startswith("?"):
            link = base_url + href
        else:
            link = base_url.rstrip("/") + "/" + href.lstrip("/")
        if link in seen:
            continue
        seen.add(link)
        out.append({"title": title, "link": link})
        if len(out) >= limit:
            break
    return out


def _extract_feed_links_from_html(html: str, page_url: str, same_host_only: bool = True) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    if not html:
        return links

    def _push(raw_url: str, title: str = "") -> None:
        u = str(raw_url or "").strip()
        if not u:
            return
        if u.startswith("//"):
            u = f"https:{u}"
        elif not (u.startswith("http://") or u.startswith("https://")):
            u = urljoin(page_url, u)
        if not _is_valid_url(u):
            return
        if same_host_only and not _same_host(page_url, u):
            return
        if u in seen:
            return
        seen.add(u)
        links.append({"url": u, "name": str(title or "").strip()})

    # 1) <link rel="alternate" type="application/rss+xml|atom+xml">
    for tag in re.findall(r"<link[^>]+>", html, flags=re.I | re.S):
        if not re.search(r"rel=['\"][^'\"]*alternate[^'\"]*['\"]", tag, flags=re.I):
            continue
        if not re.search(r"type=['\"]application/(rss|atom)\+xml['\"]", tag, flags=re.I):
            continue
        hm = re.search(r"href=['\"]([^'\"]+)['\"]", tag, flags=re.I)
        if not hm:
            continue
        tm = re.search(r"title=['\"]([^'\"]+)['\"]", tag, flags=re.I)
        _push(hm.group(1), tm.group(1) if tm else "")

    # 2) anchor links to feed-looking urls
    for m in re.finditer(r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", html, flags=re.I | re.S):
        href = str(m.group(1) or "").strip()
        text = re.sub(r"<[^>]+>", " ", str(m.group(2) or ""))
        text = re.sub(r"\s+", " ", text).strip()
        low = href.lower()
        if any(x in low for x in (".xml", ".rss", ".atom", "/rss", "/feed")) or ("rss" in text.lower()):
            _push(href, text)

    return links


def _extract_child_feeds(html: str, page_url: str, same_host_only: bool = True) -> list[dict[str, str]]:
    return _extract_feed_links_from_html(html, page_url, same_host_only=same_host_only)


def _pick_child_feed(
    feeds: list[dict[str, str]],
    policy: Any,
    keywords: list[str] | None = None,
) -> str:
    if not feeds:
        return ""
    mode = "pick_first"
    kws = [str(x).strip().lower() for x in (keywords or []) if str(x).strip()]
    if isinstance(policy, str):
        mode = policy.strip().lower() or mode
    elif isinstance(policy, dict):
        mode = str(policy.get("mode") or policy.get("strategy") or mode).strip().lower()
        raw = policy.get("keywords")
        if isinstance(raw, list) and raw:
            kws = [str(x).strip().lower() for x in raw if str(x).strip()]

    if mode == "pick_all":
        return str(feeds[0].get("url") or "")
    if mode == "pick_by_keywords":
        for row in feeds:
            t = (str(row.get("name") or "") + " " + str(row.get("url") or "")).lower()
            if any(k in t for k in kws):
                return str(row.get("url") or "")
        return str(feeds[0].get("url") or "")
    return str(feeds[0].get("url") or "")


def _is_probable_js_page(html: str) -> bool:
    if not html:
        return False
    if re.search(r"id=['\"](__NEXT_DATA__|root|app)['\"]", html, flags=re.I):
        return True
    if len(re.findall(r"<script", html, flags=re.I)) >= 8 and len(re.findall(r"<a[^>]+href=", html, flags=re.I)) <= 3:
        return True
    return False


def _generic_html_list_entries(
    html: str,
    page_url: str,
    limit: int = 3,
    selectors: dict[str, Any] | None = None,
) -> tuple[list[dict[str, str]], str]:
    selectors = selectors if isinstance(selectors, dict) else {}
    samples: list[dict[str, str]] = []
    seen: set[str] = set()

    # regex selectors (backward compatible)
    item_regex = str(selectors.get("item_regex") or "").strip()
    title_regex = str(selectors.get("title_regex") or "").strip()
    link_regex = str(selectors.get("link_regex") or "").strip()
    if item_regex and title_regex and link_regex:
        for m in re.finditer(item_regex, html, flags=re.I | re.S):
            block = m.group(0)
            tm = re.search(title_regex, block, flags=re.I | re.S)
            lm = re.search(link_regex, block, flags=re.I | re.S)
            if not tm or not lm:
                continue
            title = re.sub(r"<[^>]+>", " ", str(tm.group(1) or ""))
            title = re.sub(r"\s+", " ", title).strip()
            u = str(lm.group(1) or "").strip()
            if not u:
                continue
            if u.startswith("//"):
                u = f"https:{u}"
            elif not (u.startswith("http://") or u.startswith("https://")):
                u = urljoin(page_url, u)
            if not _is_valid_url(u):
                continue
            if not _same_host(page_url, u):
                continue
            if u in seen:
                continue
            seen.add(u)
            samples.append({"title": title, "url": u})
            if len(samples) >= limit:
                return samples, "selector_regex"

    cleaned = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    # Prefer article blocks.
    article_blocks = re.findall(r"(?is)<article[^>]*>.*?</article>", cleaned)
    blocks = article_blocks if article_blocks else [cleaned]

    for block in blocks:
        for m in re.finditer(r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", block, flags=re.I | re.S):
            href = str(m.group(1) or "").strip()
            title = re.sub(r"<[^>]+>", " ", str(m.group(2) or ""))
            title = re.sub(r"\s+", " ", title).strip()
            if not href or not title or len(title) < 8:
                continue
            low = href.lower()
            if any(x in low for x in ("javascript:", "mailto:", "#", "/login", "signin", "register")):
                continue
            if href.startswith("//"):
                u = f"https:{href}"
            elif href.startswith("http://") or href.startswith("https://"):
                u = href
            else:
                u = urljoin(page_url, href)
            if not _is_valid_url(u):
                continue
            if not _same_host(page_url, u):
                continue
            if u in seen:
                continue
            seen.add(u)
            samples.append({"title": title, "url": u})
            if len(samples) >= limit:
                return samples, "generic_html"

    return samples, "generic_html_empty"


def _fetch_url_with_retry(url: str, headers: dict[str, str], timeout: int, retries: int) -> dict[str, Any]:
    attempt = 0
    last_err = ""
    last_type = "network_error"
    while attempt <= max(0, retries):
        attempt += 1
        req = Request(url, headers=headers)
        try:
            with urlopen(req, timeout=timeout) as r:
                data = r.read()
                status = int(getattr(r, "status", 200))
                ctype = str(getattr(r, "headers", {}).get("Content-Type", "")) if getattr(r, "headers", None) else ""
                return {
                    "ok": True,
                    "data": data,
                    "http_status": status,
                    "content_type": ctype,
                    "error_type": "",
                    "error_message": "",
                }
        except HTTPError as e:
            last_type = "http_error"
            last_err = f"HTTPError: {getattr(e, 'code', '')} {e}"
            if int(getattr(e, "code", 0) or 0) in (403, 404):
                break
        except TimeoutError as e:
            last_type = "timeout"
            last_err = f"TimeoutError: {e}"
        except URLError as e:
            msg = str(getattr(e, "reason", e))
            if "Name or service not known" in msg or "nodename nor servname provided" in msg:
                last_type = "dns_error"
            elif "timed out" in msg.lower():
                last_type = "timeout"
            else:
                last_type = "network_error"
            last_err = f"URLError: {msg}"
        except Exception as e:
            last_type = "network_error"
            last_err = f"{type(e).__name__}: {e}"
        if attempt <= retries:
            time.sleep(min(1.5, 0.25 * (2 ** (attempt - 1))))
    return {
        "ok": False,
        "data": b"",
        "http_status": None,
        "content_type": "",
        "error_type": last_type,
        "error_message": last_err,
    }


def _resolve_rules_root(project_root: Path, rules_root: Path | None = None) -> Path:
    if rules_root is not None:
        return rules_root
    env_root = os.environ.get("RULES_WORKSPACE_DIR", "").strip()
    if env_root:
        return Path(env_root)
    return project_root / "rules"


def _candidate_registry_paths(project_root: Path, rules_root: Path | None = None) -> list[Path]:
    rr = _resolve_rules_root(project_root, rules_root)
    root_rules = project_root / "rules"
    out = [rr / REGISTRY_FILE_NAME]
    if root_rules != rr:
        out.append(root_rules / REGISTRY_FILE_NAME)
    # de-dup while preserving order
    dedup: list[Path] = []
    seen: set[str] = set()
    for p in out:
        sp = str(p)
        if sp in seen:
            continue
        seen.add(sp)
        dedup.append(p)
    return dedup


def _first_existing(paths: list[Path]) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None


def _overrides_path(project_root: Path) -> Path:
    return project_root / "data" / OVERRIDES_FILE_NAME


def _registry_path(project_root: Path, rules_root: Path | None = None) -> Path:
    reg_candidates = _candidate_registry_paths(project_root, rules_root=rules_root)
    return _first_existing(reg_candidates) or reg_candidates[0]


def _load_overrides(project_root: Path) -> dict[str, Any]:
    p = _overrides_path(project_root)
    if not p.exists():
        return {"enabled": {}}
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return {"enabled": {}}
        en = obj.get("enabled", {})
        if not isinstance(en, dict):
            obj["enabled"] = {}
        return obj
    except Exception:
        return {"enabled": {}}


def _save_overrides(project_root: Path, obj: dict[str, Any]) -> None:
    p = _overrides_path(project_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _apply_overrides(sources: list[dict[str, Any]], overrides: dict[str, Any]) -> list[dict[str, Any]]:
    enabled_map = overrides.get("enabled", {}) if isinstance(overrides.get("enabled"), dict) else {}
    out: list[dict[str, Any]] = []
    for s in sources:
        row = dict(s)
        sid = str(row.get("id", "")).strip()
        if sid in enabled_map:
            row["enabled"] = bool(enabled_map.get(sid))
            row["enabled_overridden"] = True
        else:
            row["enabled_overridden"] = False
        out.append(row)
    return out


def _load_split_sources(root: Path) -> list[dict[str, Any]]:
    src_root = root / "sources"
    if not src_root.exists():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(src_root.glob("*.y*ml")):
        doc = _load_yaml(p)
        sources = doc.get("sources", [])
        if not isinstance(sources, list):
            raise SourceRegistryError(f"{p}: sources must be array")
        for s in sources:
            if isinstance(s, dict):
                out.append(_normalize_source(s, p))
    return out


def _infer_groups_from_tags(sources: list[dict[str, Any]]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for s in sources:
        sid = str(s.get("id", "")).strip()
        if not sid:
            continue
        tags = {str(x).strip().lower() for x in s.get("tags", []) if str(x).strip()}
        region = str(s.get("region", "")).strip()
        if "regulatory" in tags and region == "中国":
            groups.setdefault("regulatory_cn", []).append(sid)
        elif "regulatory" in tags and region in ("亚太", "中国"):
            groups.setdefault("regulatory_apac", []).append(sid)
        elif "regulatory" in tags and region in ("北美", "欧洲"):
            groups.setdefault("regulatory_us_eu", []).append(sid)
        elif "journal" in tags or "preprint" in tags:
            groups.setdefault("journals_preprints", []).append(sid)
        elif "procurement" in tags:
            groups.setdefault("procurement_global", []).append(sid)
        else:
            groups.setdefault("media_global", []).append(sid)
    return groups


def load_sources_registry_bundle(
    project_root: Path,
    rules_root: Path | None = None,
) -> dict[str, Any]:
    """
    Returns a merged registry bundle:
    {
      version: str,
      sources: list[dict],
      groups: dict[str, list[str]],
      source_file: str,
      overrides_file: str,
    }
    """
    reg_path = _first_existing(_candidate_registry_paths(project_root, rules_root=rules_root))
    if reg_path is not None:
        doc = _load_yaml(reg_path)
        version = str(doc.get("version", "1.0.0"))
        raw_sources = doc.get("sources", [])
        if not isinstance(raw_sources, list):
            raise SourceRegistryError(f"{reg_path}: sources must be array")
        groups = doc.get("groups", {})
        if not isinstance(groups, dict):
            raise SourceRegistryError(f"{reg_path}: groups must be object")

        sources: list[dict[str, Any]] = []
        id_set: set[str] = set()
        for s in raw_sources:
            if not isinstance(s, dict):
                continue
            row = _normalize_source(s, reg_path)
            sid = str(row.get("id", "")).strip()
            if not sid:
                continue
            if sid in id_set:
                raise SourceRegistryError(f"{reg_path}: duplicate source id={sid}")
            id_set.add(sid)
            sources.append(row)

        gid_to_ids: dict[str, list[str]] = {}
        sid_to_groups: dict[str, list[str]] = {}
        for g, raw_ids in groups.items():
            gid = str(g).strip()
            if not gid:
                continue
            if not isinstance(raw_ids, list):
                raise SourceRegistryError(f"{reg_path}: groups.{gid} must be array")
            ids: list[str] = []
            for x in raw_ids:
                sid = str(x).strip()
                if not sid:
                    continue
                ids.append(sid)
                sid_to_groups.setdefault(sid, []).append(gid)
            gid_to_ids[gid] = ids

        for row in sources:
            sid = str(row.get("id", "")).strip()
            row["registry_groups"] = sid_to_groups.get(sid, [])

        overrides = _load_overrides(project_root)
        merged = _apply_overrides(sources, overrides)
        return {
            "version": version,
            "sources": merged,
            "groups": gid_to_ids,
            "source_file": str(reg_path),
            "overrides_file": str(_overrides_path(project_root)),
        }

    # Legacy fallback path: existing DB-first behavior, then split yaml.
    if rules_root is None and not os.environ.get("RULES_WORKSPACE_DIR", "").strip():
        store = RulesStore(project_root)
        db_sources = store.list_sources()
        if db_sources:
            out: list[dict[str, Any]] = []
            for s in db_sources:
                ss = dict(s)
                ss["source_file"] = "db://sources"
                out.append(ss)
            return {
                "version": "db",
                "sources": out,
                "groups": _infer_groups_from_tags(out),
                "source_file": "db://sources",
                "overrides_file": str(_overrides_path(project_root)),
            }

    root = _resolve_rules_root(project_root, rules_root)
    split_sources = _load_split_sources(root)
    return {
        "version": "split.v1",
        "sources": split_sources,
        "groups": _infer_groups_from_tags(split_sources),
        "source_file": str(root / "sources"),
        "overrides_file": str(_overrides_path(project_root)),
    }


def load_sources_registry(project_root: Path, rules_root: Path | None = None) -> list[dict[str, Any]]:
    return list(load_sources_registry_bundle(project_root, rules_root=rules_root).get("sources", []))


def _validate_bundle_v1(bundle: dict[str, Any], schema_path: Path) -> dict[str, Any]:
    sources = bundle.get("sources", [])
    groups = bundle.get("groups", {})
    if not isinstance(sources, list):
        raise SourceRegistryError("sources must be array")
    if not isinstance(groups, dict):
        raise SourceRegistryError("groups must be object")
    ids: set[str] = set()
    for idx, s in enumerate(sources):
        if not isinstance(s, dict):
            raise SourceRegistryError(f"sources[{idx}] must be object")
        sid = str(s.get("id", "")).strip()
        if not sid:
            raise SourceRegistryError(f"sources[{idx}].id required")
        if sid in ids:
            raise SourceRegistryError(f"duplicate id: {sid}")
        ids.add(sid)
        fetcher = _canonical_fetcher(str(s.get("fetcher") or s.get("connector") or "").strip())
        if fetcher not in ALLOWED_FETCHERS:
            raise SourceRegistryError(f"{sid}: fetcher must be one of {sorted(ALLOWED_FETCHERS)}")
        url = str(s.get("url", "")).strip()
        if fetcher in {"rss", "html", "google_news", "web", "api"} and not _is_valid_url(url):
            raise SourceRegistryError(f"{sid}: invalid url")
        if fetcher == "rsshub" and not (url.startswith("rsshub://") or _is_valid_url(url)):
            raise SourceRegistryError(f"{sid}: rsshub url must be rsshub:// or http(s) URL")
        tt = str(s.get("trust_tier", "")).strip()
        if tt not in {"A", "B", "C"}:
            raise SourceRegistryError(f"{sid}: trust_tier must be A|B|C")
        try:
            pr = int(s.get("priority", 0))
        except Exception:
            raise SourceRegistryError(f"{sid}: priority must be int") from None
        if pr < 0 or pr > 1000:
            raise SourceRegistryError(f"{sid}: priority out of range 0..1000")

    for g, raw_ids in groups.items():
        if not isinstance(raw_ids, list):
            raise SourceRegistryError(f"groups.{g} must be array")
        for sid in raw_ids:
            ss = str(sid).strip()
            if ss and ss not in ids:
                raise SourceRegistryError(f"groups.{g}: unknown source id={ss}")
    return {
        "ok": True,
        "schema": str(schema_path),
        "source_count": len(ids),
        "group_count": len(groups),
    }


def validate_sources_registry(project_root: Path, rules_root: Path | None = None) -> dict[str, Any]:
    resolved_root = _resolve_rules_root(project_root, rules_root)
    schema_v1_candidates = [
        resolved_root / "sources_registry.schema.json",
        resolved_root / "schemas" / "sources_registry.schema.json",
        project_root / "rules" / "sources_registry.schema.json",
        project_root / "rules" / "schemas" / "sources_registry.schema.json",
    ]
    schema_v1 = _first_existing(schema_v1_candidates)
    reg_path = _first_existing(_candidate_registry_paths(project_root, rules_root=rules_root))

    # Prefer unified registry v1 when present.
    if reg_path and schema_v1:
        bundle = load_sources_registry_bundle(project_root, rules_root=rules_root)
        out = _validate_bundle_v1(bundle, schema_v1)
        ov = _load_overrides(project_root)
        en = ov.get("enabled", {}) if isinstance(ov.get("enabled"), dict) else {}
        known = {str(s.get("id", "")).strip() for s in bundle.get("sources", []) if isinstance(s, dict)}
        bad = [str(k) for k in en.keys() if str(k).strip() and str(k).strip() not in known]
        if bad:
            raise SourceRegistryError(f"overrides.enabled contains unknown source ids: {','.join(sorted(bad)[:10])}")
        out["registry_file"] = str(reg_path)
        out["overrides_file"] = str(_overrides_path(project_root))
        out["overrides_count"] = len(en)
        return out

    # Legacy fallback validation (split files + sources.schema.json).
    root = resolved_root / "sources"
    schema_path = resolved_root / "schemas" / "sources.schema.json"
    if not schema_path.exists():
        schema_path = project_root / "rules" / "schemas" / "sources.schema.json"
    if not schema_path.exists():
        raise SourceRegistryError(f"missing schema: {schema_path}")

    _ = json.loads(schema_path.read_text(encoding="utf-8"))
    errors: list[str] = []
    ids: dict[str, str] = {}
    connectors = {"rss", "web", "html", "api", "rsshub", "google_news"}
    for p in sorted(root.glob("*.y*ml")):
        doc = _load_yaml(p)
        if "version" not in doc:
            errors.append(f"{p}: missing version")
        sources = doc.get("sources", [])
        if not isinstance(sources, list):
            errors.append(f"{p}: sources must be array")
            continue
        for idx, s in enumerate(sources):
            if not isinstance(s, dict):
                errors.append(f"{p}[{idx}]: source must be object")
                continue
            sid = str(s.get("id", "")).strip()
            if not sid:
                errors.append(f"{p}[{idx}]: id required")
                continue
            if sid in ids:
                errors.append(f"duplicate id: {sid} in {p} and {ids[sid]}")
            ids[sid] = str(p)
            connector = _canonical_fetcher(str(s.get("connector", "")).strip())
            if connector not in connectors:
                errors.append(f"{p}[{idx}] {sid}: invalid connector={connector}")
            if connector in ("rss", "web", "html", "google_news", "api"):
                url = str(s.get("url", "")).strip()
                if not _is_valid_url(url):
                    errors.append(f"{p}[{idx}] {sid}: invalid url={url}")
    if errors:
        raise SourceRegistryError("; ".join(errors[:20]))
    return {
        "ok": True,
        "schema": str(schema_path),
        "source_count": len(ids),
        "files": [str(p) for p in sorted(root.glob("*.y*ml"))],
    }


def select_sources(
    sources: list[dict[str, Any]],
    selector: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    selector = selector or {}
    trust_rank = {"A": 3, "B": 2, "C": 1}
    min_tt = str(selector.get("min_trust_tier", "C")).strip().upper() or "C"
    min_rank = trust_rank.get(min_tt, 1)
    include_tags = set(str(x) for x in selector.get("include_tags", []))
    exclude_tags = set(str(x) for x in selector.get("exclude_tags", []))
    include_ids = set(str(x) for x in selector.get("include_source_ids", []))
    exclude_ids = set(str(x) for x in selector.get("exclude_source_ids", []))
    include_groups = set(str(x) for x in selector.get("include_groups", []))
    exclude_groups = set(str(x) for x in selector.get("exclude_groups", []))
    enabled_only = bool(selector.get("default_enabled_only", True))

    out = []
    for s in sources:
        tt = str(s.get("trust_tier", "C")).strip().upper() or "C"
        if trust_rank.get(tt, 0) < min_rank:
            continue
        sid = str(s.get("id", ""))
        if enabled_only and not bool(s.get("enabled", True)):
            continue
        if include_ids and sid not in include_ids:
            continue
        if sid in exclude_ids:
            continue

        src_groups = {str(x) for x in s.get("registry_groups", [])}
        if include_groups and not (include_groups & src_groups):
            continue
        if exclude_groups and (exclude_groups & src_groups):
            continue

        tags = {str(x) for x in s.get("tags", [])}
        if include_tags and not (include_tags & tags):
            continue
        if exclude_tags and (exclude_tags & tags):
            continue
        out.append(s)
    return out


def _read_profile_selector(
    project_root: Path,
    profile: str,
    rules_root: Path | None = None,
) -> dict[str, Any]:
    path = _resolve_rules_root(project_root, rules_root) / "content_rules" / f"{profile}.yaml"
    if not path.exists():
        return {}
    doc = _load_yaml(path)
    defaults = doc.get("defaults", {}) if isinstance(doc.get("defaults"), dict) else {}
    sel = defaults.get("content_sources", {})
    return sel if isinstance(sel, dict) else {}


def list_sources_for_profile(
    project_root: Path,
    profile: str,
    rules_root: Path | None = None,
) -> list[dict[str, Any]]:
    bundle = load_sources_registry_bundle(project_root, rules_root=rules_root)
    sources = bundle.get("sources", []) if isinstance(bundle, dict) else []
    groups = bundle.get("groups", {}) if isinstance(bundle, dict) else {}
    # Ensure each source carries reverse group membership.
    if isinstance(groups, dict):
        rev: dict[str, list[str]] = {}
        for g, ids in groups.items():
            if not isinstance(ids, list):
                continue
            for sid in ids:
                ss = str(sid).strip()
                if ss:
                    rev.setdefault(ss, []).append(str(g))
        for s in sources:
            sid = str(s.get("id", "")).strip()
            s["registry_groups"] = rev.get(sid, s.get("registry_groups", []))
    selector = _read_profile_selector(project_root, profile, rules_root=rules_root)
    return select_sources(sources, selector)


def effective_source_ids_for_profile(
    project_root: Path,
    profile: str,
    rules_root: Path | None = None,
) -> list[str]:
    return sorted([str(s.get("id", "")).strip() for s in list_sources_for_profile(project_root, profile, rules_root=rules_root) if str(s.get("id", "")).strip()])


def diff_sources_for_profiles(
    project_root: Path,
    from_profile: str,
    to_profile: str,
    rules_root: Path | None = None,
) -> dict[str, Any]:
    a = {s.get("id") for s in list_sources_for_profile(project_root, from_profile, rules_root=rules_root)}
    b = {s.get("id") for s in list_sources_for_profile(project_root, to_profile, rules_root=rules_root)}
    return {
        "from": from_profile,
        "to": to_profile,
        "added": sorted(list(b - a)),
        "removed": sorted(list(a - b)),
        "common": sorted(list(a & b)),
    }


def _write_registry_doc(path: Path, doc: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")


def upsert_source_registry(
    project_root: Path,
    source: dict[str, Any],
    rules_root: Path | None = None,
) -> dict[str, Any]:
    reg_path = _registry_path(project_root, rules_root=rules_root)
    if reg_path.exists():
        doc = _load_yaml(reg_path)
    else:
        doc = {"version": "1.0.0", "sources": [], "groups": {}}
    sources = doc.get("sources", [])
    if not isinstance(sources, list):
        sources = []
    sid = str(source.get("id", "")).strip()
    if not sid:
        raise SourceRegistryError("source id required")

    normalized = dict(source)
    fetcher = _canonical_fetcher(str(normalized.get("fetcher") or normalized.get("connector") or ""))
    if fetcher:
        normalized["fetcher"] = fetcher
        normalized["connector"] = "web" if fetcher == "html" else fetcher
    if "enabled" not in normalized:
        normalized["enabled"] = True

    found = False
    for idx, s in enumerate(sources):
        if not isinstance(s, dict):
            continue
        if str(s.get("id", "")).strip() == sid:
            sources[idx] = normalized
            found = True
            break
    if not found:
        sources.append(normalized)
    doc["sources"] = sources
    doc.setdefault("groups", {})
    _write_registry_doc(reg_path, doc)
    return {"ok": True, "source": _normalize_source(normalized, reg_path), "registry_file": str(reg_path)}


def set_source_enabled_override(
    project_root: Path,
    source_id: str,
    enabled: bool | None = None,
    rules_root: Path | None = None,
) -> dict[str, Any]:
    bundle = load_sources_registry_bundle(project_root, rules_root=rules_root)
    src = next((s for s in bundle.get("sources", []) if str(s.get("id", "")).strip() == source_id), None)
    if not isinstance(src, dict):
        raise SourceRegistryError(f"source not found: {source_id}")
    new_enabled = (not bool(src.get("enabled", True))) if enabled is None else bool(enabled)
    ov = _load_overrides(project_root)
    em = ov.get("enabled", {}) if isinstance(ov.get("enabled"), dict) else {}
    em[str(source_id)] = bool(new_enabled)
    ov["enabled"] = em
    ov["updated_at"] = datetime.now(timezone.utc).isoformat()
    _save_overrides(project_root, ov)
    src2 = dict(src)
    src2["enabled"] = bool(new_enabled)
    src2["enabled_overridden"] = True
    return {"ok": True, "source": src2, "overrides_file": str(_overrides_path(project_root))}


def _normalize_sample_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for x in rows[: max(1, limit)]:
        t = str(x.get("title") or "").strip()
        u = str(x.get("url") or x.get("link") or "").strip()
        d = str(x.get("published_at") or x.get("date") or "").strip()
        if not t and not u:
            continue
        row = {"title": t, "url": u}
        if d:
            row["published_at"] = d
        out.append(row)
    return out


def _rss_entries_from_bytes(data: bytes, limit: int) -> list[dict[str, str]]:
    feed = feedparser.parse(data)
    rows: list[dict[str, str]] = []
    for e in getattr(feed, "entries", [])[: max(1, limit)]:
        rows.append(
            {
                "title": str(getattr(e, "title", "") or "").strip(),
                "url": str(getattr(e, "link", "") or "").strip(),
                "published_at": str(getattr(e, "published", "") or getattr(e, "updated", "") or "").strip(),
            }
        )
    return _normalize_sample_rows(rows, limit)


def fetch_source_entries(
    source: dict[str, Any],
    *,
    limit: int = 50,
    timeout_seconds: int | None = None,
    retries: int | None = None,
) -> dict[str, Any]:
    """
    Unified source fetch logic used by both test endpoint and runtime collectors.
    """
    t0 = time.monotonic()
    connector = _canonical_fetcher(str(source.get("connector") or source.get("fetcher") or ""))
    url = str(source.get("url", "")).strip()
    sid = str(source.get("id", "")).strip()
    fetch = source.get("fetch", {}) if isinstance(source.get("fetch"), dict) else {}
    timeout = _safe_int(timeout_seconds if timeout_seconds is not None else fetch.get("timeout_seconds"), 20)
    retry_n = _safe_int(retries if retries is not None else fetch.get("retry"), 2)
    headers = fetch.get("headers_json", {}) if isinstance(fetch.get("headers_json"), dict) else {}
    auth_ref = str(fetch.get("auth_ref") or "").strip()
    if auth_ref:
        token = os.environ.get(auth_ref, "").strip()
        if token:
            headers = dict(headers)
            if "Authorization" not in headers:
                headers["Authorization"] = token if (" " in token) else f"Bearer {token}"
    h = {"User-Agent": "Mozilla/5.0 CodexIVD/1.0"}
    h.update({str(k): str(v) for k, v in headers.items()})

    out: dict[str, Any] = {
        "source_id": sid,
        "id": sid,
        "name": str(source.get("name", "")),
        "connector": connector,
        "fetcher": connector,
        "enabled": bool(source.get("enabled", True)),
        "url": url,
        "ok": False,
        "status": "fail",
        "items_count": 0,
        "samples": [],
        "entries": [],
        "errors": [],
        "sample": [],
        "http_status": None,
        "error": None,
        "error_type": "",
        "error_message": "",
        "duration_ms": 0,
        "discovered_feed_url": "",
        "discovered_child_feeds": [],
    }

    rss_discovery_enabled = _env_bool("SOURCES_RSS_DISCOVERY_ENABLED", True)
    index_discovery_enabled = _env_bool("SOURCES_INDEX_DISCOVERY_ENABLED", True)
    html_fallback_enabled = _env_bool("SOURCES_HTML_FALLBACK_ENABLED", True)

    try:
        if connector == "rss":
            res = _fetch_url_with_retry(url, h, timeout, retry_n)
            out["http_status"] = res.get("http_status")
            if not res.get("ok"):
                out["error_type"] = str(res.get("error_type") or "network_error")
                out["error_message"] = str(res.get("error_message") or "request failed")
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
                return out

            data = bytes(res.get("data") or b"")
            samples = _rss_entries_from_bytes(data, limit)
            if not samples and rss_discovery_enabled:
                html = data.decode("utf-8", errors="ignore")
                feeds = _extract_feed_links_from_html(html, url, same_host_only=True)
                if not feeds:
                    parsed = urlparse(url)
                    if parsed.scheme and parsed.netloc:
                        base = f"{parsed.scheme}://{parsed.netloc}"
                        for p in ("/rss", "/rss.xml", "/feed", "/atom.xml"):
                            candidate = base + p
                            if _is_valid_url(candidate):
                                feeds.append({"url": candidate, "name": p})
                chosen = str(feeds[0]["url"]) if feeds else ""
                out["discovered_feed_url"] = chosen
                if chosen:
                    r2 = _fetch_url_with_retry(chosen, h, timeout, retry_n)
                    if r2.get("ok"):
                        out["http_status"] = r2.get("http_status") or out["http_status"]
                        samples = _rss_entries_from_bytes(bytes(r2.get("data") or b""), limit)
                    else:
                        out["error_type"] = str(r2.get("error_type") or "")
                        out["error_message"] = str(r2.get("error_message") or "")
            if not samples and html_fallback_enabled:
                html = data.decode("utf-8", errors="ignore")
                rows, parse_mode = _generic_html_list_entries(
                    html, url, limit=limit, selectors=source.get("selectors", {})
                )
                samples = _normalize_sample_rows(rows, limit)
                if not samples and _is_probable_js_page(html):
                    out["status"] = "skip"
                    out["error_type"] = "js_required"
                    out["error_message"] = "page likely requires JS rendering"
                    out["errors"] = [out["error_message"]]
                    out["error"] = out["error_message"]
                    return out
                if not samples:
                    out["error_type"] = "parse_empty" if parse_mode != "selector_regex" else "selector_miss"
                    out["error_message"] = "no items parsed from rss/html fallback"
                    out["errors"] = [out["error_message"]]
                    out["error"] = out["error_message"]
                    return out
            out["samples"] = samples
            out["entries"] = samples
            out["sample"] = samples
            out["items_count"] = len(samples)
            out["ok"] = bool(samples)
            out["status"] = "ok" if out["ok"] else "fail"
            return out

        if connector == "html":
            res = _fetch_url_with_retry(url, h, timeout, retry_n)
            out["http_status"] = res.get("http_status")
            if not res.get("ok"):
                out["error_type"] = str(res.get("error_type") or "network_error")
                out["error_message"] = str(res.get("error_message") or "request failed")
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
                return out
            html = bytes(res.get("data") or b"").decode("utf-8", errors="ignore")

            tags = [str(x).strip().lower() for x in source.get("tags", [])] if isinstance(source.get("tags"), list) else []
            if index_discovery_enabled and "regulatory" in tags:
                child = _extract_child_feeds(html, url, same_host_only=True)
                out["discovered_child_feeds"] = child
                if child:
                    default_kws = ["medical devices", "devices", "cdrh", "ivd", "guidance", "alert", "news", "update"]
                    policy = source.get("discovery_policy", "pick_by_keywords")
                    chosen = _pick_child_feed(child, policy, keywords=default_kws)
                    out["discovered_feed_url"] = chosen
                    if chosen:
                        r2 = _fetch_url_with_retry(chosen, h, timeout, retry_n)
                        out["http_status"] = r2.get("http_status") or out["http_status"]
                        if r2.get("ok"):
                            samples = _rss_entries_from_bytes(bytes(r2.get("data") or b""), limit)
                            if samples:
                                out["samples"] = samples
                                out["entries"] = samples
                                out["sample"] = samples
                                out["items_count"] = len(samples)
                                out["ok"] = True
                                out["status"] = "ok"
                                return out

            if not html_fallback_enabled:
                out["status"] = "skip"
                out["error_type"] = "html_fallback_disabled"
                out["error_message"] = "html fallback parser disabled"
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
                return out

            rows, parse_mode = _generic_html_list_entries(
                html, url, limit=limit, selectors=source.get("selectors", {})
            )
            samples = _normalize_sample_rows(rows, limit)
            if not samples and _is_probable_js_page(html):
                out["status"] = "skip"
                out["error_type"] = "js_required"
                out["error_message"] = "page likely requires JS rendering"
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
                return out
            if not samples:
                out["error_type"] = "parse_empty" if parse_mode != "selector_regex" else "selector_miss"
                out["error_message"] = "no items parsed from html page"
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
                return out
            out["samples"] = samples
            out["entries"] = samples
            out["sample"] = samples
            out["items_count"] = len(samples)
            out["ok"] = True
            out["status"] = "ok"
            return out

        if connector == "rsshub":
            base = os.environ.get("RSSHUB_BASE_URL", "").strip().rstrip("/")
            route = str(fetch.get("rsshub_route") or source.get("rsshub_route") or "").strip()
            if not base:
                out["status"] = "skip"
                out["error_type"] = "missing_config"
                out["error_message"] = "skipped: missing base url"
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
                return out
            if not route:
                out["error_type"] = "missing_config"
                out["error_message"] = "missing rsshub_route"
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
                return out
            full = f"{base}{route if route.startswith('/') else '/' + route}"
            out["url"] = full
            res = _fetch_url_with_retry(full, h, timeout, retry_n)
            out["http_status"] = res.get("http_status")
            if not res.get("ok"):
                out["error_type"] = str(res.get("error_type") or "network_error")
                out["error_message"] = str(res.get("error_message") or "request failed")
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
                return out
            samples = _rss_entries_from_bytes(bytes(res.get("data") or b""), limit)
            out["samples"] = samples
            out["entries"] = samples
            out["sample"] = samples
            out["items_count"] = len(samples)
            out["ok"] = bool(samples)
            out["status"] = "ok" if out["ok"] else "fail"
            if not out["ok"]:
                out["error_type"] = "parse_empty"
                out["error_message"] = "rsshub feed parsed but empty"
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
            return out

        if connector == "google_news":
            res = _fetch_url_with_retry(url, h, timeout, retry_n)
            out["http_status"] = res.get("http_status")
            if not res.get("ok"):
                out["error_type"] = str(res.get("error_type") or "network_error")
                out["error_message"] = str(res.get("error_message") or "request failed")
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
                return out
            raw_samples = _rss_entries_from_bytes(bytes(res.get("data") or b""), max(1, limit * 5))
            seen: set[tuple[str, str]] = set()
            samples: list[dict[str, str]] = []
            for row in raw_samples:
                t = re.sub(r"\s+", " ", re.sub(r"[^\w\u4e00-\u9fff ]+", " ", str(row.get("title", "")).lower())).strip()
                host = urlparse(str(row.get("url", ""))).netloc.lower()
                key = (host, t)
                if key in seen:
                    continue
                seen.add(key)
                samples.append(row)
                if len(samples) >= max(1, limit):
                    break
            out["samples"] = samples
            out["entries"] = samples
            out["sample"] = samples
            out["items_count"] = len(samples)
            out["ok"] = bool(samples)
            out["status"] = "ok" if out["ok"] else "fail"
            if not out["ok"]:
                out["error_type"] = "parse_empty"
                out["error_message"] = "google_news feed parsed but empty"
                out["errors"] = [out["error_message"]]
                out["error"] = out["error_message"]
            return out

        out["error_type"] = "unsupported_fetcher"
        out["error_message"] = f"unsupported fetcher={connector}"
        out["errors"] = [out["error_message"]]
        out["error"] = out["error_message"]
        return out
    except Exception as e:
        out["error_type"] = "unexpected"
        out["error_message"] = f"{type(e).__name__}: {e}"
        out["errors"] = [out["error_message"]]
        out["error"] = out["error_message"]
        return out
    finally:
        out["duration_ms"] = int((time.monotonic() - t0) * 1000)


def test_source(
    source: dict[str, Any],
    limit: int = 3,
    timeout_seconds: int | None = None,
    retries: int | None = None,
) -> dict[str, Any]:
    """
    Backward-compatible single-source test contract.
    """
    out = fetch_source_entries(
        source,
        limit=max(1, limit),
        timeout_seconds=timeout_seconds,
        retries=retries,
    )
    # backward-compatible status naming used by existing UI.
    if out.get("status") == "ok":
        out["status"] = "success"
    elif out.get("status") == "fail":
        out["status"] = "failed"
    return out


def _classify_failure_reason(row: dict[str, Any]) -> str:
    et = str(row.get("error_type") or "").strip().lower()
    msg = str(row.get("error_message") or row.get("error") or "").lower()
    hs = int(row.get("http_status") or 0)
    if et in {"js_required", "selector_miss", "parse_empty", "timeout", "dns_error"}:
        return et
    if hs == 403 or " 403" in msg:
        return "http_403"
    if hs == 404 or " 404" in msg:
        return "http_404"
    if "timed out" in msg:
        return "timeout"
    if "name or service not known" in msg or "nodename nor servname provided" in msg:
        return "dns_error"
    if "selector" in msg:
        return "selector_miss"
    if "empty" in msg:
        return "parse_empty"
    return et or "unknown"


def _to_markdown_report(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Sources Test Health Report")
    lines.append("")
    lines.append(
        f"- 总数: {summary.get('total',0)} | 成功: {summary.get('ok',0)} | 失败: {summary.get('fail',0)} | 跳过: {summary.get('skip',0)}"
    )
    lines.append("")
    lines.append("## 按 Fetcher 成功率")
    for k, v in (summary.get("by_fetcher") or {}).items():
        total = int(v.get("total", 0))
        ok = int(v.get("ok", 0))
        rate = (ok / total * 100.0) if total else 0.0
        lines.append(f"- {k}: {ok}/{total} ({rate:.1f}%)")
    lines.append("")
    lines.append("## 失败原因 TOP10")
    for x in (summary.get("top_failure_reasons") or [])[:10]:
        lines.append(f"- {x.get('reason')}: {x.get('count')}")
    lines.append("")
    lines.append("## items_count TOP10")
    for x in (summary.get("top_items_count") or [])[:10]:
        lines.append(f"- {x.get('id')}: {x.get('items_count')} ({x.get('fetcher')})")
    lines.append("")
    lines.append("## 需要人工确认")
    for x in (summary.get("manual_review_sources") or []):
        lines.append(f"- {x.get('id')}: {x.get('reason')}")
    lines.append("")
    lines.append("## TOP失败源")
    for x in (summary.get("top_failed_sources") or [])[:10]:
        lines.append(
            f"- {x.get('id')} | {x.get('fetcher')} | http={x.get('http_status')} | reason={x.get('error_type') or x.get('error')}"
        )
    lines.append("")
    return "\n".join(lines)


def run_sources_test_harness(
    project_root: Path,
    *,
    rules_root: Path | None = None,
    enabled_only: bool = True,
    source_ids: list[str] | None = None,
    limit: int = 3,
    max_workers: int = 6,
    timeout_seconds: int = 20,
    retries: int = 2,
) -> dict[str, Any]:
    bundle = load_sources_registry_bundle(project_root, rules_root=rules_root)
    rows = bundle.get("sources", []) if isinstance(bundle, dict) else []
    ids = {str(x).strip() for x in (source_ids or []) if str(x).strip()}
    targets: list[dict[str, Any]] = []
    for s in rows:
        sid = str(s.get("id", "")).strip()
        if not sid:
            continue
        if enabled_only and not bool(s.get("enabled", True)):
            continue
        if ids and sid not in ids:
            continue
        targets.append(s)

    workers = max(1, min(8, int(max_workers or 6)))
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut_map = {
            ex.submit(test_source, src, max(1, limit), timeout_seconds, retries): str(src.get("id", ""))
            for src in targets
        }
        for fut in as_completed(fut_map):
            sid = fut_map[fut]
            try:
                row = fut.result()
            except Exception as e:  # pragma: no cover - defensive
                row = {
                    "id": sid,
                    "source_id": sid,
                    "status": "failed",
                    "ok": False,
                    "http_status": None,
                    "items_count": 0,
                    "samples": [],
                    "duration_ms": 0,
                    "error_type": "unexpected",
                    "error_message": f"{type(e).__name__}: {e}",
                    "error": f"{type(e).__name__}: {e}",
                    "fetcher": "",
                    "enabled": True,
                    "url": "",
                    "name": sid,
                }
            results.append(row)

    ok_n = sum(1 for r in results if bool(r.get("ok")))
    skip_n = sum(1 for r in results if str(r.get("status", "")).lower() in {"skip", "skipped"})
    fail_n = len(results) - ok_n - skip_n

    by_fetcher: dict[str, dict[str, int]] = {}
    reasons: dict[str, int] = {}
    for r in results:
        f = str(r.get("fetcher") or r.get("connector") or "unknown")
        st = by_fetcher.setdefault(f, {"total": 0, "ok": 0, "fail": 0, "skip": 0})
        st["total"] += 1
        if bool(r.get("ok")):
            st["ok"] += 1
        elif str(r.get("status", "")).lower() in {"skip", "skipped"}:
            st["skip"] += 1
        else:
            st["fail"] += 1
            rr = _classify_failure_reason(r)
            reasons[rr] = reasons.get(rr, 0) + 1

    top_failed = [r for r in results if not bool(r.get("ok")) and str(r.get("status", "")).lower() not in {"skip", "skipped"}]
    top_failed.sort(key=lambda x: int(x.get("duration_ms") or 0), reverse=True)
    top_items = sorted(results, key=lambda x: int(x.get("items_count") or 0), reverse=True)

    manual: list[dict[str, str]] = []
    for r in results:
        er = _classify_failure_reason(r)
        if er in {"js_required", "http_403", "selector_miss"}:
            manual.append({"id": str(r.get("id", "")), "reason": er})

    summary = {
        "total": len(results),
        "ok": ok_n,
        "fail": fail_n,
        "skip": skip_n,
        "coverage_enabled_only": bool(enabled_only),
        "by_fetcher": by_fetcher,
        "top_failure_reasons": [
            {"reason": k, "count": v} for k, v in sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))
        ],
        "top_failed_sources": top_failed[:10],
        "top_items_count": [
            {"id": str(r.get("id", "")), "items_count": int(r.get("items_count") or 0), "fetcher": str(r.get("fetcher", ""))}
            for r in top_items[:10]
        ],
        "manual_review_sources": manual[:20],
    }
    return {
        "ok": True,
        "summary": summary,
        "results": results,
        "markdown": _to_markdown_report(summary),
    }


# Prevent pytest from collecting service function as a test case.
test_source.__test__ = False


def retire_source(
    project_root: Path,
    source_id: str,
    reason: str = "retired via CLI",
    rules_root: Path | None = None,
) -> dict[str, Any]:
    # Prefer unified registry.
    reg_path = _first_existing(_candidate_registry_paths(project_root, rules_root=rules_root))
    now = datetime.now(timezone.utc).isoformat()
    if reg_path is not None:
        doc = _load_yaml(reg_path)
        sources = doc.get("sources", [])
        if not isinstance(sources, list):
            raise SourceRegistryError(f"{reg_path}: sources must be array")
        changed = False
        for s in sources:
            if not isinstance(s, dict):
                continue
            if str(s.get("id", "")).strip() != source_id:
                continue
            s["enabled"] = False
            s["retired_reason"] = reason
            s["retired_at"] = now
            changed = True
        if changed:
            doc["sources"] = sources
            _write_registry_doc(reg_path, doc)
            return {"ok": True, "source_id": source_id, "file": str(reg_path), "reason": reason, "retired_at": now}
        raise SourceRegistryError(f"source_id not found: {source_id}")

    # Legacy fallback split files.
    root = _resolve_rules_root(project_root, rules_root) / "sources"
    for p in sorted(root.glob("*.y*ml")):
        doc = _load_yaml(p)
        sources = doc.get("sources", [])
        if not isinstance(sources, list):
            continue
        changed = False
        for s in sources:
            if not isinstance(s, dict):
                continue
            if str(s.get("id", "")) != source_id:
                continue
            s["enabled"] = False
            s["retired_reason"] = reason
            s["retired_at"] = now
            changed = True
        if changed:
            p.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")
            return {"ok": True, "source_id": source_id, "file": str(p), "reason": reason, "retired_at": now}
    raise SourceRegistryError(f"source_id not found: {source_id}")


def merge_registry_sources(
    project_root: Path,
    incoming_sources: list[dict[str, Any]],
    incoming_groups: dict[str, list[str]] | None = None,
    rules_root: Path | None = None,
) -> dict[str, Any]:
    """
    Merge sources with de-dup rules:
    1) same URL -> merge
    2) same name + same hostname -> merge
    Keep existing id when conflict.
    """
    reg_path = _registry_path(project_root, rules_root=rules_root)
    doc: dict[str, Any]
    if reg_path.exists():
        doc = _load_yaml(reg_path)
    else:
        doc = {"version": 1, "sources": [], "groups": {}}
    existing = doc.get("sources", [])
    if not isinstance(existing, list):
        existing = []
    normalized_existing = [_normalize_source(s, reg_path) for s in existing if isinstance(s, dict)]
    merged = list(normalized_existing)

    def _host(u: str) -> str:
        return urlparse(str(u or "")).netloc.lower().strip()

    def _tier_rank(v: str) -> int:
        return {"A": 3, "B": 2, "C": 1}.get(str(v or "").upper(), 0)

    def _merge_row(base: dict[str, Any], inc: dict[str, Any]) -> dict[str, Any]:
        out = dict(base)
        if _tier_rank(inc.get("trust_tier")) > _tier_rank(base.get("trust_tier")):
            out["trust_tier"] = inc.get("trust_tier")
        out["tags"] = sorted(set([str(x) for x in (base.get("tags", []) or [])] + [str(x) for x in (inc.get("tags", []) or [])]))
        out["enabled"] = bool(base.get("enabled", False) or inc.get("enabled", False))
        n1 = str(base.get("notes", "")).strip()
        n2 = str(inc.get("notes", "")).strip()
        if n2 and n2 not in n1:
            out["notes"] = (n1 + " | " + n2).strip(" |")
        if not str(out.get("url", "")).strip() and str(inc.get("url", "")).strip():
            out["url"] = inc.get("url")
        if str(inc.get("fetcher", "")).strip():
            out["fetcher"] = _canonical_fetcher(str(inc.get("fetcher")))
            out["connector"] = "web" if out["fetcher"] == "html" else out["fetcher"]
        if isinstance(inc.get("fetch"), dict):
            out["fetch"] = {**(out.get("fetch", {}) if isinstance(out.get("fetch"), dict) else {}), **inc.get("fetch", {})}
        return out

    merged_map: dict[str, dict[str, Any]] = {str(s.get("id")): s for s in merged if str(s.get("id", "")).strip()}
    for inc0 in incoming_sources:
        if not isinstance(inc0, dict):
            continue
        inc = _normalize_source(inc0, reg_path)
        inc_id = str(inc.get("id", "")).strip()
        if not inc_id or inc_id.startswith("ref_"):
            continue
        match_id: str | None = None
        inc_url = str(inc.get("url", "")).strip()
        inc_name = str(inc.get("name", "")).strip().lower()
        inc_host = _host(inc_url)
        for eid, row in merged_map.items():
            eurl = str(row.get("url", "")).strip()
            if inc_url and eurl and inc_url == eurl:
                match_id = eid
                break
            ename = str(row.get("name", "")).strip().lower()
            ehost = _host(eurl)
            if inc_name and ename == inc_name and inc_host and inc_host == ehost:
                match_id = eid
                break
        if match_id:
            merged_map[match_id] = _merge_row(merged_map[match_id], inc)
        elif inc_id in merged_map:
            merged_map[inc_id] = _merge_row(merged_map[inc_id], inc)
        else:
            merged_map[inc_id] = inc

    out_sources = sorted(merged_map.values(), key=lambda x: str(x.get("id", "")))
    groups = doc.get("groups", {}) if isinstance(doc.get("groups"), dict) else {}
    if incoming_groups:
        for g, ids in incoming_groups.items():
            gg = str(g).strip()
            if not gg:
                continue
            cur = set(str(x).strip() for x in groups.get(gg, []) if str(x).strip())
            cur.update(str(x).strip() for x in ids if str(x).strip() and not str(x).strip().startswith("ref_"))
            groups[gg] = sorted(cur)

    valid_ids = {str(s.get("id", "")).strip() for s in out_sources}
    for g, ids in list(groups.items()):
        if not isinstance(ids, list):
            groups[g] = []
            continue
        groups[g] = [str(x).strip() for x in ids if str(x).strip() in valid_ids]

    doc["version"] = doc.get("version") or 1
    doc["sources"] = out_sources
    doc["groups"] = groups
    _write_registry_doc(reg_path, doc)
    return {"ok": True, "registry_file": str(reg_path), "source_count": len(out_sources), "group_count": len(groups)}
