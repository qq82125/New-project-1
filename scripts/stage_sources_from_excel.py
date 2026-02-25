#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import time
import zipfile
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
import xml.etree.ElementTree as ET

import yaml


TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "igshid",
    "spm",
}


def _col_to_index(col_letters: str) -> int:
    n = 0
    for ch in col_letters:
        n = n * 26 + (ord(ch.upper()) - ord("A") + 1)
    return n - 1


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        node = cell.find(".//{*}t")
        return (node.text or "").strip() if node is not None else ""
    v = cell.find("{*}v")
    if v is None or v.text is None:
        return ""
    raw = v.text.strip()
    if cell_type == "s":
        try:
            idx = int(raw)
            return shared_strings[idx].strip()
        except Exception:
            return ""
    return raw


def _load_sheet_xml(xlsx_path: Path, sheet_name: str) -> tuple[list[str], list[dict[str, str]]]:
    ns_main = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    ns_rel = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
    with zipfile.ZipFile(xlsx_path, "r") as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            sst = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in sst.findall(".//m:si", ns_main):
                parts = []
                for t in si.findall(".//m:t", ns_main):
                    parts.append(t.text or "")
                shared_strings.append("".join(parts))

        wb = ET.fromstring(zf.read("xl/workbook.xml"))
        rid = None
        for sheet in wb.findall(".//m:sheet", ns_main):
            name = sheet.attrib.get("name", "")
            if name == sheet_name:
                rid = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
                break
        if not rid:
            raise RuntimeError(f"sheet not found: {sheet_name}")

        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        target = None
        for rel in rels.findall(".//r:Relationship", ns_rel):
            if rel.attrib.get("Id") == rid:
                target = rel.attrib.get("Target", "")
                break
        if not target:
            raise RuntimeError(f"sheet relationship missing: {sheet_name}")
        sheet_xml = f"xl/{target.lstrip('/')}"
        root = ET.fromstring(zf.read(sheet_xml))

        rows_cells: list[dict[int, str]] = []
        for row in root.findall(".//m:sheetData/m:row", ns_main):
            row_map: dict[int, str] = {}
            for c in row.findall("m:c", ns_main):
                cref = c.attrib.get("r", "")
                m = re.match(r"([A-Z]+)\d+", cref)
                if not m:
                    continue
                idx = _col_to_index(m.group(1))
                row_map[idx] = _cell_text(c, shared_strings)
            if row_map:
                rows_cells.append(row_map)

    if not rows_cells:
        return [], []
    header_row = rows_cells[0]
    max_col = max(header_row.keys())
    headers = [header_row.get(i, "").strip() for i in range(max_col + 1)]
    data_rows: list[dict[str, str]] = []
    for row_map in rows_cells[1:]:
        row: dict[str, str] = {}
        max_idx = max(max_col, max(row_map.keys()))
        for i in range(max_idx + 1):
            key = headers[i] if i < len(headers) else ""
            if not key:
                continue
            row[key] = row_map.get(i, "").strip()
        if any(str(v).strip() for v in row.values()):
            data_rows.append(row)
    return headers, data_rows


def _read_excel_rows(excel_path: Path, sheet_name: str) -> list[dict[str, str]]:
    return _load_sheet_xml(excel_path, sheet_name)[1]


def _slug(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower())
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def _first_non_empty(row: dict[str, str], aliases: list[str]) -> str:
    row_norm = {re.sub(r"\s+", "", k).lower(): v for k, v in row.items()}
    for a in aliases:
        key = re.sub(r"\s+", "", a).lower()
        if key in row_norm and str(row_norm[key]).strip():
            return str(row_norm[key]).strip()
    return ""


def normalize_url(url: str, fetcher: str) -> str:
    s = (url or "").strip()
    if not s:
        return ""
    try:
        p = urlsplit(s)
    except Exception:
        return s
    scheme = (p.scheme or "https").lower()
    netloc = p.netloc.lower()
    path = p.path or "/"
    if path != "/":
        path = path.rstrip("/")
    query_pairs = parse_qsl(p.query, keep_blank_values=True)

    filtered: list[tuple[str, str]] = []
    if fetcher == "google_news" or "news.google.com/rss" in s:
        keep = {"q", "hl", "gl", "ceid"}
        for k, v in query_pairs:
            if k in keep:
                filtered.append((k, v))
    else:
        for k, v in query_pairs:
            kl = k.lower()
            if kl.startswith("utm_"):
                continue
            if kl in TRACKING_QUERY_KEYS:
                continue
            filtered.append((k, v))
    filtered.sort(key=lambda x: (x[0], x[1]))
    q = urlencode(filtered, doseq=True)
    return urlunsplit((scheme, netloc, path, q, ""))


def _domain(url: str) -> str:
    try:
        host = urlsplit(url).netloc.lower()
    except Exception:
        host = ""
    host = re.sub(r"^www\.", "", host)
    return host


def infer_fetcher(url: str) -> str:
    u = (url or "").lower()
    if "news.google.com/rss" in u:
        return "google_news"
    return "rss"


def infer_region(country_or_region: str, category: str) -> str:
    text = f"{country_or_region} {category}".lower()
    if "欧盟" in text or "欧洲" in text or "eu" in text:
        return "欧洲"
    if "美国" in text or "北美" in text or "us" in text or "usa" in text:
        return "北美"
    if "中国" in text or "cn" in text:
        return "中国"
    if "全球" in text or "global" in text or "international" in text:
        return "Global"
    return "Global"


def infer_tags(fetcher: str, category: str, source_type: str, country_or_region: str) -> list[str]:
    text = f"{category} {source_type} {country_or_region}".lower()
    tags: list[str] = []
    if fetcher == "google_news":
        tags = ["aggregator", "google_news", "ivd"]
        if "中国" in text or "cn" in text:
            tags.append("cn")
        return tags

    if ("监管" in text) or ("regulatory" in text) or ("官方" in text):
        tags = ["regulatory"]
    elif ("媒体" in text) or ("news" in text) or ("资讯" in text):
        tags = ["media"]
    elif ("预印本" in text) or ("preprint" in text):
        tags = ["evidence", "preprint"]
    elif ("期刊" in text) or ("journal" in text) or ("科研" in text) or ("literature" in text):
        tags = ["evidence", "journal"]
    else:
        tags = ["media"]

    if "美国" in text or "北美" in text or " us" in text:
        tags.extend(["us", "en"])
    elif "欧盟" in text or "欧洲" in text or " eu" in text:
        tags.extend(["eu", "en"])
    elif "中国" in text or " cn" in text:
        tags.extend(["cn", "zh"])
    else:
        tags.append("en")
    return list(dict.fromkeys(tags))


def infer_tier_priority(tags: list[str], category: str, source_type: str, fetcher: str) -> tuple[str, int]:
    text = f"{category} {source_type}".lower()
    if fetcher == "google_news" or "aggregator" in tags:
        return "C", 18
    if "regulatory" in tags or "官方" in text or "监管" in text:
        return "A", 85
    if "preprint" in tags:
        return "C", 30
    if "evidence" in tags:
        return "B", 40
    if "media" in tags:
        return "B", 60
    return "B", 55


def build_id(
    *,
    tags: list[str],
    url: str,
    name: str,
    used_ids: set[str],
) -> str:
    primary = "source"
    for tag in tags:
        if tag in {"regulatory", "media", "evidence", "aggregator", "google_news"}:
            primary = tag
            break
    dom = _slug(_domain(url))
    name_slug = _slug(name)
    if not name_slug:
        name_slug = "src"
    base = f"{primary}-{dom}-{name_slug}".strip("-")
    base = re.sub(r"-{2,}", "-", base)
    base = base[:60].rstrip("-")
    if not base:
        base = "source-generated"
    if base not in used_ids:
        used_ids.add(base)
        return base
    suffix = hashlib.sha1(f"{url}|{name}".encode("utf-8")).hexdigest()[:4]
    cand = f"{base[:55].rstrip('-')}-{suffix}"
    idx = 1
    while cand in used_ids:
        idx += 1
        cand = f"{base[:53].rstrip('-')}-{suffix}{idx}"
    used_ids.add(cand)
    return cand


def load_registry(registry_path: Path) -> tuple[dict, list[dict], set[str], set[str]]:
    data = yaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
    sources = data.get("sources", []) if isinstance(data, dict) else []
    urls: set[str] = set()
    ids: set[str] = set()
    for s in sources:
        if not isinstance(s, dict):
            continue
        u = normalize_url(str(s.get("url", "")), str(s.get("fetcher", "rss")))
        if u:
            urls.add(u)
        sid = str(s.get("id", "")).strip()
        if sid:
            ids.add(sid)
    return data, sources, urls, ids


def append_entries_before_groups(registry_path: Path, entries: list[dict], backup_path: Path) -> None:
    raw = registry_path.read_text(encoding="utf-8")
    marker = "\ngroups:\n"
    insert_at = raw.find(marker)
    if insert_at < 0:
        marker = "\ngroups:"
        insert_at = raw.find(marker)
    patch_text = yaml.safe_dump(entries, allow_unicode=True, sort_keys=False).rstrip() + "\n"
    if insert_at >= 0:
        new_raw = raw[:insert_at].rstrip() + "\n" + patch_text + raw[insert_at:]
    else:
        new_raw = raw.rstrip() + "\n" + patch_text
    shutil.copy2(registry_path, backup_path)
    registry_path.write_text(new_raw, encoding="utf-8")


def stage(
    *,
    excel: Path,
    registry: Path,
    out_yaml: Path,
    out_report: Path,
    sheet_name: str,
    apply: bool,
) -> int:
    rows = _read_excel_rows(excel, sheet_name=sheet_name)
    _, _, existing_urls, used_ids = load_registry(registry)

    added_entries: list[dict] = []
    report: dict[str, object] = {
        "excel": str(excel),
        "sheet": sheet_name,
        "registry": str(registry),
        "rows_total": len(rows),
        "added_count": 0,
        "duplicate_count": 0,
        "error_count": 0,
        "added_ids": [],
        "duplicates": [],
        "errors": [],
        "added_preview": [],
    }

    seen_new_urls: set[str] = set()
    for idx, row in enumerate(rows, start=2):
        try:
            category = _first_non_empty(row, ["类别", "分类", "category"])
            name = _first_non_empty(row, ["名称", "name"])
            country = _first_non_empty(row, ["国家/地区", "国家地区", "国家", "地区", "country/region"])
            url = _first_non_empty(row, ["RSS链接", "rss链接", "RSS 链接", "url", "链接"])
            source_type = _first_non_empty(row, ["来源类型", "source_type", "来源"])
            note = _first_non_empty(row, ["备注", "notes", "说明"])
            if not name or not url:
                report["errors"].append(
                    {
                        "row": idx,
                        "error": "missing required name/url",
                        "name": name,
                        "url": url,
                    }
                )
                continue

            fetcher = infer_fetcher(url)
            url_norm = normalize_url(url, fetcher)
            if not url_norm:
                report["errors"].append({"row": idx, "error": "invalid url", "name": name, "url": url})
                continue
            if url_norm in existing_urls or url_norm in seen_new_urls:
                report["duplicates"].append(
                    {
                        "row": idx,
                        "name": name,
                        "url": url,
                        "url_norm": url_norm,
                        "reason": "url_norm_exists",
                    }
                )
                continue

            tags = infer_tags(fetcher, category, source_type, country)
            region = infer_region(country, category)
            trust_tier, priority = infer_tier_priority(tags, category, source_type, fetcher)
            sid = build_id(tags=tags, url=url_norm, name=name, used_ids=used_ids)

            notes = note or ""
            if fetcher == "google_news" and "噪音高" not in notes:
                notes = (notes + "；" if notes else "") + "噪音高，仅兜底"

            entry = {
                "tags": tags,
                "region": region,
                "url": url_norm,
                "name": name.strip(),
                "id": sid,
                "fetcher": fetcher,
                "enabled": False,
                "notes": notes,
                "trust_tier": trust_tier,
                "priority": int(priority),
            }
            added_entries.append(entry)
            seen_new_urls.add(url_norm)
            report["added_ids"].append(sid)
            report["added_preview"].append(
                {
                    "id": sid,
                    "name": entry["name"],
                    "fetcher": fetcher,
                    "url": url_norm,
                    "tags": tags,
                }
            )
        except Exception as e:
            report["errors"].append(
                {
                    "row": idx,
                    "error": f"{type(e).__name__}: {e}",
                    "row_data": row,
                }
            )

    report["added_count"] = len(added_entries)
    report["duplicate_count"] = len(report["duplicates"])
    report["error_count"] = len(report["errors"])

    out_yaml.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    out_yaml.write_text(
        yaml.safe_dump({"sources": added_entries}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    out_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if apply and added_entries:
        ts = time.strftime("%Y%m%d-%H%M%S")
        backup = registry.with_name(registry.name + f".bak.{ts}")
        append_entries_before_groups(registry, added_entries, backup)
        report["apply"] = {
            "applied": True,
            "backup": str(backup),
            "applied_count": len(added_entries),
        }
        out_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    elif apply:
        report["apply"] = {"applied": False, "reason": "no new entries"}
        out_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({"ok": True, "added": len(added_entries), "out_yaml": str(out_yaml), "out_report": str(out_report)}, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Stage sources from Excel into sources_registry patch")
    p.add_argument("--excel", required=True, help="Path to Excel .xlsx file")
    p.add_argument("--sheet", default="Sheet1", help="Sheet name")
    p.add_argument("--registry", required=True, help="Path to sources_registry.v1.yaml")
    p.add_argument("--out-yaml", required=True, help="Output staging patch yaml")
    p.add_argument("--out-report", required=True, help="Output report json")
    p.add_argument("--apply", action="store_true", help="Apply patch into registry with backup")
    args = p.parse_args(argv)

    excel = Path(args.excel)
    if not excel.exists():
        print(json.dumps({"ok": False, "error": f"excel not found: {excel}"}, ensure_ascii=False, indent=2))
        return 2
    registry = Path(args.registry)
    if not registry.exists():
        print(json.dumps({"ok": False, "error": f"registry not found: {registry}"}, ensure_ascii=False, indent=2))
        return 2

    try:
        return stage(
            excel=excel,
            registry=registry,
            out_yaml=Path(args.out_yaml),
            out_report=Path(args.out_report),
            sheet_name=args.sheet,
            apply=bool(args.apply),
        )
    except Exception as e:
        print(json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main(None))
