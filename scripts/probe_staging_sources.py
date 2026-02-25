#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any
import sys

import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.source_registry import load_sources_registry_bundle, run_sources_test_harness


def classify_error(row: dict[str, Any]) -> str:
    if bool(row.get("ok")):
        if int(row.get("items_count") or 0) > 0:
            return "OK"
        return "EMPTY_FEED"

    hs = int(row.get("http_status") or 0)
    et = str(row.get("error_type") or "").strip().lower()
    msg = str(row.get("error_message") or row.get("error") or "").lower()

    if hs:
        if hs == 404:
            return "NOT_FOUND"
        if hs == 403:
            return "CF_BLOCK"
        if hs in {408, 429}:
            return "TIMEOUT"
        return f"HTTP_{hs}"

    if et in {"dns_error"} or "nodename nor servname provided" in msg or "name or service not known" in msg:
        return "DNS_ERROR"
    if et in {"timeout"} or "timed out" in msg:
        return "TIMEOUT"
    if et in {"empty_feed", "parse_empty"}:
        return "EMPTY_FEED"
    if et in {"js_required", "http_403"}:
        return "CF_BLOCK"
    if et in {"selector_miss", "parse_error", "unsupported_fetcher"}:
        return "PARSE_ERROR"
    if "403" in msg:
        return "CF_BLOCK"
    if "404" in msg:
        return "NOT_FOUND"
    return "UNKNOWN"


def to_sample_titles(row: dict[str, Any], n: int = 3) -> list[str]:
    out: list[str] = []
    for s in (row.get("samples") or [])[:n]:
        if isinstance(s, dict):
            t = str(s.get("title", "")).strip()
            if t:
                out.append(t)
    return out


def load_added_ids(report_path: Path) -> list[str]:
    data = json.loads(report_path.read_text(encoding="utf-8"))
    ids = data.get("added_ids", []) if isinstance(data, dict) else []
    return [str(x).strip() for x in ids if str(x).strip()]


def load_registry_sources(registry_path: Path) -> dict[str, dict[str, Any]]:
    data = yaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
    sources = data.get("sources", []) if isinstance(data, dict) else []
    out: dict[str, dict[str, Any]] = {}
    for s in sources:
        if isinstance(s, dict) and s.get("id"):
            out[str(s["id"])] = s
    return out


def pick_batches(rows: list[dict[str, Any]], by_id: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    batch1: list[dict[str, Any]] = []
    batch2: list[dict[str, Any]] = []
    batch3: list[dict[str, Any]] = []
    media_ok: list[dict[str, Any]] = []

    for r in rows:
        sid = str(r.get("source_id", "")).strip()
        src = by_id.get(sid, {})
        tags = [str(t) for t in (src.get("tags") or [])]
        ec = str(r.get("error_class", "UNKNOWN"))
        items_count = int(r.get("items_count") or 0)
        if ec != "OK" or items_count <= 0:
            continue

        if "aggregator" in tags or "google_news" in tags:
            continue
        if "regulatory" in tags:
            batch1.append({"id": sid, "enabled": True})
            continue
        if "media" in tags:
            media_ok.append({"id": sid, "enabled": True})
            continue
        if "evidence" in tags or "journal" in tags or "preprint" in tags:
            batch3.append({"id": sid, "enabled": True})
            continue

    media_ok = sorted(media_ok, key=lambda x: x["id"])
    batch1.extend(media_ok[:8])
    batch2.extend(media_ok[8:])

    # De-duplicate ids between batches
    seen: set[str] = set()
    for b in (batch1, batch2, batch3):
        uniq = []
        for x in b:
            sid = x["id"]
            if sid in seen:
                continue
            seen.add(sid)
            uniq.append(x)
        b[:] = uniq

    return {"batch1": batch1, "batch2": batch2, "batch3": batch3}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Probe staging sources and build enable batches")
    p.add_argument("--registry", required=True, help="Path to sources_registry.v1.yaml")
    p.add_argument("--staging-report", required=True, help="Path to staging_sources_report.json")
    p.add_argument("--out-report", required=True, help="Output source probe report JSON path")
    p.add_argument("--out-dir", default="artifacts", help="Directory for enable batch yaml files")
    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--timeout-seconds", type=int, default=20)
    p.add_argument("--retries", type=int, default=2)
    args = p.parse_args(argv)

    registry = Path(args.registry)
    staging_report = Path(args.staging_report)
    out_report = Path(args.out_report)
    out_dir = Path(args.out_dir)
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    source_ids = load_added_ids(staging_report)
    if not source_ids:
        print(json.dumps({"ok": False, "error": "no added_ids in staging report"}, ensure_ascii=False, indent=2))
        return 2

    # load bundle through service to keep behavior aligned with runtime
    bundle = load_sources_registry_bundle(registry.parent.parent, rules_root=registry.parent)
    _ = bundle
    out = run_sources_test_harness(
        registry.parent.parent,
        rules_root=registry.parent,
        enabled_only=False,
        source_ids=source_ids,
        limit=max(1, int(args.limit)),
        max_workers=max(1, int(args.workers)),
        timeout_seconds=max(3, int(args.timeout_seconds)),
        retries=max(0, int(args.retries)),
    )

    by_id = load_registry_sources(registry)
    normalized_rows: list[dict[str, Any]] = []
    for row in out.get("results", []):
        sid = str(row.get("id") or row.get("source_id") or "").strip()
        src = by_id.get(sid, {})
        normalized_rows.append(
            {
                "source_id": sid,
                "name": str(row.get("name", "")),
                "fetcher": str(row.get("fetcher", "")),
                "url": str(row.get("url", "")),
                "status_code": int(row.get("http_status") or 0),
                "content_type": str(row.get("content_type", "")),
                "items_count": int(row.get("items_count") or 0),
                "error_class": classify_error(row),
                "elapsed_ms": int(row.get("duration_ms") or 0),
                "sample_titles": to_sample_titles(row),
                "tags": [str(x) for x in (src.get("tags") or [])],
                "error_type": str(row.get("error_type", "")),
                "error_message": str(row.get("error_message") or row.get("error") or ""),
            }
        )

    batches = pick_batches(normalized_rows, by_id)
    ts = time.strftime("%Y%m%d-%H%M%S")
    b1 = out_dir / "enable_batch_1.yaml"
    b2 = out_dir / "enable_batch_2.yaml"
    b3 = out_dir / "enable_batch_3.yaml"
    b1.write_text(yaml.safe_dump({"updates": batches["batch1"]}, allow_unicode=True, sort_keys=False), encoding="utf-8")
    b2.write_text(yaml.safe_dump({"updates": batches["batch2"]}, allow_unicode=True, sort_keys=False), encoding="utf-8")
    b3.write_text(yaml.safe_dump({"updates": batches["batch3"]}, allow_unicode=True, sort_keys=False), encoding="utf-8")

    # Notes append suggestions for failed rows.
    failed_notes = []
    for r in normalized_rows:
        if r["error_class"] != "OK":
            failed_notes.append({"id": r["source_id"], "notes_append": f"probe:{r['error_class']}"})

    payload = {
        "ok": True,
        "generated_at": ts,
        "source_count": len(source_ids),
        "summary": out.get("summary", {}),
        "rows": normalized_rows,
        "enable_batches": {
            "batch1": str(b1),
            "batch2": str(b2),
            "batch3": str(b3),
            "batch1_count": len(batches["batch1"]),
            "batch2_count": len(batches["batch2"]),
            "batch3_count": len(batches["batch3"]),
        },
        "notes_append_suggestions": failed_notes,
    }
    out_report.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "out_report": str(out_report), "batch1": len(batches["batch1"]), "batch2": len(batches["batch2"]), "batch3": len(batches["batch3"])}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(None))
