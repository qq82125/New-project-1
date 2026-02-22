from __future__ import annotations

import json
from glob import glob
from pathlib import Path
from typing import Any


def find_latest_json(glob_pattern: str) -> str | None:
    candidates = [Path(p) for p in glob(glob_pattern, recursive=True)]
    files = [p for p in candidates if p.is_file()]
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(files[0])


def safe_load_json(path: str | None) -> tuple[dict[str, Any] | None, str | None]:
    if not path:
        return None, "file missing"
    p = Path(path)
    if not p.exists():
        return None, f"file missing: {path}"
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return None, f"json root is not object: {path}"
        return obj, None
    except Exception as exc:
        return None, f"parse failed: {path}: {exc}"


def _pick_timestamp(obj: dict[str, Any]) -> str:
    for k in ("generated_at", "started_at", "ts", "date"):
        v = obj.get(k)
        if v is not None and str(v).strip():
            return str(v)
    return ""


def _top_n_map(raw: Any, limit: int = 5) -> list[dict[str, Any]]:
    if not isinstance(raw, dict):
        return []
    rows = [{"key": str(k), "count": int(v)} for k, v in raw.items() if isinstance(v, (int, float)) and not isinstance(v, bool)]
    rows.sort(key=lambda x: int(x.get("count", 0)), reverse=True)
    return rows[: max(1, int(limit or 5))]


def normalize_metrics(obj: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"timestamp": _pick_timestamp(obj)}
    analysis = obj.get("analysis", {}) if isinstance(obj.get("analysis"), dict) else {}
    quality_pack = obj.get("quality_pack", {}) if isinstance(obj.get("quality_pack"), dict) else {}
    summary = obj.get("summary", {}) if isinstance(obj.get("summary"), dict) else {}

    if analysis:
        out.update(
            {
                "run_id": obj.get("run_id"),
                "status": obj.get("status"),
                "profile": obj.get("profile"),
                "items_before_dedupe": analysis.get("items_before_dedupe"),
                "items_after_dedupe": analysis.get("items_after_dedupe"),
                "analysis_cache_hit": analysis.get("analysis_cache_hit"),
                "analysis_cache_miss": analysis.get("analysis_cache_miss"),
                "analysis_degraded_count": analysis.get("analysis_degraded_count"),
                "evidence_missing_core_count": analysis.get("evidence_missing_core_count"),
                "opportunity_signals_written": analysis.get("opportunity_signals_written"),
                "opportunity_signals_deduped": analysis.get("opportunity_signals_deduped"),
            }
        )
        unknown_rates = {}
        for k, v in analysis.items():
            if k.startswith("unknown_"):
                unknown_rates[k] = v
        if unknown_rates:
            out["unknown_metrics"] = unknown_rates
        return out

    if "assets_written_count" in obj or str(obj.get("purpose", "")) == "collect":
        out.update(
            {
                "run_id": obj.get("run_id"),
                "profile": obj.get("profile"),
                "assets_written_count": obj.get("assets_written_count"),
                "sources_failed_count": obj.get("sources_failed_count"),
                "dropped_static_or_listing_count": obj.get("dropped_static_or_listing_count"),
                "dropped_static_or_listing_top_domains": obj.get("dropped_static_or_listing_top_domains", []),
                "dropped_by_source_policy_count": obj.get("dropped_by_source_policy_count"),
                "top_domains": obj.get("dropped_static_or_listing_top_domains", []),
            }
        )
        return out

    if "checks" in obj and "quality_pack" in obj:
        checks = obj.get("checks", [])
        checks_passed = int(summary.get("pass", 0))
        if isinstance(checks, list) and checks and checks_passed == 0:
            checks_passed = sum(1 for c in checks if isinstance(c, dict) and str(c.get("status", "")).lower() == "pass")
        out.update(
            {
                "ok": obj.get("ok"),
                "as_of": obj.get("as_of"),
                "checks_total": int(summary.get("total", len(checks) if isinstance(checks, list) else 0)),
                "checks_passed": checks_passed,
                "checks_failed": int(summary.get("fail", 0)),
                "quality_pack_selected_total": quality_pack.get("selected_total"),
            }
        )
        return out

    if "totals" in obj and "per_source" in obj:
        per_source = obj.get("per_source", [])
        per_source_rows: list[dict[str, Any]] = []
        if isinstance(per_source, list):
            for row in per_source[:5]:
                if not isinstance(row, dict):
                    continue
                per_source_rows.append(
                    {
                        "source_id": row.get("source_id"),
                        "status": row.get("status"),
                        "error_kind": row.get("error_kind"),
                        "enabled": row.get("enabled"),
                        "written_count": row.get("written_count"),
                        "dropped_count_by_reason": row.get("dropped_count_by_reason", {}),
                    }
                )
        out.update(
            {
                "ok": obj.get("ok"),
                "totals": obj.get("totals", {}),
                "by_error_kind": _top_n_map(obj.get("by_error_kind", {}), limit=5),
                "per_source": per_source_rows,
            }
        )
        return out

    # Generic fallback for unknown payloads
    out.update(
        {
            "run_id": obj.get("run_id"),
            "status": obj.get("status"),
            "ok": obj.get("ok"),
        }
    )
    return out


def evaluate_health(metrics: dict[str, Any]) -> dict[str, Any]:
    triggered: list[dict[str, Any]] = []

    digest = metrics.get("digest", {}) if isinstance(metrics.get("digest"), dict) else {}
    collect = metrics.get("collect", {}) if isinstance(metrics.get("collect"), dict) else {}
    acceptance = metrics.get("acceptance", {}) if isinstance(metrics.get("acceptance"), dict) else {}
    probe = metrics.get("procurement_probe", {}) if isinstance(metrics.get("procurement_probe"), dict) else {}

    def push(metric: str, value: Any, threshold: Any, level: str) -> None:
        triggered.append({"metric": metric, "value": value, "threshold": threshold, "level": level})

    # 1) unknown_lane_rate
    unknown_lane_rate = digest.get("unknown_metrics", {}).get("unknown_lane_rate")
    if isinstance(unknown_lane_rate, (int, float)):
        if unknown_lane_rate > 0.7:
            push("unknown_lane_rate", float(unknown_lane_rate), 0.7, "red")
        elif unknown_lane_rate > 0.5:
            push("unknown_lane_rate", float(unknown_lane_rate), 0.5, "yellow")

    # 2) degraded_ratio
    degraded_count = digest.get("analysis_degraded_count")
    items_after = digest.get("items_after_dedupe")
    if isinstance(degraded_count, (int, float)) and isinstance(items_after, (int, float)):
        denom = max(int(items_after), 1)
        degraded_ratio = float(degraded_count) / float(denom)
        if degraded_ratio > 0.6:
            push("degraded_ratio", degraded_ratio, 0.6, "red")
        elif degraded_ratio > 0.4:
            push("degraded_ratio", degraded_ratio, 0.4, "yellow")

    # 3) analysis_cache_hit_ratio
    hit = digest.get("analysis_cache_hit")
    miss = digest.get("analysis_cache_miss")
    if isinstance(hit, (int, float)) and isinstance(miss, (int, float)):
        denom = max(int(hit) + int(miss), 1)
        ratio = float(hit) / float(denom)
        if float(hit) == 0:
            push("analysis_cache_hit_ratio", ratio, 0.0, "red")
        elif ratio < 0.2:
            push("analysis_cache_hit_ratio", ratio, 0.2, "yellow")

    # 4) procurement probe
    totals = probe.get("totals", {}) if isinstance(probe.get("totals"), dict) else {}
    if isinstance(totals.get("ok"), (int, float)) and isinstance(totals.get("error"), (int, float)):
        if int(totals.get("ok", 0)) == 0 and int(totals.get("error", 0)) > 0:
            push("procurement_probe_totals", {"ok": int(totals.get("ok", 0)), "error": int(totals.get("error", 0))}, "ok==0 & error>0", "yellow")

    by_error_kind = probe.get("by_error_kind", [])
    if isinstance(by_error_kind, list):
        total_errors = sum(int(x.get("count", 0)) for x in by_error_kind if isinstance(x, dict))
        dns_errors = sum(
            int(x.get("count", 0))
            for x in by_error_kind
            if isinstance(x, dict) and "dns" in str(x.get("key", "")).lower()
        )
        if total_errors > 0 and (float(dns_errors) / float(total_errors)) > 0.5:
            push("procurement_probe_dns_ratio", float(dns_errors) / float(total_errors), 0.5, "yellow")

    per_source = probe.get("per_source", [])
    if isinstance(per_source, list):
        for row in per_source:
            if not isinstance(row, dict):
                continue
            error_kind = str(row.get("error_kind", "")).lower()
            enabled = bool(row.get("enabled"))
            if "needs_api_key" in error_kind and enabled:
                push("procurement_probe_needs_api_key", error_kind, "enabled=true", "yellow")
                break

    # 5) dropped_static_or_listing_count
    dropped_count = collect.get("dropped_static_or_listing_count")
    if isinstance(dropped_count, (int, float)) and int(dropped_count) > 10:
        push("dropped_static_or_listing_count", int(dropped_count), 10, "yellow")

    # 6) acceptance.ok
    if acceptance and acceptance.get("ok") is False:
        push("acceptance.ok", False, True, "red")

    overall = "green"
    if any(str(x.get("level")) == "red" for x in triggered):
        overall = "red"
    elif any(str(x.get("level")) == "yellow" for x in triggered):
        overall = "yellow"

    return {"overall": overall, "rules_triggered": triggered}
