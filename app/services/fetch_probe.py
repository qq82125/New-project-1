from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from app.services.collect_asset_store import CollectAssetStore
from app.services.source_registry import fetch_source_entries, load_sources_registry_bundle


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if not s:
        return default
    return s in {"1", "true", "yes", "y", "on"}


def _load_split_procurement_sources(project_root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    src_root = project_root / "rules" / "sources"
    if not src_root.exists():
        return out
    for p in sorted(src_root.glob("*.y*ml")):
        try:
            doc = yaml.safe_load(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = doc.get("sources", []) if isinstance(doc, dict) else []
        if not isinstance(rows, list):
            continue
        for r in rows:
            if not isinstance(r, dict):
                continue
            sg = str(r.get("source_group", "")).strip().lower()
            tags = {str(x).strip().lower() for x in (r.get("tags", []) if isinstance(r.get("tags"), list) else [])}
            if sg == "procurement" or ("procurement" in tags):
                out.append(dict(r))
    return out


def _select_sources(
    project_root: Path,
    *,
    include_source_ids: list[str],
    include_source_groups: list[str],
    force: bool,
    max_sources: int | None,
) -> list[dict[str, Any]]:
    bundle = load_sources_registry_bundle(project_root, include_deleted=False)
    rows = list(bundle.get("sources", [])) if isinstance(bundle.get("sources", []), list) else []
    by_id: dict[str, dict[str, Any]] = {}
    for r in rows + _load_split_procurement_sources(project_root):
        sid = str(r.get("id", "")).strip()
        if not sid:
            continue
        if sid not in by_id:
            by_id[sid] = dict(r)
    ids = {str(x).strip() for x in include_source_ids if str(x).strip()}
    groups = {str(x).strip().lower() for x in include_source_groups if str(x).strip()}
    if not groups:
        groups = {"procurement"}

    selected: list[dict[str, Any]] = []
    for sid in sorted(by_id.keys()):
        s = dict(by_id[sid])
        sg = str(s.get("source_group", "")).strip().lower()
        tags = {str(x).strip().lower() for x in (s.get("tags", []) if isinstance(s.get("tags"), list) else [])}
        group_hit = (sg in groups) or bool(groups & tags)
        if ids and sid not in ids:
            continue
        if (not ids) and (not group_hit):
            continue
        if (not force) and (not bool(s.get("enabled", True))):
            continue
        selected.append(s)
        if isinstance(max_sources, int) and max_sources > 0 and len(selected) >= int(max_sources):
            break
    return selected


def _classify_error_kind(source: dict[str, Any], result: dict[str, Any]) -> tuple[str, str]:
    et = str(result.get("error_type", "")).strip().lower()
    msg = str(result.get("error_message") or result.get("error") or "").strip().lower()
    status = int(result.get("http_status", 0) or 0)
    url = str(source.get("url", "")).strip()
    mode = str((source.get("fetch", {}) if isinstance(source.get("fetch"), dict) else {}).get("mode", "")).strip().lower()
    if "your_key" in url.lower() or "your_token" in url.lower() or et == "needs_api_key":
        return "needs_api_key", "去 api.data.gov / SAM.gov 申请 key，并在 source 的 auth_ref 或 env 注入"
    if et == "dns_error" or "nodename nor servname" in msg or "name or service not known" in msg:
        return "dns", "检查网络/DNS 或代理配置；必要时改为可访问镜像源"
    if et == "timeout" or "timed out" in msg:
        return "timeout", "提高 timeout/retry 或降低抓取频率"
    if status == 403 or "403" in msg:
        return "http_403", "目标站点拒绝访问；检查 UA、频率或是否需认证"
    if status == 404 or "404" in msg:
        return "http_404", "URL 已失效；更新 source.url 或 link_regex"
    if status == 429:
        return "http_429", "触发限流；拉长 interval 或加退避"
    if status >= 500:
        return "http_5xx", "源站异常；稍后重试并设置熔断"
    if et in {"static_or_listing_page"}:
        return "guard_static_listing", "这是列表/栏目页；改 rss 或 html_list 提链"
    if et in {"not_article", "too_short", "too_few_paragraphs", "no_h1"}:
        return "not_article", "详情页结构不像文章；降低 procurement 的 article_min_text_chars 或修 link_regex"
    if et in {"unsupported_mode"}:
        return "unsupported_mode", "调整 fetch.mode 为 rss/html_article/html_list/api_json 之一"
    if et in {"parse_empty", "selector_miss"}:
        if mode == "rss":
            return "empty_feed", "URL 不是 RSS feed；改用真正 feed URL 或换 html_list"
        return "parse", "检查 list_link_regex 或页面结构，必要时更换 fetch.mode"
    if et in {"parse_error"}:
        return "parse", "接口返回结构异常；检查 JSON 路径和认证参数"
    if et in {"js_required"}:
        return "guard_static_listing", "页面依赖 JS 渲染；建议改 RSS/API 源"
    return "unknown", "查看 error_message 并针对 source URL/模式做定向调整"


def _build_probe_result(source: dict[str, Any], result: dict[str, Any], *, attempted: bool, written_count: int = 0) -> dict[str, Any]:
    error_kind, action = _classify_error_kind(source, result)
    dropped_reasons = result.get("drop_reasons", {}) if isinstance(result.get("drop_reasons"), dict) else {}
    ok_items = int(result.get("items_count", 0) or 0)
    status = "ok"
    if not attempted:
        status = "dropped"
        error_kind = "unknown"
        action = "源被跳过（可能因 disabled 且未 --force）"
    elif not bool(result.get("ok", False)):
        status = "empty" if error_kind in {"empty_feed", "parse"} else "error"
        if int(sum(int(v or 0) for v in dropped_reasons.values())) > 0 and error_kind in {"guard_static_listing", "not_article"}:
            status = "dropped"
    elif ok_items <= 0:
        status = "empty"
    return {
        "source_id": str(source.get("id", "")).strip(),
        "name": str(source.get("name", "")).strip(),
        "source_group": str(source.get("source_group", "")).strip() or "__unknown__",
        "trust_tier": str(source.get("trust_tier", "")).strip() or "C",
        "mode": str((source.get("fetch", {}) if isinstance(source.get("fetch"), dict) else {}).get("mode", "")).strip() or "rss",
        "url": str(source.get("url", "")).strip(),
        "attempted": bool(attempted),
        "status": status,
        "written_count": int(written_count),
        "dropped_count_by_reason": {str(k): int(v or 0) for k, v in dropped_reasons.items()},
        "error_kind": error_kind,
        "http_status": result.get("http_status"),
        "elapsed_ms": int(result.get("duration_ms", 0) or 0),
        "notes": str(result.get("error_message") or result.get("error") or ""),
        "recommended_action": action,
    }


def _render_probe_md(report: dict[str, Any]) -> str:
    lines = [
        "# Procurement Probe Report",
        "",
        f"- generated_at: {report.get('generated_at','')}",
        f"- selected_sources: {report.get('selected_sources',0)}",
        f"- write_assets: {report.get('write_assets',False)}",
        "",
        "## Totals",
        f"- ok: {report.get('totals',{}).get('ok',0)}",
        f"- empty: {report.get('totals',{}).get('empty',0)}",
        f"- dropped: {report.get('totals',{}).get('dropped',0)}",
        f"- error: {report.get('totals',{}).get('error',0)}",
        "",
        "## Error Kind Distribution",
    ]
    by = report.get("by_error_kind", {})
    if isinstance(by, dict):
        for k, v in sorted(by.items(), key=lambda kv: (-int(kv[1]), str(kv[0]))):
            lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Per Source")
    for r in report.get("per_source", []):
        if not isinstance(r, dict):
            continue
        lines.extend(
            [
                f"### {r.get('source_id')} ({r.get('name')})",
                f"- url: {r.get('url')}",
                f"- mode: {r.get('mode')}",
                f"- status: {r.get('status')} | written: {r.get('written_count')} | http_status: {r.get('http_status')}",
                f"- error_kind: {r.get('error_kind')}",
                f"- notes: {r.get('notes')}",
                f"- recommended_action: {r.get('recommended_action')}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def run_procurement_probe(
    *,
    project_root: Path,
    force: bool = True,
    max_sources: int | None = None,
    fetch_limit: int = 5,
    include_source_ids: list[str] | None = None,
    include_source_groups: list[str] | None = None,
    write_assets: bool = False,
    output_dir: str = "artifacts/procurement",
) -> dict[str, Any]:
    include_source_ids = include_source_ids or []
    include_source_groups = include_source_groups or ["procurement"]
    selected = _select_sources(
        project_root,
        include_source_ids=include_source_ids,
        include_source_groups=include_source_groups,
        force=force,
        max_sources=max_sources,
    )
    per: list[dict[str, Any]] = []
    by_error: dict[str, int] = {}
    totals = {"ok": 0, "empty": 0, "dropped": 0, "error": 0}
    collector = CollectAssetStore(project_root, asset_dir="artifacts/collect")
    today = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
    run_id = f"procurement-probe-{today}"

    for s in selected:
        res = fetch_source_entries(
            s,
            limit=max(1, int(fetch_limit or 5)),
            source_guard={
                "enabled": True,
                "enforce_article_only": True,
                "article_min_paragraphs": int((s.get("fetch", {}) if isinstance(s.get("fetch"), dict) else {}).get("article_min_paragraphs", 1) or 1),
                "article_min_text_chars": int((s.get("fetch", {}) if isinstance(s.get("fetch"), dict) else {}).get("article_min_text_chars", 80) or 80),
                "allow_body_fetch_for_rss": False,
            },
        )
        written = 0
        if write_assets and bool(res.get("ok")):
            entries = list(res.get("entries", [])) if isinstance(res.get("entries"), list) else []
            wr = collector.append_items(
                run_id=run_id,
                source_id=str(s.get("id", "")),
                source_name=str(s.get("name", "")),
                source_group="procurement",
                source_trust_tier=str(s.get("trust_tier", "A")),
                items=entries,
            )
            written = int(wr.get("written", 0) or 0)
        row = _build_probe_result(s, res, attempted=True, written_count=written)
        per.append(row)
        st = str(row.get("status", "error"))
        totals[st] = totals.get(st, 0) + 1
        ek = str(row.get("error_kind", "unknown"))
        by_error[ek] = by_error.get(ek, 0) + 1

    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M")
    out_root = project_root / output_dir
    out_root.mkdir(parents=True, exist_ok=True)
    report = {
        "ok": True,
        "generated_at": _now_iso(),
        "selected_sources": len(selected),
        "write_assets": bool(write_assets),
        "totals": totals,
        "by_error_kind": by_error,
        "per_source": per,
    }
    json_path = out_root / f"probe_report-{ts}.json"
    md_path = out_root / f"probe_report-{ts}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(_render_probe_md(report), encoding="utf-8")
    report["artifacts"] = {"json": str(json_path), "md": str(md_path)}
    report["ok"] = True
    return report
