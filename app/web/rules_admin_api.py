from __future__ import annotations

import os
import re
import json
import html
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBasic, HTTPBasicCredentials, HTTPBearer
from pydantic import BaseModel, Field

from app.rules.engine import RuleEngine
from app.services.rules_store import RulesStore
from app.services.rules_versioning import get_workspace_rules_root
from app.services.source_registry import (
    load_sources_registry_bundle,
    set_source_enabled_override,
    test_source,
    upsert_source_registry,
)
from app.workers.dryrun import run_dryrun


def _type_ok(expected: str, value: Any) -> bool:
    mapping = {
        "object": dict,
        "array": list,
        "string": str,
        "number": (int, float),
        "integer": int,
        "boolean": bool,
    }
    py_t = mapping.get(expected)
    if py_t is None:
        return True
    if expected in ("number", "integer") and isinstance(value, bool):
        return False
    return isinstance(value, py_t)


def _validate_schema_structured(
    data: Any,
    schema: dict[str, Any],
    path: str = "$",
    root_schema: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    root_schema = root_schema or schema
    expected_type = schema.get("type")
    if expected_type and not _type_ok(str(expected_type), data):
        errors.append({"path": path, "message": f"期望类型 {expected_type}，实际 {type(data).__name__}"})
        return errors

    if "const" in schema and data != schema["const"]:
        errors.append({"path": path, "message": f"必须等于 {schema['const']!r}"})
    if "enum" in schema and data not in schema["enum"]:
        errors.append({"path": path, "message": f"必须是枚举值之一 {schema['enum']!r}"})

    if isinstance(data, (int, float)) and not isinstance(data, bool):
        if "minimum" in schema and data < schema["minimum"]:
            errors.append({"path": path, "message": f"值 {data} 小于最小值 {schema['minimum']}"})
        if "maximum" in schema and data > schema["maximum"]:
            errors.append({"path": path, "message": f"值 {data} 大于最大值 {schema['maximum']}"})

    if isinstance(data, str) and "minLength" in schema and len(data) < schema["minLength"]:
        errors.append({"path": path, "message": f"字符串长度小于 {schema['minLength']}"})

    if isinstance(data, list):
        if "minItems" in schema and len(data) < schema["minItems"]:
            errors.append({"path": path, "message": f"数组长度小于 {schema['minItems']}"})
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for idx, item in enumerate(data):
                errors.extend(_validate_schema_structured(item, item_schema, f"{path}[{idx}]", root_schema))

    if isinstance(data, dict):
        required = schema.get("required", [])
        for k in required:
            if k not in data:
                errors.append({"path": path, "message": f"缺少必填字段 '{k}'"})
        properties = schema.get("properties", {})
        defs = root_schema.get("$defs", {})
        for k, subschema in properties.items():
            if k not in data:
                continue
            if isinstance(subschema, dict) and "$ref" in subschema:
                ref = subschema["$ref"]
                if ref.startswith("#/$defs/"):
                    name = ref.split("/")[-1]
                    target = defs.get(name)
                    if isinstance(target, dict):
                        errors.extend(_validate_schema_structured(data[k], target, f"{path}.{k}", root_schema))
                    else:
                        errors.append({"path": f"{path}.{k}", "message": f"未解析的引用 {ref}"})
                else:
                    errors.append({"path": f"{path}.{k}", "message": f"不支持的引用 {ref}"})
            elif isinstance(subschema, dict):
                errors.extend(_validate_schema_structured(data[k], subschema, f"{path}.{k}", root_schema))
    return errors


def _workspace_schemas(project_root: Path) -> Path:
    return get_workspace_rules_root(project_root) / "schemas"


def _load_rules_schema(project_root: Path, ruleset: str) -> dict[str, Any]:
    name_map = {
        "email_rules": "email_rules.schema.json",
        "content_rules": "content_rules.schema.json",
        "qc_rules": "qc_rules.schema.json",
        "output_rules": "output_rules.schema.json",
        "scheduler_rules": "scheduler_rules.schema.json",
    }
    name = name_map.get(ruleset, "")
    if not name:
        raise RuntimeError(f"unsupported ruleset: {ruleset}")
    p = _workspace_schemas(project_root) / name
    if not p.exists():
        p = project_root / "rules" / "schemas" / name
    if not p.exists():
        raise RuntimeError(f"schema not found: {name}")
    return json.loads(p.read_text(encoding="utf-8"))


def _utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _next_version_tag(store: RulesStore, ruleset: str, profile: str) -> str:
    base = f"db-{_utc_now_compact()}"
    existing = {str(x["version"]) for x in store.list_versions(ruleset, profile=profile)}
    if base not in existing:
        return base
    idx = 2
    while True:
        candidate = f"{base}-{idx}"
        if candidate not in existing:
            return candidate
        idx += 1


def _is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return bool(p.scheme in ("http", "https") and p.netloc)
    except Exception:
        return False


def _source_errors(source: dict[str, Any], store: RulesStore) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    sid = str(source.get("id", "")).strip()
    if not sid:
        out.append({"path": "$.id", "message": "id 为必填"})
    elif not re.match(r"^[a-zA-Z0-9._-]{2,80}$", sid):
        out.append({"path": "$.id", "message": "id 仅允许字母数字._- 且长度 2-80"})

    fetcher = str(source.get("fetcher") or source.get("connector") or "").strip()
    if fetcher == "web":
        fetcher = "html"
    if fetcher not in {"rss", "html", "rsshub", "google_news", "api"}:
        out.append({"path": "$.fetcher", "message": "fetcher 必须为 rss|html|rsshub|google_news|api"})

    url = str(source.get("url", "")).strip()
    if fetcher in {"rss", "html", "google_news", "api"} and not _is_valid_url(url):
        out.append({"path": "$.url", "message": "url 非法或为空"})
    if fetcher == "rsshub":
        if not (url.startswith("rsshub://") or _is_valid_url(url)):
            out.append({"path": "$.url", "message": "rsshub url 必须是 rsshub:// 或 http(s) 地址"})
        fetch = source.get("fetch", {}) if isinstance(source.get("fetch"), dict) else {}
        if not str(fetch.get("rsshub_route") or "").strip():
            out.append({"path": "$.fetch.rsshub_route", "message": "rsshub 需要填写 rsshub_route（例如 /newyorktimes/world）"})
    if fetcher == "api":
        if not _is_valid_url(url):
            out.append({"path": "$.url", "message": "API base_url 非法或为空（请填写 url）"})
        fetch = source.get("fetch", {}) if isinstance(source.get("fetch"), dict) else {}
        endpoint = str(fetch.get("endpoint", "")).strip()
        if not endpoint:
            out.append({"path": "$.fetch.endpoint", "message": "API 必须填写 endpoint（如 /v1/news）"})
    if url and sid and store.source_url_exists(url, exclude_id=sid):
        out.append({"path": "$.url", "message": "url 已被其他 source 使用"})

    tt = str(source.get("trust_tier", "")).strip()
    if tt not in {"A", "B", "C"}:
        out.append({"path": "$.trust_tier", "message": "trust_tier 必须为 A|B|C"})

    try:
        pr = int(source.get("priority", 0))
        if pr < 0 or pr > 1000:
            out.append({"path": "$.priority", "message": "priority 超出范围 0..1000"})
    except Exception:
        out.append({"path": "$.priority", "message": "priority 必须是整数"})

    tags = source.get("tags", [])
    if tags is not None and not isinstance(tags, list):
        out.append({"path": "$.tags", "message": "tags 必须是数组"})

    fetch = source.get("fetch", {})
    if fetch is not None and not isinstance(fetch, dict):
        out.append({"path": "$.fetch", "message": "fetch 必须是对象"})
    else:
        f = fetch if isinstance(fetch, dict) else {}
        if "interval_minutes" in f:
            try:
                im = int(f.get("interval_minutes") or 0)
                if im < 1 or im > 1440:
                    out.append({"path": "$.fetch.interval_minutes", "message": "interval_minutes 超出范围 1..1440"})
            except Exception:
                out.append({"path": "$.fetch.interval_minutes", "message": "interval_minutes 必须是整数"})
        if "timeout_seconds" in f:
            try:
                ts = int(f.get("timeout_seconds") or 0)
                if ts < 1 or ts > 120:
                    out.append({"path": "$.fetch.timeout_seconds", "message": "timeout_seconds 超出范围 1..120"})
            except Exception:
                out.append({"path": "$.fetch.timeout_seconds", "message": "timeout_seconds 必须是整数"})
        headers = f.get("headers_json", {})
        if headers is not None and not isinstance(headers, dict):
            out.append({"path": "$.fetch.headers_json", "message": "headers_json 必须是对象"})
    auth_ref = f.get("auth_ref")
    if auth_ref is not None and not isinstance(auth_ref, str):
        out.append({"path": "$.fetch.auth_ref", "message": "auth_ref 必须是字符串（引用 env/secret 名称）"})
    if isinstance(auth_ref, str) and auth_ref.strip():
        # Env var name only (no ${} templates, no lowercase, no spaces).
        if not re.match(r"^[A-Z][A-Z0-9_]{1,63}$", auth_ref.strip()):
            out.append({"path": "$.fetch.auth_ref", "message": "auth_ref 必须为环境变量名（例如 NMPA_API_KEY，仅大写字母/数字/_）"})

    rl = source.get("rate_limit", {})
    if rl is not None and not isinstance(rl, dict):
        out.append({"path": "$.rate_limit", "message": "rate_limit 必须是对象"})
    else:
        rld = rl if isinstance(rl, dict) else {}
        if "rps" in rld:
            try:
                rps = float(rld.get("rps") or 0.0)
                if rps < 0.1 or rps > 50:
                    out.append({"path": "$.rate_limit.rps", "message": "rps 超出范围 0.1..50"})
            except Exception:
                out.append({"path": "$.rate_limit.rps", "message": "rps 必须是数字"})
        if "burst" in rld:
            try:
                b = int(rld.get("burst") or 0)
                if b < 1 or b > 100:
                    out.append({"path": "$.rate_limit.burst", "message": "burst 超出范围 1..100"})
            except Exception:
                out.append({"path": "$.rate_limit.burst", "message": "burst 必须是整数"})

    parsing = source.get("parsing", {})
    if parsing is not None and not isinstance(parsing, dict):
        out.append({"path": "$.parsing", "message": "parsing 必须是对象"})
    else:
        pd = parsing if isinstance(parsing, dict) else {}
        if "parse_profile" in pd and not isinstance(pd.get("parse_profile"), str):
            out.append({"path": "$.parsing.parse_profile", "message": "parse_profile 必须是字符串"})
    return out


class RulesDraftPayload(BaseModel):
    profile: str = Field(default="enhanced", min_length=1, max_length=64)
    config_json: dict[str, Any]
    created_by: str = Field(default="rules-admin", min_length=1, max_length=128)


class RulesPublishPayload(BaseModel):
    profile: str = Field(default="enhanced", min_length=1, max_length=64)
    draft_id: int | None = None
    created_by: str = Field(default="rules-admin", min_length=1, max_length=128)


class RulesRollbackPayload(BaseModel):
    profile: str = Field(default="enhanced", min_length=1, max_length=64)


class SourcePayload(BaseModel):
    id: str
    name: str
    connector: str = ""
    fetcher: str = ""
    url: str = ""
    region: str = "全球"
    enabled: bool = True
    priority: int = 0
    trust_tier: str
    tags: list[str] = Field(default_factory=list)
    rate_limit: dict[str, Any] = Field(default_factory=dict)
    fetch: dict[str, Any] = Field(default_factory=dict)
    parsing: dict[str, Any] = Field(default_factory=dict)


class TogglePayload(BaseModel):
    enabled: bool | None = None


basic = HTTPBasic(auto_error=False)
bearer = HTTPBearer(auto_error=False)


def _is_loopback(host: str | None) -> bool:
    return host in {"127.0.0.1", "::1", "localhost"}


def _auth_guard(
    request: Request,
    basic_cred: HTTPBasicCredentials | None = Depends(basic),
    bearer_cred: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> dict[str, str]:
    admin_token = os.environ.get("ADMIN_TOKEN", "").strip()
    admin_user = os.environ.get("ADMIN_USER", "").strip()
    admin_pass = os.environ.get("ADMIN_PASS", "").strip()

    if admin_token:
        if bearer_cred and bearer_cred.scheme.lower() == "bearer" and bearer_cred.credentials == admin_token:
            return {"auth": "bearer", "principal": "token-user"}
        raise HTTPException(
            status_code=401,
            detail="unauthorized: bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if admin_user and admin_pass:
        if basic_cred and basic_cred.username == admin_user and basic_cred.password == admin_pass:
            return {"auth": "basic", "principal": basic_cred.username}
        raise HTTPException(
            status_code=401,
            detail="unauthorized: basic auth required",
            headers={"WWW-Authenticate": 'Basic realm="RulesAdminAPI"'},
        )

    # No explicit credentials: only allow local requests.
    host = request.client.host if request.client else None
    if _is_loopback(host):
        return {"auth": "local", "principal": "localhost"}
    raise HTTPException(
        status_code=401,
        detail="unauthorized: configure ADMIN_TOKEN or ADMIN_USER/ADMIN_PASS",
        headers={"WWW-Authenticate": 'Basic realm="RulesAdminAPI"'},
    )


def create_app(project_root: Path | None = None) -> FastAPI:
    root = project_root or Path(__file__).resolve().parents[2]
    store = RulesStore(root)
    engine = RuleEngine(project_root=root)
    app = FastAPI(title="Rules Admin API", version="1.0.0")

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        # No auth: used by container healthchecks.
        obs = store.observability_info() if hasattr(store, "observability_info") else {}
        return {
            "ok": True,
            "service": "admin-api",
            "db_path": str(getattr(store, "db_path", "")),
            "db_url": str(obs.get("db_url", "")),
            "db_backend": str(obs.get("db_backend", "")),
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    @app.exception_handler(HTTPException)
    async def _http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:  # type: ignore[override]
        payload = {"ok": False, "error": {"code": f"HTTP_{exc.status_code}", "message": str(exc.detail)}}
        return JSONResponse(status_code=exc.status_code, content=payload, headers=exc.headers)

    def _active_rules(ruleset: str, profile: str) -> dict[str, Any]:
        data = store.get_active_rules(ruleset, profile)
        if data is not None:
            meta = data.pop("_store_meta", {})
            return {"source": "db", "config_json": data, "meta": meta}
        sel = engine.load(ruleset, profile)
        return {
            "source": "file",
            "config_json": sel.data,
            "meta": {"profile": sel.profile, "version": sel.version, "path": str(sel.path)},
        }

    def _read_scheduler_status() -> dict[str, Any]:
        hb_path = root / "logs" / "scheduler_worker_heartbeat.json"
        st_path = root / "logs" / "scheduler_worker_status.json"
        hb: dict[str, Any] | None = None
        st: dict[str, Any] | None = None
        try:
            if hb_path.exists():
                hb = json.loads(hb_path.read_text(encoding="utf-8"))
        except Exception:
            hb = None
        try:
            if st_path.exists():
                st = json.loads(st_path.read_text(encoding="utf-8"))
        except Exception:
            st = None
        return {"heartbeat": hb, "status": st}

    def _normalize_time_for_status(raw: str | None) -> str:
        if not raw:
            return ""
        s = str(raw).strip()
        if not s:
            return ""
        return s.replace("Z", "+00:00")

    def _short_summary(text: str, max_chars: int = 120) -> str:
        t = str(text or "").strip()
        if len(t) <= max_chars:
            return t
        return t[: max_chars - 1] + "…"

    def _run_records_from_send_attempts(limit: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        rows_raw = store.list_send_attempts(limit=limit) if hasattr(store, "list_send_attempts") else []
        for r in rows_raw:
            st = str(r.get("status") or "").upper()
            status_map = {
                "SUCCESS": "success",
                "FAILED": "failed",
                "STARTED": "running",
                "SKIP": "skipped",
            }
            mapped = status_map.get(st, st.lower() or "unknown")
            created = _normalize_time_for_status(str(r.get("created_at") or ""))
            rows.append(
                {
                    "source": "fallback",
                    "run_id": str(r.get("run_id") or ""),
                    "time": created,
                    "status": mapped,
                    "failed_reason_summary": _short_summary(str(r.get("error") or "")),
                    "subject": str(r.get("subject") or ""),
                    "to_email": str(r.get("to_email") or ""),
                    "date": str(r.get("date") or ""),
                    "trigger": "backup",
                    "schedule_id": "manual_or_job",
                }
            )
        return rows

    def _run_records_from_artifacts(limit: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        art_root = root / "artifacts"
        if not art_root.exists():
            return []
        files: list[tuple[float, Path]] = []
        for p in art_root.glob("*/*"):
            if p.name != "run_meta.json":
                continue
            try:
                files.append((p.stat().st_mtime, p))
            except Exception:
                pass
        if not files:
            return []
        files.sort(key=lambda item: item[0], reverse=True)
        for _, p in files:
            if len(out) >= limit:
                break
            try:
                payload = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            status = str(payload.get("status", "")).strip().lower()
            if not status:
                continue
            trigger = str(payload.get("trigger", "digest"))
            purpose = str(payload.get("purpose", "")).strip().lower()
            if purpose == "collect":
                continue
            if trigger == "collect":
                continue
            run_id = str(payload.get("run_id", "")).strip()
            if not run_id:
                continue
            started_at = _normalize_time_for_status(str(payload.get("started_at", "")))
            if not started_at and p.exists():
                try:
                    started_at = datetime.fromtimestamp(p.stat().st_mtime, timezone.utc).isoformat()
                except Exception:
                    started_at = ""
            dt_obj = None
            if started_at:
                try:
                    dt_obj = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                except Exception:
                    dt_obj = None
            date_text = (
                dt_obj.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
                if dt_obj is not None
                else str(payload.get("date", ""))
            )
            out.append(
                {
                    "source": "scheduler",
                    "run_id": run_id,
                    "time": started_at,
                    "status": status,
                    "failed_reason_summary": _short_summary(str(payload.get("error_summary", ""))),
                    "subject": "",
                    "to_email": "",
                    "date": date_text,
                    "trigger": trigger,
                    "schedule_id": str(payload.get("schedule_id", "")),
                }
            )
        return out

    def _merge_run_records(*, limit: int = 30) -> list[dict[str, Any]]:
        send_records = _run_records_from_send_attempts(limit)
        artifact_records = _run_records_from_artifacts(limit)
        merged: list[dict[str, Any]] = send_records + artifact_records

        def _to_sort_key(item: dict[str, Any]) -> tuple[int, str]:
            t = str(item.get("time") or "")
            ts = ""
            try:
                if t:
                    dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                    ts = dt.isoformat()
            except Exception:
                pass
            return (1 if ts else 0, ts)

        merged.sort(key=_to_sort_key, reverse=True)
        return merged[:limit]

    def _is_today(item: dict[str, Any], today: str) -> bool:
        dt_text = str(item.get("date") or "").strip()
        if dt_text:
            return dt_text == today
        time_text = str(item.get("time") or "").strip()
        if not time_text:
            return False
        try:
            dt = datetime.fromisoformat(time_text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat() == today
        except Exception:
            return time_text.startswith(today)

    def _page_shell(title: str, body_html: str, script_js: str = "") -> str:
        nav = """
        <aside class="sidebar">
          <div class="brand">IVD全球CBO</div>
          <a class="nav" href="/admin/email">邮件规则</a>
          <a class="nav" href="/admin/content">采集规则</a>
          <a class="nav" href="/admin/qc">质控规则</a>
          <a class="nav" href="/admin/output">输出规则</a>
          <a class="nav" href="/admin/scheduler">调度规则</a>
          <a class="nav" href="/admin/sources">信源管理</a>
          <a class="nav" href="/admin/runs">运行状态</a>
          <a class="nav" href="/admin/versions">版本与回滚</a>
          <div class="sidehint">必须先通过草稿校验，才允许发布生效</div>
        </aside>
        """
        tpl = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>@@TITLE@@</title>
  <style>
        :root {
          --bg: #0b0f17;
          --panel: #0f1523;
          --panel2: #101a2d;
          --text: #e8eefc;
          --muted: rgba(232,238,252,.70);
          --border: rgba(232,238,252,.12);
          --ok: #4be38f;
          --err: #ff667a;
          --warn: #ffd166;
          --accent: #7aa6ff;
          --activeRowBg: rgba(75,227,143,.10);
          --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
        }
    body { font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif; margin: 0; background: radial-gradient(1200px 600px at 20% 0%, #162245 0%, var(--bg) 60%); color: var(--text); }
    a { color: var(--accent); text-decoration: none; }
    a:hover { text-decoration: underline; }
    .app { display:grid; grid-template-columns: 240px 1fr; min-height: 100vh; }
    .sidebar { border-right: 1px solid var(--border); padding: 18px 14px; background: rgba(8,12,20,.55); backdrop-filter: blur(6px); }
    .brand { font-weight: 800; letter-spacing: .4px; margin-bottom: 14px; }
    .nav { display:block; padding: 10px 10px; border-radius: 10px; margin-bottom: 6px; color: var(--text); border: 1px solid transparent; }
    .nav:hover { background: rgba(122,166,255,.08); border-color: rgba(122,166,255,.18); }
    .sidehint { margin-top: 14px; font-size: 12px; color: var(--muted); }
    .main { padding: 18px 18px 40px; }
    .topbar { display:flex; align-items:center; justify-content: space-between; gap: 12px; margin-bottom: 14px; }
    .title { font-size: 18px; font-weight: 800; }
    .statusbar { font-size: 12px; color: var(--muted); }
    .layout { display:grid; grid-template-columns: minmax(340px, 380px) minmax(0, 1fr); gap: 14px; align-items: start; }
    @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }
    .card { border:1px solid var(--border); border-radius:14px; padding:12px; background: linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.01)); min-width: 0; }
    .card h4 { margin: 0 0 8px; }
    label { display:block; margin:8px 0 4px; font-weight:600; }
    input, textarea, select { width:100%; box-sizing:border-box; padding:9px 10px; border-radius: 10px; border: 1px solid var(--border); background: rgba(10,16,28,.65); color: var(--text); outline: none; }
    input::placeholder, textarea::placeholder { color: rgba(232,238,252,.35); }
    textarea { font-family: var(--mono); }
    button { margin-right:8px; margin-top:8px; padding: 8px 10px; border-radius: 10px; border: 1px solid var(--border); background: rgba(122,166,255,.10); color: var(--text); cursor:pointer; }
    button:hover { background: rgba(122,166,255,.18); }
    button:disabled { opacity: .45; cursor:not-allowed; }
    pre {
      background: rgba(0,0,0,.25);
      padding:10px;
      border-radius:12px;
      overflow:auto;
      border:1px solid var(--border);
      /* Make previews readable without horizontal scrolling. */
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    table { width:100%; border-collapse: collapse; table-layout: fixed; }
    th, td { border-bottom:1px solid var(--border); padding:8px 6px; text-align:left; vertical-align:top; overflow:hidden; }
        th { font-size: 12px; color: var(--muted); font-weight: 700; }
        tr:hover td { background: rgba(122,166,255,.06); }
        /* Active version highlight: keep contrast on dark background */
        tr.activeRow td { background: var(--activeRowBg) !important; color: var(--text) !important; }
        tr.activeRow td:first-child { box-shadow: inset 3px 0 0 rgba(75,227,143,.65); }
        tr.activeRow a { color: var(--text) !important; text-decoration: underline dotted rgba(232,238,252,.40); }
        .ok { color: var(--ok); font-weight:700; }
        .err { color: var(--err); font-weight:700; }
        .warn { color: var(--warn); font-weight:700; }
    .pill { display:inline-block; font-size: 12px; padding: 2px 8px; border: 1px solid var(--border); border-radius: 999px; color: var(--muted); }
    /* Rows often contain multiple small controls; allow wrap to avoid overflow on narrow widths. */
    .row { display:flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .row > * { min-width: 0; }
    .row input, .row select { width: auto; flex: 1 1 140px; }
    .grow { flex: 1; }
    .toastWrap { position: fixed; right: 14px; top: 14px; display:flex; flex-direction: column; gap: 10px; z-index: 50; }
    .toast { border: 1px solid var(--border); border-radius: 12px; padding: 10px 12px; background: rgba(15,21,35,.92); box-shadow: 0 20px 60px rgba(0,0,0,.35); max-width: 420px; }
    .toast .t { font-weight: 800; margin-bottom: 4px; }
        .drawer { margin-top: 10px; border:1px solid var(--border); border-radius: 12px; padding: 10px; background: rgba(10,16,28,.55); }
        .kvs { display:grid; grid-template-columns: 120px 1fr; gap: 6px 10px; font-size: 12px; color: var(--muted); }
        .kvs b { color: var(--text); font-weight: 700; }
        details.help { margin-top: 10px; }
        details.help summary { cursor: pointer; color: var(--muted); font-size: 12px; }
        details.help .box { margin-top: 8px; border: 1px solid var(--border); border-radius: 12px; padding: 10px; background: rgba(10,16,28,.45); }
        details.help ul { margin: 0; padding-left: 18px; color: var(--muted); font-size: 12px; }
        details.help li { margin: 6px 0; }
        .cards { display:grid; grid-template-columns: 1fr; gap: 10px; }
            .clusterCard { border:1px solid var(--border); border-radius: 12px; padding: 10px; background: rgba(16,26,45,.55); }
            .clusterTitle { font-weight: 800; }
        .small { font-size: 12px; color: var(--muted); }
        .chk { display:inline-flex; align-items:center; gap: 8px; margin: 4px 12px 4px 0; font-size: 12px; color: var(--muted); user-select: none; }
        .chk input { width: auto; }
        td.urlcol { width: 340px; }
        a.url { display:inline-block; max-width: 100%; overflow:hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: bottom; }
        td.tagcol { width: 220px; }
    .nowrap { white-space: nowrap; }
    td.actionscol { width: 160px; }
  </style>
</head>
<body>
  <div class="toastWrap" id="toasts"></div>
  <div class="app">
    @@NAV@@
    <main class="main">
          <div class="topbar">
            <div class="title">@@TITLE@@</div>
            <div class="statusbar" id="statusbar">就绪</div>
          </div>
          @@BODY@@
        </main>
      </div>
  <script>
  function setStatus(s) {
    const el = document.getElementById('statusbar');
    if (!el) return;
    const t = String(s||'');
    if (t.startsWith('done ')) { el.textContent = '完成 ' + t.slice(5); return; }
    if (t.startsWith('GET ') || t.startsWith('POST ') || t.startsWith('PUT ') || t.startsWith('DELETE ')) {
      el.textContent = '请求 ' + t;
      return;
    }
    el.textContent = t;
  }
  function toast(kind, title, msg) {
    const wrap = document.getElementById('toasts');
    if (!wrap) return;
    const el = document.createElement('div');
    el.className = 'toast';
    const k = kind === 'ok' ? 'ok' : (kind === 'warn' ? 'warn' : 'err');
    el.innerHTML = `<div class="t ${k}">${title}</div><div class="small">${String(msg||'')}</div>`;
    wrap.appendChild(el);
    setTimeout(()=>{ try{ el.remove(); }catch(e){} }, 4000);
  }
  async function api(path, method='GET', body=null) {
    setStatus(`${method} ${path}`);
    const opts = { method, headers: { 'Content-Type':'application/json' } };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(path, opts);
    const txt = await res.text();
    let data = null;
    try { data = JSON.parse(txt); } catch(e) { data = {ok:false, raw:txt}; }
    setStatus(`done ${res.status}`);
    if (!res.ok && data && data.error) return data;
    return data;
  }
  @@SCRIPT@@
  </script>
</body>
</html>"""
        return (
            tpl.replace("@@TITLE@@", title)
            .replace("@@NAV@@", nav)
            .replace("@@BODY@@", body_html)
            .replace("@@SCRIPT@@", script_js)
        )

    def _version_rows(ruleset: str, profile: str) -> list[dict[str, Any]]:
        rows = store.list_versions(ruleset, profile=profile)
        active = store.get_active_rules(ruleset, profile)
        active_ver = ""
        if isinstance(active, dict):
            m = active.get("_store_meta", {})
            if isinstance(m, dict):
                active_ver = str(m.get("version", ""))
        for r in rows:
            r["active"] = bool(active_ver and str(r.get("version")) == active_ver)
        return rows

    def _config_for_version(ruleset: str, profile: str, version: str) -> dict[str, Any] | None:
        if ruleset not in {"email_rules", "content_rules", "qc_rules", "output_rules", "scheduler_rules"}:
            return None
        return store.get_version_config(ruleset, profile, version)

    def _diff_keys(left: dict[str, Any], right: dict[str, Any]) -> list[str]:
        keys = sorted(set(left.keys()) | set(right.keys()))
        changed: list[str] = []
        for k in keys:
            if left.get(k) != right.get(k):
                changed.append(k)
        return changed

    def _draft_validate(ruleset: str, profile: str, config: dict[str, Any]) -> list[dict[str, str]]:
        schema = _load_rules_schema(root, ruleset)
        errs = _validate_schema_structured(config, schema)
        if str(config.get("ruleset", "")) != ruleset:
            errs.append({"path": "$.ruleset", "message": f"必须等于 {ruleset}"})
        if str(config.get("profile", "")) != profile:
            errs.append({"path": "$.profile", "message": f"必须等于 {profile}"})
        return errs

    def _publish_from_draft(ruleset: str, payload: RulesPublishPayload) -> dict[str, Any]:
        draft = store.get_draft(ruleset=ruleset, profile=payload.profile, draft_id=payload.draft_id)
        if draft is None:
            raise HTTPException(status_code=404, detail="draft not found")
        errors = _draft_validate(ruleset, payload.profile, draft["config_json"])
        if errors:
            return {"ok": False, "published": False, "draft_id": draft["id"], "errors": errors}
        version = _next_version_tag(store, ruleset, payload.profile)
        out = store.create_version(
            ruleset,
            profile=payload.profile,
            version=version,
            config=draft["config_json"],
            created_by=payload.created_by,
            activate=True,
        )
        return {"ok": True, "published": True, "draft_id": draft["id"], "version": out["version"], "profile": payload.profile}

    @app.get("/admin/api/email_rules/active")
    def email_rules_active(
        profile: str = "enhanced",
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return {"ok": True, "ruleset": "email_rules", "profile": profile, **_active_rules("email_rules", profile)}

    @app.post("/admin/api/email_rules/draft")
    def email_rules_draft(
        payload: RulesDraftPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        errors = _draft_validate("email_rules", payload.profile, payload.config_json)
        draft = store.create_draft(
            ruleset="email_rules",
            profile=payload.profile,
            config=payload.config_json,
            created_by=payload.created_by,
            validation_errors=errors,
        )
        return {"ok": len(errors) == 0, "draft": draft}

    @app.post("/admin/api/email_rules/publish")
    def email_rules_publish(
        payload: RulesPublishPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return _publish_from_draft("email_rules", payload)

    @app.post("/admin/api/email_rules/rollback")
    def email_rules_rollback(
        payload: RulesRollbackPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return store.rollback("email_rules", profile=payload.profile)

    @app.get("/admin/api/content_rules/active")
    def content_rules_active(
        profile: str = "enhanced",
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return {"ok": True, "ruleset": "content_rules", "profile": profile, **_active_rules("content_rules", profile)}

    @app.get("/admin/api/qc_rules/active")
    def qc_rules_active(
        profile: str = "enhanced",
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return {"ok": True, "ruleset": "qc_rules", "profile": profile, **_active_rules("qc_rules", profile)}

    @app.get("/admin/api/output_rules/active")
    def output_rules_active(
        profile: str = "enhanced",
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return {"ok": True, "ruleset": "output_rules", "profile": profile, **_active_rules("output_rules", profile)}

    @app.get("/admin/api/scheduler_rules/active")
    def scheduler_rules_active(
        profile: str = "enhanced",
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "ruleset": "scheduler_rules",
            "profile": profile,
            **_active_rules("scheduler_rules", profile),
        }

    @app.post("/admin/api/email_rules/dryrun")
    def email_rules_dryrun(
        payload: dict[str, Any],
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        profile = str(payload.get("profile", "enhanced"))
        date = str(payload.get("date", "") or "")
        out = run_dryrun(profile=profile, report_date=date or None)
        preview_path = Path(str(out.get("artifacts", {}).get("preview", "")))
        preview_text = preview_path.read_text(encoding="utf-8") if preview_path.exists() else ""
        return {
            "ok": True,
            "run_id": out.get("run_id"),
            "profile": profile,
            "items_count": out.get("items_count"),
            "preview_markdown": preview_text,
            "artifacts_dir": out.get("artifacts_dir"),
        }

    @app.post("/admin/api/content_rules/dryrun")
    def content_rules_dryrun(
        payload: dict[str, Any],
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        profile = str(payload.get("profile", "enhanced"))
        date = str(payload.get("date", "") or "")
        out = run_dryrun(profile=profile, report_date=date or None)
        return {
            "ok": True,
            "run_id": out.get("run_id"),
            "profile": profile,
            "items_before_count": out.get("items_before_count"),
            "items_after_count": out.get("items_after_count"),
            "items_count": out.get("items_count"),
            "top_clusters": out.get("top_clusters", []),
            "platform_diag": out.get("platform_diag", {}),
            "lane_diag": out.get("lane_diag", {}),
            "event_diag": out.get("event_diag", {}),
            "keyword_pack_stats": out.get("keyword_pack_stats", {}),
            "exclude_diag": out.get("exclude_diag", {}),
            "artifacts_dir": out.get("artifacts_dir"),
        }

    @app.post("/admin/api/qc_rules/dryrun")
    def qc_rules_dryrun(
        payload: dict[str, Any],
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        profile = str(payload.get("profile", "enhanced"))
        date = str(payload.get("date", "") or "")
        out = run_dryrun(profile=profile, report_date=date or None)
        artifacts = out.get("artifacts", {}) if isinstance(out.get("artifacts"), dict) else {}
        qc_path = Path(str(artifacts.get("qc_report", "")))
        qc_report = json.loads(qc_path.read_text(encoding="utf-8")) if qc_path.exists() else {}
        return {"ok": True, "run_id": out.get("run_id"), "profile": profile, "qc_report": qc_report}

    @app.post("/admin/api/output_rules/dryrun")
    def output_rules_dryrun(
        payload: dict[str, Any],
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        profile = str(payload.get("profile", "enhanced"))
        date = str(payload.get("date", "") or "")
        out = run_dryrun(profile=profile, report_date=date or None)
        artifacts = out.get("artifacts", {}) if isinstance(out.get("artifacts"), dict) else {}
        out_path = Path(str(artifacts.get("output_render", "")))
        output_render = json.loads(out_path.read_text(encoding="utf-8")) if out_path.exists() else {}
        return {"ok": True, "run_id": out.get("run_id"), "profile": profile, "output_render": output_render}

    @app.post("/admin/api/dryrun")
    def unified_dryrun(
        date: str | None = None,
        profile: str = "enhanced",
        lite: bool = False,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        """
        Unified dry-run: one call returns preview + clustered items + explain payloads.
        Note: dry-run only, no DB write, no email send.
        """
        out = run_dryrun(profile=profile, report_date=(date or None))
        artifacts = out.get("artifacts", {}) if isinstance(out.get("artifacts"), dict) else {}

        def _read_json(path: str) -> Any:
            try:
                p = Path(path)
                if p.exists():
                    return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                return None
            return None

        def _read_text(path: str) -> str:
            try:
                p = Path(path)
                if p.exists():
                    return p.read_text(encoding="utf-8")
            except Exception:
                return ""
            return ""

        preview_text = _read_text(str(artifacts.get("preview", "")))
        preview_html = f"<pre>{html.escape(preview_text)}</pre>" if preview_text else ""

        payload: dict[str, Any] = {
            "ok": True,
            "run_id": out.get("run_id"),
            "profile": out.get("profile"),
            "date": out.get("date"),
            "items_before": int(out.get("items_before_count") or 0),
            "items_after": int(out.get("items_after_count") or 0),
            "preview_text": preview_text,
            "preview_html": preview_html,
            "qc_report": _read_json(str(artifacts.get("qc_report", ""))) or {},
            "output_render": _read_json(str(artifacts.get("output_render", ""))) or {},
            "run_meta": _read_json(str(artifacts.get("run_meta", ""))) or {},
            "artifacts": artifacts,
        }
        if not lite:
            payload["clustered_items"] = _read_json(str(artifacts.get("clustered_items", "")))
            payload["explain"] = _read_json(str(artifacts.get("explain", "")))
            payload["explain_cluster"] = _read_json(str(artifacts.get("cluster_explain", "")))
            payload["items"] = _read_json(str(artifacts.get("items", ""))) or []
        return payload

    @app.get("/admin/api/run_status")
    def run_status(
        limit: int = 30,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        lim = max(1, min(int(limit or 30), 200))
        rows = _merge_run_records(limit=lim)
        if not rows:
            rows = []

        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        today = now.date().isoformat()
        today_rows = [r for r in rows if _is_today(r, today)]
        last_error = ""
        for item in rows:
            if str(item.get("status", "")).lower() == "failed":
                last_error = str(item.get("failed_reason_summary") or "")
                break

        today_sent = any(str(r.get("status", "")).lower() == "success" for r in today_rows)
        today_fallback = any(
            str(r.get("source", "")).lower() in {"fallback", "backup"}
            and _is_today(r, today)
            for r in rows
        )

        return {
            "ok": True,
            "runs": rows,
            "count": len(rows),
            "today": {
                "date": today,
                "sent": bool(today_sent),
                "fallback_triggered": bool(today_fallback),
                "last_error": last_error,
            },
            "scheduler": _read_scheduler_status(),
        }

    @app.post("/admin/api/content_rules/draft")
    def content_rules_draft(
        payload: RulesDraftPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        errors = _draft_validate("content_rules", payload.profile, payload.config_json)
        draft = store.create_draft(
            ruleset="content_rules",
            profile=payload.profile,
            config=payload.config_json,
            created_by=payload.created_by,
            validation_errors=errors,
        )
        return {"ok": len(errors) == 0, "draft": draft}

    @app.post("/admin/api/content_rules/publish")
    def content_rules_publish(
        payload: RulesPublishPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return _publish_from_draft("content_rules", payload)

    @app.post("/admin/api/content_rules/rollback")
    def content_rules_rollback(
        payload: RulesRollbackPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return store.rollback("content_rules", profile=payload.profile)

    @app.post("/admin/api/qc_rules/draft")
    def qc_rules_draft(
        payload: RulesDraftPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        errors = _draft_validate("qc_rules", payload.profile, payload.config_json)
        draft = store.create_draft(
            ruleset="qc_rules",
            profile=payload.profile,
            config=payload.config_json,
            created_by=payload.created_by,
            validation_errors=errors,
        )
        return {"ok": len(errors) == 0, "draft": draft}

    @app.post("/admin/api/qc_rules/publish")
    def qc_rules_publish(
        payload: RulesPublishPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return _publish_from_draft("qc_rules", payload)

    @app.post("/admin/api/qc_rules/rollback")
    def qc_rules_rollback(
        payload: RulesRollbackPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return store.rollback("qc_rules", profile=payload.profile)

    @app.post("/admin/api/output_rules/draft")
    def output_rules_draft(
        payload: RulesDraftPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        errors = _draft_validate("output_rules", payload.profile, payload.config_json)
        draft = store.create_draft(
            ruleset="output_rules",
            profile=payload.profile,
            config=payload.config_json,
            created_by=payload.created_by,
            validation_errors=errors,
        )
        return {"ok": len(errors) == 0, "draft": draft}

    @app.post("/admin/api/output_rules/publish")
    def output_rules_publish(
        payload: RulesPublishPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return _publish_from_draft("output_rules", payload)

    @app.post("/admin/api/output_rules/rollback")
    def output_rules_rollback(
        payload: RulesRollbackPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return store.rollback("output_rules", profile=payload.profile)

    def _touch_scheduler_reload_signal() -> None:
        p = root / "data" / "scheduler_reload.signal"
        p.parent.mkdir(parents=True, exist_ok=True)
        # Update content so mtime reliably changes on some filesystems.
        p.write_text(str(time.time()), encoding="utf-8")

    @app.post("/admin/api/scheduler_rules/draft")
    def scheduler_rules_draft(
        payload: RulesDraftPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        errors = _draft_validate("scheduler_rules", payload.profile, payload.config_json)
        draft = store.create_draft(
            ruleset="scheduler_rules",
            profile=payload.profile,
            config=payload.config_json,
            created_by=payload.created_by,
            validation_errors=errors,
        )
        return {"ok": len(errors) == 0, "draft": draft}

    @app.post("/admin/api/scheduler_rules/publish")
    def scheduler_rules_publish(
        payload: RulesPublishPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        out = _publish_from_draft("scheduler_rules", payload)
        try:
            _touch_scheduler_reload_signal()
        except Exception:
            pass
        return out

    @app.post("/admin/api/scheduler_rules/rollback")
    def scheduler_rules_rollback(
        payload: RulesRollbackPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        out = store.rollback("scheduler_rules", profile=payload.profile)
        try:
            _touch_scheduler_reload_signal()
        except Exception:
            pass
        return out

    @app.get("/admin/api/scheduler/status")
    def scheduler_status(_: dict[str, str] = Depends(_auth_guard)) -> dict[str, Any]:
        hb_path = root / "logs" / "scheduler_worker_heartbeat.json"
        st_path = root / "logs" / "scheduler_worker_status.json"
        hb: dict[str, Any] | None = None
        st: dict[str, Any] | None = None
        try:
            if hb_path.exists():
                hb = json.loads(hb_path.read_text(encoding="utf-8"))
        except Exception:
            hb = None
        try:
            if st_path.exists():
                st = json.loads(st_path.read_text(encoding="utf-8"))
        except Exception:
            st = None
        return {"ok": True, "heartbeat": hb, "status": st}

    class SchedulerTriggerPayload(BaseModel):
        profile: str = Field(default="enhanced")
        purpose: str = Field(default="collect")
        schedule_id: str = Field(default="manual")

    @app.post("/admin/api/scheduler/trigger")
    def scheduler_trigger(
        payload: SchedulerTriggerPayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        purpose = str(payload.purpose or "collect").strip()
        if purpose not in {"collect", "digest"}:
            raise HTTPException(status_code=400, detail="purpose must be collect|digest")
        cmd_dir = root / "data" / "scheduler_commands"
        cmd_dir.mkdir(parents=True, exist_ok=True)
        fname = f"cmd-{int(time.time())}-{os.getpid()}.json"
        (cmd_dir / fname).write_text(
            json.dumps(
                {
                    "cmd": "trigger",
                    "purpose": purpose,
                    "profile": str(payload.profile or "enhanced"),
                    "schedule_id": str(payload.schedule_id or "manual"),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return {"ok": True, "enqueued": True, "file": fname}

    @app.get("/admin/api/sources")
    def sources_list(_: dict[str, str] = Depends(_auth_guard)) -> dict[str, Any]:
        bundle = load_sources_registry_bundle(root, rules_root=engine.rules_root)
        rows = bundle.get("sources", []) if isinstance(bundle, dict) else []
        out = []
        for s in rows:
            src = dict(s)
            fetch = src.get("fetch", {}) if isinstance(src.get("fetch"), dict) else {}
            auth_ref = str(fetch.get("auth_ref") or "").strip()
            if auth_ref:
                src["auth_configured"] = bool(os.environ.get(auth_ref, "").strip())
            out.append(src)
        return {
            "ok": True,
            "count": len(out),
            "sources": out,
            "registry_file": (bundle.get("source_file") if isinstance(bundle, dict) else None),
            "overrides_file": (bundle.get("overrides_file") if isinstance(bundle, dict) else None),
        }

    @app.post("/admin/api/sources")
    def sources_upsert(
        payload: SourcePayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        source = payload.model_dump()
        if not str(source.get("fetcher", "")).strip():
            source["fetcher"] = source.get("connector", "")
        if str(source.get("fetcher", "")).strip() == "web":
            source["fetcher"] = "html"
        source["connector"] = "web" if source.get("fetcher") == "html" else source.get("fetcher", "")
        errs = _source_errors(source, store)
        if errs:
            return {"ok": False, "error": {"code": "SOURCE_VALIDATION_FAILED", "details": errs}}
        return upsert_source_registry(root, source, rules_root=engine.rules_root)

    @app.post("/admin/api/sources/{source_id}/toggle")
    def sources_toggle(
        source_id: str,
        payload: TogglePayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return set_source_enabled_override(root, source_id, enabled=payload.enabled, rules_root=engine.rules_root)

    @app.post("/admin/api/sources/{source_id}/test")
    def sources_test(source_id: str, _: dict[str, str] = Depends(_auth_guard)) -> dict[str, Any]:
        bundle = load_sources_registry_bundle(root, rules_root=engine.rules_root)
        rows = bundle.get("sources", []) if isinstance(bundle, dict) else []
        source = next((x for x in rows if str(x.get("id", "")).strip() == source_id), None)
        if not isinstance(source, dict):
            raise HTTPException(status_code=404, detail="source not found")
        result = test_source(source, limit=3)
        try:
            store.record_source_test(
                source_id,
                ok=bool(result.get("ok")),
                http_status=int(result.get("http_status")) if result.get("http_status") is not None else None,
                error=str(result.get("error") or "") if not bool(result.get("ok")) else None,
            )
        except Exception:
            pass
        return {"ok": True, "result": result}

    @app.get("/admin/api/versions")
    def versions_list(
        profile: str = "enhanced",
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "profile": profile,
            "email_rules": _version_rows("email_rules", profile),
            "content_rules": _version_rows("content_rules", profile),
            "qc_rules": _version_rows("qc_rules", profile),
            "output_rules": _version_rows("output_rules", profile),
            "scheduler_rules": _version_rows("scheduler_rules", profile),
        }

    def _json_diff(left: Any, right: Any, path: str = "$", out: list[dict[str, Any]] | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if out is None:
            out = []
        if len(out) >= limit:
            return out
        if type(left) != type(right):
            out.append({"path": path, "op": "type_changed", "from": type(left).__name__, "to": type(right).__name__})
            return out
        if isinstance(left, dict):
            keys = set(left.keys()) | set(right.keys())
            for k in sorted(keys):
                if len(out) >= limit:
                    break
                p = f"{path}.{k}"
                if k not in left:
                    out.append({"path": p, "op": "added", "to": right.get(k)})
                elif k not in right:
                    out.append({"path": p, "op": "removed", "from": left.get(k)})
                else:
                    _json_diff(left.get(k), right.get(k), p, out, limit)
            return out
        if isinstance(left, list):
            if left == right:
                return out
            # Deep diff for list-of-dict keyed by "id" (e.g. rules arrays).
            left_id_map = {
                str(x.get("id")): x
                for x in left
                if isinstance(x, dict) and str(x.get("id", "")).strip()
            }
            right_id_map = {
                str(x.get("id")): x
                for x in right
                if isinstance(x, dict) and str(x.get("id", "")).strip()
            }
            if left_id_map and right_id_map:
                keys = sorted(set(left_id_map.keys()) | set(right_id_map.keys()))
                for k in keys:
                    if len(out) >= limit:
                        break
                    p = f"{path}[id={k}]"
                    if k not in left_id_map:
                        out.append({"path": p, "op": "added", "to": right_id_map.get(k)})
                    elif k not in right_id_map:
                        out.append({"path": p, "op": "removed", "from": left_id_map.get(k)})
                    else:
                        _json_diff(left_id_map.get(k), right_id_map.get(k), p, out, limit)
                # If nothing diffed (e.g., duplicate ids edge case), keep coarse fallback.
                if out:
                    return out
            out.append({"path": path, "op": "changed", "from": f"list(len={len(left)})", "to": f"list(len={len(right)})"})
            return out
        if left != right:
            out.append({"path": path, "op": "changed", "from": left, "to": right})
        return out

    @app.get("/admin/api/versions/diff")
    def versions_diff(
        ruleset: str,
        profile: str,
        from_version: str,
        to_version: str,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        if ruleset not in {"email_rules", "content_rules", "qc_rules", "output_rules", "scheduler_rules"}:
            raise HTTPException(
                status_code=400,
                detail="ruleset must be email_rules|content_rules|qc_rules|output_rules|scheduler_rules",
            )
        left = _config_for_version(ruleset, profile, from_version)
        right = _config_for_version(ruleset, profile, to_version)
        if left is None or right is None:
            raise HTTPException(status_code=404, detail="version not found")
        changes = _json_diff(left, right, "$")
        return {
            "ok": True,
            "ruleset": ruleset,
            "profile": profile,
            "from_version": from_version,
            "to_version": to_version,
            "changed_top_level_keys": _diff_keys(left, right),
            "changes": changes,
            "truncated": len(changes) >= 200,
        }

    @app.get("/admin")
    def admin_root(_: dict[str, str] = Depends(_auth_guard)) -> RedirectResponse:
        return RedirectResponse(url="/admin/email", status_code=307)

    @app.get("/admin/email", response_class=HTMLResponse)
    def page_email(_: dict[str, str] = Depends(_auth_guard)) -> str:
        body = """
            <div class="layout">
              <div class="card">
                <label>规则档位（Profile）</label>
                <select id="profile"><option value="enhanced">增强（enhanced）</option><option value="legacy">保持现状（legacy）</option></select>
                <label>启用开关</label><select id="enabled"><option value="true">启用</option><option value="false">停用</option></select>
                <label>发送时间(小时)</label><input id="hour" type="number" min="0" max="23"/>
                <label>发送时间(分钟)</label><input id="minute" type="number" min="0" max="59"/>
                <label>收件人列表(逗号分隔)</label><input id="recipients" placeholder="a@b.com,c@d.com"/>
                <label>主题模板</label><input id="subject_template" />
                <label>主题前缀（subject_prefix，可选）</label><input id="subject_prefix" placeholder="例如：[Enhanced] "/>
                <div class="small">如果你不想要标题前面的 <code>[Enhanced]</code>，把这里清空并发布即可。</div>
                <label>操作人</label><input id="created_by" value="rules-admin-ui"/>
                <div>
                  <button onclick="saveDraft()">保存草稿并校验</button>
                  <button id="btnPublish" onclick="publishDraft()" disabled>发布生效</button>
                  <span class="pill" id="verPill">生效版本: -</span>
                </div>
                <p id="status"></p>
                <details class="help">
                  <summary>字段说明/用法</summary>
                  <div class="box">
                    <ul>
                      <li><b>规则档位（Profile）</b>：灰度/配置档位。通常用 <code>legacy</code> 保持原行为，用 <code>enhanced</code> 启用增强规则。</li>
                      <li><b>启用开关</b>：关闭后不发送邮件（用于临时停发或维护窗口）。</li>
                      <li><b>发送时间</b>：按北京时间定时触发（GitHub Actions 兜底补发逻辑不受这里影响）。</li>
                      <li><b>收件人列表</b>：逗号分隔邮箱地址（将写入 rules 的 recipient 字段）。</li>
                      <li><b>主题模板</b>：支持占位符（例如 <code>{{date}}</code>），用于生成邮件主题。</li>
                      <li><b>主题前缀</b>：会拼在主题模板前（例如 <code>[Enhanced] </code>）。留空则不加前缀。</li>
                    </ul>
                  </div>
                </details>
              </div>
              <div class="card">
                <label>试跑日期（可选，YYYY-MM-DD）</label><input id="dryrun_date" placeholder="例如：2026-02-18"/>
                <div class="row">
                  <button onclick="preview()">试跑预览(不发信)</button>
                  <button onclick="copyPreview()">复制预览</button>
                </div>
                <div>
                  <label>高亮关键词（逗号分隔，留空自动从采集规则读取包含/排除词）</label>
                  <input id="highlight_terms" />
                </div>
                <pre id="preview" style="min-height:320px">（点击“试跑预览(不发信)”后，这里会显示预览正文）</pre>
                <details class="help">
                  <summary>预览说明</summary>
                  <div class="box">
                    <ul>
                      <li><b>试跑预览</b>：走一遍内容生成链路并输出预览，但不会真实发信。</li>
                      <li><b>高亮关键词</b>：用于在预览里标注命中词；留空会自动读取当前采集规则里的包含/排除词。</li>
                    </ul>
                  </div>
                </details>
              </div>
            </div>
            """
        js = """
        let currentConfig = null;
        let currentDraftId = null;
        function escHtml(s){
          return String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
        }
        function splitCsv(s){ return (s||'').split(',').map(x=>x.trim()).filter(Boolean); }
        function highlightInEscaped(escapedText, terms, className){
          if(!terms || !terms.length) return escapedText;
          let out = escapedText;
          const uniq = Array.from(new Set(terms.filter(t=>t.length>=3)));
          uniq.sort((a,b)=>b.length-a.length);
          for(const t of uniq){
            const re = new RegExp(t.replace(/[.*+?^${}()|[\\]\\\\]/g,'\\\\$&'), 'gi');
            out = out.replace(re, (m)=>`<mark class="${className}">${m}</mark>`);
          }
          return out;
        }
            async function loadActive() {
              const profile = document.getElementById('profile').value;
              const j = await api(`/admin/api/email_rules/active?profile=${encodeURIComponent(profile)}`);
              if (!j || !j.ok) { document.getElementById('status').textContent = JSON.stringify(j); toast('err','加载失败','读取生效配置失败'); return; }
              currentConfig = j.config_json;
              document.getElementById('verPill').textContent = `生效版本: ${j.meta?.version || j.meta?.path || '-'}`;
          document.getElementById('enabled').value = String(!!(currentConfig.overrides||{}).enabled);
          const sw = (currentConfig.defaults||{}).send_window || {};
          document.getElementById('hour').value = sw.hour ?? 8;
          document.getElementById('minute').value = sw.minute ?? 30;
          const rec = (currentConfig.defaults||{}).recipient || '';
          document.getElementById('recipients').value = rec;
          document.getElementById('subject_template').value = (currentConfig.defaults||{}).subject_template || '';
          document.getElementById('subject_prefix').value = (currentConfig.overrides||{}).subject_prefix || '';
              document.getElementById('status').innerHTML = '<span class="ok">已加载生效配置</span>';
          document.getElementById('btnPublish').disabled = true;
          currentDraftId = null;
        }
        function buildConfig() {
          const cfg = JSON.parse(JSON.stringify(currentConfig||{}));
          if (!cfg.defaults) cfg.defaults = {};
          if (!cfg.defaults.send_window) cfg.defaults.send_window = {};
          if (!cfg.overrides) cfg.overrides = {};
          cfg.defaults.send_window.hour = Number(document.getElementById('hour').value||8);
          cfg.defaults.send_window.minute = Number(document.getElementById('minute').value||30);
          cfg.defaults.recipient = document.getElementById('recipients').value.trim();
          cfg.defaults.subject_template = document.getElementById('subject_template').value.trim();
          cfg.overrides.enabled = document.getElementById('enabled').value === 'true';
          cfg.overrides.subject_prefix = document.getElementById('subject_prefix').value;
          cfg.profile = document.getElementById('profile').value;
          cfg.ruleset = 'email_rules';
          return cfg;
        }
            async function saveDraft() {
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/email_rules/draft','POST',{ profile, created_by, config_json: buildConfig() });
          if (!j) return;
          currentDraftId = j.draft?.id || null;
          const errs = j.draft?.validation_errors || [];
              if (j.ok) {
                document.getElementById('status').innerHTML = `<span class="ok">草稿校验通过（draft_id=${currentDraftId}）</span>`;
                document.getElementById('btnPublish').disabled = false;
                toast('ok','草稿校验通过',`draft_id=${currentDraftId}`);
              } else {
                document.getElementById('status').innerHTML = `<span class="err">草稿校验失败</span>\\n${JSON.stringify(errs,null,2)}`;
                document.getElementById('btnPublish').disabled = true;
                toast('err','草稿校验失败', `${errs.length} 处错误`);
              }
            }
        async function publishDraft() {
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/email_rules/publish','POST',{ profile, draft_id: currentDraftId, created_by });
              if (j && j.ok) {
            document.getElementById('status').innerHTML = `<span class="ok">已生效版本号：${j.version}</span>`;
            document.getElementById('btnPublish').disabled = true;
            toast('ok','发布成功', `version=${j.version}`);
            await loadActive();
              } else {
                document.getElementById('status').innerHTML = `<span class="err">发布失败</span>\\n${JSON.stringify(j,null,2)}`;
                toast('err','发布失败', JSON.stringify(j?.errors||j?.error||j));
              }
            }
        async function preview() {
          const profile = document.getElementById('profile').value;
          const date = document.getElementById('dryrun_date').value.trim();
          const previewEl = document.getElementById('preview');
          if(previewEl) previewEl.textContent = '运行中...（不会发信）';
          const j = await api('/admin/api/email_rules/dryrun','POST',{ profile, date });
          if (!j || !j.ok) { document.getElementById('preview').textContent = JSON.stringify(j,null,2); return; }
          let includeTerms = [];
          let excludeTerms = [];
          const custom = splitCsv(document.getElementById('highlight_terms').value);
          if(custom.length){
            includeTerms = custom;
          } else {
            // Avoid freezing the UI by default (keyword packs can be large).
            // If you need highlight, fill highlight_terms explicitly.
            includeTerms = [];
            excludeTerms = [];
          }
              const md = j.preview_markdown || '(空)';
          const base = escHtml(md);
          const html = highlightInEscaped(base, includeTerms, 'hl-inc');
          const html2 = highlightInEscaped(html, excludeTerms, 'hl-exc');
          document.getElementById('preview').innerHTML = html2;
              toast('ok','试跑完成', `运行ID=${j.run_id}`);
        }
        async function copyPreview(){
          const el = document.getElementById('preview');
          const txt = el ? el.textContent : '';
          try { await navigator.clipboard.writeText(txt||''); toast('ok','已复制','预览已复制到剪贴板'); }
          catch(e){ toast('err','复制失败', String(e)); }
        }
        document.getElementById('profile').addEventListener('change', loadActive);
        loadActive();
        """
        style = """
        <style>
          mark.hl-inc { background: #fff2a8; }
          mark.hl-exc { background: #ffb3b3; }
        </style>
        """
        return _page_shell("邮件规则", style + body, js)

    @app.get("/admin/content", response_class=HTMLResponse)
    def page_content(_: dict[str, str] = Depends(_auth_guard)) -> str:
        body = """
                    <div class="layout">
                      <div class="card">
                        <label>规则档位（Profile）</label>
                        <select id="profile"><option value="enhanced">增强（enhanced）</option><option value="legacy">保持现状（legacy）</option></select>
                        <label>24小时优先窗口（primary_hours，小时）</label><input id="primary_hours" type="number" min="1" max="72" placeholder="例如：24"/>
                        <div class="small">优先选择最近 N 小时发布的信息，通常填 24。</div>
                        <label>回补窗口（fallback_days，天）</label><input id="fallback_days" type="number" min="1" max="14" placeholder="例如：7"/>
                        <div class="small">当 24h 条目不足时，从最近 N 天候选池补齐，并标注“7天补充”。</div>

                        <label>条目数量目标（min/max）</label>
                        <div class="row">
                          <input id="items_min" type="number" min="1" max="30"/>
                          <input id="items_max" type="number" min="1" max="50"/>
                        </div>
                        <label>24小时内不足该值则启用7天回补（topup_if_24h_lt）</label>
                        <input id="topup_if_24h_lt" type="number" min="0" max="30"/>

                        <label>区域纠偏目标（apac_min_share / china_min_share / eu_na_min_share，0-1）</label>
                        <div class="row">
                          <input id="apac_min_share" type="number" step="0.01" min="0" max="1"/>
                          <input id="china_min_share" type="number" step="0.01" min="0" max="1"/>
                          <input id="eu_na_min_share" type="number" step="0.01" min="0" max="1" placeholder="欧美占比 0.40"/>
                        </div>

                        <label>关键词包（keywords_pack，多选）</label>
                        <div class="box">
                          <label class="chk"><input type="checkbox" name="keywords_pack" value="ivd_core"/> IVD核心（ivd_core）</label>
                          <label class="chk"><input type="checkbox" name="keywords_pack" value="regulatory_procurement"/> 监管/招采（regulatory_procurement）</label>
                          <label class="chk"><input type="checkbox" name="keywords_pack" value="oncology"/> 肿瘤（oncology）</label>
                          <label class="chk"><input type="checkbox" name="keywords_pack" value="infection"/> 感染（infection）</label>
                          <label class="chk"><input type="checkbox" name="keywords_pack" value="repro_genetics"/> 生殖遗传（repro_genetics）</label>
                          <label class="chk"><input type="checkbox" name="keywords_pack" value="policy_market"/> 政策市场（policy_market）</label>
                          <label class="chk"><input type="checkbox" name="keywords_pack" value="papers_clinical"/> 论文/临床证据（papers_clinical）</label>
                          <div class="small" style="margin-top:6px">提示：关键词包用于“包含过滤器”的默认词库，避免漏抓；可与自定义包含/排除关键词叠加。</div>
                        </div>
                        <label>最低信源可信等级（content_sources.min_trust_tier）</label>
                        <select id="min_trust_tier">
                          <option value="A">A（最严格）</option>
                          <option value="B">B</option>
                          <option value="C">C（最宽松）</option>
                        </select>
                        <label>关键词包含(逗号分隔)</label>
                        <div class="row">
                          <input id="include_keywords"/>
                          <button type="button" onclick="fillIncludeTemplate()">加载推荐模板</button>
                        </div>
                        <div class="small">用于“补盲”（高精度长词）。主覆盖仍建议靠关键词包（keywords_pack）。</div>
                        <label>关键词排除(逗号分隔)</label>
                        <div class="row">
                          <input id="exclude_keywords"/>
                          <button type="button" onclick="fillExcludeTemplate()">加载推荐模板</button>
                        </div>
                        <div class="small">用于“降噪”（财报/股价/纯药物研发）。避免放过泛短词，防止误伤。</div>

                        <label>去重与重复率阈值</label>
                        <div class="row">
                          <input id="title_similarity_threshold" type="number" step="0.01" min="0.5" max="1" placeholder="标题相似度阈值 0.78"/>
                          <input id="daily_max_repeat_rate" type="number" step="0.01" min="0" max="1" placeholder="昨日报重复率上限 0.25"/>
                        </div>
                        <input id="recent_7d_max_repeat_rate" type="number" step="0.01" min="0" max="1" placeholder="近7日峰值重复率上限 0.40"/>

                        <label>故事级聚合（多源同事件合并）</label>
                        <div class="row">
                          <label style="margin:0;min-width:140px">启用</label>
                          <select id="cluster_enabled" style="width:140px"><option value="true">启用</option><option value="false">停用</option></select>
                          <label style="margin:0;min-width:140px">窗口(小时)</label>
                          <input id="cluster_window_hours" type="number" min="1" max="720" style="width:140px"/>
                        </div>
                        <div class="box" style="margin-top:8px">
                          <div class="small" style="margin-bottom:6px">聚合键策略（按顺序尝试，命中即用）</div>
                          <label class="chk"><input type="checkbox" name="cluster_key" value="canonical_url"/> 规范链接（canonical_url）</label>
                          <label class="chk"><input type="checkbox" name="cluster_key" value="normalized_url_host_path"/> 归一化URL（normalized_url_host_path）</label>
                          <label class="chk"><input type="checkbox" name="cluster_key" value="title_fingerprint_v1"/> 标题指纹（title_fingerprint_v1）</label>
                        </div>
                        <div class="box" style="margin-top:8px">
                          <div class="small" style="margin-bottom:6px">主条目选择（同一簇内，按顺序比较）</div>
                          <label class="chk"><input type="checkbox" name="cluster_primary" value="source_priority"/> 信源优先级（source_priority）</label>
                          <label class="chk"><input type="checkbox" name="cluster_primary" value="evidence_grade"/> 证据等级（evidence_grade）</label>
                          <label class="chk"><input type="checkbox" name="cluster_primary" value="published_at_earliest"/> 最早发布日期（published_at_earliest）</label>
                          <label class="chk"><input type="checkbox" name="cluster_primary" value="published_at_latest"/> 最晚发布日期（published_at_latest）</label>
                          <label class="chk"><input type="checkbox" name="cluster_primary" value="first_seen_earliest"/> 最早抓取时间（first_seen_earliest）</label>
                        </div>
                        <label>other_sources 最大保留数（max_other_sources）</label>
                        <input id="cluster_max_other_sources" type="number" min="0" max="20"/>

                        <label>赛道映射（lane_mapping）</label>
                        <div class="row">
                          <button type="button" onclick="fillLaneTemplate()">加载推荐模板</button>
                        </div>
                        <textarea id="lane_mapping" rows="6" placeholder="示例：\n肿瘤检测: 肿瘤, 癌, oncology, cancer\n感染检测: 感染, 病原, virus, influenza\n生殖与遗传检测: 生殖, 遗传, NIPT, prenatal\n其他: 免疫, 代谢, 心血管"></textarea>
                        <div class="small">格式：每行 “标签: 关键词1,关键词2”。用于自动打标签与分栏汇总。</div>
                        <label>技术平台映射（platform_mapping）</label>
                        <div class="row">
                          <button type="button" onclick="fillPlatformTemplate()">加载推荐模板</button>
                        </div>
                        <textarea id="platform_mapping" rows="6" placeholder="示例：\nNGS: ngs, sequencing, wgs\nPCR: pcr, 核酸\n数字PCR: ddpcr, digital pcr, 数字pcr\n免疫诊断（化学发光/ELISA/IHC等）: 化学发光, immunoassay, elisa\nPOCT/分子POCT: poct, rapid test\n微流控/单分子: microfluidic, single molecule"></textarea>
                        <div class="small">用于技术平台雷达（C 段）与标签展示。</div>
                    <label>事件类型映射（event_mapping）</label>
                    <div class="row">
                      <button type="button" onclick="fillEventTemplate()">加载推荐模板</button>
                    </div>
                    <textarea id="event_mapping" rows="6" placeholder="示例：\n监管审批与指南: NMPA, CMDE, FDA, guideline, approval\n并购融资/IPO与合作: acquisition, financing, IPO, partnership\n注册上市: registration, launch\n产品发布: product, assay, kit\n临床与科研证据: clinical, study, trial\n支付与招采: tender, procurement, 招采, 采购\n政策与市场动态: policy, reimbursement, market"></textarea>
                    <div class="small">用于事件类型判定与 QC 面板统计（regulatory/commercial）。</div>
                        <label>地区过滤（逗号分隔）</label><input id="allowed_regions" placeholder="例如：cn,apac,na,eu"/>
                    <label>赛道过滤（多选）</label>
                    <div class="box">
                      <label class="chk"><input type="checkbox" name="track_sel" value="肿瘤检测"/> 肿瘤检测</label>
                      <label class="chk"><input type="checkbox" name="track_sel" value="感染检测"/> 感染检测</label>
                      <label class="chk"><input type="checkbox" name="track_sel" value="生殖与遗传检测"/> 生殖与遗传检测</label>
                      <label class="chk"><input type="checkbox" name="track_sel" value="其他"/> 其他</label>
                      <div class="row" style="margin-top:6px">
                        <button type="button" onclick="selectAllTracks(true)">全选</button>
                        <button type="button" onclick="selectAllTracks(false)">全不选</button>
                        <button type="button" onclick="tracksFromLaneMapping()">从赛道映射提取</button>
                      </div>
                      <div class="small">提示：这里控制“覆盖赛道集合”（影响分栏统计与候选聚焦）。不选则默认使用规则里的 coverage_tracks。</div>
                    </div>
                    <input id="tracks" type="hidden"/>
                    <label>最低可信度（0-1）</label><input id="min_confidence" type="number" step="0.01" min="0" max="1" placeholder="例如：0.6"/>
                    <label>操作人</label><input id="created_by" value="rules-admin-ui"/>
                    <div>
                      <button onclick="saveDraft()">保存草稿并校验</button>
                      <button id="btnPublish" onclick="publishDraft()" disabled>发布生效</button>
                      <span class="pill" id="verPill">生效版本: -</span>
                    </div>
                    <p id="status"></p>
                        <details class="help">
                          <summary>字段说明/用法</summary>
                          <div class="box">
                            <ul>
                              <li><b>24小时优先窗口/回补窗口</b>：严格优先选择过去24小时内信息；不足时从近7天候选池回补，并在每条前标注“24小时内/7天补充”。</li>
                              <li><b>条目数量目标</b>：控制 A 段“今日要点”的候选数量区间。<code>topup_if_24h_lt</code> 用于触发“24h不足则7d回补”。</li>
                              <li><b>区域纠偏目标</b>：用于约束候选集的区域占比（例如 亚太>=40%），不足时可配合 QC 的回补偏好优先补 CN/APAC 监管/招采。</li>
                              <li><b>关键词包含/排除</b>：基础过滤器。包含为空通常表示不过滤；排除用于剔除软文/非相关噪音。</li>
                              <li><b>去重/重复率阈值</b>：标题相似度阈值越高越严格；重复率阈值用于 QC 审计（建议配合 Versions/Dry-run 验证）。</li>
                              <li><b>故事级聚合</b>：同一新闻多源转载只保留 1 条主条目，其他来源挂载到 <code>other_sources</code>（不丢信息）。</li>
                              <li><b>映射规则（lane/platform/event）</b>：用于把标题/摘要中的关键词映射到“赛道/平台/事件类型”标签。支持两种填写方式：每行 <code>标签: 关键词1,关键词2</code>；或直接粘贴 JSON（对象：key 为标签，value 为关键词数组）。</li>
                              <li><b>地区/赛道过滤</b>：用于聚焦某些区域或业务方向（例如 <code>cn</code>/<code>apac</code>、<code>肿瘤</code>/<code>感染</code> 等）。</li>
                              <li><b>最低可信度</b>：0-1 的阈值，低于该值的条目不会入选候选集。</li>
                            </ul>
                          </div>
                        </details>
                  </div>
              <div class="card">
              <label>试跑日期（可选，YYYY-MM-DD）</label><input id="dryrun_date" placeholder="例如：2026-02-18"/>
                <div class="row">
                  <button onclick="preview()">试跑预览(不发信)</button>
                  <button onclick="copyPreview()">复制预览</button>
                </div>
                <div class="drawer" id="summary"></div>
                <div class="cards" id="clusters"></div>
                <details class="help" id="platformDiagBox" style="margin-top:10px; display:none">
                  <summary>未标注诊断（用于补平台关键词）</summary>
                  <div class="box">
                    <div class="small">仅展示“平台=未标注”的原因统计与最多 10 条样例。建议把真实缩写/同义词补到左侧“技术平台映射”。</div>
                    <pre id="platformDiag" style="margin-top:8px"></pre>
                  </div>
                </details>
                <details class="help" id="laneDiagBox" style="margin-top:10px; display:none">
                  <summary>其他赛道诊断（用于补赛道关键词）</summary>
                  <div class="box">
                    <div class="small">仅展示“赛道=其他”的原因统计与最多 10 条样例。建议把关键词补到左侧“赛道映射”。</div>
                    <pre id="laneDiag" style="margin-top:8px"></pre>
                  </div>
                </details>
                <details class="help" id="eventDiagBox" style="margin-top:10px; display:none">
                  <summary>事件类型诊断（用于补事件关键词）</summary>
                  <div class="box">
                    <div class="small">仅展示“未命中映射而走 fallback_heuristic”的统计与最多 10 条样例（说明 event_mapping 覆盖不足或关键词过泛）。</div>
                    <pre id="eventDiag" style="margin-top:8px"></pre>
                  </div>
                </details>
                <details class="help" id="kwPackBox" style="margin-top:10px; display:none">
                  <summary>关键词包命中统计（用于优化抓取漏斗）</summary>
                  <div class="box">
                    <div class="small">展示每个关键词包的命中次数（matched）与最终保留次数（kept）。如果某包 matched 很高但 kept 很低，通常说明关键词过泛或被排除词击中较多。</div>
                    <pre id="kwPack" style="margin-top:8px"></pre>
                  </div>
                </details>
                <details class="help" id="excludeDiagBox" style="margin-top:10px; display:none">
                  <summary>被排除样例诊断（用于修排除词）</summary>
                  <div class="box">
                    <div class="small">展示本次被 exclude 词剔除的数量、被 keep_if 兜底数量，以及最多 15 条样例（命中的排除词）。</div>
                    <pre id="excludeDiag" style="margin-top:8px"></pre>
                  </div>
                </details>
                <details class="help">
                  <summary>预览说明</summary>
                  <div class="box">
                    <ul>
                      <li><b>候选条数</b>：过滤/分类前的入选候选数量。</li>
                      <li><b>聚合后条数</b>：启用 story 聚合后，只保留主条目的数量（other_sources 会挂在主条目下）。</li>
                          <li><b>聚合簇列表</b>：展示聚合规模较大的簇，便于检查去重是否“过度合并”。</li>
                    </ul>
                  </div>
                </details>
              </div>
            </div>
            """
        js = """
                let currentConfig = null;
                let currentDraftId = null;
                let lastDryrun = null;
                function csv(v){ return (v||[]).join(','); }
                function arr(v){ return (v||'').split(',').map(s=>s.trim()).filter(Boolean); }
                function esc(s){ return String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
                function setChecks(name, values){
                  const set = new Set((values||[]).map(String));
                  for(const el of document.querySelectorAll(`input[name="${name}"]`)){
                    el.checked = set.has(String(el.value));
                  }
                }
                function getChecks(name){
                  const out = [];
                  for(const el of document.querySelectorAll(`input[name="${name}"]:checked`)){
                    out.push(String(el.value));
                  }
                  return out;
                }
                function selectAllTracks(v){
                  const vals = v ? ['肿瘤检测','感染检测','生殖与遗传检测','其他'] : [];
                  setChecks('track_sel', vals);
                  document.getElementById('tracks').value = vals.join(',');
                  toast('ok', v ? '已全选赛道' : '已清空赛道', '');
                }
                function tracksFromLaneMapping(){
                  try{
                    const m = parseMapping(document.getElementById('lane_mapping').value);
                    const keys = Object.keys(m||{}).map(x=>String(x||'').trim()).filter(Boolean);
                    if(keys.length){
                      setChecks('track_sel', keys);
                      document.getElementById('tracks').value = keys.join(',');
                      toast('ok','已从赛道映射提取', keys.join(','));
                      return;
                    }
                  }catch(e){}
                  toast('warn','提取失败','请先确保“赛道映射”按每行“标签: 关键词”格式填写');
                }
                function mappingToText(m){
                  if(!m || typeof m !== 'object') return '';
                  const lines = [];
                  for(const k of Object.keys(m)){
                const v = m[k];
                if(Array.isArray(v)){
                  const toks = v.map(x=>String(x||'').trim()).filter(Boolean);
                  if(toks.length) lines.push(`${k}: ${toks.join(', ')}`);
                }
              }
              return lines.join('\\n');
            }
                function parseMapping(text){
              const t = String(text||'').trim();
              if(!t) return {};
              if(t.startsWith('{')){
                try{
                  const obj = JSON.parse(t);
                  if(obj && typeof obj === 'object' && !Array.isArray(obj)) return obj;
                }catch(e){}
              }
              const out = {};
              for(const raw of t.split(/\\r?\\n/)){
                const line = raw.trim();
                if(!line) continue;
                const idx = line.indexOf(':');
                if(idx < 0) continue;
                const label = line.slice(0, idx).trim();
                const rhs = line.slice(idx+1).trim();
                if(!label || !rhs) continue;
                const parts = rhs.split(/[，,;；]/).map(s=>s.trim()).filter(Boolean);
                if(parts.length) out[label] = parts;
              }
                  return out;
                }
                function fillLaneTemplate(){
                  const t = [
                    '肿瘤检测: 肿瘤, 癌, 癌症, 肿瘤标志物, 伴随诊断, ctDNA, MRD, PD-L1, EGFR, ALK, HER2, oncology, cancer, tumor, biomarker, companion diagnostic, IHC',
                    '感染检测: 感染, 病原, 病原体, 细菌, 真菌, 病毒, 耐药, 血培养, 呼吸道, 流感, 新冠, RSV, HPV, HBV, HCV, HIV, infection, pathogen, virus, influenza, sepsis, respiratory',
                    '生殖与遗传检测: 产前, 唐筛, 染色体, CNV, 遗传, 遗传病, 地贫, 不孕不育, 胚胎, PGT, IVF, 新生儿筛查, prenatal, NIPT, fertility, reproductive, genetic, hereditary',
                    '其他: 心肌, 心衰, 心梗, 凝血, D-二聚体, 糖化, HbA1c, 肝肾功能, 炎症, PCT, CRP, 自免, 免疫, 代谢, 心血管, cardiovascular, metabolic, immunology',
                  ].join('\\n');
                  document.getElementById('lane_mapping').value = t;
                  toast('ok','已加载模板','你可以在此基础上删改关键词');
                }
                function fillPlatformTemplate(){
                  const t = [
                    'NGS: 测序, 二代测序, 高通量测序, 全外显子, 全基因组, 靶向测序, panel, NGS, sequencing, next-generation sequencing, WES, WGS, whole exome, whole genome, RNA-seq, targeted sequencing',
                    'PCR: 核酸, 核酸检测, 扩增, 等温扩增, LAMP, PCR, qPCR, RT-PCR, real-time PCR, isothermal, lamp, rt-lamp',
                    '数字PCR: 数字PCR, 数字pcr, 微滴数字PCR, ddPCR, digital PCR, dPCR, dpcr',
                    '流式细胞: 流式, 流式细胞术, flow cytometry, cytometry, FACS',
                    '质谱: 质谱, 串联质谱, LC-MS/MS, MALDI-TOF, mass spec, lc-ms, ms/ms, maldi, maldi-tof',
                    '免疫诊断（化学发光/ELISA/IHC等）: 化学发光, 免疫, 免疫荧光, ELISA, IHC, CLIA, immunoassay, chemiluminescence, lateral flow immunoassay, LFA, 侧向层析, 免疫层析, 胶体金',
                    'POCT/分子POCT: POCT, 即时检测, 快检, 自测, rapid test, self-test, point-of-care, cartridge, sample-to-answer',
                    '微流控/单分子: 微流控, 单分子, 数字免疫, digital immunoassay, microfluidic, lab-on-a-chip, single molecule, Simoa',
                  ].join('\\n');
                  document.getElementById('platform_mapping').value = t;
                  toast('ok','已加载模板','你可以在此基础上删改关键词');
                }
                function fillEventTemplate(){
                  const t = [
                    '监管审批与指南: 通告, 公告, 指导原则, 技术审评, 审评, 批准, 受理, 注册证, 变更, 延续, 召回, 警戒, 安全警示, 飞检, 体系, GMP, GSP, UDI, 追溯, NMPA, CMDE, FDA, PMDA, MFDS, TGA, HSA, guideline, guidance, approval, cleared, clearance, recall, safety alert, field safety',
                    '并购融资/IPO与合作: 并购, 收购, 合并, 投资, 融资, 定增, 战略合作, 合作, 联合开发, 渠道合作, 授权, 许可, IPO, 上市辅导, acquisition, acquire, merger, financing, funding, raise, partnership, collaboration, deal, licensing',
                    '注册上市/产品发布: 获批, 获证, 注册, 上市, 发布, 推出, 新品, 新菜单, 获得认证, CE, 510(k), De Novo, clearance, launch, launched, introduce, new test, new assay, registered, CE mark, FDA clearance',
                    '临床与科研证据: 研究, 临床, 试验, 队列, 回顾性, 前瞻性, 多中心, 验证, 真实世界, RWD, 论文, 发表, study, clinical, trial, evidence, validation, publication, real-world',
                    '支付与招采: 招标, 招采, 采购, 中标, 挂网, 集采, 议价, 目录, 医保, 支付, DRG, DIP, tender, procurement, bid, reimbursement, payment, CCGP',
                    '政策与市场动态: 政策, 市场, 行业, 渠道, 需求, 渗透率, 增长, 价格, 出海, policy, market, guideline, reimbursement, coverage, pricing',
                  ].join('\\n');
                  document.getElementById('event_mapping').value = t;
                  toast('ok','已加载模板','你可以在此基础上删改关键词');
                }
                function fillIncludeTemplate(){
                  const kws = [
                    'digital immunoassay','simoa','maldi-tof','lc-ms/ms','lab-on-a-chip','microfluidic',
                    '伴随诊断','液体活检','mrd','ctdna','ddpcr','dpcr','rt-pcr','wes','wgs',
                    '挂网','drg','dip','医保目录','技术审评','注册证',
                  ];
                  document.getElementById('include_keywords').value = kws.join(',');
                  toast('ok','已加载补盲模板','建议保留高精度长词，避免过泛关键词');
                }
                function fillExcludeTemplate(){
                  const kws = [
                    'earnings','quarterly revenue','eps','dividend','layoff','restructuring',
                    'lawsuit','class action','stock','shares','guidance raised',
                    'phase 3 drug','therapy only','vaccine only','glp-1'
                  ];
                  document.getElementById('exclude_keywords').value = kws.join(',');
                  toast('ok','已加载降噪模板','可结合“被排除样例诊断”继续微调');
                }
            function ensureRule(cfg, type, id, priority){
              let r = (cfg.rules||[]).find(x=>x.type===type);
              if(!r){
                r = {id, enabled:true, priority, type, params:{}};
                cfg.rules = (cfg.rules||[]).concat([r]);
              }
              if(!r.id) r.id = id;
              if(r.priority == null) r.priority = priority;
              if(r.enabled == null) r.enabled = true;
              if(!r.params || typeof r.params !== 'object') r.params = {};
              return r;
            }
                async function loadActive() {
                  const profile = document.getElementById('profile').value;
                  const j = await api(`/admin/api/content_rules/active?profile=${encodeURIComponent(profile)}`);
                  if (!j || !j.ok) { document.getElementById('status').textContent = JSON.stringify(j); toast('err','加载失败','读取生效配置失败'); return; }
                  currentConfig = j.config_json;
                  document.getElementById('verPill').textContent = `生效版本: ${j.meta?.version || j.meta?.path || '-'}`;
                  const tw = (currentConfig.defaults||{}).time_window || {};
                  document.getElementById('primary_hours').value = tw.primary_hours ?? 24;
                  document.getElementById('fallback_days').value = tw.fallback_days ?? 7;
                  const il = (currentConfig.defaults||{}).item_limit || {};
                  document.getElementById('items_min').value = il.min ?? 10;
                  document.getElementById('items_max').value = il.max ?? 15;
                  document.getElementById('topup_if_24h_lt').value = il.topup_if_24h_lt ?? 10;
                  const rf = (currentConfig.defaults||{}).region_filter || {};
                  document.getElementById('apac_min_share').value = rf.apac_min_share ?? 0.4;
                  document.getElementById('china_min_share').value = rf.china_min_share ?? 0.2;
                  document.getElementById('eu_na_min_share').value = rf.eu_na_min_share ?? 0.4;
                  const ov = currentConfig.overrides||{};
                  setChecks('keywords_pack', ov.keywords_pack||[]);
                  const cs = (currentConfig.defaults||{}).content_sources || {};
                  document.getElementById('min_trust_tier').value = cs.min_trust_tier || 'C';
                  const includeRule = ((currentConfig.rules||[]).find(r=>r.type==='include_filter')||{}).params||{};
                  const excludeRule = ((currentConfig.rules||[]).find(r=>r.type==='exclude_filter')||{}).params||{};
                  const laneRule = (currentConfig.rules||[]).find(r=>r.type==='lane_mapping') || {};
                  const platformRule = (currentConfig.rules||[]).find(r=>r.type==='platform_mapping') || {};
                  const eventRule = (currentConfig.rules||[]).find(r=>r.type==='event_mapping') || {};
                  const dedupeRule = (currentConfig.rules||[]).find(r=>r.type==='dedupe' && (r.params||{}).title_similarity_threshold != null) || {};
                  const dp = dedupeRule.params || {};
                  document.getElementById('title_similarity_threshold').value = dp.title_similarity_threshold ?? 0.78;
                  document.getElementById('daily_max_repeat_rate').value = dp.daily_max_repeat_rate ?? 0.25;
                  document.getElementById('recent_7d_max_repeat_rate').value = dp.recent_7d_max_repeat_rate ?? 0.40;
                  const dc = (currentConfig.defaults||{}).dedupe_cluster || {};
                  document.getElementById('cluster_enabled').value = String(dc.enabled ?? false);
                  document.getElementById('cluster_window_hours').value = dc.window_hours ?? 72;
                  document.getElementById('cluster_max_other_sources').value = dc.max_other_sources ?? 5;
                  setChecks('cluster_key', dc.key_strategies || ['canonical_url','normalized_url_host_path','title_fingerprint_v1']);
                  setChecks('cluster_primary', dc.primary_select || ['source_priority','evidence_grade','published_at_earliest']);
                  document.getElementById('include_keywords').value = csv(includeRule.include_keywords||[]);
                  document.getElementById('exclude_keywords').value = csv(excludeRule.exclude_keywords||[]);
                  document.getElementById('lane_mapping').value = mappingToText(laneRule.params||{});
                  document.getElementById('platform_mapping').value = mappingToText(platformRule.params||{});
                  document.getElementById('event_mapping').value = mappingToText(eventRule.params||{});
                  document.getElementById('allowed_regions').value = csv((rf.allowed_regions||[]));
                  const cov = (currentConfig.defaults||{}).coverage_tracks||[];
                  document.getElementById('tracks').value = csv(cov);
                  setChecks('track_sel', cov && cov.length ? cov : ['肿瘤检测','感染检测','生殖与遗传检测','其他']);
                  document.getElementById('min_confidence').value = Number(((currentConfig.overrides||{}).min_confidence ?? 0));
                  document.getElementById('status').innerHTML = '<span class="ok">已加载生效配置</span>';
                  document.getElementById('btnPublish').disabled = true;
                  currentDraftId = null;
                }
                function buildConfig() {
                  const cfg = JSON.parse(JSON.stringify(currentConfig||{}));
                  if (!cfg.defaults) cfg.defaults = {};
                  if (!cfg.defaults.time_window) cfg.defaults.time_window = {};
                  if (!cfg.defaults.item_limit) cfg.defaults.item_limit = {};
                  if (!cfg.defaults.region_filter) cfg.defaults.region_filter = {};
                  if (!cfg.defaults.dedupe_cluster) cfg.defaults.dedupe_cluster = {};
                  if (!cfg.defaults.content_sources) cfg.defaults.content_sources = {};
                  if (!cfg.defaults.region_filter) cfg.defaults.region_filter = {};
                  if (!cfg.overrides) cfg.overrides = {};
                  cfg.defaults.time_window.primary_hours = Number(document.getElementById('primary_hours').value||24);
                  cfg.defaults.time_window.fallback_days = Number(document.getElementById('fallback_days').value||7);
                  cfg.defaults.item_limit.min = Number(document.getElementById('items_min').value||10);
                  cfg.defaults.item_limit.max = Number(document.getElementById('items_max').value||15);
                  cfg.defaults.item_limit.topup_if_24h_lt = Number(document.getElementById('topup_if_24h_lt').value||10);
                  cfg.defaults.region_filter.allowed_regions = arr(document.getElementById('allowed_regions').value);
                  cfg.defaults.region_filter.apac_min_share = Number(document.getElementById('apac_min_share').value||0.4);
                  cfg.defaults.region_filter.china_min_share = Number(document.getElementById('china_min_share').value||0.2);
                  cfg.defaults.region_filter.eu_na_min_share = Number(document.getElementById('eu_na_min_share').value||0.4);
                  const selectedTracks = getChecks('track_sel');
                  cfg.defaults.coverage_tracks = selectedTracks.length ? selectedTracks : arr(document.getElementById('tracks').value);
                  cfg.overrides.min_confidence = Number(document.getElementById('min_confidence').value||0);
                  cfg.overrides.keywords_pack = getChecks('keywords_pack');
                  cfg.defaults.content_sources.min_trust_tier = document.getElementById('min_trust_tier').value || 'C';
                  cfg.profile = document.getElementById('profile').value;
                  cfg.ruleset = 'content_rules';
              const includeKeywords = arr(document.getElementById('include_keywords').value);
              const excludeKeywords = arr(document.getElementById('exclude_keywords').value);
              let includeRule = (cfg.rules||[]).find(r=>r.type==='include_filter');
              if (!includeRule) { includeRule = {id:'include-ui',enabled:true,priority:10,type:'include_filter',params:{}}; cfg.rules = (cfg.rules||[]).concat([includeRule]); }
              includeRule.params = includeRule.params || {};
              includeRule.params.include_keywords = includeKeywords;
              let excludeRule = (cfg.rules||[]).find(r=>r.type==='exclude_filter');
              if (!excludeRule) { excludeRule = {id:'exclude-ui',enabled:true,priority:20,type:'exclude_filter',params:{}}; cfg.rules = (cfg.rules||[]).concat([excludeRule]); }
              excludeRule.params = excludeRule.params || {};
              excludeRule.params.exclude_keywords = excludeKeywords;

              const laneMap = parseMapping(document.getElementById('lane_mapping').value);
              const platformMap = parseMapping(document.getElementById('platform_mapping').value);
                  const eventMap = parseMapping(document.getElementById('event_mapping').value);
                  const laneR = ensureRule(cfg, 'lane_mapping', 'lane-map-ui', 30);
                  laneR.params = laneMap;
                  laneR.enabled = Object.keys(laneMap||{}).length > 0;
                  const platR = ensureRule(cfg, 'platform_mapping', 'platform-map-ui', 40);
                  platR.params = platformMap;
                  platR.enabled = Object.keys(platformMap||{}).length > 0;
                  const eventR = ensureRule(cfg, 'event_mapping', 'event-map-ui', 50);
                  eventR.params = eventMap;
                  eventR.enabled = Object.keys(eventMap||{}).length > 0;

                  // Dedupe thresholds (title repeat controls)
                  const dedupeT = ensureRule(cfg, 'dedupe', 'dedupe-ui-thresholds', 70);
                  dedupeT.params = dedupeT.params || {};
                  dedupeT.params.title_similarity_threshold = Number(document.getElementById('title_similarity_threshold').value||0.78);
                  dedupeT.params.daily_max_repeat_rate = Number(document.getElementById('daily_max_repeat_rate').value||0.25);
                  dedupeT.params.recent_7d_max_repeat_rate = Number(document.getElementById('recent_7d_max_repeat_rate').value||0.40);

                  // Story clustering config lives in defaults (and optionally mirrored into a dedupe rule).
                  cfg.defaults.dedupe_cluster.enabled = document.getElementById('cluster_enabled').value === 'true';
                  cfg.defaults.dedupe_cluster.window_hours = Number(document.getElementById('cluster_window_hours').value||72);
                  cfg.defaults.dedupe_cluster.key_strategies = getChecks('cluster_key');
                  cfg.defaults.dedupe_cluster.primary_select = getChecks('cluster_primary');
                  cfg.defaults.dedupe_cluster.max_other_sources = Number(document.getElementById('cluster_max_other_sources').value||5);
                  const dedupeC = (cfg.rules||[]).find(r=>r.type==='dedupe' && (r.params||{}).dedupe_cluster) || null;
                  if(dedupeC){
                    dedupeC.params = dedupeC.params || {};
                    dedupeC.params.dedupe_cluster = JSON.parse(JSON.stringify(cfg.defaults.dedupe_cluster));
                  }

                  return cfg;
                }
            async function saveDraft() {
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/content_rules/draft','POST',{ profile, created_by, config_json: buildConfig() });
          if (!j) return;
          currentDraftId = j.draft?.id || null;
          const errs = j.draft?.validation_errors || [];
              if (j.ok) {
                document.getElementById('status').innerHTML = `<span class="ok">草稿校验通过（draft_id=${currentDraftId}）</span>`;
                document.getElementById('btnPublish').disabled = false;
                toast('ok','草稿校验通过',`draft_id=${currentDraftId}`);
              } else {
                document.getElementById('status').innerHTML = `<span class="err">草稿校验失败</span>\\n${JSON.stringify(errs,null,2)}`;
                document.getElementById('btnPublish').disabled = true;
                toast('err','草稿校验失败', `${errs.length} 处错误`);
              }
            }
        async function publishDraft() {
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/content_rules/publish','POST',{ profile, draft_id: currentDraftId, created_by });
              if (j && j.ok) {
            document.getElementById('status').innerHTML = `<span class="ok">已生效版本号：${j.version}</span>`;
            document.getElementById('btnPublish').disabled = true;
            toast('ok','发布成功', `version=${j.version}`);
            await loadActive();
              } else {
                document.getElementById('status').innerHTML = `<span class="err">发布失败</span>\\n${JSON.stringify(j,null,2)}`;
                toast('err','发布失败', JSON.stringify(j?.errors||j?.error||j));
              }
            }
        async function preview() {
          const profile = document.getElementById('profile').value;
          const date = document.getElementById('dryrun_date').value.trim();
          // Show loading state immediately so users know the click is effective.
          const summaryEl = document.getElementById('summary');
          const clustersEl = document.getElementById('clusters');
          const pdBox = document.getElementById('platformDiagBox');
          const pdEl = document.getElementById('platformDiag');
          const kwBox = document.getElementById('kwPackBox');
          const kwEl = document.getElementById('kwPack');
          const exBox = document.getElementById('excludeDiagBox');
          const exEl = document.getElementById('excludeDiag');
          const ldBox = document.getElementById('laneDiagBox');
          const ldEl = document.getElementById('laneDiag');
          const edBox = document.getElementById('eventDiagBox');
          const edEl = document.getElementById('eventDiag');
          if (summaryEl) summaryEl.textContent = '运行中...（不会发信）';
          if (clustersEl) clustersEl.innerHTML = '<div class="small">运行中...</div>';
          if (pdBox) pdBox.style.display = 'none';
          if (pdEl) pdEl.textContent = '';
          if (kwBox) kwBox.style.display = 'none';
          if (kwEl) kwEl.textContent = '';
          if (exBox) exBox.style.display = 'none';
          if (exEl) exEl.textContent = '';
          if (ldBox) ldBox.style.display = 'none';
          if (ldEl) ldEl.textContent = '';
          if (edBox) edBox.style.display = 'none';
          if (edEl) edEl.textContent = '';
          const j = await api('/admin/api/content_rules/dryrun','POST',{ profile, date });
          lastDryrun = j;
          if (!j || !j.ok) { document.getElementById('summary').textContent = JSON.stringify(j,null,2); return; }
              document.getElementById('summary').innerHTML = `
                <div class="kvs">
                  <div>运行ID</div><b>${esc(j.run_id||'')}</b>
                  <div>候选条数</div><b>${esc(j.items_before_count)}</b>
                  <div>聚合后条数</div><b>${esc(j.items_after_count)}</b>
                  <div>要点条数(A段)</div><b>${esc(j.items_count)}</b>
                </div>`;
          const cards = (j.top_clusters||[]).map(c=>{
            const p = c.primary||{};
            const title = esc(p.title||'');
            const why = esc(c.dedupe_reason||c.key_strategy||'');
            const size = esc(c.cluster_size||0);
                const links = (c.other_sources||[]).map(o=>`<div class="small"><a href="${esc(o.url||'')}" target="_blank">${esc(o.source||'信源')}</a> ${esc(o.published_at||'')}</div>`).join('');
                return `<div class="clusterCard">
                  <div class="row"><div class="clusterTitle grow">${title}</div><span class="pill">数量 ${size}</span></div>
                  <div class="small">${why}</div>
                  <div style="margin-top:6px">${links}</div>
                </div>`;
          }).join('');
          document.getElementById('clusters').innerHTML = cards || '<div class="small">无可展示聚合簇</div>';
              const pd = j.platform_diag || {};
              if(pd && typeof pd === 'object' && (pd.unlabeled_count || (pd.samples||[]).length)){
                if(pdEl) pdEl.textContent = JSON.stringify(pd, null, 2);
                if(pdBox) pdBox.style.display = 'block';
              }
              const kw = j.keyword_pack_stats || {};
              if(kw && typeof kw === 'object' && (kw.candidates_checked || Object.keys(kw.packs||{}).length)){
                if(kwEl) kwEl.textContent = JSON.stringify(kw, null, 2);
                if(kwBox) kwBox.style.display = 'block';
              }
              const ex = j.exclude_diag || {};
              if(ex && typeof ex === 'object' && ((ex.excluded_count||0) > 0 || (ex.rescued_count||0) > 0 || (ex.samples||[]).length)){
                if(exEl) exEl.textContent = JSON.stringify(ex, null, 2);
                if(exBox) exBox.style.display = 'block';
              }
              const ld = j.lane_diag || {};
              if(ld && typeof ld === 'object' && (ld.other_count || (ld.samples||[]).length)){
                if(ldEl) ldEl.textContent = JSON.stringify(ld, null, 2);
                if(ldBox) ldBox.style.display = 'block';
              }
              const ed = j.event_diag || {};
              if(ed && typeof ed === 'object' && (ed.fallback_count || (ed.samples||[]).length)){
                if(edEl) edEl.textContent = JSON.stringify(ed, null, 2);
                if(edBox) edBox.style.display = 'block';
              }
              toast('ok','试跑完成', `运行ID=${j.run_id}`);
        }
        async function copyPreview(){
          if(!lastDryrun){
            toast('warn','尚无预览','请先点击“试跑预览(不发信)”');
            return;
          }
          const txt = JSON.stringify(lastDryrun, null, 2);
          try { await navigator.clipboard.writeText(txt||''); toast('ok','已复制','已复制本次试跑 JSON 到剪贴板'); }
          catch(e){ toast('err','复制失败', String(e)); }
        }
        document.getElementById('profile').addEventListener('change', loadActive);
        loadActive();
        """
        return _page_shell("采集规则", body, js)

    @app.get("/admin/qc", response_class=HTMLResponse)
    def page_qc(_: dict[str, str] = Depends(_auth_guard)) -> str:
        body = """
        <div class="layout">
          <div class="card">
            <label>规则档位（Profile）</label>
            <select id="profile"><option value="enhanced">增强（enhanced）</option><option value="legacy">保持现状（legacy）</option></select>
            <label>启用开关</label><select id="enabled"><option value="true">启用</option><option value="false">停用</option></select>

            <label>24小时内最低条数（min_24h_items）</label><input id="min_24h_items" type="number" min="0" max="50"/>
            <label>回补天数（fallback_days）</label><input id="fallback_days" type="number" min="1" max="14"/>
            <label>7天回补上限（7d_topup_limit）</label><input id="topup_limit" type="number" min="0" max="200"/>

            <label>亚太占比目标（apac_min_share, 0-1）</label><input id="apac_min_share" type="number" step="0.01" min="0" max="1"/>
            <label>中国占比目标（china_min_share, 0-1）</label><input id="china_min_share" type="number" step="0.01" min="0" max="1"/>

            <label>昨日报重复率上限（daily_repeat_rate_max, 0-1）</label><input id="daily_repeat_rate_max" type="number" step="0.01" min="0" max="1"/>
            <label>近7日峰值重复率上限（recent_7d_repeat_rate_max, 0-1）</label><input id="recent_7d_repeat_rate_max" type="number" step="0.01" min="0" max="1"/>

            <label>必查信源清单（从“信源管理”强绑定选择）</label>
            <select id="required_sources_ids" multiple size="10" style="height:220px"></select>
            <div class="small">用于 G 段审计：检查当日入选条目是否命中这些 <code>source_id</code>。按住 <code>Cmd</code>/<code>Shift</code> 可多选。</div>
            <div class="small" id="required_sources_note"></div>

            <label>传闻标记开关（rumor_policy.enabled）</label><select id="rumor_enabled"><option value="true">启用</option><option value="false">停用</option></select>
            <label>传闻触发词（rumor_policy.trigger_terms，逗号分隔）</label><input id="rumor_terms" placeholder="rumor,unconfirmed,据传,传闻"/>
            <label>传闻标签（rumor_policy.label）</label><input id="rumor_label" placeholder="传闻（未确认）"/>

            <label>QC fail 策略（fail_policy.mode）</label>
            <select id="fail_mode">
              <option value="only_warn">仅提示（only_warn）</option>
              <option value="auto_topup">自动补齐（auto_topup）</option>
              <option value="degrade_output_legacy">降级输出（degrade_output_legacy）</option>
              <option value="require_manual_review">需要人工复核（require_manual_review）</option>
            </select>
            <div class="small">建议先 dry-run 看 QC 面板再发布；“自动补齐”只会在候选池内二次选择，不会重新抓网。</div>
            <label>回补偏好（fail_policy.topup_prefer，逗号分隔）</label>
            <input id="topup_prefer" placeholder="regulatory_cn,regulatory_apac,procurement_cn"/>

            <label>事件类型结构目标（可选：regulatory_vs_commercial_mix）</label>
            <div class="row">
              <select id="mix_enabled" style="width:140px"><option value="false">关闭</option><option value="true">启用</option></select>
              <input id="mix_reg_min" type="number" step="0.01" min="0" max="1" placeholder="监管占比下限 0.25"/>
              <input id="mix_com_min" type="number" step="0.01" min="0" max="1" placeholder="商业占比下限 0.35"/>
            </div>

            <label>条目字段齐全检查（completeness_policy）</label>
            <div class="row">
              <select id="comp_enabled" style="width:140px"><option value="true">启用</option><option value="false">停用</option></select>
              <input id="comp_min_share" type="number" step="0.01" min="0" max="1" placeholder="最小合格占比 1.00"/>
            </div>
            <label>必填字段（逗号分隔）</label>
            <input id="comp_fields" placeholder="title,summary,published,link,region,lane,platform,event_type"/>
            <label>摘要句数要求（min/max）</label>
            <div class="row">
              <input id="comp_sum_min" type="number" min="1" max="10"/>
              <input id="comp_sum_max" type="number" min="1" max="10"/>
            </div>

            <label>操作人</label><input id="created_by" value="rules-admin-ui"/>
            <div>
              <button onclick="saveDraft()">保存草稿并校验</button>
              <button id="btnPublish" onclick="publishDraft()" disabled>发布生效</button>
              <span class="pill" id="verPill">生效版本: -</span>
            </div>
            <p id="status"></p>
            <details class="help">
              <summary>字段说明/用法</summary>
              <div class="box">
                <ul>
                  <li><b>min_24h_items</b>：过去24小时有效条目不足时视为 QC 风险。</li>
                  <li><b>fallback_days / 7d_topup_limit</b>：回补窗口与回补上限（回补仅从本次候选池选择，不重新抓网）。</li>
                  <li><b>required_sources_checklist</b>：必查信源命中审计项（G 段展示）。</li>
                  <li><b>fail_policy.mode</b>：QC 未达标时的动作策略（仅 dry-run 生效，不影响线上定时）。</li>
                  <li><b>completeness_policy</b>：用于保证每条“标题/摘要/日期/链接/地区/赛道/平台/事件类型”等字段齐全，并校验摘要句数（建议 2-3 句）。</li>
                </ul>
              </div>
            </details>
          </div>
          <div class="card">
            <label>试跑日期（可选，YYYY-MM-DD）</label><input id="dryrun_date" placeholder="例如：2026-02-18"/>
            <div class="row">
              <button onclick="preview()">试跑预览(不发信)</button>
              <button onclick="copyPreview()">复制预览</button>
            </div>
            <h4>QC 面板</h4>
            <pre id="qcPanel"></pre>
            <h4>A–G 预览</h4>
            <pre id="preview"></pre>
          </div>
        </div>
        """
        js = """
        let currentConfig = null;
        let currentDraftId = null;
        function splitCsv(s){ return (s||'').split(',').map(x=>x.trim()).filter(Boolean); }
        function setMultiSelect(id, values){
          const set = new Set((values||[]).map(String));
          const el = document.getElementById(id);
          if(!el) return;
          for(const opt of el.options){
            opt.selected = set.has(String(opt.value));
          }
        }
        function getMultiSelect(id){
          const el = document.getElementById(id);
          if(!el) return [];
          const out = [];
          for(const opt of el.selectedOptions){ out.push(String(opt.value)); }
          return out;
        }
        function esc2(s){ return esc(String(s||'')); }
        async function loadSources(){
          const j = await api('/admin/api/sources','GET',null);
          if(!j||!j.ok) return { ids: new Set(), byName: {}, sources: [] };
          const sources = (j.sources||[]).filter(x=>x && typeof x==='object');
          sources.sort((a,b)=> String(a.name||'').localeCompare(String(b.name||'')) || String(a.id||'').localeCompare(String(b.id||'')));
          const sel = document.getElementById('required_sources_ids');
          if(sel){
            sel.innerHTML = '';
            for(const s of sources){
              const id = String(s.id||'').trim();
              if(!id) continue;
              const name = String(s.name||id);
              const opt = document.createElement('option');
              opt.value = id;
              opt.textContent = `${name} [${id}]`;
              sel.appendChild(opt);
            }
          }
          const ids = new Set(sources.map(s=>String(s.id||'').trim()).filter(Boolean));
          const byName = {};
          for(const s of sources){
            const nm = String(s.name||'').trim().toLowerCase();
            const id = String(s.id||'').trim();
            if(nm && id && !byName[nm]) byName[nm] = id;
          }
          return { ids, byName, sources };
        }

        async function loadActive(){
          const profile = document.getElementById('profile').value;
          const j = await api(`/admin/api/qc_rules/active?profile=${encodeURIComponent(profile)}`);
          if(!j||!j.ok){ document.getElementById('status').textContent = JSON.stringify(j); toast('err','加载失败','读取生效配置失败'); return; }
          currentConfig = j.config_json;
          const src = await loadSources();
          document.getElementById('verPill').textContent = `生效版本: ${j.meta?.version || j.meta?.path || '-'}`;
          document.getElementById('enabled').value = String(!!(currentConfig.overrides||{}).enabled);
          const d = currentConfig.defaults||{};
          document.getElementById('min_24h_items').value = d.min_24h_items ?? 10;
          document.getElementById('fallback_days').value = d.fallback_days ?? 7;
          document.getElementById('topup_limit').value = d['7d_topup_limit'] ?? 20;
          document.getElementById('apac_min_share').value = d.apac_min_share ?? 0.4;
          document.getElementById('china_min_share').value = d.china_min_share ?? 0.2;
          document.getElementById('daily_repeat_rate_max').value = d.daily_repeat_rate_max ?? 0.25;
          document.getElementById('recent_7d_repeat_rate_max').value = d.recent_7d_repeat_rate_max ?? 0.4;
          const qp = d.quality_policy || {};
          // Back-compat: prefer new quality_policy.required_sources_checklist if present.
          const reqRaw = (Array.isArray(qp.required_sources_checklist) && qp.required_sources_checklist.length) ? qp.required_sources_checklist : (d.required_sources_checklist||[]);
          // Strong binding: map legacy names to source_id when possible.
          const selected = [];
          const missing = [];
          for(const x of (reqRaw||[])){
            const v = String(x||'').trim();
            if(!v) continue;
            if(src.ids.has(v)){ selected.push(v); continue; }
            const mapped = src.byName[String(v).toLowerCase()];
            if(mapped && src.ids.has(mapped)){ selected.push(mapped); continue; }
            missing.push(v);
          }
          setMultiSelect('required_sources_ids', selected);
          const note = document.getElementById('required_sources_note');
          if(note){
            note.innerHTML = missing.length
              ? `提示：有 ${missing.length} 个旧值无法映射为 source_id：<code>${esc2(missing.join(', '))}</code>。建议到“信源管理”创建对应信源后再选择。`
              : '提示：已按 source_id 强绑定（建议后续只用本选择器管理清单）。';
          }
          const rp = d.rumor_policy||{};
          document.getElementById('rumor_enabled').value = String(!!rp.enabled);
          document.getElementById('rumor_terms').value = (rp.trigger_terms||[]).join(',');
          document.getElementById('rumor_label').value = rp.label || '传闻（未确认）';
          const fp = d.fail_policy||{};
          document.getElementById('fail_mode').value = fp.mode || 'only_warn';
          document.getElementById('topup_prefer').value = (fp.topup_prefer||[]).join(',');
          const mix = d.regulatory_vs_commercial_mix || {};
          document.getElementById('mix_enabled').value = String(!!mix.enabled);
          document.getElementById('mix_reg_min').value = mix.regulatory_min ?? 0.25;
          document.getElementById('mix_com_min').value = mix.commercial_min ?? 0.35;
          const cp = d.completeness_policy || {};
          document.getElementById('comp_enabled').value = String(cp.enabled ?? true);
          document.getElementById('comp_min_share').value = cp.min_complete_share ?? 1.0;
          document.getElementById('comp_fields').value = (cp.required_fields||[]).join(',');
          document.getElementById('comp_sum_min').value = cp.summary_sentences_min ?? 2;
          document.getElementById('comp_sum_max').value = cp.summary_sentences_max ?? 3;
          document.getElementById('btnPublish').disabled = true;
          currentDraftId = null;
          document.getElementById('status').innerHTML = '<span class="ok">已加载生效配置</span>';
        }

        function buildConfig(){
          const cfg = JSON.parse(JSON.stringify(currentConfig||{}));
          if(!cfg.defaults) cfg.defaults = {};
          if(!cfg.overrides) cfg.overrides = {};
          cfg.ruleset = 'qc_rules';
          cfg.profile = document.getElementById('profile').value;
          cfg.overrides.enabled = document.getElementById('enabled').value === 'true';
          cfg.defaults.timezone = cfg.defaults.timezone || 'Asia/Shanghai';
          cfg.defaults.min_24h_items = Number(document.getElementById('min_24h_items').value||10);
          cfg.defaults.fallback_days = Number(document.getElementById('fallback_days').value||7);
          cfg.defaults['7d_topup_limit'] = Number(document.getElementById('topup_limit').value||20);
          cfg.defaults.apac_min_share = Number(document.getElementById('apac_min_share').value||0.4);
          cfg.defaults.china_min_share = Number(document.getElementById('china_min_share').value||0.2);
          cfg.defaults.daily_repeat_rate_max = Number(document.getElementById('daily_repeat_rate_max').value||0.25);
          cfg.defaults.recent_7d_repeat_rate_max = Number(document.getElementById('recent_7d_repeat_rate_max').value||0.4);
          const reqList = getMultiSelect('required_sources_ids');
          cfg.defaults.required_sources_checklist = reqList;
          // v2 structured field (prompt7)
          cfg.defaults.quality_policy = cfg.defaults.quality_policy || {};
          cfg.defaults.quality_policy.required_sources_checklist = reqList;
          // Keep the rules[] required_sources rule consistent with defaults (if present).
          cfg.rules = cfg.rules || [];
          let rr = (cfg.rules||[]).find(r=>r && r.type==='required_sources');
          if(!rr){
            rr = { id:'required-sources-ui', enabled:true, priority:80, type:'required_sources', params:{} };
            cfg.rules = (cfg.rules||[]).concat([rr]);
          }
          rr.params = rr.params || {};
          rr.params.required_sources_checklist = reqList;
          cfg.defaults.rumor_policy = {
            enabled: document.getElementById('rumor_enabled').value === 'true',
            trigger_terms: splitCsv(document.getElementById('rumor_terms').value),
            label: document.getElementById('rumor_label').value.trim() || '传闻（未确认）',
          };
          cfg.defaults.fail_policy = {
            mode: document.getElementById('fail_mode').value,
            topup_prefer: splitCsv(document.getElementById('topup_prefer').value),
          };
          cfg.defaults.regulatory_vs_commercial_mix = {
            enabled: document.getElementById('mix_enabled').value === 'true',
            regulatory_min: Number(document.getElementById('mix_reg_min').value||0.25),
            commercial_min: Number(document.getElementById('mix_com_min').value||0.35),
          };
          cfg.defaults.completeness_policy = {
            enabled: document.getElementById('comp_enabled').value === 'true',
            min_complete_share: Number(document.getElementById('comp_min_share').value||1.0),
            required_fields: splitCsv(document.getElementById('comp_fields').value),
            summary_sentences_min: Number(document.getElementById('comp_sum_min').value||2),
            summary_sentences_max: Number(document.getElementById('comp_sum_max').value||3),
          };
          cfg.output = cfg.output || { format: 'json', panel_enabled: true };
          return cfg;
        }

        async function saveDraft(){
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/qc_rules/draft','POST',{ profile, created_by, config_json: buildConfig() });
          currentDraftId = j.draft?.id || null;
          const errs = j.draft?.validation_errors || [];
          if(j.ok){
            document.getElementById('status').innerHTML = `<span class="ok">草稿校验通过（draft_id=${currentDraftId}）</span>`;
            document.getElementById('btnPublish').disabled = false;
            toast('ok','草稿校验通过',`draft_id=${currentDraftId}`);
          } else {
            document.getElementById('status').innerHTML = `<span class="err">草稿校验失败</span>\\n${JSON.stringify(errs,null,2)}`;
            document.getElementById('btnPublish').disabled = true;
            toast('err','草稿校验失败', `${errs.length} 处错误`);
          }
        }

        async function publishDraft(){
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/qc_rules/publish','POST',{ profile, draft_id: currentDraftId, created_by });
          if(j && j.ok){
            document.getElementById('status').innerHTML = `<span class="ok">已生效版本号：${j.version}</span>`;
            document.getElementById('btnPublish').disabled = true;
            toast('ok','发布成功', `version=${j.version}`);
            await loadActive();
          } else {
            document.getElementById('status').innerHTML = `<span class="err">发布失败</span>\\n${JSON.stringify(j,null,2)}`;
            toast('err','发布失败', JSON.stringify(j?.errors||j?.error||j));
          }
        }

        async function preview(){
          const profile = document.getElementById('profile').value;
          const date = document.getElementById('dryrun_date').value.trim();
          document.getElementById('qcPanel').textContent = '运行中...';
          document.getElementById('preview').textContent = '';
          const j = await api(`/admin/api/dryrun?lite=1&profile=${encodeURIComponent(profile)}&date=${encodeURIComponent(date)}`,'POST',null);
          if(!j||!j.ok){
            document.getElementById('qcPanel').textContent = '';
            document.getElementById('preview').textContent = JSON.stringify(j,null,2);
            return;
          }
          document.getElementById('qcPanel').textContent = JSON.stringify(j.qc_report||{}, null, 2);
          document.getElementById('preview').textContent = j.preview_text || '';
          toast('ok','试跑完成', `运行ID=${j.run_id}`);
        }
        async function copyPreview(){
          const txt = document.getElementById('preview').textContent || '';
          try { await navigator.clipboard.writeText(txt||''); toast('ok','已复制','预览已复制到剪贴板'); }
          catch(e){ toast('err','复制失败', String(e)); }
        }

        document.getElementById('profile').addEventListener('change', loadActive);
        loadActive();
        """
        return _page_shell("质控规则", body, js)

    @app.get("/admin/output", response_class=HTMLResponse)
    def page_output(_: dict[str, str] = Depends(_auth_guard)) -> str:
        body = """
        <div class="layout">
          <div class="card">
            <label>规则档位（Profile）</label>
            <select id="profile"><option value="enhanced">增强（enhanced）</option><option value="legacy">保持现状（legacy）</option></select>
            <label>启用开关</label><select id="enabled"><option value="true">启用</option><option value="false">停用</option></select>

            <label>栏目顺序（A..G，逗号分隔，G 必须最后）</label>
            <input id="sections_order" placeholder="A,B,C,D,E,F,G"/>
            <div class="small">强约束：G 段必须置尾；A-F 不允许出现质量指标字段。</div>
            <label>栏目开关（sections.enabled）</label>
            <div class="box">
              <label class="chk"><input type="checkbox" name="sec_enabled" value="A"/> A 今日要点</label>
              <label class="chk"><input type="checkbox" name="sec_enabled" value="B"/> B 分赛道速览</label>
              <label class="chk"><input type="checkbox" name="sec_enabled" value="C"/> C 技术平台雷达</label>
              <label class="chk"><input type="checkbox" name="sec_enabled" value="D"/> D 区域热力图</label>
              <label class="chk"><input type="checkbox" name="sec_enabled" value="E"/> E 趋势判断</label>
              <label class="chk"><input type="checkbox" name="sec_enabled" value="F"/> F 信息缺口</label>
              <label class="chk"><input type="checkbox" name="sec_enabled" value="G"/> G 质量指标</label>
            </div>

            <label>A 段条数最小/最大</label>
            <div class="row"><input id="a_min" type="number" min="1" max="30"/><input id="a_max" type="number" min="1" max="30"/></div>
            <label>A 段摘要句数（min/max）</label>
            <div class="row"><input id="sum_min" type="number" min="1" max="5"/><input id="sum_max" type="number" min="1" max="5"/></div>
            <label>A 段摘要最大字数</label><input id="sum_chars" type="number" min="50" max="2000"/>

            <label>展示标签（show_tags）</label><select id="show_tags"><option value="true">展示</option><option value="false">不展示</option></select>
            <label>展示 other_sources（show_other_sources）</label><select id="show_other_sources"><option value="true">展示</option><option value="false">不展示</option></select>
            <label>展示来源链接（show_source_link）</label><select id="show_source_link"><option value="true">展示</option><option value="false">不展示</option></select>

            <label>趋势判断条数（trends_count）</label><input id="trends_count" type="number" min="1" max="10"/>
            <label>缺口清单条数（gaps_count min/max）</label>
            <div class="row"><input id="gaps_min" type="number" min="1" max="10"/><input id="gaps_max" type="number" min="1" max="10"/></div>
            <label>热力图区域（heatmap_regions，逗号分隔）</label>
            <input id="heatmap_regions" placeholder="北美,欧洲,亚太,中国"/>

            <label>风格（style）</label>
            <div class="row">
              <select id="style_lang" style="width:140px">
                <option value="zh">中文(zh)</option>
                <option value="en">英文(en)</option>
              </select>
              <select id="style_tone" class="grow">
                <option value="concise_decision">简洁可决策</option>
                <option value="neutral">中性</option>
              </select>
              <select id="style_no_fluff" style="width:140px"><option value="true">不写空话</option><option value="false">允许更长</option></select>
            </div>

            <label>操作人</label><input id="created_by" value="rules-admin-ui"/>
            <div>
              <button onclick="saveDraft()">保存草稿并校验</button>
              <button id="btnPublish" onclick="publishDraft()" disabled>发布生效</button>
              <span class="pill" id="verPill">生效版本: -</span>
            </div>
            <p id="status"></p>
          </div>
          <div class="card">
            <label>试跑日期（可选，YYYY-MM-DD）</label><input id="dryrun_date" placeholder="例如：2026-02-18"/>
            <div class="row">
              <button onclick="preview()">试跑预览(不发信)</button>
              <button onclick="copyPreview()">复制预览</button>
            </div>
            <h4>QC 面板</h4>
            <pre id="qcPanel"></pre>
            <h4>A–G 预览</h4>
            <pre id="preview"></pre>
          </div>
        </div>
        """
        js = """
        let currentConfig = null;
        let currentDraftId = null;
        function splitCsv(s){ return (s||'').split(',').map(x=>x.trim()).filter(Boolean); }
        function ensureRule(cfg, type, id, priority){
          cfg.rules = cfg.rules || [];
          let r = (cfg.rules||[]).find(x=>x && typeof x==='object' && x.type===type);
          if(!r){
            r = { id, enabled:true, priority, type, description:'', params:{} };
            cfg.rules.push(r);
          }
          r.id = r.id || id;
          r.enabled = (r.enabled !== false);
          r.priority = Number(r.priority ?? priority);
          r.type = type;
          r.params = (r.params && typeof r.params==='object') ? r.params : {};
          return r;
        }
        function setChecks(name, values){
          const set = new Set((values||[]).map(String));
          for(const el of document.querySelectorAll(`input[name="${name}"]`)){
            el.checked = set.has(String(el.value));
          }
        }
        function getChecks(name){
          const out = [];
          for(const el of document.querySelectorAll(`input[name="${name}"]:checked`)){
            out.push(String(el.value));
          }
          return out;
        }

        async function loadActive(){
          const profile = document.getElementById('profile').value;
          const j = await api(`/admin/api/output_rules/active?profile=${encodeURIComponent(profile)}`);
          if(!j||!j.ok){ document.getElementById('status').textContent = JSON.stringify(j); toast('err','加载失败','读取生效配置失败'); return; }
          currentConfig = j.config_json;
          document.getElementById('verPill').textContent = `生效版本: ${j.meta?.version || j.meta?.path || '-'}`;
          document.getElementById('enabled').value = String(!!(currentConfig.overrides||{}).enabled);
          const d = currentConfig.defaults||{};
          const order = (currentConfig.output && currentConfig.output.sections_order) ? currentConfig.output.sections_order : ['A','B','C','D','E','F','G'];
          document.getElementById('sections_order').value = (order||[]).join(',');
          const secs = (d.sections||[]).filter(x=>x && typeof x==='object');
          const enabledSecs = secs.filter(s=>s.enabled).map(s=>String(s.id));
          setChecks('sec_enabled', enabledSecs.length ? enabledSecs : ['A','B','C','D','E','F','G']);
          const A = d.A||{};
          document.getElementById('a_min').value = (A.items_range||{}).min ?? 8;
          document.getElementById('a_max').value = (A.items_range||{}).max ?? 15;
          document.getElementById('sum_min').value = (A.summary_sentences||{}).min ?? 2;
          document.getElementById('sum_max').value = (A.summary_sentences||{}).max ?? 3;
          document.getElementById('sum_chars').value = A.summary_max_chars ?? 260;
          document.getElementById('show_tags').value = String(!!A.show_tags);
          document.getElementById('show_other_sources').value = String(!!A.show_other_sources);
          document.getElementById('show_source_link').value = String(!!A.show_source_link);
          // IMPORTANT: the effective sizes might be overridden by rules[].type=section_sizes.
          let tc = (d.E||{}).trends_count ?? 3;
          let gmin = (((d.F||{}).gaps_count)||{}).min ?? 3;
          let gmax = (((d.F||{}).gaps_count)||{}).max ?? 5;
          try{
            const rs = (currentConfig.rules||[]).find(x=>x && typeof x==='object' && x.type==='section_sizes');
            if(rs && rs.params && typeof rs.params==='object'){
              if(rs.params.trends_count != null) tc = rs.params.trends_count;
              if(rs.params.gaps_min != null) gmin = rs.params.gaps_min;
              if(rs.params.gaps_max != null) gmax = rs.params.gaps_max;
            }
          }catch(e){}
          document.getElementById('trends_count').value = Number(tc||3);
          document.getElementById('gaps_min').value = Number(gmin||3);
          document.getElementById('gaps_max').value = Number(gmax||5);
          document.getElementById('heatmap_regions').value = ((d.D||{}).heatmap_regions||['北美','欧洲','亚太','中国']).join(',');
          const st = d.style || {};
          document.getElementById('style_lang').value = st.language || 'zh';
          document.getElementById('style_tone').value = st.tone || 'concise_decision';
          document.getElementById('style_no_fluff').value = String(st.no_fluff ?? true);
          document.getElementById('btnPublish').disabled = true;
          currentDraftId = null;
          document.getElementById('status').innerHTML = '<span class="ok">已加载生效配置</span>';
        }

        function buildConfig(){
          const cfg = JSON.parse(JSON.stringify(currentConfig||{}));
          if(!cfg.defaults) cfg.defaults = {};
          if(!cfg.overrides) cfg.overrides = {};
          cfg.ruleset = 'output_rules';
          cfg.profile = document.getElementById('profile').value;
          cfg.overrides.enabled = document.getElementById('enabled').value === 'true';
          cfg.defaults.format = cfg.defaults.format || 'plain_text';
          cfg.output = cfg.output || {};
          cfg.output.sections_order = splitCsv(document.getElementById('sections_order').value);
          const enabledSet = new Set(getChecks('sec_enabled'));
          cfg.defaults.sections = (cfg.output.sections_order||[]).map(id=>({id, enabled: enabledSet.has(String(id))}));
          cfg.defaults.A = cfg.defaults.A || {};
          cfg.defaults.A.items_range = { min: Number(document.getElementById('a_min').value||8), max: Number(document.getElementById('a_max').value||15) };
          cfg.defaults.A.sort_by = 'importance';
          cfg.defaults.A.summary_sentences = { min: Number(document.getElementById('sum_min').value||2), max: Number(document.getElementById('sum_max').value||3) };
          cfg.defaults.A.summary_max_chars = Number(document.getElementById('sum_chars').value||260);
          cfg.defaults.A.show_tags = document.getElementById('show_tags').value === 'true';
          cfg.defaults.A.show_other_sources = document.getElementById('show_other_sources').value === 'true';
          cfg.defaults.A.show_source_link = document.getElementById('show_source_link').value === 'true';
          const tc = Number(document.getElementById('trends_count').value||3);
          const gmin = Number(document.getElementById('gaps_min').value||3);
          const gmax = Number(document.getElementById('gaps_max').value||5);
          cfg.defaults.E = { trends_count: tc };
          cfg.defaults.F = { gaps_count: { min: gmin, max: gmax } };
          cfg.defaults.D = { heatmap_regions: splitCsv(document.getElementById('heatmap_regions').value) };
          cfg.defaults.style = {
            language: document.getElementById('style_lang').value || 'zh',
            tone: document.getElementById('style_tone').value || 'concise_decision',
            no_fluff: document.getElementById('style_no_fluff').value === 'true',
          };
          cfg.defaults.constraints = { g_must_be_last: true, a_to_f_must_not_include_quality_metrics: true };

          // Keep rules in sync with defaults to avoid "rule overrides defaults" surprises.
          // This is the root cause of "trends_count set to 5 but output still shows 3".
          const rs = ensureRule(cfg, 'section_sizes', 'render-e-f-config', 70);
          rs.description = '趋势判断与缺口清单条数';
          rs.params.trends_count = tc;
          rs.params.gaps_min = gmin;
          rs.params.gaps_max = gmax;

          return cfg;
        }

        async function saveDraft(){
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/output_rules/draft','POST',{ profile, created_by, config_json: buildConfig() });
          currentDraftId = j.draft?.id || null;
          const errs = j.draft?.validation_errors || [];
          if(j.ok){
            document.getElementById('status').innerHTML = `<span class="ok">草稿校验通过（draft_id=${currentDraftId}）</span>`;
            document.getElementById('btnPublish').disabled = false;
            toast('ok','草稿校验通过',`draft_id=${currentDraftId}`);
          } else {
            document.getElementById('status').innerHTML = `<span class="err">草稿校验失败</span>\\n${JSON.stringify(errs,null,2)}`;
            document.getElementById('btnPublish').disabled = true;
            toast('err','草稿校验失败', `${errs.length} 处错误`);
          }
        }

        async function publishDraft(){
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/output_rules/publish','POST',{ profile, draft_id: currentDraftId, created_by });
          if(j && j.ok){
            document.getElementById('status').innerHTML = `<span class="ok">已生效版本号：${j.version}</span>`;
            document.getElementById('btnPublish').disabled = true;
            toast('ok','发布成功', `version=${j.version}`);
            await loadActive();
          } else {
            document.getElementById('status').innerHTML = `<span class="err">发布失败</span>\\n${JSON.stringify(j,null,2)}`;
            toast('err','发布失败', JSON.stringify(j?.errors||j?.error||j));
          }
        }

        async function preview(){
          const profile = document.getElementById('profile').value;
          const date = document.getElementById('dryrun_date').value.trim();
          // Loading state: show immediately to avoid "no response" perception.
          const qcEl = document.getElementById('qcPanel');
          const preEl = document.getElementById('preview');
          if(qcEl) qcEl.textContent = '运行中...（不会发信）';
          if(preEl) preEl.textContent = '运行中...（不会发信）';
          const j = await api(`/admin/api/dryrun?profile=${encodeURIComponent(profile)}&date=${encodeURIComponent(date)}`,'POST',null);
          if(!j||!j.ok){
            if(qcEl) qcEl.textContent = '';
            if(preEl) preEl.textContent = JSON.stringify(j,null,2);
            return;
          }
          document.getElementById('qcPanel').textContent = JSON.stringify(j.qc_report||{}, null, 2);
          document.getElementById('preview').textContent = j.preview_text || '';
          toast('ok','试跑完成', `运行ID=${j.run_id}`);
        }
        async function copyPreview(){
          const txt = document.getElementById('preview').textContent || '';
          try { await navigator.clipboard.writeText(txt||''); toast('ok','已复制','预览已复制到剪贴板'); }
          catch(e){ toast('err','复制失败', String(e)); }
        }

        document.getElementById('profile').addEventListener('change', loadActive);
        loadActive();
        """
        return _page_shell("输出规则", body, js)

    @app.get("/admin/scheduler", response_class=HTMLResponse)
    def page_scheduler(_: dict[str, str] = Depends(_auth_guard)) -> str:
        body = """
            <div class="layout">
              <div class="card">
                <label>规则档位（Profile）</label>
                <select id="profile"><option value="enhanced">增强（enhanced）</option><option value="legacy">保持现状（legacy）</option></select>
                <label>启用开关（enabled）</label><select id="enabled"><option value="true">启用</option><option value="false">停用</option></select>
                <label>时区（timezone）</label><input id="timezone" placeholder="例如：Asia/Shanghai"/>
                <div class="small">Cron/Interval 的时间都按该时区计算。若你希望按北京时间出报，建议填 <code>Asia/Shanghai</code>。</div>

                <label>并发（concurrency.max_instances）</label><input id="max_instances" type="number" min="1" max="5"/>
                <label>合并错过触发（concurrency.coalesce）</label><select id="coalesce"><option value="true">开启（只补最近一次）</option><option value="false">关闭</option></select>
                <label>misfire_grace_seconds（容忍延迟秒数）</label><input id="misfire" type="number" min="0" max="86400"/>
                <div class="small">例如 600 表示触发延迟 10 分钟内仍可补跑（配合 coalesce 使用）。</div>

                <label>暂停开关（pause_switch）</label><select id="pause_switch"><option value="false">正常运行</option><option value="true">暂停（不跑任何任务）</option></select>
                <label>允许手动触发（allow_manual_trigger）</label><select id="allow_manual"><option value="true">允许</option><option value="false">不允许</option></select>

                <div class="divider"></div>
                <h4>任务 schedules</h4>
                <div class="row">
                  <button onclick="addCron()">+ 定时任务（Cron）</button>
                  <button onclick="addInterval()">+ 间隔任务（Interval）</button>
                </div>
                <div class="small">用途说明：<code>collect</code>=按信源抓取；<code>digest</code>=生成日报并按邮件规则投递。</div>
                <div id="schedules"></div>

                <label>操作人</label><input id="created_by" value="rules-admin-ui"/>
                <div>
                  <button onclick="saveDraft()">保存草稿并校验</button>
                  <button id="btnPublish" onclick="publishDraft()" disabled>发布生效</button>
                  <span class="pill" id="verPill">生效版本: -</span>
                </div>
                <p id="status"></p>
                <details class="help">
                  <summary>字段说明/用法</summary>
                  <div class="box">
                    <ul>
                      <li><b>enabled</b>：是否启用调度（关闭后不再触发计划任务）。</li>
                      <li><b>timezone</b>：cron/interval 的时区。</li>
                      <li><b>schedules</b>：定义计划任务。<code>cron</code> 用 crontab 表达式；<code>interval</code> 用分钟间隔。</li>
                      <li><b>purpose</b>：<code>collect</code> 表示只按信源抓取；<code>digest</code> 表示执行日报主链路（生成并发信）。</li>
                      <li><b>pause_switch</b>：true 表示暂停调度（不运行任何计划任务，手动 trigger 也会被跳过）。</li>
                      <li><b>立即生效</b>：发布/回滚后会触发 worker reload（无需重启容器）。</li>
                    </ul>
                  </div>
                </details>
              </div>

              <div class="card">
                <div class="row">
                  <button onclick="pauseNow(true)">暂停</button>
                  <button onclick="pauseNow(false)">恢复</button>
                  <button onclick="triggerNow('collect')">立即触发：采集（collect）</button>
                  <button onclick="triggerNow('digest')">立即触发：出日报（digest）</button>
                  <button onclick="loadStatus()">刷新状态</button>
                </div>
                <div class="drawer" id="schedStatus"></div>
                <pre id="nextRuns"></pre>
              </div>
            </div>
            """
        js = """
        let currentConfig = null;
        let currentDraftId = null;
        let schedules = [];

        function esc(s){ return String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
        function mkRow(s, idx){
          const isCron = (s.type||'') === 'cron';
          const cronPart = isCron
            ? `<label>cron 表达式</label><input value="${esc(s.cron||'')}" placeholder="例如：0 9 * * *" onchange="schedules[${idx}].cron=this.value"/>`
            : `<label>间隔分钟（interval_minutes）</label><input type="number" min="1" max="1440" value="${esc(s.interval_minutes??60)}" onchange="schedules[${idx}].interval_minutes=Number(this.value)"/>`;
          return `
            <div class="box" style="margin-top:8px">
              <div class="row" style="justify-content:space-between">
                <b>任务 ${idx+1}</b>
                <button onclick="delSchedule(${idx})">删除</button>
              </div>
              <label>任务ID（id）</label><input value="${esc(s.id||'')}" placeholder="例如：digest_daily_0830" onchange="schedules[${idx}].id=this.value"/>
              <label>任务类型（type）</label>
              <select onchange="schedules[${idx}].type=this.value; renderSchedules();">
                <option value="cron" ${isCron?'selected':''}>定时（cron）</option>
                <option value="interval" ${!isCron?'selected':''}>间隔（interval）</option>
              </select>
              ${cronPart}
              <label>用途（purpose）</label>
              <select onchange="schedules[${idx}].purpose=this.value;">
                <option value="collect" ${(s.purpose||'')==='collect'?'selected':''}>采集（collect）</option>
                <option value="digest" ${(s.purpose||'')==='digest'?'selected':''}>出日报（digest）</option>
              </select>
              <label>运行档位（profile）</label><input value="${esc(s.profile||'enhanced')}" placeholder="enhanced 或 legacy" onchange="schedules[${idx}].profile=this.value"/>
              <label>随机抖动（jitter_seconds）</label><input type="number" min="0" max="300" value="${esc(s.jitter_seconds??0)}" onchange="schedules[${idx}].jitter_seconds=Number(this.value)"/>
            </div>
          `;
        }
        function renderSchedules(){
          const html = schedules.map(mkRow).join('');
          document.getElementById('schedules').innerHTML = html || '<div class="small">暂无任务</div>';
        }
        function addCron(){
          schedules.push({id:'daily-digest', type:'cron', cron:'0 9 * * *', purpose:'digest', profile:'enhanced', jitter_seconds:0});
          renderSchedules();
        }
        function addInterval(){
          schedules.push({id:'hourly-collect', type:'interval', interval_minutes:60, purpose:'collect', profile:'enhanced', jitter_seconds:0});
          renderSchedules();
        }
        function delSchedule(i){ schedules.splice(i,1); renderSchedules(); }

        function buildConfig(){
          const cfg = JSON.parse(JSON.stringify(currentConfig||{}));
          if(!cfg.defaults) cfg.defaults = {};
          if(!cfg.defaults.concurrency) cfg.defaults.concurrency = {};
          if(!cfg.defaults.run_policies) cfg.defaults.run_policies = {};
          cfg.profile = document.getElementById('profile').value;
          cfg.ruleset = 'scheduler_rules';
          cfg.defaults.enabled = document.getElementById('enabled').value === 'true';
          cfg.defaults.timezone = (document.getElementById('timezone').value||'Asia/Singapore').trim();
          cfg.defaults.schedules = schedules;
          cfg.defaults.concurrency.max_instances = Number(document.getElementById('max_instances').value||1);
          cfg.defaults.concurrency.coalesce = document.getElementById('coalesce').value === 'true';
          cfg.defaults.concurrency.misfire_grace_seconds = Number(document.getElementById('misfire').value||600);
          cfg.defaults.run_policies.pause_switch = document.getElementById('pause_switch').value === 'true';
          cfg.defaults.run_policies.allow_manual_trigger = document.getElementById('allow_manual').value === 'true';
          return cfg;
        }

        async function loadActive(){
          const profile = document.getElementById('profile').value;
          const j = await api(`/admin/api/scheduler_rules/active?profile=${encodeURIComponent(profile)}`);
          if(!j || !j.ok){ document.getElementById('status').textContent = JSON.stringify(j); toast('err','加载失败','读取生效配置失败'); return; }
          currentConfig = j.config_json;
          document.getElementById('verPill').textContent = `生效版本: ${j.meta?.version || j.meta?.path || '-'}`;
          const d = (currentConfig.defaults||{});
          document.getElementById('enabled').value = String(!!d.enabled);
          document.getElementById('timezone').value = d.timezone || 'Asia/Singapore';
          const c = d.concurrency || {};
          document.getElementById('max_instances').value = c.max_instances ?? 1;
          document.getElementById('coalesce').value = String(c.coalesce ?? true);
          document.getElementById('misfire').value = c.misfire_grace_seconds ?? 600;
          const p = d.run_policies || {};
          document.getElementById('pause_switch').value = String(!!p.pause_switch);
          document.getElementById('allow_manual').value = String(p.allow_manual_trigger ?? true);
          schedules = Array.isArray(d.schedules) ? d.schedules : [];
          renderSchedules();
          document.getElementById('status').innerHTML = '<span class="ok">已加载生效配置</span>';
          document.getElementById('btnPublish').disabled = true;
          currentDraftId = null;
        }

        async function saveDraft(){
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/scheduler_rules/draft','POST',{ profile, created_by, config_json: buildConfig() });
          currentDraftId = j.draft?.id || null;
          const errs = j.draft?.validation_errors || [];
          if(j.ok){
            document.getElementById('status').innerHTML = `<span class="ok">草稿校验通过（draft_id=${currentDraftId}）</span>`;
            document.getElementById('btnPublish').disabled = false;
            toast('ok','草稿校验通过',`draft_id=${currentDraftId}`);
          } else {
            document.getElementById('status').innerHTML = `<span class="err">草稿校验失败</span>\\n${JSON.stringify(errs,null,2)}`;
            document.getElementById('btnPublish').disabled = true;
            toast('err','草稿校验失败', `${errs.length} 处错误`);
          }
        }
        async function publishDraft(){
          const profile = document.getElementById('profile').value;
          const created_by = document.getElementById('created_by').value || 'rules-admin-ui';
          const j = await api('/admin/api/scheduler_rules/publish','POST',{ profile, draft_id: currentDraftId, created_by });
          if(j && j.ok){
            document.getElementById('status').innerHTML = `<span class="ok">已生效版本号：${j.version}</span>`;
            document.getElementById('btnPublish').disabled = true;
            toast('ok','发布成功', `version=${j.version}`);
            await loadActive();
            await loadStatus();
          } else {
            document.getElementById('status').innerHTML = `<span class="err">发布失败</span>\\n${JSON.stringify(j,null,2)}`;
            toast('err','发布失败', JSON.stringify(j?.errors||j?.error||j));
          }
        }

        async function loadStatus(){
          const j = await api('/admin/api/scheduler/status');
          const st = j.status || {};
          const hb = j.heartbeat || {};
          const enabled = (st.enabled ?? hb.enabled) ? '启用' : '未启用';
          const paused = (st.paused ?? hb.paused) ? '暂停' : '运行';
          document.getElementById('schedStatus').innerHTML = `
            <div class="kvs">
              <div>状态</div><b>${esc(enabled)} / ${esc(paused)}</b>
              <div>档位</div><b>${esc(st.profile||hb.profile||'')}</b>
              <div>生效版本</div><b>${esc(st.active_version||hb.active_version||'')}</b>
              <div>心跳</div><b>${esc(hb.ts||'')}</b>
            </div>`;
          const jobs = (st.jobs||[]);
          document.getElementById('nextRuns').textContent = JSON.stringify(jobs, null, 2);
        }

        async function pauseNow(v){
          document.getElementById('pause_switch').value = String(!!v);
          await saveDraft();
          if(!document.getElementById('btnPublish').disabled) await publishDraft();
        }
        async function triggerNow(purpose){
          const profile = document.getElementById('profile').value;
          await api('/admin/api/scheduler/trigger','POST',{ profile, purpose, schedule_id:'manual' });
          toast('ok','已触发', purpose);
          await loadStatus();
        }

        document.getElementById('profile').addEventListener('change', async ()=>{ await loadActive(); await loadStatus(); });
        loadActive(); loadStatus();
        setInterval(loadStatus, 5000);
        """
        return _page_shell("调度规则", body, js)

    @app.get("/admin/sources", response_class=HTMLResponse)
    def page_sources(_: dict[str, str] = Depends(_auth_guard)) -> str:
        style = """
        <style>
          /* Keep the Sources table readable on narrower screens */
          .actionscol button { padding: 6px 8px; font-size: 12px; }
          .actionscol select { width: 100%; padding: 6px 8px; font-size: 12px; }
          details.help { margin-top: 10px; }
          details.help summary { cursor: pointer; color: var(--muted); font-size: 12px; }
          details.help .box { margin-top: 8px; border: 1px solid var(--border); border-radius: 12px; padding: 10px; background: rgba(10,16,28,.45); }
          details.help ul { margin: 0; padding-left: 18px; color: var(--muted); font-size: 12px; }
          details.help li { margin: 6px 0; }
        </style>
        """
        body = """
        <div class="layout">
          <div class="card">
            <h4>新增 / 编辑信源</h4>
            <div class="row">
              <input class="grow" id="q" placeholder="搜索：id/名称/标签/url..."/>
              <select id="filter_enabled" style="width:140px">
                <option value="all">全部</option>
                <option value="enabled">仅启用</option>
                <option value="disabled">仅停用</option>
              </select>
              <input id="filter_tag" style="width:160px" placeholder="按标签过滤"/>
              <input id="filter_region" style="width:140px" placeholder="按地区过滤"/>
            </div>
            <label>信源ID（唯一）</label><input id="id" placeholder="例如：reuters-health-rss"/>
	            <label>名称</label><input id="name" placeholder="例如：Reuters Healthcare"/>
	            <label>采集方式</label>
              <select id="connector">
                <option value="rss">RSS（rss）</option>
                <option value="html">网页（html）</option>
                <option value="rsshub">RSSHub（rsshub）</option>
                <option value="google_news">Google News RSS（google_news）</option>
                <option value="api">API（api，可选）</option>
              </select>
	            <label>URL（rss/html/google_news 必填；api 填 base_url）</label><input id="url" placeholder="https://... 或 rsshub://"/>
	            <label>地区</label><input id="region" placeholder="例如：中国/亚太/北美/欧洲/全球"/>
	            <label>API endpoint（仅 api 需要，如 /v1/news）</label><input id="api_endpoint" placeholder="/v1/news"/>

	            <label>采集频率 interval_minutes（1..1440，留空=跟随全局/调度）</label><input id="fetch_interval" type="number" min="1" max="1440" placeholder="例如：60"/>
              <div class="small">表示该信源“最小抓取间隔”。例如：媒体 60，监管公告 360-1440。</div>
	            <label>超时 timeout_seconds（1..120）</label><input id="fetch_timeout" type="number" min="1" max="120" placeholder="例如：20"/>
	            <label>请求头 headers_json（可选，JSON 对象）</label><textarea id="fetch_headers" rows="3" placeholder='{"User-Agent":"..."}'></textarea>
	            <label>鉴权引用 auth_ref（可选：env/secret 名称，不存明文）</label><input id="fetch_auth_ref" placeholder="例如：NMPA_API_KEY"/>
              <div class="small" id="auth_ref_hint">鉴权状态：-</div>

	            <label>限速 rate_limit（rps 0.1..50 / burst 1..100）</label>
	            <div class="row">
	              <input id="rl_rps" type="number" min="0.1" max="50" step="0.1" placeholder="rps 1.0"/>
	              <input id="rl_burst" type="number" min="1" max="100" step="1" placeholder="burst 5"/>
	            </div>
              <div class="small">用于抓取端节流，避免被封或触发风控。一般 rps 0.5-2 足够。</div>

	            <label>解析配置 parse_profile（可选，用于选择解析器模板）</label><input id="parse_profile" placeholder="例如：pmda_list_v1"/>
	            <label>优先级（0-1000，越大越优先）</label><input id="priority" type="number" min="0" max="1000" value="50"/>
	            <label>可信等级</label><select id="trust_tier"><option>A</option><option>B</option><option>C</option></select>
	            <label>标签（逗号分隔）</label><input id="tags" placeholder="media,global,en"/>
            <label>是否启用</label><select id="enabled"><option value="true">启用</option><option value="false">停用</option></select>
            <div class="row">
              <button onclick="saveSource()">保存</button>
              <button onclick="clearForm()">清空</button>
            </div>
            <p id="status"></p>
            <details class="help">
              <summary>这些参数怎么用？</summary>
              <div class="box">
                <ul>
                  <li><b>信源ID</b>：全局唯一标识。建议用小写字母/数字/.-_ 组合，例如 <code>reuters-health-rss</code>。后续规则引用、统计、去重聚合都会用到它。</li>
	                  <li><b>采集方式</b>：<code>rss</code> 支持自动发现 feed；<code>html</code> 抓取网页列表；<code>rsshub</code> 用路由转 RSS；<code>google_news</code> 为聚合 RSS（默认建议关闭）；<code>api</code> 适合 JSON API。</li>
	                  <li><b>URL</b>：rss/html/google_news 必填。api 时作为 <b>base_url</b> 使用。rsshub 可填 <code>rsshub://</code> 占位并配 <code>fetch.rsshub_route</code>。</li>
	                  <li><b>API endpoint</b>：仅 api 需要，形如 <code>/v1/news</code>。test 会请求 base_url + endpoint。</li>
	                  <li><b>interval_minutes</b>：每个信源的采集频率（分钟）。后续 scheduler 会按该值决定是否抓取该 source。</li>
	                  <li><b>timeout_seconds</b>：单次抓取超时（秒）。</li>
	                  <li><b>headers_json</b>：附加请求头（JSON 对象）。</li>
	                  <li><b>auth_ref</b>：只能填写“环境变量名”（例如 <code>NMPA_API_KEY</code>）。运行时会从 env 取值并注入到请求头（默认写入 <code>Authorization</code>，env 若不含空格将按 <code>Bearer &lt;token&gt;</code> 处理）。UI 只显示“是否已配置”，不会展示明文。</li>
	                  <li><b>rate_limit</b>：每秒请求数与突发上限（用于采集端限速）。</li>
	                  <li><b>parse_profile</b>：解析模板名（用于网页/接口解析器选择）。</li>
                  <li><b>优先级</b>：用于 story 聚合选“主条目”与排序，数值越大越优先（例如 Reuters > 垂媒 > 泛 RSS）。</li>
                  <li><b>可信等级</b>：A/B/C 用于内容可信度分层与筛选（规则里可用）。</li>
                  <li><b>标签</b>：用于批量 include/exclude（例如 <code>regulatory</code>/<code>cn</code>/<code>apac</code>/<code>en</code>）。</li>
                  <li><b>是否启用</b>：停用后不会进入采集列表（但记录保留，便于回放/replay 一致性）。</li>
                </ul>
              </div>
            </details>
          </div>
          <div class="card">
            <h4>信源列表</h4>
            <button onclick="loadSources()">刷新</button>
            <div id="table"></div>
            <div class="drawer" id="drawer" style="display:none"></div>
          </div>
        </div>
        """
        js = """
        function arr(v){ return (v||'').split(',').map(s=>s.trim()).filter(Boolean); }
        function esc(s){ return String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
        function parseJsonOrEmpty(txt){
          const t = (txt||'').trim();
          if(!t) return {};
          try { const o = JSON.parse(t); return (o && typeof o === 'object' && !Array.isArray(o)) ? o : {}; }
          catch(e){ return { __parse_error__: String(e) }; }
        }
        function fmtTime(iso){
          if(!iso) return '';
          try { return new Date(iso).toLocaleString(); } catch(e) { return String(iso); }
        }
        function prettyUrl(u){
          try {
            const x = new URL(u);
            return x.hostname + (x.pathname||'/');
          } catch(e) {
            return u || '';
          }
        }
        function matchQ(s, q){
          if(!q) return true;
          const t = (s.id+' '+s.name+' '+(s.region||'')+' '+(s.tags||[]).join(' ')+' '+(s.url||'')).toLowerCase();
          return t.includes(q.toLowerCase());
        }
        function showDrawer(title, obj){
          const d = document.getElementById('drawer');
          d.style.display = 'block';
          d.innerHTML = `<div class="row"><div class="grow"><b>${esc(title)}</b></div><button onclick="hideDrawer()">关闭</button></div><pre>${esc(JSON.stringify(obj,null,2))}</pre>`;
        }
        function hideDrawer(){
          const d = document.getElementById('drawer');
          d.style.display = 'none';
          d.innerHTML = '';
        }
	        function renderRow(s){
	          const lastFetchRaw = s.last_fetched_at || '';
	          const lastFetch = esc(fmtTime(lastFetchRaw) || lastFetchRaw || '-');
	          // Fetch status = worker/scheduler real fetch health (may differ from manual "test").
	          const st = String(s.last_fetch_status||'').toLowerCase();
	          const stLabel = st==='ok' ? '抓取成功' : (st==='fail' ? '抓取失败' : (st==='skipped' ? '抓取跳过' : ''));
	          const stPill = stLabel ? `<span class="pill ${st==='fail'?'err':''}" title="来源：调度抓取（worker）">${esc(stLabel)}</span>` : '';
	          const fetchErr = s.last_fetch_error ? `<span class="pill err" title="${esc('抓取异常: '+s.last_fetch_error)}">抓取异常</span>` : '';
	          const fetchHs = s.last_fetch_http_status ? `<span class="pill" title="抓取 HTTP 状态码">${esc(s.last_fetch_http_status)}</span>` : '';

	          // Test status = admin console "sources:test" result.
	          const testOk = !!(s.last_success_at && !s.last_error);
	          const testFail = !!(s.last_error);
	          const testLabel = testOk ? '测试成功' : (testFail ? '测试失败' : '');
	          const testTitle = testOk
	            ? (`来源：手动测试（/sources/{id}/test）\\n时间：${String(s.last_success_at||'')}` + (s.last_http_status ? `\\nHTTP：${String(s.last_http_status)}` : ''))
	            : (testFail ? (`来源：手动测试（/sources/{id}/test）\\n错误：${String(s.last_error||'')}`) : '');
	          const testPill = testLabel ? `<span class="pill ${testFail?'err':''}" title="${esc(testTitle)}">${esc(testLabel)}</span>` : '';
	          const testHs = s.last_http_status ? `<span class="pill" title="测试 HTTP 状态码">${esc(s.last_http_status)}</span>` : '';

	          const statusPills = [stPill, fetchErr, fetchHs, testPill, testHs].filter(Boolean).join(' ');
	          const urlText = prettyUrl(s.url||'');
	          const urlCell = s.url ? `<a class="url" href="${esc(s.url)}" target="_blank" title="${esc(s.url)}">${esc(urlText)}</a>` : '';
	          const toggleLabel = s.enabled ? '停用' : '启用';
            const f = s.fetch || {};
            const authRef = (f.auth_ref || '').trim();
            const authPill = authRef
              ? (s.auth_configured ? `<span class="pill">鉴权✓</span>` : `<span class="pill err" title="未在容器环境变量中找到该 auth_ref">鉴权×</span>`)
              : '';
	          return `
	            <tr data-id="${esc(s.id)}">
	              <td class="nowrap">${s.enabled ? '启用' : '停用'}</td>
	              <td>${esc(s.name)} ${authPill} ${statusPills}</td>
	              <td class="nowrap">${esc(s.fetcher || s.connector)}</td>
	              <td class="urlcol">${urlCell}</td>
	              <td class="nowrap">${s.priority}</td>
	              <td class="nowrap">${esc(s.region||'')}</td>
	              <td class="tagcol">${esc((s.tags||[]).join(','))}</td>
	              <td>${lastFetch}</td>
	              <td class="actionscol">
                <select onchange="doAction('${esc(s.id)}', this.value); this.value='';">
                  <option value="">选择…</option>
                  <option value="edit">编辑</option>
                  <option value="toggle">${toggleLabel}</option>
                  <option value="test">测试</option>
                </select>
              </td>
            </tr>`;
        }
	        async function loadSources() {
          const j = await api('/admin/api/sources');
          if (!j || !j.ok) { document.getElementById('table').textContent = JSON.stringify(j,null,2); return; }
          const q = (document.getElementById('q').value||'').trim();
          const fe = document.getElementById('filter_enabled').value;
          const ft = (document.getElementById('filter_tag').value||'').trim().toLowerCase();
          const fr = (document.getElementById('filter_region').value||'').trim().toLowerCase();
          const filtered = (j.sources||[]).filter(s=>{
            if(fe==='enabled' && !s.enabled) return false;
            if(fe==='disabled' && s.enabled) return false;
            if(ft){
              const tags = (s.tags||[]).map(x=>String(x).toLowerCase());
              if(!tags.some(x=>x.includes(ft))) return false;
            }
            if(fr){
              const rg = String(s.region||'').toLowerCase();
              if(!rg.includes(fr)) return false;
            }
            return matchQ(s,q);
          });
          const rows = filtered.map(renderRow).join('');
		          document.getElementById('table').innerHTML = `<table><thead><tr><th>状态</th><th>名称</th><th>方式</th><th>链接</th><th>优先级</th><th>地区</th><th>标签</th><th>最近抓取</th><th>操作</th></tr></thead><tbody>${rows}</tbody></table>`;
	          window._sources = j.sources||[];
	        }
        function editSource(id){
          const s = (window._sources||[]).find(x=>x.id===id); if(!s) return;
          document.getElementById('id').value = s.id||'';
          document.getElementById('name').value = s.name||'';
          document.getElementById('connector').value = (s.fetcher||s.connector||'rss');
          document.getElementById('url').value = s.url||'';
          document.getElementById('region').value = s.region||'全球';
          const f = s.fetch||{};
          document.getElementById('api_endpoint').value = f.endpoint||'';
          document.getElementById('fetch_interval').value = (f.interval_minutes ?? '');
          document.getElementById('fetch_timeout').value = (f.timeout_seconds ?? '');
          document.getElementById('fetch_headers').value = (f.headers_json && Object.keys(f.headers_json||{}).length) ? JSON.stringify(f.headers_json||{}) : '';
          document.getElementById('fetch_auth_ref').value = f.auth_ref||'';
          const ar = (f.auth_ref||'').trim();
          document.getElementById('auth_ref_hint').textContent = ar ? (`鉴权状态：` + (s.auth_configured ? '已配置' : '未配置')) : '鉴权状态：未设置';
          const rl = s.rate_limit||{};
          document.getElementById('rl_rps').value = (rl.rps ?? '');
          document.getElementById('rl_burst').value = (rl.burst ?? '');
          const p = s.parsing||{};
          document.getElementById('parse_profile').value = p.parse_profile||'';
          document.getElementById('priority').value = s.priority??0;
          document.getElementById('trust_tier').value = s.trust_tier||'B';
          document.getElementById('tags').value = (s.tags||[]).join(',');
          document.getElementById('enabled').value = String(!!s.enabled);
          toast('ok','已载入到左侧表单', id);
        }
        function clearForm(){
          document.getElementById('id').value = '';
          document.getElementById('name').value = '';
          document.getElementById('connector').value = 'rss';
          document.getElementById('url').value = '';
          document.getElementById('region').value = '';
          document.getElementById('api_endpoint').value = '';
          document.getElementById('fetch_interval').value = '';
          document.getElementById('fetch_timeout').value = '';
          document.getElementById('fetch_headers').value = '';
          document.getElementById('fetch_auth_ref').value = '';
          document.getElementById('auth_ref_hint').textContent = '鉴权状态：未设置';
          document.getElementById('rl_rps').value = '';
          document.getElementById('rl_burst').value = '';
          document.getElementById('parse_profile').value = '';
          document.getElementById('priority').value = '50';
          document.getElementById('trust_tier').value = 'B';
          document.getElementById('tags').value = '';
          document.getElementById('enabled').value = 'true';
        }
        async function saveSource(){
          const headersObj = parseJsonOrEmpty(document.getElementById('fetch_headers').value);
          if(headersObj.__parse_error__){
            toast('err','headers_json JSON 解析失败', headersObj.__parse_error__);
            return;
          }
          const payload = {
            id: document.getElementById('id').value.trim(),
            name: document.getElementById('name').value.trim(),
            connector: document.getElementById('connector').value,
            fetcher: document.getElementById('connector').value,
            url: document.getElementById('url').value.trim(),
            region: document.getElementById('region').value.trim() || '全球',
            priority: Number(document.getElementById('priority').value||0),
            trust_tier: document.getElementById('trust_tier').value,
            tags: arr(document.getElementById('tags').value),
            enabled: document.getElementById('enabled').value === 'true',
            fetch: {
              interval_minutes: document.getElementById('fetch_interval').value ? Number(document.getElementById('fetch_interval').value) : undefined,
              timeout_seconds: document.getElementById('fetch_timeout').value ? Number(document.getElementById('fetch_timeout').value) : undefined,
              headers_json: headersObj,
              auth_ref: document.getElementById('fetch_auth_ref').value.trim() || undefined,
              endpoint: document.getElementById('api_endpoint').value.trim() || undefined,
            },
            rate_limit: {
              rps: document.getElementById('rl_rps').value ? Number(document.getElementById('rl_rps').value) : undefined,
              burst: document.getElementById('rl_burst').value ? Number(document.getElementById('rl_burst').value) : undefined,
            },
            parsing: {
              parse_profile: document.getElementById('parse_profile').value.trim() || undefined
            }
          };
          const j = await api('/admin/api/sources','POST',payload);
          if(j && j.ok){ document.getElementById('status').innerHTML = '<span class="ok">保存成功</span>'; toast('ok','保存成功',payload.id); loadSources(); }
          else { document.getElementById('status').innerHTML = `<span class="err">保存失败</span>\\n${JSON.stringify(j,null,2)}`; toast('err','保存失败', JSON.stringify(j)); }
        }
        async function doAction(id, action){
          if(!action) return;
          if(action==='edit'){ editSource(id); return; }
          if(action==='toggle'){
            const s = (window._sources||[]).find(x=>x.id===id);
            const enabled = s ? !s.enabled : true;
            await toggleSource(id, enabled);
            return;
          }
          if(action==='test'){ await testSource(id); return; }
        }
        async function toggleSource(id, enabled){
          const j = await api(`/admin/api/sources/${encodeURIComponent(id)}/toggle`,'POST',{enabled});
          if(j && j.ok) { toast('ok','已更新',id); loadSources(); } else { toast('err','更新失败',JSON.stringify(j)); }
        }
        async function testSource(id){
          const j = await api(`/admin/api/sources/${encodeURIComponent(id)}/test`,'POST',{});
          showDrawer(`测试结果: ${id}`, j);
          if(j && j.ok && j.result && j.result.ok) toast('ok','测试成功',id);
          else toast('err','测试失败',id);
          loadSources();
        }
        document.getElementById('q').addEventListener('input', ()=>{ loadSources(); });
        document.getElementById('filter_enabled').addEventListener('change', ()=>{ loadSources(); });
        document.getElementById('filter_tag').addEventListener('input', ()=>{ loadSources(); });
        document.getElementById('filter_region').addEventListener('input', ()=>{ loadSources(); });
        loadSources();
        """
        return _page_shell("信源管理", style + body, js)

    @app.get("/admin/versions", response_class=HTMLResponse)
    def page_versions(_: dict[str, str] = Depends(_auth_guard)) -> str:
        body = """
        <style>
          /* Versions page wants symmetric columns (not the global left-fixed layout). */
          .versionsGrid { display:grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 14px; align-items: start; }
          @media (max-width: 1100px) { .versionsGrid { grid-template-columns: 1fr; } }
        </style>
        <div class="card">
          <div class="row">
            <div class="grow">
              <label>规则档位（Profile）</label>
              <select id="profile"><option value="enhanced">增强（enhanced）</option><option value="legacy">保持现状（legacy）</option></select>
            </div>
            <div>
              <button onclick="loadVersions()">刷新版本</button>
              <button onclick="rollbackEmail()">回滚邮件规则</button>
              <button onclick="rollbackContent()">回滚采集规则</button>
              <button onclick="rollbackQc()">回滚质控规则</button>
              <button onclick="rollbackOutput()">回滚输出规则</button>
              <button onclick="rollbackScheduler()">回滚调度规则</button>
            </div>
          </div>
          <div class="versionsGrid" style="margin-top:12px">
            <div><h4>邮件规则版本</h4><div id="emailTable"></div></div>
            <div><h4>采集规则版本</h4><div id="contentTable"></div></div>
            <div><h4>质控规则版本</h4><div id="qcTable"></div></div>
            <div><h4>输出规则版本</h4><div id="outputTable"></div></div>
            <div><h4>调度规则版本</h4><div id="schedulerTable"></div></div>
          </div>
          <h4>版本差异对比</h4>
          <label>规则集（ruleset）</label>
          <select id="diff_ruleset">
            <option value="email_rules">邮件（email_rules）</option>
            <option value="content_rules">采集（content_rules）</option>
            <option value="qc_rules">质控（qc_rules）</option>
            <option value="output_rules">输出（output_rules）</option>
            <option value="scheduler_rules">调度（scheduler_rules）</option>
          </select>
          <label>对比起点版本（from_version）</label><input id="from_version" list="versions_datalist" placeholder="点击上方版本号，或在此选择/输入版本号"/>
          <label>对比终点版本（to_version）</label><input id="to_version" list="versions_datalist" placeholder="点击上方版本号，或在此选择/输入版本号"/>
          <datalist id="versions_datalist"></datalist>
          <div class="row">
            <button onclick="fillLatestTwo()">填入最近两版</button>
            <button onclick="showDiff()">查看差异</button>
            <button onclick="clearDiff()">清空</button>
          </div>
          <details class="help">
            <summary>字段说明/用法</summary>
            <div class="box">
              <ul>
                <li><b>规则档位（Profile）</b>：规则灰度/配置档位。通常用 <code>legacy</code> 保持原行为，用 <code>enhanced</code> 启用增强规则。</li>
                <li><b>版本号</b>：发布时生成的 DB 版本标识。点击版本号会自动填入 from/to。</li>
                <li><b>规则集（ruleset）</b>：<code>email_rules</code> 邮件规则，<code>content_rules</code> 采集规则，<code>qc_rules</code> 质控规则，<code>output_rules</code> 输出渲染规则，<code>scheduler_rules</code> 调度规则。</li>
                <li><b>from/to</b>：选择两个版本做差异对比，下面会列出字段级变更（新增/移除/变更）。</li>
                <li><b>回滚</b>：将当前生效版本（active）切回上一个版本（同 profile）。建议回滚前先做试跑预览验证。</li>
              </ul>
            </div>
          </details>
          <div class="drawer" id="diffSummary"></div>
          <div id="diffTable"></div>
          <pre id="diffOut"></pre>
        </div>
        """
        js = """
        function esc(s){ return String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
        function setDatalistOptions(ruleset){
          const dl = document.getElementById('versions_datalist');
          const rows = (window._versions && window._versions[ruleset]) ? window._versions[ruleset] : [];
          const top = (rows||[]).slice(0, 12);
          dl.innerHTML = top.map(r=>`<option value="${esc(r.version)}"></option>`).join('');
        }
        function render(rows){
          return `<table><thead><tr><th>版本号</th><th>发布时间</th><th>发布人</th><th>生效</th></tr></thead><tbody>` +
            (rows||[]).map(r=>`<tr class="${r.active?'activeRow':''}"><td><a href="#" onclick="pickVersion('${esc(r.version)}');return false;">${esc(r.version)}</a></td><td>${esc(r.created_at)}</td><td>${esc(r.created_by)}</td><td>${r.active?'是':''}</td></tr>`).join('') +
            `</tbody></table>`;
        }
        function pickVersion(v){
          const from = document.getElementById('from_version');
          const to = document.getElementById('to_version');
          if(!from.value) from.value = v;
          else if(!to.value) to.value = v;
          else { from.value = v; to.value = ''; }
        }
        function clearDiff(){
          document.getElementById('from_version').value = '';
          document.getElementById('to_version').value = '';
          document.getElementById('diffSummary').innerHTML = '';
          document.getElementById('diffTable').innerHTML = '';
          document.getElementById('diffOut').textContent = '';
        }
        function fillLatestTwo(){
          const ruleset = document.getElementById('diff_ruleset').value;
          const rows = (window._versions && window._versions[ruleset]) ? window._versions[ruleset] : [];
          const to = document.getElementById('to_version');
          const from = document.getElementById('from_version');
          if(!rows.length){ toast('warn','无可选版本','请先刷新版本'); return; }
          to.value = rows[0].version || '';
          from.value = (rows[1] ? rows[1].version : rows[0].version) || '';
          toast('ok','已填入最近两版', `${from.value} -> ${to.value}`);
        }
        async function loadVersions(){
          const profile = document.getElementById('profile').value;
          const j = await api(`/admin/api/versions?profile=${encodeURIComponent(profile)}`);
          if(!j||!j.ok){ document.getElementById('diffOut').textContent = JSON.stringify(j,null,2); return; }
          window._versions = {
            email_rules: j.email_rules || [],
            content_rules: j.content_rules || [],
            qc_rules: j.qc_rules || [],
            output_rules: j.output_rules || [],
            scheduler_rules: j.scheduler_rules || []
          };
          document.getElementById('emailTable').innerHTML = render(j.email_rules);
          document.getElementById('contentTable').innerHTML = render(j.content_rules);
          document.getElementById('qcTable').innerHTML = render(j.qc_rules);
          document.getElementById('outputTable').innerHTML = render(j.output_rules);
          document.getElementById('schedulerTable').innerHTML = render(j.scheduler_rules);
          setDatalistOptions(document.getElementById('diff_ruleset').value);
        }
        async function rollbackEmail(){
          const profile = document.getElementById('profile').value;
          const j = await api('/admin/api/email_rules/rollback','POST',{profile});
          toast(j&&j.ok?'ok':'err','回滚邮件规则', JSON.stringify(j)); loadVersions();
        }
        async function rollbackContent(){
          const profile = document.getElementById('profile').value;
          const j = await api('/admin/api/content_rules/rollback','POST',{profile});
          toast(j&&j.ok?'ok':'err','回滚采集规则', JSON.stringify(j)); loadVersions();
        }
        async function rollbackQc(){
          const profile = document.getElementById('profile').value;
          const j = await api('/admin/api/qc_rules/rollback','POST',{profile});
          toast(j&&j.ok?'ok':'err','回滚质控规则', JSON.stringify(j)); loadVersions();
        }
        async function rollbackOutput(){
          const profile = document.getElementById('profile').value;
          const j = await api('/admin/api/output_rules/rollback','POST',{profile});
          toast(j&&j.ok?'ok':'err','回滚输出规则', JSON.stringify(j)); loadVersions();
        }
        async function rollbackScheduler(){
          const profile = document.getElementById('profile').value;
          const j = await api('/admin/api/scheduler_rules/rollback','POST',{profile});
          toast(j&&j.ok?'ok':'err','回滚调度规则', JSON.stringify(j)); loadVersions();
        }
        async function showDiff(){
          const profile = document.getElementById('profile').value;
          const ruleset = document.getElementById('diff_ruleset').value;
          const from_version = document.getElementById('from_version').value.trim();
          const to_version = document.getElementById('to_version').value.trim();
          if(!from_version || !to_version) { toast('warn','缺少版本号','请先选择 from/to'); return; }
          const q = new URLSearchParams({ruleset, profile, from_version, to_version});
          const j = await api(`/admin/api/versions/diff?${q.toString()}`);
          if(!j||!j.ok){ document.getElementById('diffOut').textContent = JSON.stringify(j,null,2); return; }
          const opLabel = (op)=>{
            if(op==='added') return '新增';
            if(op==='removed') return '移除';
            if(op==='changed') return '变更';
            if(op==='type_changed') return '类型变化';
            return op||'';
          };
          const counts = {added:0, removed:0, changed:0, type_changed:0};
          for(const c of (j.changes||[])){ counts[c.op] = (counts[c.op]||0)+1; }
          document.getElementById('diffSummary').innerHTML = `
            <div class="kvs">
              <div>规则集</div><b>${esc(j.ruleset)}</b>
              <div>起点</div><b>${esc(j.from_version)}</b>
              <div>终点</div><b>${esc(j.to_version)}</b>
              <div>新增</div><b>${esc(counts.added||0)}</b>
              <div>移除</div><b>${esc(counts.removed||0)}</b>
              <div>变更</div><b>${esc(counts.changed||0)}</b>
              <div>类型变化</div><b>${esc(counts.type_changed||0)}</b>
            </div>`;
          const rows = (j.changes||[]).map(c=>`<tr><td>${esc(opLabel(c.op))}</td><td>${esc(c.path)}</td><td>${esc(JSON.stringify(c.from??''))}</td><td>${esc(JSON.stringify(c.to??''))}</td></tr>`).join('');
          document.getElementById('diffTable').innerHTML = `<table><thead><tr><th>操作</th><th>字段路径</th><th>原值</th><th>新值</th></tr></thead><tbody>${rows}</tbody></table>` + (j.truncated?'<div class="err">差异已截断</div>':'');
          document.getElementById('diffOut').textContent = JSON.stringify({changed_top_level_keys:j.changed_top_level_keys},null,2);
          toast('ok','差异已生成', `${ruleset} ${from_version} -> ${to_version}`);
        }
        document.getElementById('profile').addEventListener('change', loadVersions);
        document.getElementById('diff_ruleset').addEventListener('change', ()=>{ setDatalistOptions(document.getElementById('diff_ruleset').value); });
        loadVersions();
        """
        return _page_shell("版本与回滚", body, js)

    @app.get("/admin/runs", response_class=HTMLResponse)
    def page_runs(_: dict[str, str] = Depends(_auth_guard)) -> str:
        body = """
        <div class="layout">
          <div class="card">
            <h4>今日状态</h4>
            <div class="kvs" id="todayStatus">
              <div>日期</div><b>-</b>
              <div>是否已发送</div><b>-</b>
              <div>是否触发兜底</div><b>-</b>
              <div>最后错误</div><b>-</b>
            </div>
            <div class="row" style="margin-top:8px">
              <button onclick="loadRuns()">刷新</button>
            </div>
            <details class="help">
              <summary>字段说明</summary>
              <div class="box">
                <ul>
                  <li><b>是否已发送</b>：按今日日期统计，任意一条成功发送则为是。</li>
                  <li><b>是否触发兜底</b>：今日是否有兜底/补发路径执行记录。</li>
                  <li><b>最后错误</b>：最近一次失败的摘要（截断展示）。</li>
                </ul>
              </div>
            </details>
          </div>
          <div class="card">
            <h4>最近 30 条运行记录（按时间降序）</h4>
            <div id="runTable"></div>
          </div>
        </div>
        """
        js = """
        function esc(s){ return String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
        function fmtTime(raw){
          const s = String(raw||'').trim();
          if(!s) return '';
          const d = new Date(s);
          if(Number.isNaN(d.getTime())) return s;
          const dtf = new Intl.DateTimeFormat('zh-CN', {
            timeZone: 'Asia/Shanghai',
            year: 'numeric', month: '2-digit', day: '2-digit',
            hour: '2-digit', minute: '2-digit', second: '2-digit',
            hour12: false,
          });
          // "2026/02/18 18:56:22" -> "2026-02-18 18:56:22"
          return dtf.format(d).replaceAll('/', '-');
        }
        function mapStatus(s){
          const st = String(s||'').toLowerCase();
          if(st === 'success') return '成功';
          if(st === 'failed') return '失败';
          if(st === 'running') return '进行中';
          if(st === 'skipped') return '已跳过';
          if(st === 'unknown') return '未知';
          return st || '未知';
        }
        function short(s, n=120){
          const t = String(s||'').trim();
          return t.length > n ? t.slice(0, n-1) + '…' : t;
        }
        function summarizeFail(raw){
          const s = String(raw||'').trim();
          if(!s) return {title:'-', hint:'', raw:''};
          // Common, actionable patterns
          if(s.includes('missing TO_EMAIL')) return {title:'缺少收件人（TO_EMAIL）', hint:'设置 TO_EMAIL 或在邮件规则里配置收件人。', raw:s};
          if(s.includes('unauthorized') || s.includes('HTTP_401')) return {title:'鉴权失败', hint:'检查 ADMIN_USER/ADMIN_PASS 或 ADMIN_TOKEN。', raw:s};
          if(s.includes('Could not resolve host')) return {title:'网络/DNS 解析失败', hint:'检查网络或 DNS；常见于 SMTP/外部站点不可达。', raw:s};
          if(s.toLowerCase().includes('timed out') || s.toLowerCase().includes('timeout')) return {title:'网络超时', hint:'可尝试提高 timeout 或稍后重试。', raw:s};
          const mCurl = s.match(/curl:\\s*\\((\\d+)\\)\\s*([^\\n]+)/i);
          if(mCurl) return {title:`SMTP/HTTP 传输失败（curl ${mCurl[1]}）`, hint: short(mCurl[2], 80), raw:s};
          const mRcpt = s.match(/RCPT failed:\\s*(\\d+)/i);
          if(mRcpt) return {title:`收件人被拒绝（${mRcpt[1]}）`, hint:'检查收件人邮箱格式/是否允许外域投递。', raw:s};
          if(s.startsWith('Command') || s.includes('send_mail_icloud.sh')) return {title:'发送脚本执行失败', hint:'查看 logs/mail_send.log 或检查 SMTP 配置。', raw:s};
          return {title: short(s, 60), hint:'', raw:s};
        }
        async function loadRuns(){
          const j = await api('/admin/api/run_status?limit=30');
          if(!j || !j.ok){
            toast('err','加载失败', JSON.stringify(j));
            return;
          }
          const t = j.today || {};
          const last = summarizeFail(t.last_error||'');
          const todayCard = document.getElementById('todayStatus');
          if(todayCard){
            todayCard.innerHTML = `
              <div>日期</div><b>${esc(t.date||'')}</b>
              <div>是否已发送</div><b>${t.sent ? '是' : '否'}</b>
              <div>是否触发兜底</div><b>${t.fallback_triggered ? '是' : '否'}</b>
              <div>最后错误</div><b title="${esc(last.raw||'')}">${esc(last.title || '无')}</b>
            `;
          }

          const rows = (j.runs||[]).map(r=>{
            const fail = summarizeFail(r.failed_reason_summary||'');
            return `<tr>
              <td>${esc(r.run_id||'')}</td>
              <td class="nowrap" title="${esc(String(r.time||''))}">${esc(fmtTime(r.time||''))}<div class="small">北京时间</div></td>
              <td>${esc(mapStatus(r.status))}</td>
              <td>${esc(r.source||'')}</td>
              <td title="${esc(fail.raw||'')}">
                <div style="font-weight:700">${esc(fail.title||'')}</div>
                <div class="small">${esc(fail.hint||'')}</div>
              </td>
            </tr>`;
          }).join('');
          document.getElementById('runTable').innerHTML = `<table>
            <thead><tr><th>Run ID</th><th>时间</th><th>状态</th><th>来源</th><th>失败原因摘要</th></tr></thead>
            <tbody>${rows || '<tr><td colspan=\"5\">暂无数据</td></tr>'}</tbody>
          </table>`;
        }
        loadRuns();
        """
        return _page_shell("运行状态", body, js)

    return app


app = create_app()


def run_server() -> None:
    host = os.environ.get("ADMIN_API_HOST", "127.0.0.1")
    port = int(os.environ.get("ADMIN_API_PORT", "8789"))
    uvicorn.run("app.web.rules_admin_api:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run_server()
