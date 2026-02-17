from __future__ import annotations

import json
import html
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBasic, HTTPBasicCredentials, HTTPBearer
from pydantic import BaseModel, Field

from app.rules.engine import RuleEngine
from app.services.rules_store import RulesStore
from app.services.rules_versioning import get_workspace_rules_root
from app.services.source_registry import test_source
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
    name = "email_rules.schema.json" if ruleset == "email_rules" else "content_rules.schema.json"
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

    connector = str(source.get("connector", "")).strip()
    if connector not in {"rss", "web", "api"}:
        out.append({"path": "$.connector", "message": "connector 必须为 rss|web|api"})

    url = str(source.get("url", "")).strip()
    if connector in {"rss", "web"} and not _is_valid_url(url):
        out.append({"path": "$.url", "message": "url 非法或为空"})
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
    connector: str
    url: str = ""
    enabled: bool = True
    priority: int = 0
    trust_tier: str
    tags: list[str] = Field(default_factory=list)
    rate_limit: dict[str, Any] = Field(default_factory=dict)


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

    @app.exception_handler(HTTPException)
    async def _http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:  # type: ignore[override]
        payload = {"ok": False, "error": {"code": f"HTTP_{exc.status_code}", "message": str(exc.detail)}}
        return JSONResponse(status_code=exc.status_code, content=payload, headers=exc.headers)

    def _active_rules(ruleset: str, profile: str) -> dict[str, Any]:
        data = (
            store.get_active_email_rules(profile)
            if ruleset == "email_rules"
            else store.get_active_content_rules(profile)
        )
        if data is not None:
            meta = data.pop("_store_meta", {})
            return {"source": "db", "config_json": data, "meta": meta}
        sel = engine.load(ruleset, profile)
        return {
            "source": "file",
            "config_json": sel.data,
            "meta": {"profile": sel.profile, "version": sel.version, "path": str(sel.path)},
        }

    def _page_shell(title: str, body_html: str, script_js: str = "") -> str:
        nav = """
        <aside class="sidebar">
          <div class="brand">规则控制台</div>
          <a class="nav" href="/admin/email">邮件规则</a>
          <a class="nav" href="/admin/content">采集规则</a>
          <a class="nav" href="/admin/sources">信源管理</a>
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
    pre { background: rgba(0,0,0,.25); padding:10px; border-radius:12px; overflow:auto; border:1px solid var(--border); }
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
    .row { display:flex; gap: 10px; align-items: center; }
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
        active = (
            store.get_active_email_rules(profile)
            if ruleset == "email_rules"
            else store.get_active_content_rules(profile)
        )
        active_ver = ""
        if isinstance(active, dict):
            m = active.get("_store_meta", {})
            if isinstance(m, dict):
                active_ver = str(m.get("version", ""))
        for r in rows:
            r["active"] = bool(active_ver and str(r.get("version")) == active_ver)
        return rows

    def _config_for_version(ruleset: str, profile: str, version: str) -> dict[str, Any] | None:
        table = "email_rules_versions" if ruleset == "email_rules" else "content_rules_versions"
        with store._connect() as conn:  # noqa: SLF001
            row = conn.execute(
                f"SELECT config_json FROM {table} WHERE profile = ? AND version = ? LIMIT 1",
                (profile, version),
            ).fetchone()
        if row is None:
            return None
        obj = json.loads(str(row["config_json"]))
        return obj if isinstance(obj, dict) else None

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
            "artifacts_dir": out.get("artifacts_dir"),
        }

    @app.post("/admin/api/dryrun")
    def unified_dryrun(
        date: str | None = None,
        profile: str = "enhanced",
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

        return {
            "ok": True,
            "run_id": out.get("run_id"),
            "profile": out.get("profile"),
            "date": out.get("date"),
            "items_before": int(out.get("items_before_count") or 0),
            "items_after": int(out.get("items_after_count") or 0),
            "preview_text": preview_text,
            "preview_html": preview_html,
            "clustered_items": _read_json(str(artifacts.get("clustered_items", ""))),
            "explain": _read_json(str(artifacts.get("explain", ""))),
            "explain_cluster": _read_json(str(artifacts.get("cluster_explain", ""))),
            "items": _read_json(str(artifacts.get("items", ""))) or [],
            "artifacts": artifacts,
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

    @app.get("/admin/api/sources")
    def sources_list(_: dict[str, str] = Depends(_auth_guard)) -> dict[str, Any]:
        rows = store.list_sources()
        return {"ok": True, "count": len(rows), "sources": rows}

    @app.post("/admin/api/sources")
    def sources_upsert(
        payload: SourcePayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        source = payload.model_dump()
        errs = _source_errors(source, store)
        if errs:
            return {"ok": False, "error": {"code": "SOURCE_VALIDATION_FAILED", "details": errs}}
        return store.upsert_source(source)

    @app.post("/admin/api/sources/{source_id}/toggle")
    def sources_toggle(
        source_id: str,
        payload: TogglePayload,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        return store.toggle_source(source_id, enabled=payload.enabled)

    @app.post("/admin/api/sources/{source_id}/test")
    def sources_test(source_id: str, _: dict[str, str] = Depends(_auth_guard)) -> dict[str, Any]:
        source = store.get_source(source_id)
        if source is None:
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
        }

    @app.get("/admin/api/versions/diff")
    def _json_diff(left: Any, right: Any, path: str = "$", out: list[dict[str, Any]] | None = None, limit: int = 200) -> list[dict[str, Any]]:
        out = out or []
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
            out.append({"path": path, "op": "changed", "from": f"list(len={len(left)})", "to": f"list(len={len(right)})"})
            return out
        if left != right:
            out.append({"path": path, "op": "changed", "from": left, "to": right})
        return out

    def versions_diff(
        ruleset: str,
        profile: str,
        from_version: str,
        to_version: str,
        _: dict[str, str] = Depends(_auth_guard),
    ) -> dict[str, Any]:
        if ruleset not in {"email_rules", "content_rules"}:
            raise HTTPException(status_code=400, detail="ruleset must be email_rules|content_rules")
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
	            <label>规则 Profile</label>
	            <select id="profile"><option value="enhanced">enhanced</option><option value="legacy">legacy</option></select>
	            <label>启用开关</label><select id="enabled"><option value="true">true</option><option value="false">false</option></select>
	            <label>发送时间(小时)</label><input id="hour" type="number" min="0" max="23"/>
	            <label>发送时间(分钟)</label><input id="minute" type="number" min="0" max="59"/>
	            <label>收件人列表(逗号分隔)</label><input id="recipients" placeholder="a@b.com,c@d.com"/>
	            <label>主题模板</label><input id="subject_template" />
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
	                  <li><b>规则 Profile</b>：灰度/配置档位。通常用 <code>legacy</code> 保持原行为，用 <code>enhanced</code> 启用增强规则。</li>
	                  <li><b>启用开关</b>：关闭后不发送邮件（用于临时停发或维护窗口）。</li>
	                  <li><b>发送时间</b>：按北京时间定时触发（GitHub Actions 兜底补发逻辑不受这里影响）。</li>
	                  <li><b>收件人列表</b>：逗号分隔邮箱地址（将写入 rules 的 recipient 字段）。</li>
	                  <li><b>主题模板</b>：支持占位符（例如 <code>{{date}}</code>），用于生成邮件主题。</li>
	                </ul>
	              </div>
	            </details>
	          </div>
	          <div class="card">
	            <label>试跑日期(可选, YYYY-MM-DD)</label><input id="dryrun_date" />
	            <div class="row">
	              <button onclick="preview()">试跑预览(不发信)</button>
	              <button onclick="copyPreview()">复制预览</button>
	            </div>
	            <div>
	              <label>高亮关键词(逗号分隔，留空自动从 content_rules 读取 include/exclude)</label>
	              <input id="highlight_terms" />
	            </div>
	            <pre id="preview"></pre>
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
          const j = await api('/admin/api/email_rules/dryrun','POST',{ profile, date });
          if (!j || !j.ok) { document.getElementById('preview').textContent = JSON.stringify(j,null,2); return; }
          let includeTerms = [];
          let excludeTerms = [];
          const custom = splitCsv(document.getElementById('highlight_terms').value);
          if(custom.length){
            includeTerms = custom;
          } else {
            const cj = await api(`/admin/api/content_rules/active?profile=${encodeURIComponent(profile)}`);
            const rules = (cj && cj.ok) ? (cj.config_json.rules||[]) : [];
            const inc = (rules.find(r=>r.type==='include_filter')||{}).params||{};
            const exc = (rules.find(r=>r.type==='exclude_filter')||{}).params||{};
            includeTerms = inc.include_keywords||[];
            excludeTerms = exc.exclude_keywords||[];
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
	            <label>规则 Profile</label>
	            <select id="profile"><option value="enhanced">enhanced</option><option value="legacy">legacy</option></select>
	            <label>全局采集频次(小时)</label><input id="primary_hours" type="number" min="1" max="168"/>
	            <label>去重窗口(天)</label><input id="fallback_days" type="number" min="1" max="30"/>
	            <label>关键词包含(逗号分隔)</label><input id="include_keywords"/>
	            <label>关键词排除(逗号分隔)</label><input id="exclude_keywords"/>
	            <label>地区过滤(逗号分隔)</label><input id="allowed_regions"/>
	            <label>赛道过滤(逗号分隔)</label><input id="tracks"/>
	            <label>最低可信度</label><input id="min_confidence" type="number" step="0.01" min="0" max="1"/>
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
	                  <li><b>全局采集频次</b>：控制主要抓取周期（小时）。越短越及时，但更耗时/更易触发限流。</li>
	                  <li><b>去重窗口</b>：用于“24小时优先，不足则补近7天”的时间窗策略，以及 story 聚合/去重的窗口基准。</li>
	                  <li><b>关键词包含/排除</b>：基础过滤器。包含为空通常表示不过滤；排除用于剔除软文/非相关噪音。</li>
	                  <li><b>地区/赛道过滤</b>：用于聚焦某些区域或业务方向（例如 <code>cn</code>/<code>apac</code>、<code>肿瘤</code>/<code>感染</code> 等）。</li>
	                  <li><b>最低可信度</b>：0-1 的阈值，低于该值的条目不会入选候选集。</li>
	                </ul>
	              </div>
	            </details>
	          </div>
	          <div class="card">
	            <label>试跑日期(可选, YYYY-MM-DD)</label><input id="dryrun_date" />
	            <button onclick="preview()">试跑预览(不发信)</button>
	            <div class="drawer" id="summary"></div>
	            <div class="cards" id="clusters"></div>
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
        function csv(v){ return (v||[]).join(','); }
        function arr(v){ return (v||'').split(',').map(s=>s.trim()).filter(Boolean); }
        function esc(s){ return String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
	        async function loadActive() {
	          const profile = document.getElementById('profile').value;
	          const j = await api(`/admin/api/content_rules/active?profile=${encodeURIComponent(profile)}`);
	          if (!j || !j.ok) { document.getElementById('status').textContent = JSON.stringify(j); toast('err','加载失败','读取生效配置失败'); return; }
	          currentConfig = j.config_json;
	          document.getElementById('verPill').textContent = `生效版本: ${j.meta?.version || j.meta?.path || '-'}`;
          const tw = (currentConfig.defaults||{}).time_window || {};
          document.getElementById('primary_hours').value = tw.primary_hours ?? 24;
          document.getElementById('fallback_days').value = tw.fallback_days ?? 7;
          const includeRule = ((currentConfig.rules||[]).find(r=>r.type==='include_filter')||{}).params||{};
          const excludeRule = ((currentConfig.rules||[]).find(r=>r.type==='exclude_filter')||{}).params||{};
          document.getElementById('include_keywords').value = csv(includeRule.include_keywords||[]);
          document.getElementById('exclude_keywords').value = csv(excludeRule.exclude_keywords||[]);
          document.getElementById('allowed_regions').value = csv(((currentConfig.defaults||{}).region_filter||{}).allowed_regions||[]);
          document.getElementById('tracks').value = csv((currentConfig.defaults||{}).coverage_tracks||[]);
          document.getElementById('min_confidence').value = Number(((currentConfig.overrides||{}).min_confidence ?? 0));
	          document.getElementById('status').innerHTML = '<span class="ok">已加载生效配置</span>';
          document.getElementById('btnPublish').disabled = true;
          currentDraftId = null;
        }
        function buildConfig() {
          const cfg = JSON.parse(JSON.stringify(currentConfig||{}));
          if (!cfg.defaults) cfg.defaults = {};
          if (!cfg.defaults.time_window) cfg.defaults.time_window = {};
          if (!cfg.defaults.region_filter) cfg.defaults.region_filter = {};
          if (!cfg.overrides) cfg.overrides = {};
          cfg.defaults.time_window.primary_hours = Number(document.getElementById('primary_hours').value||24);
          cfg.defaults.time_window.fallback_days = Number(document.getElementById('fallback_days').value||7);
          cfg.defaults.region_filter.allowed_regions = arr(document.getElementById('allowed_regions').value);
          cfg.defaults.coverage_tracks = arr(document.getElementById('tracks').value);
          cfg.overrides.min_confidence = Number(document.getElementById('min_confidence').value||0);
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
          const j = await api('/admin/api/content_rules/dryrun','POST',{ profile, date });
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
	          toast('ok','试跑完成', `运行ID=${j.run_id}`);
        }
        document.getElementById('profile').addEventListener('change', loadActive);
        loadActive();
        """
        return _page_shell("采集规则", body, js)

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
            </div>
            <label>信源ID（唯一）</label><input id="id" placeholder="例如：reuters-health-rss"/>
            <label>名称</label><input id="name" placeholder="例如：Reuters Healthcare"/>
            <label>采集方式</label><select id="connector"><option>rss</option><option>web</option><option>api</option></select>
            <label>URL（rss/web 必填）</label><input id="url" placeholder="https://..."/>
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
                  <li><b>采集方式</b>：<code>rss</code> 适合有 RSS 的媒体/公告；<code>web</code> 适合网页列表页（当前仅做基础 title 抓取/连通性测试）；<code>api</code> 预留（可先建档用于优先级/聚合主源选择）。</li>
                  <li><b>URL</b>：rss/web 必填，必须是 http/https。用于抓取与 test。</li>
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
          const t = (s.id+' '+s.name+' '+(s.tags||[]).join(' ')+' '+(s.url||'')).toLowerCase();
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
          const lastRaw = s.last_success_at || s.updated_at || '';
          const last = esc(fmtTime(lastRaw) || lastRaw || '-');
	          const err = s.last_error ? `<span class="pill err" title="${esc(s.last_error)}">异常</span>` : '';
          const hs = s.last_http_status ? `<span class="pill">${esc(s.last_http_status)}</span>` : '';
          const urlText = prettyUrl(s.url||'');
          const urlCell = s.url ? `<a class="url" href="${esc(s.url)}" target="_blank" title="${esc(s.url)}">${esc(urlText)}</a>` : '';
          const toggleLabel = s.enabled ? '停用' : '启用';
          return `
            <tr data-id="${esc(s.id)}">
              <td class="nowrap">${s.enabled ? '启用' : '停用'}</td>
              <td>${esc(s.name)} ${err} ${hs}</td>
              <td class="nowrap">${esc(s.connector)}</td>
              <td class="urlcol">${urlCell}</td>
              <td class="nowrap">${s.priority}</td>
              <td class="tagcol">${esc((s.tags||[]).join(','))}</td>
              <td>${last}</td>
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
          const filtered = (j.sources||[]).filter(s=>{
            if(fe==='enabled' && !s.enabled) return false;
            if(fe==='disabled' && s.enabled) return false;
            return matchQ(s,q);
          });
          const rows = filtered.map(renderRow).join('');
          document.getElementById('table').innerHTML = `<table><thead><tr><th>状态</th><th>名称</th><th>方式</th><th>链接</th><th>优先级</th><th>标签</th><th>最近成功</th><th>操作</th></tr></thead><tbody>${rows}</tbody></table>`;
          window._sources = j.sources||[];
        }
        function editSource(id){
          const s = (window._sources||[]).find(x=>x.id===id); if(!s) return;
          document.getElementById('id').value = s.id||'';
          document.getElementById('name').value = s.name||'';
          document.getElementById('connector').value = s.connector||'rss';
          document.getElementById('url').value = s.url||'';
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
          document.getElementById('priority').value = '50';
          document.getElementById('trust_tier').value = 'B';
          document.getElementById('tags').value = '';
          document.getElementById('enabled').value = 'true';
        }
        async function saveSource(){
          const payload = {
            id: document.getElementById('id').value.trim(),
            name: document.getElementById('name').value.trim(),
            connector: document.getElementById('connector').value,
            url: document.getElementById('url').value.trim(),
            priority: Number(document.getElementById('priority').value||0),
            trust_tier: document.getElementById('trust_tier').value,
            tags: arr(document.getElementById('tags').value),
            enabled: document.getElementById('enabled').value === 'true',
            rate_limit: {}
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
              <label>规则 Profile</label>
              <select id="profile"><option value="enhanced">enhanced</option><option value="legacy">legacy</option></select>
            </div>
            <div>
              <button onclick="loadVersions()">刷新版本</button>
              <button onclick="rollbackEmail()">回滚邮件规则</button>
              <button onclick="rollbackContent()">回滚采集规则</button>
            </div>
          </div>
          <div class="versionsGrid" style="margin-top:12px">
            <div><h4>邮件规则版本</h4><div id="emailTable"></div></div>
            <div><h4>采集规则版本</h4><div id="contentTable"></div></div>
          </div>
          <h4>版本差异对比</h4>
          <label>规则集（ruleset）</label><select id="diff_ruleset"><option>email_rules</option><option>content_rules</option></select>
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
                <li><b>规则 Profile</b>：规则灰度/配置档位。通常用 <code>legacy</code> 保持原行为，用 <code>enhanced</code> 启用增强规则。</li>
                <li><b>版本号</b>：发布时生成的 DB 版本标识。点击版本号会自动填入 from/to。</li>
                <li><b>规则集（ruleset）</b>：<code>email_rules</code> 为邮件规则，<code>content_rules</code> 为采集规则。</li>
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
          window._versions = { email_rules: j.email_rules || [], content_rules: j.content_rules || [] };
          document.getElementById('emailTable').innerHTML = render(j.email_rules);
          document.getElementById('contentTable').innerHTML = render(j.content_rules);
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

    return app


app = create_app()


def run_server() -> None:
    host = os.environ.get("ADMIN_API_HOST", "127.0.0.1")
    port = int(os.environ.get("ADMIN_API_PORT", "8789"))
    uvicorn.run("app.web.rules_admin_api:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run_server()
