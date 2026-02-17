from __future__ import annotations

import base64
import json
import os
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from datetime import datetime
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from app.rules.engine import RuleEngine
from app.services.rules_versioning import (
    ensure_bootstrap_published,
    get_published_pointer,
    get_runtime_rules_root,
    list_versions,
    publish_rules_version,
    rollback_to_previous,
    stage_rules_overlay,
)
from app.services.source_registry import validate_sources_registry


EDITABLE_FILES = [
    "email_rules/legacy.yaml",
    "email_rules/enhanced.yaml",
    "content_rules/legacy.yaml",
    "content_rules/enhanced.yaml",
    "sources/rss.yaml",
    "sources/web.yaml",
    "sources/api.yaml",
]


def _json(handler: BaseHTTPRequestHandler, code: int, obj: dict) -> None:
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _read_body_json(handler: BaseHTTPRequestHandler) -> dict:
    length = int(handler.headers.get("Content-Length", "0") or "0")
    raw = handler.rfile.read(length) if length > 0 else b"{}"
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def _auth_ok(handler: BaseHTTPRequestHandler) -> bool:
    token = os.environ.get("RULES_CONSOLE_TOKEN", "").strip()
    if token:
        hdr = handler.headers.get("X-Console-Token", "")
        return hdr == token

    user = os.environ.get("RULES_CONSOLE_USER", "").strip()
    pwd = os.environ.get("RULES_CONSOLE_PASS", "").strip()
    if not user or not pwd:
        return False

    auth = handler.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return False
    try:
        raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
    except Exception:
        return False
    if ":" not in raw:
        return False
    u, p = raw.split(":", 1)
    return u == user and p == pwd


def _require_auth(handler: BaseHTTPRequestHandler) -> bool:
    if _auth_ok(handler):
        return True
    handler.send_response(401)
    handler.send_header("WWW-Authenticate", 'Basic realm="RulesConsole"')
    handler.end_headers()
    return False


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_rules_bundle(project_root: Path) -> dict[str, str]:
    ensure_bootstrap_published(project_root)
    root = get_runtime_rules_root(project_root)
    out: dict[str, str] = {}
    for rel in EDITABLE_FILES:
        p = root / rel
        out[rel] = p.read_text(encoding="utf-8") if p.exists() else ""
    return out


def _validate_staged(project_root: Path, staged_rules_root: Path) -> dict:
    old = os.environ.get("RULES_WORKSPACE_DIR")
    try:
        os.environ["RULES_WORKSPACE_DIR"] = str(staged_rules_root)
        engine = RuleEngine(project_root=project_root)
        validated = []
        for profile in ("legacy", "enhanced"):
            try:
                validated.append(engine.validate_profile_pair(profile))
            except Exception as e:
                validated.append({"profile": profile, "ok": False, "error": str(e)})
        source = validate_sources_registry(project_root, rules_root=staged_rules_root)
        return {"ok": True, "rules": validated, "sources": source}
    finally:
        if old is None:
            os.environ.pop("RULES_WORKSPACE_DIR", None)
        else:
            os.environ["RULES_WORKSPACE_DIR"] = old


def _dryrun_staged(project_root: Path, staged_rules_root: Path, profile: str, date: str) -> dict:
    env = os.environ.copy()
    env["RULES_WORKSPACE_DIR"] = str(staged_rules_root)
    cmd = [
        "python3",
        "-m",
        "app.workers.cli",
        "rules:dryrun",
        "--profile",
        profile,
        "--date",
        date,
    ]
    proc = subprocess.run(cmd, cwd=project_root, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr or proc.stdout or "dryrun failed")
    return json.loads(proc.stdout)


def _publish_from_payload(project_root: Path, payload: dict) -> dict:
    files = payload.get("files", {})
    created_by = str(payload.get("created_by", "rules-console"))
    note = str(payload.get("note", ""))
    if not isinstance(files, dict):
        raise ValueError("files must be object")

    overlays = {str(k): str(v) for k, v in files.items() if str(k) in EDITABLE_FILES}
    staged = stage_rules_overlay(project_root, overlays)
    _ = _validate_staged(project_root, staged)
    return publish_rules_version(project_root, staged, created_by=created_by, note=note)


HTML = """<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>Rules Console</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; }
    textarea { width: 100%; height: 220px; font-family: Menlo, monospace; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .card { border: 1px solid #ddd; padding: 10px; border-radius: 8px; }
    button { margin-right: 8px; }
    pre { background: #f7f7f7; padding: 10px; border-radius: 8px; overflow: auto; }
  </style>
</head>
<body>
  <h2>Rules Console</h2>
  <div>
    <button onclick=\"loadCurrent()\">加载当前发布版本</button>
    <button onclick=\"validateDraft()\">校验</button>
    <button onclick=\"previewDraft()\">Dry-run预览</button>
    <button onclick=\"publishDraft()\">发布</button>
    <button onclick=\"rollback()\">回滚到上一版本</button>
    <button onclick=\"loadVersions()\">版本列表</button>
  </div>
  <p></p>
  <div class=\"grid\"> 
    <div class=\"card\"><h4>email_rules/enhanced.yaml</h4><textarea id=\"f_email_enhanced\"></textarea></div>
    <div class=\"card\"><h4>content_rules/enhanced.yaml</h4><textarea id=\"f_content_enhanced\"></textarea></div>
    <div class=\"card\"><h4>sources/rss.yaml</h4><textarea id=\"f_sources_rss\"></textarea></div>
    <div class=\"card\"><h4>sources/web.yaml</h4><textarea id=\"f_sources_web\"></textarea></div>
  </div>
  <h4>输出</h4>
  <pre id=\"out\"></pre>
<script>
async function api(path, method='GET', body=null) {
  const opts = {method, headers: {'Content-Type':'application/json'}};
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(path, opts);
  const t = await r.text();
  try { return JSON.parse(t); } catch(e) { return {ok:false, raw:t}; }
}

function filesPayload() {
  return {
    'email_rules/enhanced.yaml': document.getElementById('f_email_enhanced').value,
    'content_rules/enhanced.yaml': document.getElementById('f_content_enhanced').value,
    'sources/rss.yaml': document.getElementById('f_sources_rss').value,
    'sources/web.yaml': document.getElementById('f_sources_web').value,
  };
}

async function loadCurrent() {
  const j = await api('/api/rules/current');
  document.getElementById('f_email_enhanced').value = j.files?.['email_rules/enhanced.yaml'] || '';
  document.getElementById('f_content_enhanced').value = j.files?.['content_rules/enhanced.yaml'] || '';
  document.getElementById('f_sources_rss').value = j.files?.['sources/rss.yaml'] || '';
  document.getElementById('f_sources_web').value = j.files?.['sources/web.yaml'] || '';
  document.getElementById('out').textContent = JSON.stringify(j, null, 2);
}

async function validateDraft() {
  const j = await api('/api/rules/validate', 'POST', {files: filesPayload()});
  document.getElementById('out').textContent = JSON.stringify(j, null, 2);
}

async function previewDraft() {
  const j = await api('/api/rules/preview', 'POST', {files: filesPayload(), profile:'enhanced'});
  document.getElementById('out').textContent = JSON.stringify(j, null, 2);
}

async function publishDraft() {
  const by = prompt('created_by', 'rules-console-user') || 'rules-console-user';
  const note = prompt('note', 'publish from rules console') || '';
  const j = await api('/api/rules/publish', 'POST', {files: filesPayload(), created_by: by, note});
  document.getElementById('out').textContent = JSON.stringify(j, null, 2);
}

async function rollback() {
  const by = prompt('created_by', 'rules-console-user') || 'rules-console-user';
  const j = await api('/api/rules/rollback', 'POST', {created_by: by});
  document.getElementById('out').textContent = JSON.stringify(j, null, 2);
}

async function loadVersions() {
  const j = await api('/api/rules/versions');
  document.getElementById('out').textContent = JSON.stringify(j, null, 2);
}

loadCurrent();
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if not _require_auth(self):
            return
        pr = urlparse(self.path)
        if pr.path == "/":
            data = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if pr.path == "/api/health":
            _json(self, 200, {"ok": True})
            return
        if pr.path == "/api/rules/current":
            root = _project_root()
            ensure_bootstrap_published(root)
            _json(
                self,
                200,
                {
                    "ok": True,
                    "files": _read_rules_bundle(root),
                    "published": get_published_pointer(root),
                },
            )
            return
        if pr.path == "/api/rules/versions":
            root = _project_root()
            ensure_bootstrap_published(root)
            _json(
                self,
                200,
                {
                    "ok": True,
                    "published": get_published_pointer(root),
                    "versions": list_versions(root),
                },
            )
            return
        _json(self, 404, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:
        if not _require_auth(self):
            return
        pr = urlparse(self.path)
        root = _project_root()
        payload = _read_body_json(self)

        try:
            if pr.path == "/api/rules/validate":
                files = payload.get("files", {}) if isinstance(payload, dict) else {}
                overlays = {str(k): str(v) for k, v in files.items() if str(k) in EDITABLE_FILES}
                staged = stage_rules_overlay(root, overlays)
                out = _validate_staged(root, staged)
                _json(self, 200, out)
                return

            if pr.path == "/api/rules/preview":
                files = payload.get("files", {}) if isinstance(payload, dict) else {}
                profile = str(payload.get("profile", "enhanced"))
                date = str(payload.get("date", "") or "")
                if not date:
                    date = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
                overlays = {str(k): str(v) for k, v in files.items() if str(k) in EDITABLE_FILES}
                staged = stage_rules_overlay(root, overlays)
                _ = _validate_staged(root, staged)
                out = _dryrun_staged(root, staged, profile=profile, date=date)
                _json(self, 200, {"ok": True, "preview": out})
                return

            if pr.path == "/api/rules/publish":
                out = _publish_from_payload(root, payload if isinstance(payload, dict) else {})
                _json(self, 200, out)
                return

            if pr.path == "/api/rules/rollback":
                created_by = str((payload or {}).get("created_by", "rules-console-user"))
                out = rollback_to_previous(root, created_by=created_by)
                _json(self, 200, out)
                return

            _json(self, 404, {"ok": False, "error": "not found"})
        except Exception as e:
            _json(self, 400, {"ok": False, "error": str(e)})


def run_server(host: str = "127.0.0.1", port: int = 8787) -> None:
    if not os.environ.get("RULES_CONSOLE_TOKEN"):
        if not (os.environ.get("RULES_CONSOLE_USER") and os.environ.get("RULES_CONSOLE_PASS")):
            raise RuntimeError("Rules Console requires RULES_CONSOLE_TOKEN or RULES_CONSOLE_USER/PASS")

    root = _project_root()
    ensure_bootstrap_published(root)

    srv = ThreadingHTTPServer((host, port), Handler)
    print(f"Rules Console listening at http://{host}:{port}")
    srv.serve_forever()


if __name__ == "__main__":
    h = os.environ.get("RULES_CONSOLE_HOST", "127.0.0.1")
    p = int(os.environ.get("RULES_CONSOLE_PORT", "8787"))
    run_server(host=h, port=p)
