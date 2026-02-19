#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.rules.engine import RuleEngine
from app.services.source_registry import load_sources_registry_bundle


def _now_tag() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _get_proxy_env() -> dict[str, str]:
    keys = ["HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"]
    return {k: os.environ.get(k, "") for k in keys if os.environ.get(k, "")}


def _resolve_host(host: str, timeout: float = 3.0) -> dict[str, Any]:
    socket.setdefaulttimeout(timeout)
    try:
        infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        ips = sorted({x[4][0] for x in infos if x and len(x) > 4 and x[4]})
        return {"ok": True, "ips": ips[:5], "error": ""}
    except Exception as e:
        return {"ok": False, "ips": [], "error": f"{type(e).__name__}: {e}"}


def _https_probe(host: str, timeout: float = 6.0) -> dict[str, Any]:
    url = f"https://{host}/"
    req = Request(url, headers={"User-Agent": "IVD-Network-Diag/1.0"})
    try:
        with urlopen(req, timeout=timeout) as r:
            code = int(getattr(r, "status", 0) or 0)
            return {"ok": True, "http_status": code, "error": ""}
    except URLError as e:
        return {"ok": False, "http_status": 0, "error": f"URLError: {e}"}
    except Exception as e:
        return {"ok": False, "http_status": 0, "error": f"{type(e).__name__}: {e}"}


def _container_dns_check(hosts: list[str], timeout: float) -> dict[str, Any]:
    if not hosts:
        return {"ok": False, "error": "no hosts", "results": {}}

    code = r"""
import json, os, socket
hosts = json.loads(os.environ.get("HOSTS_JSON","[]"))
timeout = float(os.environ.get("DIAG_TIMEOUT","3"))
socket.setdefaulttimeout(timeout)
out = {}
for h in hosts:
    try:
        infos = socket.getaddrinfo(h, 443, proto=socket.IPPROTO_TCP)
        ips = sorted({x[4][0] for x in infos if x and len(x)>4 and x[4]})
        out[h] = {"ok": True, "ips": ips[:5], "error": ""}
    except Exception as e:
        out[h] = {"ok": False, "ips": [], "error": f"{type(e).__name__}: {e}"}
print(json.dumps(out, ensure_ascii=False))
"""
    env = os.environ.copy()
    env["HOSTS_JSON"] = json.dumps(hosts, ensure_ascii=False)
    env["DIAG_TIMEOUT"] = str(timeout)
    cmd = ["docker", "compose", "exec", "-T", "admin-api", "python3", "-c", code]
    try:
        p = subprocess.run(cmd, cwd=ROOT, env=env, check=True, capture_output=True, text=True)
        data = json.loads(p.stdout.strip() or "{}")
        return {"ok": True, "error": "", "results": data}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "results": {}}


def _container_proxy_env() -> dict[str, Any]:
    code = r"""
import json, os
keys = ["HTTP_PROXY","HTTPS_PROXY","NO_PROXY","http_proxy","https_proxy","no_proxy"]
print(json.dumps({k: os.environ.get(k,"") for k in keys if os.environ.get(k,"")}, ensure_ascii=False))
"""
    cmd = ["docker", "compose", "exec", "-T", "admin-api", "python3", "-c", code]
    try:
        p = subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True)
        return {"ok": True, "env": json.loads(p.stdout.strip() or "{}"), "error": ""}
    except Exception as e:
        return {"ok": False, "env": {}, "error": f"{type(e).__name__}: {e}"}


def _hosts_from_enabled_sources(limit_hosts: int) -> list[str]:
    engine = RuleEngine()
    bundle = load_sources_registry_bundle(engine.project_root, rules_root=engine.rules_root)
    rows = bundle.get("sources", []) if isinstance(bundle, dict) else []
    freq: dict[str, int] = {}
    for s in rows:
        if not bool(s.get("enabled", True)):
            continue
        u = str(s.get("url", "")).strip()
        if not u.startswith(("http://", "https://")):
            continue
        h = (urlparse(u).hostname or "").lower().strip()
        if not h:
            continue
        freq[h] = freq.get(h, 0) + 1
    hosts = [k for k, _ in sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))]
    return hosts[: max(1, int(limit_hosts))]


def _diagnose(hosts: list[str], timeout: float) -> dict[str, Any]:
    local = {}
    for h in hosts:
        local[h] = {"dns": _resolve_host(h, timeout=timeout), "https": _https_probe(h, timeout=max(4.0, timeout * 2))}
    c_dns = _container_dns_check(hosts, timeout)
    c_proxy = _container_proxy_env()
    return {
        "hosts": hosts,
        "local": local,
        "container_dns": c_dns,
        "local_proxy_env": _get_proxy_env(),
        "container_proxy_env": c_proxy,
    }


def _summarize(payload: dict[str, Any]) -> dict[str, Any]:
    hosts = payload.get("hosts", [])
    local = payload.get("local", {})
    c_dns = payload.get("container_dns", {}).get("results", {})
    local_dns_fail = [h for h in hosts if not bool(((local.get(h) or {}).get("dns") or {}).get("ok"))]
    container_dns_fail = [h for h in hosts if not bool((c_dns.get(h) or {}).get("ok"))] if isinstance(c_dns, dict) else hosts[:]
    local_https_fail = [h for h in hosts if not bool(((local.get(h) or {}).get("https") or {}).get("ok"))]

    findings: list[str] = []
    if len(local_dns_fail) == len(hosts) and hosts:
        findings.append("宿主机 DNS 全量失败：先检查本机网络/DNS/代理。")
    elif len(container_dns_fail) == len(hosts) and hosts:
        findings.append("容器 DNS 全量失败：优先检查 Docker Desktop DNS/代理与 daemon 网络。")
    elif container_dns_fail and not local_dns_fail:
        findings.append("宿主机可解析但容器失败：高概率是 Docker 容器 DNS 配置问题。")
    elif local_dns_fail:
        findings.append("部分域名 DNS 失败：可能是区域网络策略或站点 DNS 不稳定。")

    if local_https_fail and len(local_https_fail) >= max(3, len(hosts) // 2):
        findings.append("HTTPS 连通失败较多：可能存在代理证书/出口限制。")

    if not findings:
        findings.append("网络层未见显著异常，建议继续排查目标站反爬/结构变更。")

    return {
        "total_hosts": len(hosts),
        "local_dns_fail": len(local_dns_fail),
        "container_dns_fail": len(container_dns_fail),
        "local_https_fail": len(local_https_fail),
        "local_dns_fail_hosts": local_dns_fail[:15],
        "container_dns_fail_hosts": container_dns_fail[:15],
        "local_https_fail_hosts": local_https_fail[:15],
        "findings": findings,
    }


def _to_markdown(payload: dict[str, Any], summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Network Diagnose Report")
    lines.append("")
    lines.append(
        f"- hosts={summary.get('total_hosts',0)} | local_dns_fail={summary.get('local_dns_fail',0)} | "
        f"container_dns_fail={summary.get('container_dns_fail',0)} | local_https_fail={summary.get('local_https_fail',0)}"
    )
    lines.append("")
    lines.append("## 结论")
    for x in summary.get("findings", []):
        lines.append(f"- {x}")
    lines.append("")
    lines.append("## 宿主机代理环境")
    lines.append(f"- {json.dumps(payload.get('local_proxy_env', {}), ensure_ascii=False)}")
    lines.append("")
    lines.append("## 容器代理环境(admin-api)")
    cpe = payload.get("container_proxy_env", {})
    if cpe.get("ok"):
        lines.append(f"- {json.dumps(cpe.get('env', {}), ensure_ascii=False)}")
    else:
        lines.append(f"- 读取失败: {cpe.get('error')}")
    lines.append("")
    lines.append("## 失败域名样例")
    if summary.get("container_dns_fail_hosts"):
        lines.append(f"- container_dns_fail: {', '.join(summary.get('container_dns_fail_hosts', []))}")
    if summary.get("local_dns_fail_hosts"):
        lines.append(f"- local_dns_fail: {', '.join(summary.get('local_dns_fail_hosts', []))}")
    if summary.get("local_https_fail_hosts"):
        lines.append(f"- local_https_fail: {', '.join(summary.get('local_https_fail_hosts', []))}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Diagnose host/container network issues for source fetching.")
    ap.add_argument("--limit-hosts", type=int, default=20, help="Top N enabled-source hostnames to test")
    ap.add_argument("--timeout", type=float, default=3.0, help="DNS timeout in seconds")
    ap.add_argument("--json-out", default="", help="Output JSON path")
    ap.add_argument("--md-out", default="", help="Output Markdown path")
    args = ap.parse_args()

    hosts = _hosts_from_enabled_sources(args.limit_hosts)
    payload = _diagnose(hosts, timeout=max(1.0, args.timeout))
    summary = _summarize(payload)
    out = {"ok": True, "summary": summary, "details": payload, "generated_at": dt.datetime.utcnow().isoformat() + "Z"}

    tag = _now_tag()
    default_json = ROOT / "artifacts" / f"network_diag_{tag}.json"
    default_md = ROOT / "artifacts" / f"network_diag_{tag}.md"
    json_out = Path(args.json_out) if args.json_out else default_json
    md_out = Path(args.md_out) if args.md_out else default_md
    json_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    md_out.write_text(_to_markdown(payload, summary), encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
                "summary": summary,
                "json_out": str(json_out),
                "md_out": str(md_out),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

