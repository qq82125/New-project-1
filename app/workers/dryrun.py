from __future__ import annotations

import json
import os
import re
import subprocess
import uuid
from pathlib import Path

from app.rules.engine import RuleEngine


def _parse_items_from_report(text: str) -> list[dict]:
    lines = text.splitlines()
    items: list[dict] = []
    in_a = False
    current: dict | None = None

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        if line.startswith("A. 今日要点"):
            in_a = True
            continue
        if in_a and re.match(r"^[B-G]\.\s", line):
            if current:
                items.append(current)
                current = None
            break
        if not in_a:
            continue

        m = re.match(r"^(\d+)\)\s*\[(.*?)\]\s*(.*)$", line)
        if m:
            if current:
                items.append(current)
            current = {
                "index": int(m.group(1)),
                "window_tag": m.group(2),
                "title": m.group(3),
                "summary": "",
                "published": "",
                "source": "",
                "link": "",
                "region": "",
                "lane": "",
                "event_type": "",
                "platform": "",
            }
            continue

        if not current:
            continue

        if line.startswith("摘要："):
            current["summary"] = line.replace("摘要：", "", 1).strip()
        elif line.startswith("发布日期："):
            current["published"] = line.replace("发布日期：", "", 1).strip()
        elif line.startswith("来源："):
            src = line.replace("来源：", "", 1).strip()
            if "|" in src:
                left, right = src.split("|", 1)
                current["source"] = left.strip()
                current["link"] = right.strip()
            else:
                current["source"] = src
        elif line.startswith("地区："):
            current["region"] = line.replace("地区：", "", 1).strip()
        elif line.startswith("赛道："):
            current["lane"] = line.replace("赛道：", "", 1).strip()
        elif line.startswith("事件类型："):
            current["event_type"] = line.replace("事件类型：", "", 1).strip()
        elif line.startswith("技术平台："):
            current["platform"] = line.replace("技术平台：", "", 1).strip()
        elif not current.get("summary"):
            # 兼容历史格式（摘要行可能不以“摘要：”开头）
            current["summary"] = line

    if in_a and current:
        items.append(current)

    return items


def run_dryrun(profile: str = "legacy", report_date: str | None = None) -> dict:
    engine = RuleEngine()
    run_id = f"dryrun-{uuid.uuid4().hex[:10]}"
    decision = engine.build_decision(profile=profile, run_id=run_id)

    project_root = engine.project_root
    artifacts_dir = project_root / "artifacts" / run_id
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["REPORT_TZ"] = str(
        decision.get("email_decision", {}).get("schedule", {}).get("timezone", "Asia/Shanghai")
    )
    if report_date:
        env["REPORT_DATE"] = report_date
    env["REPORT_RUN_ID"] = run_id
    env["DRYRUN_ARTIFACTS_DIR"] = str(artifacts_dir)
    if profile == "enhanced":
        env["ENHANCED_RULES_PROFILE"] = "enhanced"
    else:
        env.pop("ENHANCED_RULES_PROFILE", None)

    proc = subprocess.run(
        ["python3", "scripts/generate_ivd_report.py"],
        cwd=project_root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    preview_text = proc.stdout
    items = _parse_items_from_report(preview_text)
    cluster_explain_file = artifacts_dir / "cluster_explain.json"
    clustered_items_file = artifacts_dir / "clustered_items.json"
    source_stats_file = artifacts_dir / "source_stats.json"
    cluster_payload = {}
    if cluster_explain_file.exists():
        try:
            cluster_payload = json.loads(cluster_explain_file.read_text(encoding="utf-8"))
        except Exception:
            cluster_payload = {}
    source_payload = {}
    if source_stats_file.exists():
        try:
            source_payload = json.loads(source_stats_file.read_text(encoding="utf-8"))
        except Exception:
            source_payload = {}

    explain_payload = {
        "run_id": run_id,
        "mode": "dryrun",
        "profile": profile,
        "date": report_date,
        "decision_explain": decision.get("explain", {}),
        "rules_version": decision.get("rules_version", {}),
        "notes": ["Dry-run only: no DB write, no email send."],
    }

    (artifacts_dir / "run_id.json").write_text(
        json.dumps(explain_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (artifacts_dir / "newsletter_preview.md").write_text(preview_text, encoding="utf-8")
    (artifacts_dir / "items.json").write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "run_id": run_id,
        "mode": "dryrun",
        "profile": profile,
        "date": report_date,
        "artifacts_dir": str(artifacts_dir),
        "artifacts": {
            "explain": str(artifacts_dir / "run_id.json"),
            "preview": str(artifacts_dir / "newsletter_preview.md"),
            "items": str(artifacts_dir / "items.json"),
            "clustered_items": str(clustered_items_file),
            "cluster_explain": str(cluster_explain_file),
            "source_stats": str(source_stats_file),
        },
        "items_count": len(items),
        "items_before_count": int(cluster_payload.get("items_before_count", len(items))),
        "items_after_count": int(cluster_payload.get("items_after_count", len(items))),
        "top_clusters": cluster_payload.get("top_clusters", []),
        "source_stats": source_payload.get("sources", []),
        "sent": False,
        "decision": {
            "content_decision": decision.get("content_decision", {}),
            "email_decision": decision.get("email_decision", {}),
        },
    }


def main(profile: str = "legacy", report_date: str | None = None) -> int:
    print(json.dumps(run_dryrun(profile=profile, report_date=report_date), ensure_ascii=False, indent=2))
    return 0
