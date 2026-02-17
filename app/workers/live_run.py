from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.adapters.rule_bridge import load_runtime_rules
from app.rules.engine import RuleEngine


def _utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def run_digest(
    *,
    profile: str,
    trigger: str,
    schedule_id: str,
    send: bool = True,
    report_date: str | None = None,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """
    Live run: generate newsletter (network fetch) and optionally send email.

    This intentionally reuses existing scripts to avoid breaking behavior.
    """
    engine = RuleEngine(project_root=project_root) if project_root else RuleEngine()
    root = engine.project_root

    run_id = f"run-{uuid.uuid4().hex[:10]}"
    artifacts_dir = root / "artifacts" / run_id
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    started_at = time.time()
    status = "success"
    error_summary = ""

    # Build decision once for version recording & timezone.
    decision = engine.build_decision(profile=profile, run_id=run_id)
    rules_version = decision.get("rules_version", {})
    email_decision = decision.get("email_decision", {}) if isinstance(decision.get("email_decision"), dict) else {}
    tz_name = str((email_decision.get("schedule", {}) or {}).get("timezone", "Asia/Shanghai"))

    env = os.environ.copy()
    env["REPORT_TZ"] = tz_name
    if report_date:
        env["REPORT_DATE"] = report_date
    env["REPORT_RUN_ID"] = run_id
    env["DRYRUN_ARTIFACTS_DIR"] = str(artifacts_dir)
    if profile == "enhanced":
        env["ENHANCED_RULES_PROFILE"] = "enhanced"
    else:
        env.pop("ENHANCED_RULES_PROFILE", None)

    # Determine output file path consistent with existing conventions.
    if report_date:
        date_str = report_date
    else:
        date_str = datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
    out_dir = root / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"ivd_morning_{date_str}.txt"

    try:
        proc = subprocess.run(
            ["python3", "scripts/generate_ivd_report.py"],
            cwd=root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        out_file.write_text(proc.stdout, encoding="utf-8")

        # Subject: from email rules if available, else fallback.
        rt = load_runtime_rules(date_str=date_str, env=env, run_id=run_id)
        subject = str(rt.get("email", {}).get("subject") or f"全球IVD晨报 - {date_str}")

        sent = False
        send_cmd: list[str] | None = None
        if send:
            to_email = env.get("TO_EMAIL", "")
            if not to_email:
                # Backward-compatible: if TO_EMAIL absent, pick first configured recipient or fail.
                rec = (rt.get("email", {}).get("recipients") or [])
                if isinstance(rec, list) and rec:
                    to_email = str(rec[0])
            if not to_email:
                raise RuntimeError("missing TO_EMAIL (and no recipients available)")
            send_cmd = ["./send_mail_icloud.sh", to_email, subject, str(out_file)]
            subprocess.run(send_cmd, cwd=root, env=env, check=True)
            sent = True

        return_payload = {
            "ok": True,
            "run_id": run_id,
            "mode": "live",
            "trigger": trigger,
            "schedule_id": schedule_id,
            "profile": profile,
            "date": date_str,
            "rules_version": rules_version,
            "artifacts_dir": str(artifacts_dir),
            "output_file": str(out_file),
            "sent": sent,
            "send_cmd": send_cmd,
        }
    except Exception as e:
        status = "failed"
        error_summary = str(e)
        return_payload = {
            "ok": False,
            "run_id": run_id,
            "mode": "live",
            "trigger": trigger,
            "schedule_id": schedule_id,
            "profile": profile,
            "rules_version": rules_version,
            "artifacts_dir": str(artifacts_dir),
            "status": status,
            "error_summary": error_summary,
        }
    finally:
        finished_at = time.time()
        meta = {
            "run_id": run_id,
            "trigger": trigger,
            "schedule_id": schedule_id,
            "profile": profile,
            "rules_version": rules_version,
            "status": status,
            "error_summary": error_summary,
            "started_at": _utc_iso(),
            "duration_ms": int((finished_at - started_at) * 1000),
        }
        (artifacts_dir / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    return return_payload

