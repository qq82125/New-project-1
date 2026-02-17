from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from app.rules.engine import RuleEngine


def _to_bool(v: bool | str | None, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def run_replay(
    run_id: str,
    send: bool | str = False,
    profile: str | None = None,
) -> dict:
    if not run_id:
        raise ValueError("run_id is required")

    engine = RuleEngine()
    project_root = engine.project_root
    artifacts_dir = project_root / "artifacts" / run_id
    explain_file = artifacts_dir / "run_id.json"
    preview_file = artifacts_dir / "newsletter_preview.md"
    items_file = artifacts_dir / "items.json"

    if not explain_file.exists() or not preview_file.exists() or not items_file.exists():
        raise FileNotFoundError(f"replay artifacts not complete: {artifacts_dir}")

    explain = json.loads(explain_file.read_text(encoding="utf-8"))
    items = json.loads(items_file.read_text(encoding="utf-8"))

    profile = profile or str(explain.get("profile", "legacy"))
    decision = engine.build_decision(profile=profile)

    out_dir = project_root / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"ivd_morning_replay_{run_id}.txt"
    out_file.write_text(preview_file.read_text(encoding="utf-8"), encoding="utf-8")

    do_send = _to_bool(send, default=False)
    sent = False
    send_cmd: list[str] | None = None

    if do_send:
        schedule = decision.get("email_decision", {}).get("schedule", {})
        tz_name = str(schedule.get("timezone", "Asia/Shanghai"))
        subject_tpl = str(decision.get("email_decision", {}).get("subject_template", "全球IVD晨报 - {{date}}"))
        replay_date = str(explain.get("date") or "")
        subject = subject_tpl.replace("{{date}}", replay_date or "REPLAY")

        env = os.environ.copy()
        env["REPORT_TZ"] = tz_name
        to_email = env.get("TO_EMAIL", "qq82125@gmail.com")
        send_cmd = ["./send_mail_icloud.sh", to_email, subject, str(out_file)]
        subprocess.run(send_cmd, cwd=project_root, env=env, check=True)
        sent = True

    return {
        "run_id": run_id,
        "mode": "replay",
        "profile": profile,
        "replay_source": "artifacts_only",
        "network_fetch": False,
        "output_file": str(out_file),
        "items_count": len(items) if isinstance(items, list) else 0,
        "sent": sent,
        "send_cmd": send_cmd,
        "artifacts": {
            "explain": str(explain_file),
            "preview": str(preview_file),
            "items": str(items_file),
        },
    }


def main(run_id: str, send: bool | str = False, profile: str | None = None) -> int:
    print(json.dumps(run_replay(run_id=run_id, send=send, profile=profile), ensure_ascii=False, indent=2))
    return 0
