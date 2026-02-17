from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from app.adapters.email_rules_adapter import to_email_runtime
from app.rules.engine import RuleEngine


def _today_str(tz_name: str) -> str:
    return datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")


def run_replay(
    replay_date: str | None = None,
    run_id: str | None = None,
    send: bool = False,
    email_profile: str | None = None,
    content_profile: str | None = None,
) -> dict:
    engine = RuleEngine()
    email_rule, content_rule = engine.load_pair(email_profile, content_profile)
    email_runtime = to_email_runtime(email_rule)
    tz_name = email_runtime.get("date_tz", "Asia/Shanghai")
    replay_date = replay_date or _today_str(tz_name)
    run_id = run_id or f"replay-{uuid.uuid4().hex[:10]}"

    project_root = engine.project_root
    reports_dir = project_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_file = reports_dir / f"ivd_morning_{replay_date}_replay.txt"
    source_file = reports_dir / f"ivd_morning_{replay_date}.txt"

    env = os.environ.copy()
    env["REPORT_TZ"] = tz_name
    env["REPORT_DATE"] = replay_date
    env["RUN_ID"] = run_id
    env["RULES_EMAIL_PROFILE"] = email_rule.profile
    env["RULES_CONTENT_PROFILE"] = content_rule.profile
    env["RULES_EMAIL_VERSION"] = email_rule.version
    env["RULES_CONTENT_VERSION"] = content_rule.version

    replay_source = "generated_now"
    if source_file.exists():
        out_file.write_text(source_file.read_text(encoding="utf-8"), encoding="utf-8")
        replay_source = "existing_report_snapshot"
    else:
        gen_cmd = ["python3", "scripts/generate_ivd_report.py"]
        with out_file.open("w", encoding="utf-8") as f:
            subprocess.run(gen_cmd, cwd=project_root, env=env, check=True, stdout=f)

    sent = False
    send_cmd = None
    if send:
        subject = email_runtime.get("subject_template", "全球IVD晨报 - {{date}}").replace(
            "{{date}}", replay_date
        )
        to_email = env.get("TO_EMAIL", "qq82125@gmail.com")
        send_cmd = ["./send_mail_icloud.sh", to_email, subject, str(out_file)]
        subprocess.run(send_cmd, cwd=project_root, env=env, check=True)
        sent = True

    return {
        "run_id": run_id,
        "mode": "replay",
        "replay_date": replay_date,
        "profile": {
            "email": email_rule.profile,
            "content": content_rule.profile,
        },
        "rules_version": {
            "email": email_rule.version,
            "content": content_rule.version,
        },
        "output_file": str(out_file),
        "replay_source": replay_source,
        "sent": sent,
        "send_cmd": send_cmd,
    }


def main(
    replay_date: str | None = None,
    run_id: str | None = None,
    send: bool = False,
    email_profile: str | None = None,
    content_profile: str | None = None,
) -> int:
    result = run_replay(
        replay_date=replay_date,
        run_id=run_id,
        send=send,
        email_profile=email_profile,
        content_profile=content_profile,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0
