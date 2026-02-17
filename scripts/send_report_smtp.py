#!/usr/bin/env python3
import datetime as dt
import email.utils
import os
import smtplib
import ssl
import sys
from email.mime.text import MIMEText
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.adapters.rule_bridge import load_runtime_rules


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def now_in_tz(tz_name: str) -> dt.datetime:
    try:
        return dt.datetime.now(ZoneInfo(tz_name))
    except Exception:
        return dt.datetime.utcnow()


def pick_report_file(reports_dir: Path, date_str: str) -> Path:
    p = reports_dir / f"ivd_morning_{date_str}.txt"
    if p.exists():
        return p
    candidates = sorted(reports_dir.glob("ivd_morning_*.txt"))
    if candidates:
        return candidates[-1]
    raise FileNotFoundError(f"no report file found under {reports_dir}")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"Usage: {argv[0]} <reports_dir>", file=sys.stderr)
        return 2

    reports_dir = Path(argv[1])

    smtp_host = env("SMTP_HOST")
    smtp_port = int(env("SMTP_PORT", "587"))
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")
    smtp_from = env("SMTP_FROM", smtp_user)
    to_email = env("TO_EMAIL")

    tz_name = env("REPORT_TZ", "Asia/Shanghai")
    prefix = env("REPORT_SUBJECT_PREFIX", "全球IVD晨报 - ")
    date_str = now_in_tz(tz_name).strftime("%Y-%m-%d")
    subject = f"{prefix}{date_str}"
    runtime_rules = load_runtime_rules(date_str=date_str)
    if runtime_rules.get("enabled"):
        subject = runtime_rules.get("email", {}).get("subject", subject)

    missing = [k for k in ["SMTP_HOST", "SMTP_USER", "SMTP_PASS", "TO_EMAIL"] if not env(k)]
    if missing:
        print(f"Missing required env vars: {', '.join(missing)}", file=sys.stderr)
        return 2

    report_file = pick_report_file(reports_dir, date_str)
    body = report_file.read_text(encoding="utf-8")

    msg = MIMEText(body, _charset="utf-8")
    msg["From"] = smtp_from
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Date"] = email.utils.formatdate(localtime=True)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.starttls(context=context)
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_from, [to_email], msg.as_string())

    print(
        f"sent: {to_email} subject={subject} file={report_file} "
        f"rules_profile={runtime_rules.get('active_profile', 'legacy')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
