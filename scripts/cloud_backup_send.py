#!/usr/bin/env python3
import datetime as dt
import email.utils
import imaplib
import os
import smtplib
import ssl
import sys
import uuid
from email.mime.text import MIMEText
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.adapters.rule_bridge import load_runtime_rules


def get_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def now_in_tz(tz_name: str) -> dt.datetime:
    try:
        return dt.datetime.now(ZoneInfo(tz_name))
    except Exception:
        return dt.datetime.utcnow()


def pick_sent_mailbox(conn: imaplib.IMAP4_SSL, preferred: str) -> str:
    status, boxes = conn.list()
    if status != "OK" or not boxes:
        return preferred

    decoded = []
    for raw in boxes:
        text = raw.decode("utf-8", errors="ignore")
        parts = text.split(' "/" ')
        decoded.append(parts[-1].strip('"') if parts else text)

    if preferred and preferred in decoded:
        return preferred

    candidates = ["Sent Messages", "Sent", "Sent Mail", "INBOX.Sent"]
    for c in candidates:
        if c in decoded:
            return c
    for box in decoded:
        low = box.lower()
        if "sent" in low:
            return box
    return preferred or "Sent Messages"


def already_sent_today(
    imap_host: str,
    imap_port: int,
    username: str,
    password: str,
    subject: str,
    mailbox_hint: str,
) -> bool:
    with imaplib.IMAP4_SSL(imap_host, imap_port) as conn:
        conn.login(username, password)
        mailbox = pick_sent_mailbox(conn, mailbox_hint)
        status, _ = conn.select(f'"{mailbox}"', readonly=True)
        if status != "OK":
            return False
        status, data = conn.search(None, "HEADER", "Subject", f'"{subject}"')
        if status != "OK":
            return False
        hits = data[0].split() if data and data[0] else []
        return len(hits) > 0


def load_body(report_file: str, date_str: str) -> str:
    if report_file and os.path.exists(report_file):
        with open(report_file, "r", encoding="utf-8") as f:
            return f.read()

    default_file = f"reports/ivd_morning_{date_str}.txt"
    if os.path.exists(default_file):
        with open(default_file, "r", encoding="utf-8") as f:
            return f.read()

    return (
        f"全球IVD晨报 - {date_str}\n\n"
        "【云端兜底补发】\n"
        "触发原因：未检测到当日“已发送”记录（可能为本机未联网/本机任务未运行/发送失败）。\n"
        "建议：查看GitHub Actions运行日志，并检查本机8:30任务与网络状态。\n"
    )


def ensure_report_file(body: str, date_str: str) -> Path:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    p = reports_dir / f"ivd_morning_{date_str}.txt"
    if not p.exists():
        p.write_text(body, encoding="utf-8")
    return p


def send_email(
    smtp_host: str,
    smtp_port: int,
    username: str,
    password: str,
    from_addr: str,
    to_addr: str,
    subject: str,
    body: str,
) -> None:
    msg = MIMEText(body, _charset="utf-8")
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Date"] = email.utils.formatdate(localtime=True)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.starttls(context=context)
        server.login(username, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())


def main() -> int:
    smtp_host = get_env("SMTP_HOST")
    smtp_port = int(get_env("SMTP_PORT", "587"))
    smtp_user = get_env("SMTP_USER")
    smtp_pass = get_env("SMTP_PASS")
    smtp_from = get_env("SMTP_FROM", smtp_user)
    to_email = get_env("TO_EMAIL")

    imap_host = get_env("IMAP_HOST", "imap.mail.me.com")
    imap_port = int(get_env("IMAP_PORT", "993"))
    mailbox_hint = get_env("IMAP_SENT_MAILBOX", "Sent Messages")

    tz_name = get_env("REPORT_TZ", "Asia/Shanghai")
    prefix = get_env("REPORT_SUBJECT_PREFIX", "全球IVD晨报 - ")
    date_str = now_in_tz(tz_name).strftime("%Y-%m-%d")
    subject = f"{prefix}{date_str}"
    run_id = get_env("REPORT_RUN_ID", "") or f"backup-{uuid.uuid4().hex[:10]}"
    runtime_rules = load_runtime_rules(date_str=date_str, run_id=run_id)
    if runtime_rules.get("enabled"):
        subject = runtime_rules.get("email", {}).get("subject", subject)
    rv = runtime_rules.get("rules_version", {}) if isinstance(runtime_rules, dict) else {}
    print(
        f"[RUN] run_id={run_id} profile={runtime_rules.get('active_profile','legacy')} "
        f"rules_version.email={rv.get('email','')} rules_version.content={rv.get('content','')}",
        file=sys.stderr,
    )
    report_file = get_env("REPORT_FILE")

    required = {
        "SMTP_HOST": smtp_host,
        "SMTP_USER": smtp_user,
        "SMTP_PASS": smtp_pass,
        "TO_EMAIL": to_email,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"Missing required env vars: {', '.join(missing)}", file=sys.stderr)
        return 2

    try:
        if already_sent_today(
            imap_host, imap_port, smtp_user, smtp_pass, subject, mailbox_hint
        ):
            print(
                f"Skip: found existing sent mail with subject '{subject}' "
                f"rules_profile={runtime_rules.get('active_profile', 'legacy')}"
            )
            return 0
    except Exception as e:
        print(f"IMAP check failed, continue with backup send: {e}")

    body = load_body(report_file, date_str)
    out = ensure_report_file(body, date_str)
    send_email(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        username=smtp_user,
        password=smtp_pass,
        from_addr=smtp_from,
        to_addr=to_email,
        subject=subject,
        body=body,
    )
    print(f"Backup sent (report={out}) rules_profile={runtime_rules.get('active_profile', 'legacy')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
