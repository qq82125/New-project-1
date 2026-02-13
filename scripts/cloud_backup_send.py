#!/usr/bin/env python3
import datetime as dt
import email.utils
import imaplib
import os
import smtplib
import ssl
import sys
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo


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
        "这是云端兜底补发邮件。\n"
        "本次未检测到本机8:30发送记录，已触发备份通道。\n"
        "请回复本邮件或检查自动化日志。"
    )


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
            print(f"Skip: found existing sent mail with subject '{subject}'")
            return 0
    except Exception as e:
        print(f"IMAP check failed, continue with backup send: {e}")

    body = load_body(report_file, date_str)
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
    print("Backup sent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
