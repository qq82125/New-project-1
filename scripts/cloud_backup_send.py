#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import imaplib
import sqlite3
import hashlib
import os
from typing import Any
import smtplib
import ssl
import sys
import traceback
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


def load_mail_env_file(env_file: Path) -> None:
    """
    Backward-compatible loader: cloud fallback should work with the same .mail.env
    used by send_mail_icloud.sh when process env does not provide SMTP vars.
    """
    if not env_file.exists():
        return
    try:
        for raw in env_file.read_text(encoding="utf-8").splitlines():
            ln = raw.strip()
            if not ln or ln.startswith("#") or "=" not in ln:
                continue
            k, v = ln.split("=", 1)
            key = k.strip()
            if not key:
                continue
            val = v.strip().strip('"').strip("'")
            if key not in os.environ or not str(os.environ.get(key, "")).strip():
                os.environ[key] = val
    except Exception:
        # Non-fatal: keep runtime deterministic even when .mail.env is malformed.
        return


def now_in_tz(tz_name: str) -> dt.datetime:
    try:
        return dt.datetime.now(ZoneInfo(tz_name))
    except Exception:
        return dt.datetime.utcnow()


def parse_int_env(name: str, default: int) -> int:
    value = get_env(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def prune_send_attempts(conn: sqlite3.Connection, retention_days: int) -> int:
    if retention_days <= 0:
        return 0
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=retention_days)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    cur = conn.execute(
        "DELETE FROM send_attempts WHERE created_at < ?",
        (cutoff,),
    )
    return int(cur.rowcount)


def iso_utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_reports_dir() -> Path:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    return reports_dir


def send_attempt_db_path() -> Path:
    return ROOT_DIR / "data" / "rules.db"


def with_send_attempt_db(retention_days: int | None = None) -> sqlite3.Connection:
    path = send_attempt_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    if retention_days is not None:
        prune_send_attempts(conn, retention_days)
        conn.commit()
    return conn


def _normalize_edition(value: str) -> str:
    s = str(value or "").strip().lower()
    if s in {"morning", "am"}:
        return "morning"
    if s in {"evening", "pm"}:
        return "evening"
    return "default"


def infer_edition(now_local: dt.datetime, date_str: str) -> str:
    # Keep the split simple and deterministic for backup windows.
    # Morning backup around 09:30, evening backup around 21:30.
    if str(now_local.strftime("%Y-%m-%d")) != str(date_str):
        return "default"
    return "evening" if now_local.hour >= 15 else "morning"


def build_send_key(
    date_str: str,
    subject: str,
    to_email: str,
    *,
    edition: str,
    profile: str,
) -> str:
    payload = (
        f"{date_str}|{_normalize_edition(edition)}|{str(profile or 'legacy').strip().lower()}|"
        f"{subject.strip()}|{to_email.strip().lower()}"
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def get_send_attempt_success(conn: sqlite3.Connection, send_key: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT *
        FROM send_attempts
        WHERE send_key = ? AND UPPER(status) = 'SUCCESS'
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (send_key,),
    ).fetchone()
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "status": str(row["status"]),
        "run_id": str(row["run_id"] or ""),
        "created_at": str(row["created_at"]),
        "error": str(row["error"] or ""),
    }


def record_send_attempt(
    conn: sqlite3.Connection,
    send_key: str,
    date_str: str,
    subject: str,
    to_email: str,
    status: str,
    run_id: str,
    error: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO send_attempts(send_key, date, subject, to_email, status, error, created_at, run_id)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(send_key) DO UPDATE SET
          status=excluded.status,
          error=excluded.error,
          created_at=excluded.created_at,
          run_id=excluded.run_id
        """,
        (send_key, date_str, subject, to_email, status, error, iso_utc_now(), run_id),
    )



def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def append_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(text)


def write_report(diag_path: Path, section_title: str, content: str) -> None:
    title = (section_title or "").strip()
    body = (content or "").rstrip()
    append_text(
        diag_path,
        "\n".join(
            [
                f"## {title}",
                body,
                "",
            ]
        )
        + "\n",
    )


def format_exception_truncated(exc: BaseException, limit: int = 4000) -> str:
    tb = traceback.format_exc()
    s = f"{type(exc).__name__}: {exc}\n{tb}"
    if len(s) > limit:
        return s[:limit] + "\n...(truncated)\n"
    return s


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


def imap_check_sent(
    imap_host: str,
    imap_port: int,
    username: str,
    password: str,
    subject: str,
    mailbox_hint: str,
) -> dict:
    """
    Returns a structured result. Never raises unless IMAP library itself breaks badly.
    """
    try:
        with imaplib.IMAP4_SSL(imap_host, imap_port) as conn:
            conn.login(username, password)
            mailbox = pick_sent_mailbox(conn, mailbox_hint)
            status, _ = conn.select(f'"{mailbox}"', readonly=True)
            if status != "OK":
                return {"ok": False, "error": f"select_failed:{status}", "mailbox": mailbox}
            status, data = conn.search(None, "HEADER", "Subject", f'"{subject}"')
            if status != "OK":
                return {"ok": False, "error": f"search_failed:{status}", "mailbox": mailbox}
            hits = data[0].split() if data and data[0] else []
            return {
                "ok": True,
                "mailbox": mailbox,
                "hits": len(hits),
                "already_sent": len(hits) > 0,
            }
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def parse_subject_candidates(date_str: str, primary_subject: str) -> list[str]:
    out: list[str] = []
    p = str(primary_subject or "").strip()
    if p:
        out.append(p)
    raw = get_env("REPORT_SUBJECT_CANDIDATE_PREFIXES", "")
    if raw:
        for token in raw.split(","):
            pref = str(token or "").strip()
            if not pref:
                continue
            out.append(f"{pref}{date_str}")
    # dedupe while keeping order
    unique: list[str] = []
    seen = set()
    for it in out:
        k = it.strip()
        if not k:
            continue
        if k in seen:
            continue
        seen.add(k)
        unique.append(k)
    return unique


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


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="IVD Cloud Backup Mail sender (GitHub Actions fallback)")
    p.add_argument("--date", default="", help="Report date in YYYY-MM-DD (default: now in REPORT_TZ)")
    p.add_argument("--dry-run", action="store_true", help="Do not perform IMAP/SMTP network calls")
    p.add_argument("--report-file", default="", help="Optional path to load report body from")
    p.add_argument("--force-send", action="store_true", help="Send even if IMAP indicates already sent today")
    p.add_argument(
        "--send-attempt-retention-days",
        type=int,
        default=None,
        help="Retention days for send_attempts local idempotency table",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_mail_env_file(ROOT_DIR / ".mail.env")
    retention_days = args.send_attempt_retention_days
    if retention_days is None:
        retention_days = parse_int_env("SEND_ATTEMPT_RETENTION_DAYS", 30)
    smtp_host = get_env("SMTP_HOST")
    smtp_port = int(get_env("SMTP_PORT", "587"))
    smtp_user = get_env("SMTP_USER")
    smtp_pass = get_env("SMTP_PASS")
    smtp_from = get_env("SMTP_FROM", smtp_user)
    to_email = get_env("TO_EMAIL")

    imap_host = get_env("IMAP_HOST", "imap.mail.me.com")
    imap_port = int(get_env("IMAP_PORT", "993"))
    mailbox_hint = get_env("IMAP_SENT_MAILBOX", "Sent Messages")
    # Backwards compatible: if IMAP_USER/IMAP_PASS not set, fall back to SMTP credentials.
    imap_user = get_env("IMAP_USER", smtp_user)
    imap_pass = get_env("IMAP_PASS", smtp_pass)

    tz_name = get_env("REPORT_TZ", "Asia/Shanghai")
    # Subject prefix may legitimately include a trailing space; do not strip it.
    prefix = os.environ.get("REPORT_SUBJECT_PREFIX", "全球IVD晨报 - ").replace("\n", "")
    date_str = (args.date.strip() or now_in_tz(tz_name).strftime("%Y-%m-%d"))
    subject_initial = f"{prefix}{date_str}"
    run_id = (
        get_env("REPORT_RUN_ID", "")
        or get_env("GITHUB_RUN_ID", "")
        or f"backup-{uuid.uuid4().hex[:10]}"
    )

    runtime_rules = load_runtime_rules(date_str=date_str, run_id=run_id)
    if not to_email:
        rec = runtime_rules.get("email", {}).get("recipients", []) if isinstance(runtime_rules, dict) else []
        if isinstance(rec, list) and rec:
            first = str(rec[0] or "").strip()
            if first.startswith("${") and first.endswith("}"):
                inner = first[2:-1]
                if ":-" in inner:
                    k, d = inner.split(":-", 1)
                    to_email = str(os.environ.get(k.strip(), d)).strip()
                else:
                    to_email = str(os.environ.get(inner.strip(), "")).strip()
            else:
                to_email = first
    subject_final = subject_initial
    if runtime_rules.get("enabled"):
        subject_final = runtime_rules.get("email", {}).get("subject", subject_final)
    now_local = now_in_tz(tz_name)
    edition = _normalize_edition(get_env("REPORT_EDITION", infer_edition(now_local, date_str)))
    active_profile = str(runtime_rules.get("active_profile", "legacy") or "legacy")
    send_key = build_send_key(
        date_str,
        subject_final,
        to_email,
        edition=edition,
        profile=active_profile,
    )
    subject_candidates = parse_subject_candidates(date_str, subject_final)

    # Diagnostic report is always produced (success/failure) to make Actions debuggable.
    reports_dir = ensure_reports_dir()
    diag_path = reports_dir / f"ivd_backup_{date_str}.txt"
    write_text(
        diag_path,
        "\n".join(
            [
                f"run_id: {run_id}",
                f"timestamp_utc: {iso_utc_now()}",
                f"date: {date_str}",
                f"report_tz: {tz_name}",
                f"subject_initial: {subject_initial}",
                f"subject_final: {subject_final}",
                f"subject_candidates: {subject_candidates}",
                f"edition: {edition}",
                f"profile: {active_profile}",
                f"send_key: {send_key}",
                f"dry_run: {bool(args.dry_run)}",
                f"commit_sha: {get_env('GITHUB_SHA','') or get_env('COMMIT_SHA','')}",
                "",
            ]
        )
        + "\n",
    )

    rv = runtime_rules.get("rules_version", {}) if isinstance(runtime_rules, dict) else {}
    write_report(
        diag_path,
        "Rules",
        "\n".join(
            [
                f"rules_profile={active_profile}",
                f"rules_version.email={rv.get('email','')}",
                f"rules_version.content={rv.get('content','')}",
            ]
        ),
    )
    print(
        f"[RUN] run_id={run_id} profile={active_profile} "
        f"rules_version.email={rv.get('email','')} rules_version.content={rv.get('content','')}",
        file=sys.stderr,
    )
    report_file = args.report_file.strip() or get_env("REPORT_FILE")

    required_env = [
        ("SMTP_HOST", smtp_host),
        ("SMTP_PORT", str(smtp_port) if smtp_port else ""),
        ("SMTP_USER", smtp_user),
        ("SMTP_PASS", smtp_pass),
        ("TO_EMAIL", to_email),
    ]
    missing = [k for k, v in required_env if not (v or "").strip()]
    if missing:
        msg = f"Missing required env vars: {', '.join(missing)}"
        print(msg, file=sys.stderr)
        write_report(diag_path, "Env Check", f"status=FAIL\nmissing_env={missing}\n")
        write_report(diag_path, "Result", "status=FAIL\nexit_code=2\n")
        return 2
    write_report(diag_path, "Env Check", "status=PASS\n")

    local_send_attempt = None
    try:
        with with_send_attempt_db(retention_days) as db:
            local_send_attempt = get_send_attempt_success(db, send_key)
    except Exception as e:
        write_report(
            diag_path,
            "Send Attempts",
            "\n".join(
                [
                    "status=FAIL",
                    "scope=local_idempotent_check",
                    "hint=run 'alembic upgrade head' to ensure send_attempts table exists",
                ]
            )
            + "\n",
        )
        write_report(diag_path, "Send Attempts Error", format_exception_truncated(e))

    if local_send_attempt is not None:
        write_report(
            diag_path,
            "Decision",
            "\n".join(
                [
                    "action=SKIP",
                    "reason=already_sent_in_db",
                    f"attempt_id={local_send_attempt['id']}",
                    f"attempt_status={local_send_attempt['status']}",
                    f"attempt_created_at={local_send_attempt['created_at']}",
                    f"attempt_run_id={local_send_attempt['run_id']}",
                ]
            ),
        )
        write_report(diag_path, "Result", "status=OK\nexit_code=0\n")
        print(f"Skip: existing send_attempt success for date={date_str} subject={subject_final}")
        return 0

    imap_ready = bool(imap_host and imap_port and imap_user and imap_pass)
    if args.dry_run:
        write_report(
            diag_path,
            "IMAP Check",
            (
                "\n".join(
                    [
                        "status=WOULD_CHECK",
                        f"host={imap_host}",
                        f"port={imap_port}",
                        f"mailbox_hint={mailbox_hint}",
                        f"subjects={subject_candidates}",
                    ]
                )
                if imap_ready
                else "status=SKIP\nreason=imap_credentials_missing\n"
            )
            + "\n",
        )
    else:
        if not imap_ready:
            write_report(diag_path, "IMAP Check", "status=SKIP\nreason=imap_credentials_missing\n")
        else:
            matched_subject = ""
            last_imap_res: dict[str, Any] | None = None
            for candidate_subject in subject_candidates:
                try:
                    imap_res = imap_check_sent(
                        imap_host, imap_port, imap_user, imap_pass, candidate_subject, mailbox_hint
                    )
                except Exception as e:
                    write_report(diag_path, "IMAP Check", "status=FAIL\nerror=exception_in_imap_check\n")
                    write_report(diag_path, "IMAP Exception", format_exception_truncated(e))
                    imap_res = {"ok": False, "error": f"{type(e).__name__}: {e}"}
                last_imap_res = imap_res
                if imap_res.get("ok") and bool(imap_res.get("already_sent")):
                    matched_subject = candidate_subject
                    break
                if not imap_res.get("ok"):
                    break

            if last_imap_res and last_imap_res.get("ok"):
                found = bool(matched_subject)
                write_report(
                    diag_path,
                    "IMAP Check",
                    "\n".join(
                        [
                            "status=OK",
                            f"mailbox={last_imap_res.get('mailbox','')}",
                            f"hits={last_imap_res.get('hits',0)}",
                            f"found={'true' if found else 'false'}",
                            f"matched_subject={matched_subject}",
                            f"checked_subjects={subject_candidates}",
                        ]
                    ),
                )
                if found and not args.force_send:
                    msg = (
                        f"Skip: found existing sent mail with subject '{matched_subject}' "
                        f"rules_profile={active_profile}"
                    )
                    print(msg)
                    write_report(
                        diag_path,
                        "Decision",
                        "action=SKIP\nreason=already_sent_in_imap\n",
                    )
                    write_report(diag_path, "Result", "status=OK\nexit_code=0\n")
                    return 0
            else:
                imap_err = (last_imap_res or {}).get("error", "unknown")
                write_report(
                    diag_path,
                    "IMAP Check",
                    f"status=FAIL\nerror={imap_err}\n",
                )
                print(
                    f"IMAP check failed, continue with backup send: {imap_err}",
                    file=sys.stderr,
                )

    body = load_body(report_file, date_str)
    out = ensure_report_file(body, date_str)
    write_report(
        diag_path,
        "Decision",
        "\n".join(
            [
                f"action={'WOULD_SEND' if args.dry_run else 'SEND'}",
                f"body_source={report_file or f'reports/ivd_morning_{date_str}.txt (fallback)'}",
                f"body_file_written={out}",
            ]
        ),
    )

    if args.dry_run:
        write_report(
            diag_path,
            "Send Attempts",
            "status=WOULD_START\nreason=dry_run\n",
        )
        write_report(
            diag_path,
            "SMTP Send",
            "\n".join(
                [
                    "status=WOULD_SEND",
                    f"host={smtp_host}",
                    f"port={smtp_port}",
                    f"from={smtp_from or ''}",
                    f"to={to_email}",
                    f"subject={subject_final}",
                ]
            )
            + "\n",
        )
        write_report(diag_path, "Result", "status=OK\nexit_code=0\n")
        print(
            f"Dry-run: would send backup (report={out}) rules_profile={active_profile}"
        )
        return 0

    try:
        with with_send_attempt_db(retention_days) as db:
            record_send_attempt(db, send_key, date_str, subject_final, to_email, "STARTED", run_id)
            db.commit()
        write_report(
            diag_path,
            "Send Attempts",
            "status=STARTED\n",
        )
    except Exception as e:
        write_report(
            diag_path,
            "Send Attempts",
            "status=FAIL\nscope=record_start\n",
        )
        write_report(diag_path, "Send Attempts Error", format_exception_truncated(e))

    try:
        send_email(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            username=smtp_user,
            password=smtp_pass,
            from_addr=smtp_from,
            to_addr=to_email,
            subject=subject_final,
            body=body,
        )
        try:
            with with_send_attempt_db(retention_days) as db:
                db.execute(
                    """
                    UPDATE send_attempts
                    SET status='SUCCESS', error=NULL, created_at = ?, run_id = ?
                    WHERE send_key = ? AND status = 'STARTED'
                    """,
                    (iso_utc_now(), run_id, send_key),
                )
                if db.total_changes == 0:
                    record_send_attempt(db, send_key, date_str, subject_final, to_email, "SUCCESS", run_id)
                db.commit()
            write_report(
                diag_path,
                "Send Attempts",
                "status=SUCCESS\n",
            )
        except Exception as e:
            write_report(
                diag_path,
                "Send Attempts",
                "status=FAIL\nscope=update_success\n",
            )
            write_report(diag_path, "Send Attempts Error", format_exception_truncated(e))
        write_report(diag_path, "SMTP Send", "status=OK\n")
        write_report(diag_path, "Result", "status=OK\nexit_code=0\n")
        print(
            f"Backup sent (report={out}) rules_profile={active_profile}"
        )
        return 0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        try:
            with with_send_attempt_db(retention_days) as db:
                db.execute(
                    """
                    UPDATE send_attempts
                    SET status='FAILED', error=?, created_at = ?, run_id = ?
                    WHERE send_key = ? AND status = 'STARTED'
                    """,
                    (str(err), iso_utc_now(), run_id, send_key),
                )
                if db.total_changes == 0:
                    record_send_attempt(db, send_key, date_str, subject_final, to_email, "FAILED", run_id, err)
                db.commit()
            write_report(
                diag_path,
                "Send Attempts",
                "status=FAILED\n",
            )
        except Exception as attempt_err:
            write_report(
                diag_path,
                "Send Attempts",
                "status=FAIL\nscope=update_failed\n",
            )
            write_report(diag_path, "Send Attempts Error", format_exception_truncated(attempt_err))
        write_report(diag_path, "SMTP Send", f"status=FAIL\nerror={err}\n")
        write_report(diag_path, "SMTP Exception", format_exception_truncated(e))
        write_report(diag_path, "Result", "status=FAIL\nexit_code=3\n")
        print(f"SMTP send failed: {err}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main(None))
