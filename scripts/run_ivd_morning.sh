#!/usr/bin/env bash
set -euo pipefail

# One-shot daily run: generate report -> send mail (iCloud SMTP).
# Intended to be called by Codex automation.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TO_EMAIL="${TO_EMAIL:-qq82125@gmail.com}"
TZ_NAME="${REPORT_TZ:-Asia/Shanghai}"
DATE_STR="$(TZ="$TZ_NAME" date +%F)"
RUN_ID="${RUN_ID:-run-$(date +%Y%m%d%H%M%S)}"
RULES_EMAIL_PROFILE="${RULES_EMAIL_PROFILE:-legacy}"
RULES_CONTENT_PROFILE="${RULES_CONTENT_PROFILE:-legacy}"

REPORTS_DIR="${ROOT_DIR}/reports"
mkdir -p "$REPORTS_DIR"

OUT_FILE="${REPORTS_DIR}/ivd_morning_${DATE_STR}.txt"
TMP_FILE="${OUT_FILE}.tmp"

# Optional rules preflight (off by default for backward compatibility).
if [ "${RULES_USE_ENGINE:-0}" = "1" ]; then
  python3 -m app.workers.cli rules:validate >/dev/null
fi

export RUN_ID RULES_EMAIL_PROFILE RULES_CONTENT_PROFILE
python3 scripts/generate_ivd_report.py >"$TMP_FILE"
mv "$TMP_FILE" "$OUT_FILE"

SUBJECT="全球IVD晨报 - ${DATE_STR}"
set +e
RULE_SUBJECT="$(
  DATE_STR="$DATE_STR" python3 - <<'PY'
import os
from pathlib import Path
import sys

root = Path.cwd()
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

try:
    from app.adapters.rule_bridge import load_runtime_rules
    rt = load_runtime_rules(date_str=os.environ.get("DATE_STR", ""))
    if rt.get("enabled"):
        print(rt.get("email", {}).get("subject", ""))
except Exception:
    pass
PY
)"
code=$?
set -e
if [ "$code" -eq 0 ] && [ -n "${RULE_SUBJECT}" ]; then
  SUBJECT="$RULE_SUBJECT"
fi

# Simple de-dupe: if we've already logged a successful send for today's subject, skip.
LOG_FILE="${ROOT_DIR}/logs/mail_send.log"
if [ -f "$LOG_FILE" ] && rg -F "SENT to=${TO_EMAIL} subject=${SUBJECT}" "$LOG_FILE" >/dev/null 2>&1; then
  echo "SKIP_ALREADY_SENT"
  exit 0
fi

./send_mail_icloud.sh "$TO_EMAIL" "$SUBJECT" "$OUT_FILE"
