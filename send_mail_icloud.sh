#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <to_email> <subject> <body_file>" >&2
  exit 1
fi

TO_EMAIL="$1"
SUBJECT="$2"
BODY_FILE="$3"
ENV_FILE="$(dirname "$0")/.mail.env"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_FILE="${LOG_DIR}/mail_send.log"

if [ ! -f "$ENV_FILE" ]; then
  echo "Missing $ENV_FILE" >&2
  exit 1
fi
if [ ! -f "$BODY_FILE" ]; then
  echo "Body file not found: $BODY_FILE" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"

# shellcheck disable=SC1090
source "$ENV_FILE"

TMP_EML="$(mktemp)"
trap 'rm -f "$TMP_EML"' EXIT

{
  printf 'From: %s <%s>\n' "$SMTP_FROM_NAME" "$SMTP_FROM"
  printf 'To: %s\n' "$TO_EMAIL"
  printf 'Subject: %s\n' "$SUBJECT"
  # Stable-ish Message-ID helps mailbox threading and tracking.
  printf 'Message-ID: <%s.%s@%s>\n' "$(date +%s)" "$RANDOM" "${SMTP_USER#*@}"
  printf 'MIME-Version: 1.0\n'
  printf 'Content-Type: text/plain; charset=UTF-8\n'
  printf 'Content-Transfer-Encoding: 8bit\n'
  printf '\n'
  cat "$BODY_FILE"
} > "$TMP_EML"

# Retry & timeout defaults (override via env if needed).
SMTP_CONNECT_TIMEOUT="${SMTP_CONNECT_TIMEOUT:-10}"
SMTP_MAX_TIME="${SMTP_MAX_TIME:-60}"
SMTP_RETRIES="${SMTP_RETRIES:-5}"
SMTP_BACKOFF_INITIAL="${SMTP_BACKOFF_INITIAL:-2}"
SMTP_BACKOFF_MAX="${SMTP_BACKOFF_MAX:-60}"

ts() { date '+%Y-%m-%d %H:%M:%S %z'; }

attempt=1
backoff="$SMTP_BACKOFF_INITIAL"
while :; do
  set +e
  out="$(
    curl --silent --show-error --fail \
      --connect-timeout "$SMTP_CONNECT_TIMEOUT" \
      --max-time "$SMTP_MAX_TIME" \
      --url "smtp://${SMTP_HOST}:${SMTP_PORT}" \
      --mail-from "$SMTP_FROM" \
      --mail-rcpt "$TO_EMAIL" \
      --upload-file "$TMP_EML" \
      --user "${SMTP_USER}:${SMTP_PASS}" \
      --ssl-reqd 2>&1
  )"
  code=$?
  set -e

  if [ "$code" -eq 0 ]; then
    printf '[%s] SENT to=%s subject=%q\n' "$(ts)" "$TO_EMAIL" "$SUBJECT" >>"$LOG_FILE"
    echo "SENT"
    exit 0
  fi

  printf '[%s] FAIL attempt=%s/%s code=%s to=%s subject=%q err=%q\n' \
    "$(ts)" "$attempt" "$SMTP_RETRIES" "$code" "$TO_EMAIL" "$SUBJECT" "$out" >>"$LOG_FILE"

  if [ "$attempt" -ge "$SMTP_RETRIES" ]; then
    echo "$out" >&2
    exit "$code"
  fi

  sleep "$backoff"
  attempt=$((attempt + 1))
  backoff=$((backoff * 2))
  if [ "$backoff" -gt "$SMTP_BACKOFF_MAX" ]; then
    backoff="$SMTP_BACKOFF_MAX"
  fi
done
