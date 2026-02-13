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

if [ ! -f "$ENV_FILE" ]; then
  echo "Missing $ENV_FILE" >&2
  exit 1
fi
if [ ! -f "$BODY_FILE" ]; then
  echo "Body file not found: $BODY_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

TMP_EML="$(mktemp)"
trap 'rm -f "$TMP_EML"' EXIT

{
  printf 'From: %s <%s>\n' "$SMTP_FROM_NAME" "$SMTP_FROM"
  printf 'To: %s\n' "$TO_EMAIL"
  printf 'Subject: %s\n' "$SUBJECT"
  printf 'MIME-Version: 1.0\n'
  printf 'Content-Type: text/plain; charset=UTF-8\n'
  printf 'Content-Transfer-Encoding: 8bit\n'
  printf '\n'
  cat "$BODY_FILE"
} > "$TMP_EML"

curl --silent --show-error --fail \
  --url "smtp://${SMTP_HOST}:${SMTP_PORT}" \
  --mail-from "$SMTP_FROM" \
  --mail-rcpt "$TO_EMAIL" \
  --upload-file "$TMP_EML" \
  --user "${SMTP_USER}:${SMTP_PASS}" \
  --ssl-reqd

echo "SENT"
