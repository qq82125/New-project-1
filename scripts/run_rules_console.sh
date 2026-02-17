#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${RULES_CONSOLE_ENV_FILE:-${REPO_DIR}/.rules_console.env}"

cd "${REPO_DIR}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

if [[ -z "${RULES_CONSOLE_TOKEN:-}" ]]; then
  if [[ -z "${RULES_CONSOLE_USER:-}" || -z "${RULES_CONSOLE_PASS:-}" ]]; then
    echo "missing auth config: set RULES_CONSOLE_TOKEN or RULES_CONSOLE_USER/RULES_CONSOLE_PASS in ${ENV_FILE}" >&2
    exit 1
  fi
fi

export RULES_CONSOLE_HOST="${RULES_CONSOLE_HOST:-127.0.0.1}"
export RULES_CONSOLE_PORT="${RULES_CONSOLE_PORT:-8787}"
export PYTHONUNBUFFERED=1

exec /usr/bin/env python3 -m app.web.rules_console
