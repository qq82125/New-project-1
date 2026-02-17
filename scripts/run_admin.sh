#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Optional: load local env file if present.
if [[ -f ".admin_api.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".admin_api.env"
  set +a
fi

exec python3 -m app.admin_server

