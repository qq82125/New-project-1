#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

ACTION="${1:-help}"
SOURCE_SQLITE="${SOURCE_SQLITE:-data/rules.db}"
BATCH_SIZE="${BATCH_SIZE:-500}"
TARGET_URL="${DATABASE_URL:-}"
SECONDARY_URL="${DATABASE_URL_SECONDARY:-sqlite:///data/rules.db}"

need_target() {
  if [[ -z "${TARGET_URL}" ]]; then
    echo "[db-cutover] DATABASE_URL is required" >&2
    exit 2
  fi
}

case "$ACTION" in
  precheck)
    need_target
    ./scripts/db_preflight.sh
    ;;
  migrate)
    need_target
    python3 -m app.workers.cli db:migrate --target-url "$TARGET_URL" --source-sqlite "$SOURCE_SQLITE" --batch-size "$BATCH_SIZE" --resume true
    ;;
  verify)
    need_target
    python3 -m app.workers.cli db:verify --target-url "$TARGET_URL" --source-sqlite "$SOURCE_SQLITE"
    ;;
  dual-replay)
    need_target
    python3 -m app.workers.cli db:dual-replay --primary-url "$TARGET_URL" --secondary-url "$SECONDARY_URL"
    ;;
  enable-dual)
    need_target
    cat <<EOT
# Export these env vars (and restart admin/scheduler):
export DATABASE_URL='$TARGET_URL'
export DATABASE_URL_SECONDARY='${SECONDARY_URL}'
export DB_WRITE_MODE='dual'
export DB_READ_MODE='shadow_compare'
export DB_DUAL_STRICT='false'
EOT
    ;;
  finalize)
    need_target
    cat <<EOT
# Export these env vars (and restart admin/scheduler):
export DATABASE_URL='$TARGET_URL'
unset DATABASE_URL_SECONDARY
export DB_WRITE_MODE='single'
export DB_READ_MODE='primary'
unset DB_DUAL_STRICT
EOT
    ;;
  rollback)
    cat <<EOT
# Roll back to local SQLite in <1 minute, then restart admin/scheduler:
export DATABASE_URL='sqlite:///data/rules.db'
unset DATABASE_URL_SECONDARY
export DB_WRITE_MODE='single'
export DB_READ_MODE='primary'
unset DB_DUAL_STRICT
EOT
    ;;
  go)
    need_target
    ./scripts/db_preflight.sh
    python3 -m app.workers.cli db:migrate --target-url "$TARGET_URL" --source-sqlite "$SOURCE_SQLITE" --batch-size "$BATCH_SIZE" --resume true
    python3 -m app.workers.cli db:verify --target-url "$TARGET_URL" --source-sqlite "$SOURCE_SQLITE"
    python3 -m app.workers.cli db:dual-replay --primary-url "$TARGET_URL" --secondary-url "$SECONDARY_URL"
    echo "[db-cutover] checks completed. Run: ./scripts/db_cutover.sh enable-dual"
    ;;
  *)
    cat <<'EOT'
Usage: ./scripts/db_cutover.sh <action>

Actions:
  precheck     Verify DB connectivity and run alembic upgrade head
  migrate      Migrate SQLite control-plane data to DATABASE_URL
  verify       Verify counts/content between SQLite and DATABASE_URL
  dual-replay  Compare active reads between DATABASE_URL and DATABASE_URL_SECONDARY
  enable-dual  Print env exports for dual-write + shadow-compare
  finalize     Print env exports for primary-only mode
  rollback     Print env exports to roll back to SQLite
  go           Run precheck + migrate + verify + dual-replay
EOT
    exit 2
    ;;
esac
