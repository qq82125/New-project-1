#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

: "${DATABASE_URL:?DATABASE_URL is required (target DB, usually PostgreSQL)}"
DATABASE_URL_SECONDARY="${DATABASE_URL_SECONDARY:-sqlite:///data/rules.db}"

echo "[db-preflight] project_root=$ROOT_DIR"
echo "[db-preflight] DATABASE_URL=${DATABASE_URL}"
echo "[db-preflight] DATABASE_URL_SECONDARY=${DATABASE_URL_SECONDARY}"

python3 - <<'PY'
import os
from sqlalchemy import text
from app.db.engine import make_engine

for name in ("DATABASE_URL", "DATABASE_URL_SECONDARY"):
    url = os.environ.get(name, "").strip()
    if not url:
        print(f"[db-preflight] skip {name} (empty)")
        continue
    eng = make_engine(url)
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
    print(f"[db-preflight] ok {name}")
PY

echo "[db-preflight] running alembic on DATABASE_URL"
python3 -m alembic upgrade head

echo "[db-preflight] done"
