#!/usr/bin/env bash
set -euo pipefail

# Sync local changes to GitHub: add/commit/pull --rebase/push.
# Designed to run unattended (launchd/cron).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCK_DIR="${REPO_DIR}/.git/.autosync.lock"
BRANCH="${SYNC_BRANCH:-main}"
REMOTE="${SYNC_REMOTE:-origin}"
TZ_NAME="${SYNC_TZ:-Asia/Shanghai}"

mkdir -p "${REPO_DIR}/logs"

if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
  echo "[autosync] another sync run is in progress, skip."
  exit 0
fi
trap 'rmdir "${LOCK_DIR}" >/dev/null 2>&1 || true' EXIT

cd "${REPO_DIR}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[autosync] not a git repository: ${REPO_DIR}"
  exit 1
fi

# Detect tracked/untracked changes.
HAS_CHANGES=0
if ! git diff --quiet || ! git diff --cached --quiet; then
  HAS_CHANGES=1
elif [ -n "$(git ls-files --others --exclude-standard)" ]; then
  HAS_CHANGES=1
fi

if [ "${HAS_CHANGES}" -eq 0 ]; then
  echo "[autosync] no changes."
  exit 0
fi

git add -A

if git diff --cached --quiet; then
  echo "[autosync] nothing to commit after staging."
  exit 0
fi

NOW="$(TZ="${TZ_NAME}" date '+%Y-%m-%d %H:%M:%S %z')"
git commit -m "chore(sync): auto-sync ${NOW}"

if ! git pull --rebase "${REMOTE}" "${BRANCH}"; then
  echo "[autosync] pull --rebase failed, attempting abort."
  git rebase --abort >/dev/null 2>&1 || true
  exit 2
fi

git push "${REMOTE}" "${BRANCH}"
echo "[autosync] sync complete."
