#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
HOOK_PATH="${REPO_DIR}/.git/hooks/post-commit"
LOG_PATH="${REPO_DIR}/logs/git_autopush.log"

cd "${REPO_DIR}"
mkdir -p "${REPO_DIR}/logs"

cat > "${HOOK_PATH}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(git rev-parse --show-toplevel)"
cd "${REPO_DIR}"

REMOTE="${SYNC_REMOTE:-origin}"
BRANCH="${SYNC_BRANCH:-$(git rev-parse --abbrev-ref HEAD)}"
LOG_PATH="${REPO_DIR}/logs/git_autopush.log"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] post-commit autopush start"
  git pull --rebase "${REMOTE}" "${BRANCH}"
  git push "${REMOTE}" "${BRANCH}"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] post-commit autopush done"
} >> "${LOG_PATH}" 2>&1
EOF

chmod 700 "${HOOK_PATH}"
echo "installed: ${HOOK_PATH}"
echo "log: ${LOG_PATH}"
