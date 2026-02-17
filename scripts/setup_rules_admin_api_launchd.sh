#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PLIST_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${PLIST_DIR}/com.qq82125.newproject1.rules-admin-api.plist"
LABEL="com.qq82125.newproject1.rules-admin-api"
ENV_FILE="${REPO_DIR}/.admin_api.env"
RUNTIME_DIR="${HOME}/Library/Application Support/com.qq82125.newproject1/rules-admin-api-runtime"
LOG_DIR="${HOME}/Library/Logs/com.qq82125.newproject1"
LOG_FILE="${LOG_DIR}/rules_admin_api.launchd.log"

mkdir -p "${PLIST_DIR}" "${LOG_DIR}" "${RUNTIME_DIR}"

if [[ ! -f "${ENV_FILE}" ]]; then
  if [[ -f "${REPO_DIR}/.admin_api.env.example" ]]; then
    cp "${REPO_DIR}/.admin_api.env.example" "${ENV_FILE}"
  else
    cat > "${ENV_FILE}" <<'EOF'
ADMIN_API_HOST=127.0.0.1
ADMIN_API_PORT=8789
ADMIN_USER=admin
ADMIN_PASS=change-me
EOF
  fi
  chmod 600 "${ENV_FILE}"
  echo "created ${ENV_FILE} (please update credentials)"
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

ADMIN_API_HOST="${ADMIN_API_HOST:-127.0.0.1}"
ADMIN_API_PORT="${ADMIN_API_PORT:-8789}"

if [[ -z "${ADMIN_TOKEN:-}" ]]; then
  if [[ -z "${ADMIN_USER:-}" || -z "${ADMIN_PASS:-}" ]]; then
    echo "missing auth config in ${ENV_FILE} (need ADMIN_TOKEN or ADMIN_USER/ADMIN_PASS)" >&2
    exit 1
  fi
fi

rsync -a --delete \
  --exclude ".git/" \
  --exclude ".venv/" \
  --exclude "__pycache__/" \
  --exclude "logs/" \
  "${REPO_DIR}/" "${RUNTIME_DIR}/"

cat > "${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/env</string>
    <string>python3</string>
    <string>-c</string>
    <string>import runpy,sys; sys.path.insert(0, "${RUNTIME_DIR}"); runpy.run_module("app.web.rules_admin_api", run_name="__main__")</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${RUNTIME_DIR}</string>

  <key>RunAtLoad</key>
  <true/>

  <key>KeepAlive</key>
  <true/>

  <key>StandardOutPath</key>
  <string>${LOG_FILE}</string>
  <key>StandardErrorPath</key>
  <string>${LOG_FILE}</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
    <key>ADMIN_API_HOST</key>
    <string>${ADMIN_API_HOST}</string>
    <key>ADMIN_API_PORT</key>
    <string>${ADMIN_API_PORT}</string>
EOF

if [[ -n "${ADMIN_TOKEN:-}" ]]; then
cat >> "${PLIST_PATH}" <<EOF
    <key>ADMIN_TOKEN</key>
    <string>${ADMIN_TOKEN}</string>
EOF
else
cat >> "${PLIST_PATH}" <<EOF
    <key>ADMIN_USER</key>
    <string>${ADMIN_USER}</string>
    <key>ADMIN_PASS</key>
    <string>${ADMIN_PASS}</string>
EOF
fi

cat >> "${PLIST_PATH}" <<EOF
  </dict>
</dict>
</plist>
EOF

uid="$(id -u)"
launchctl bootout "gui/${uid}/${LABEL}" >/dev/null 2>&1 || true

# launchctl can intermittently return 5 (I/O error). Treat "already loaded" scenarios as recoverable:
# retry a few times, then fall back to kickstart if the job is still present.
ok=0
for attempt in 1 2 3; do
  set +e
  launchctl bootstrap "gui/${uid}" "${PLIST_PATH}"
  rc=$?
  set -e
  if [[ $rc -eq 0 ]]; then
    ok=1
    break
  fi
  sleep 1
done

set +e
launchctl kickstart -k "gui/${uid}/${LABEL}" >/dev/null 2>&1
set -e

if [[ $ok -ne 1 ]]; then
  # If bootstrap failed but the job is already loaded/running, don't fail the install.
  if launchctl print "gui/${uid}/${LABEL}" 2>/dev/null | grep -q "state = running"; then
    ok=1
  fi
fi

if [[ $ok -ne 1 ]]; then
  echo "bootstrap failed after retries; check launchctl output/logs" >&2
  exit 5
fi

echo "installed: ${PLIST_PATH}"
echo "label: ${LABEL}"
echo "env: ${ENV_FILE}"
echo "runtime: ${RUNTIME_DIR}"
echo "log: ${LOG_FILE}"
