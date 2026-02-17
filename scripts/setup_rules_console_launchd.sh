#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PLIST_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${PLIST_DIR}/com.qq82125.newproject1.rules-console.plist"
LABEL="com.qq82125.newproject1.rules-console"
ENV_FILE="${REPO_DIR}/.rules_console.env"
RUNTIME_DIR="${HOME}/Library/Application Support/com.qq82125.newproject1/rules-console-runtime"
LOG_DIR="${HOME}/Library/Logs/com.qq82125.newproject1"
LOG_FILE="${LOG_DIR}/rules_console.launchd.log"

mkdir -p "${PLIST_DIR}" "${LOG_DIR}" "${RUNTIME_DIR}"

if [[ ! -f "${ENV_FILE}" ]]; then
  if [[ -f "${REPO_DIR}/.rules_console.env.example" ]]; then
    cp "${REPO_DIR}/.rules_console.env.example" "${ENV_FILE}"
  else
    cat > "${ENV_FILE}" <<'EOF'
RULES_CONSOLE_USER=admin
RULES_CONSOLE_PASS=change-me
RULES_CONSOLE_HOST=127.0.0.1
RULES_CONSOLE_PORT=8787
EOF
  fi
  chmod 600 "${ENV_FILE}"
  echo "created ${ENV_FILE} (please update credentials)"
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

RULES_CONSOLE_HOST="${RULES_CONSOLE_HOST:-127.0.0.1}"
RULES_CONSOLE_PORT="${RULES_CONSOLE_PORT:-8787}"

if [[ -z "${RULES_CONSOLE_TOKEN:-}" ]]; then
  if [[ -z "${RULES_CONSOLE_USER:-}" || -z "${RULES_CONSOLE_PASS:-}" ]]; then
    echo "missing auth config in ${ENV_FILE}" >&2
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
    <string>import runpy,sys; sys.path.insert(0, "${RUNTIME_DIR}"); runpy.run_module("app.web.rules_console", run_name="__main__")</string>
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
    <key>RULES_CONSOLE_HOST</key>
    <string>${RULES_CONSOLE_HOST}</string>
    <key>RULES_CONSOLE_PORT</key>
    <string>${RULES_CONSOLE_PORT}</string>
EOF

if [[ -n "${RULES_CONSOLE_TOKEN:-}" ]]; then
cat >> "${PLIST_PATH}" <<EOF
    <key>RULES_CONSOLE_TOKEN</key>
    <string>${RULES_CONSOLE_TOKEN}</string>
EOF
else
cat >> "${PLIST_PATH}" <<EOF
    <key>RULES_CONSOLE_USER</key>
    <string>${RULES_CONSOLE_USER}</string>
    <key>RULES_CONSOLE_PASS</key>
    <string>${RULES_CONSOLE_PASS}</string>
EOF
fi

cat >> "${PLIST_PATH}" <<EOF
  </dict>
</dict>
</plist>
EOF

launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true

echo "installed: ${PLIST_PATH}"
echo "label: ${LABEL}"
echo "env: ${ENV_FILE}"
echo "runtime: ${RUNTIME_DIR}"
echo "log: ${LOG_FILE}"
