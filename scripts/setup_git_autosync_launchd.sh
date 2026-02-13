#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PLIST_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${PLIST_DIR}/com.qq82125.newproject1.git-autosync.plist"
SYNC_SCRIPT="${REPO_DIR}/scripts/git_sync.sh"
LOG_FILE="${REPO_DIR}/logs/git_autosync.log"
INTERVAL="${1:-300}"

mkdir -p "${PLIST_DIR}" "${REPO_DIR}/logs"
chmod 700 "${SYNC_SCRIPT}"

cat > "${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.qq82125.newproject1.git-autosync</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${SYNC_SCRIPT}</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${REPO_DIR}</string>

  <key>StartInterval</key>
  <integer>${INTERVAL}</integer>

  <key>RunAtLoad</key>
  <true/>

  <key>StandardOutPath</key>
  <string>${LOG_FILE}</string>
  <key>StandardErrorPath</key>
  <string>${LOG_FILE}</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>SYNC_BRANCH</key>
    <string>main</string>
    <key>SYNC_REMOTE</key>
    <string>origin</string>
    <key>SYNC_TZ</key>
    <string>Asia/Shanghai</string>
  </dict>
</dict>
</plist>
EOF

launchctl unload "${PLIST_PATH}" >/dev/null 2>&1 || true
launchctl load "${PLIST_PATH}"
launchctl kickstart -k "gui/$(id -u)/com.qq82125.newproject1.git-autosync" >/dev/null 2>&1 || true

echo "installed: ${PLIST_PATH}"
echo "interval: ${INTERVAL}s"
echo "log: ${LOG_FILE}"
