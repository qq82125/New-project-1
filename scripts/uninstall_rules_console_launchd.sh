#!/usr/bin/env bash
set -euo pipefail

LABEL="com.qq82125.newproject1.rules-console"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"

launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
rm -f "${PLIST_PATH}"

echo "removed: ${PLIST_PATH}"
