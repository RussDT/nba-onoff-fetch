#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.russellthomas.wnba-player-refresh"
PLIST_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="$ROOT_DIR/logs"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
USER_DOMAIN="gui/$(id -u)"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python environment: $PYTHON_BIN" >&2
  echo "Create it with: python3 -m venv .venv && .venv/bin/pip install pandas requests curl_cffi==0.14.0" >&2
  exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents" "$LOG_DIR"

cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${ROOT_DIR}/scripts/run_local_wnba_player_refresh.sh</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>5</integer>
    <key>Minute</key>
    <integer>15</integer>
  </dict>
  <key>ThrottleInterval</key>
  <integer>300</integer>
  <key>ProcessType</key>
  <string>Background</string>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/launchd.stdout.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/launchd.stderr.log</string>
</dict>
</plist>
PLIST

plutil -lint "$PLIST_PATH"
launchctl bootout "$USER_DOMAIN" "$PLIST_PATH" 2>/dev/null || true
launchctl bootstrap "$USER_DOMAIN" "$PLIST_PATH"
launchctl enable "$USER_DOMAIN/$LABEL"

echo "Installed $LABEL from $PLIST_PATH"
echo "Run now with: launchctl kickstart -k $USER_DOMAIN/$LABEL"
