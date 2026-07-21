#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.russellthomas.wnba-player-refresh"
PLIST_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="$ROOT_DIR/logs"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
USER_DOMAIN="gui/$(id -u)"
DEPLOY_KEY_PATH="${WNBA_DEPLOY_KEY_PATH:-$HOME/.ssh/databallr_wnba_refresh}"
PUSH_URL="git@github.com:RussDT/nba-onoff-fetch.git"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python environment: $PYTHON_BIN" >&2
  echo "Create it with: python3 -m venv .venv && .venv/bin/pip install pandas requests curl_cffi==0.14.0" >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1 || ! gh auth status >/dev/null 2>&1; then
  echo "An authenticated GitHub CLI session is required to install the repo-scoped deploy key." >&2
  exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents" "$LOG_DIR" "$HOME/.ssh"
chmod 700 "$HOME/.ssh"

if [[ ! -f "$DEPLOY_KEY_PATH" ]]; then
  ssh-keygen -q -t ed25519 -N "" -f "$DEPLOY_KEY_PATH" -C "databallr WNBA refresh"
fi
chmod 600 "$DEPLOY_KEY_PATH"
chmod 644 "$DEPLOY_KEY_PATH.pub"

KEY_MATERIAL="$(awk '{print $1 " " $2}' "$DEPLOY_KEY_PATH.pub")"
if ! gh api --paginate repos/RussDT/nba-onoff-fetch/keys --jq '.[].key' | grep -Fxq "$KEY_MATERIAL"; then
  gh api repos/RussDT/nba-onoff-fetch/keys \
    --method POST \
    -f title="WNBA player refresh $(hostname -s)" \
    -f key="$KEY_MATERIAL" \
    -F read_only=false \
    >/dev/null
  echo "Registered a write-enabled deploy key scoped to RussDT/nba-onoff-fetch"
fi

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
  <key>EnvironmentVariables</key>
  <dict>
    <key>WNBA_GIT_PUSH_URL</key>
    <string>${PUSH_URL}</string>
    <key>GIT_SSH_COMMAND</key>
    <string>/usr/bin/ssh -i ${DEPLOY_KEY_PATH} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new</string>
  </dict>
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
echo "Git pushes use a write-enabled deploy key scoped only to RussDT/nba-onoff-fetch"
echo "Run now with: launchctl kickstart -k $USER_DOMAIN/$LABEL"
