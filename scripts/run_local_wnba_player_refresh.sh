#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
YEAR="${WNBA_YEAR:-$(date -u +%Y)}"
PYTHON_BIN="${WNBA_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
PUSH_TARGET="${WNBA_GIT_PUSH_URL:-origin}"
LOCK_DIR="$ROOT_DIR/.wnba-player-refresh.lock"
LOG_DIR="$ROOT_DIR/logs"

mkdir -p "$LOG_DIR"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Starting WNBA player refresh for $YEAR"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "WNBA player refresh is already running"
  exit 0
fi
trap 'rmdir "$LOCK_DIR"' EXIT

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python environment: $PYTHON_BIN" >&2
  echo "Create it with: python3 -m venv .venv && .venv/bin/pip install pandas requests curl_cffi==0.14.0" >&2
  exit 1
fi

push_pending_commits() {
  if [[ "$(git rev-list --count origin/main..HEAD)" -eq 0 ]]; then
    echo "No WNBA player commits to push"
    return 0
  fi
  if ! git push "$PUSH_TARGET" main; then
    echo "Push raced with another update; rebasing once and retrying"
    git pull --rebase origin main
    git push "$PUSH_TARGET" main
  fi
}

cd "$ROOT_DIR"
git pull --ff-only
push_pending_commits
"$PYTHON_BIN" -m unittest discover -v
"$PYTHON_BIN" fetch_wnba_player_index.py --year "$YEAR"

git add "data/${YEAR}_pbp.csv" "data/${YEAR}_pbp.meta.json" data/wnba_player_index.csv
if git diff --cached --quiet; then
  echo "No WNBA player artifact changes"
else
  git config user.name "WNBA local refresh"
  git config user.email "russdt@users.noreply.github.com"
  git commit -m "Update official WNBA player totals $(date -u +%Y-%m-%d)"
fi

push_pending_commits
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] WNBA player refresh complete"
