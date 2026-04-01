#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Update these before first use.
export NGROK_DOMAIN="unix-main.ngrok.app"
export DISABLE_NGROK="false"
export ANGEL_EXECUTOR_ONLY="false"
export ANGEL_REMOTE_EXECUTION_URL="https://windows-worker.ngrok.app/internal/angel-execute"
export ANGEL_EXECUTOR_TOKEN="replace-with-shared-secret"
export ANGEL_REMOTE_EXECUTION_TIMEOUT_SECONDS="180"

if [[ "$NGROK_DOMAIN" == "unix-main.ngrok.app" ]]; then
  echo "Update NGROK_DOMAIN in start_unix_control_plane.sh before running."
  exit 1
fi

if [[ "$ANGEL_REMOTE_EXECUTION_URL" == "https://windows-worker.ngrok.app/internal/angel-execute" ]]; then
  echo "Update ANGEL_REMOTE_EXECUTION_URL in start_unix_control_plane.sh before running."
  exit 1
fi

if [[ "$ANGEL_EXECUTOR_TOKEN" == "replace-with-shared-secret" ]]; then
  echo "Update ANGEL_EXECUTOR_TOKEN in start_unix_control_plane.sh before running."
  exit 1
fi

exec python3 Server_Start.py
