#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PORT="${PORT:-8090}"
HOST="127.0.0.1"
URL="http://${HOST}:${PORT}/webapps/quantum_exposure/dashboard.html?standalone=1"

cd "$SCRIPT_DIR"

if command -v python3 >/dev/null 2>&1; then
  PYTHON=python3
elif command -v python >/dev/null 2>&1; then
  PYTHON=python
else
  echo "Python was not found. Install Python 3 and retry."
  exit 1
fi

AUTO_UPDATE_ENABLED=1
PREFS_FILE="$SCRIPT_DIR/.standalone_prefs.json"
if [ -f "$PREFS_FILE" ]; then
  AUTO_UPDATE_ENABLED="$($PYTHON - "$PREFS_FILE" <<'PY'
import json
import sys

prefs_file = sys.argv[1]
try:
    with open(prefs_file, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    print("1" if bool(payload.get("autoUpdateEnabled", True)) else "0")
except Exception:
    print("1")
PY
)"
fi

if [ "$AUTO_UPDATE_ENABLED" = "1" ]; then
  "$PYTHON" "$SCRIPT_DIR/update_standalone_bundle.py" || true
else
  echo "Auto update is off. Skipping update check."
fi

echo "Starting Quantum Exposure standalone server on ${URL}"

"$PYTHON" -m webbrowser "$URL" >/dev/null 2>&1 || true
exec "$PYTHON" "$SCRIPT_DIR/standalone_server.py" --host "$HOST" --port "$PORT" --root "$SCRIPT_DIR"
