#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PORT="${PORT:-8090}"
HOST="127.0.0.1"
URL="http://${HOST}:${PORT}/webapps/quantum_exposure/dashboard.html?standalone=1"

cd "$SCRIPT_DIR"

echo "Starting Quantum Exposure standalone server on ${URL}"

if command -v python3 >/dev/null 2>&1; then
  PYTHON=python3
elif command -v python >/dev/null 2>&1; then
  PYTHON=python
else
  echo "Python was not found. Install Python 3 and retry."
  exit 1
fi

"$PYTHON" -m webbrowser "$URL" >/dev/null 2>&1 || true
exec "$PYTHON" -m http.server "$PORT" --bind "$HOST"
