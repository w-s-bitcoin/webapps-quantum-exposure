#!/usr/bin/env python3
"""Serve standalone dashboard files and persist simple launcher preferences."""

from __future__ import annotations

import argparse
import json
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DEFAULT_PREFS = {"autoUpdateEnabled": True}
PREFS_FILENAME = ".standalone_prefs.json"
PREFS_ENDPOINT = "/__standalone__/prefs"


def load_prefs(prefs_path: Path) -> dict:
    prefs = dict(DEFAULT_PREFS)
    try:
        if prefs_path.is_file():
            raw = json.loads(prefs_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("autoUpdateEnabled"), bool):
                prefs["autoUpdateEnabled"] = raw["autoUpdateEnabled"]
    except Exception:
        # Ignore malformed prefs and fall back to defaults.
        pass
    return prefs


def save_prefs(prefs_path: Path, prefs: dict) -> None:
    normalized = {"autoUpdateEnabled": bool(prefs.get("autoUpdateEnabled", True))}
    temp_path = prefs_path.with_suffix(prefs_path.suffix + ".tmp")
    temp_path.write_text(json.dumps(normalized, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(prefs_path)


class StandaloneHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, directory: str, prefs_path: Path, **kwargs):
        self._prefs_path = prefs_path
        super().__init__(*args, directory=directory, **kwargs)

    def _send_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == PREFS_ENDPOINT:
            self._send_json(200, load_prefs(self._prefs_path))
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        if self.path != PREFS_ENDPOINT:
            self.send_error(404, "Not found")
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length <= 0 or content_length > 10_000:
            self.send_error(400, "Invalid payload")
            return

        raw_body = self.rfile.read(content_length)
        try:
            data = json.loads(raw_body.decode("utf-8"))
        except Exception:
            self.send_error(400, "Malformed JSON")
            return

        if not isinstance(data, dict):
            self.send_error(400, "Malformed payload")
            return

        prefs = load_prefs(self._prefs_path)
        if "autoUpdateEnabled" in data:
            prefs["autoUpdateEnabled"] = bool(data["autoUpdateEnabled"])
        save_prefs(self._prefs_path, prefs)
        self._send_json(200, prefs)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Quantum Exposure standalone web server")
    parser.add_argument("--host", default="127.0.0.1", help="Interface to bind")
    parser.add_argument("--port", type=int, default=8090, help="Port to bind")
    parser.add_argument("--root", default=".", help="Directory to serve")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    prefs_path = root / PREFS_FILENAME

    def handler_factory(*handler_args, **handler_kwargs):
        return StandaloneHandler(
            *handler_args,
            directory=str(root),
            prefs_path=prefs_path,
            **handler_kwargs,
        )

    server = ThreadingHTTPServer((args.host, args.port), handler_factory)
    print(f"Serving standalone dashboard from {root}")
    print(f"Preferences endpoint: http://{args.host}:{args.port}{PREFS_ENDPOINT}")
    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
