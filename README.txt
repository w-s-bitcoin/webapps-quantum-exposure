Quantum Exposure Standalone Bundle

This package runs the FULL Quantum Exposure dashboard locally.

Quick Start
- macOS: double-click launch_macos.command
- Linux: run ./launch_linux.sh
- Windows: double-click launch_windows.bat (or run launch_windows.ps1)

Notes
- Requires Python 3 installed.
- Launchers open the dashboard directly (no home page/modal shell).
- The launcher serves files over http://127.0.0.1:8090 so browser fetches work.
- The Auto update toggle in the dashboard controls whether launchers check for updates before opening.
- If launched from a cloned git repo, updates use git pull.
- If launched from a downloaded ZIP (no .git folder), updates use the latest GitHub ZIP and overwrite changed files.
- Do not open dashboard.html directly from file://.
- To stop the server, close the terminal window or press Ctrl+C.
