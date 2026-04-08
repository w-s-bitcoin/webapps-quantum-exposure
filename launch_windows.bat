@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PORT=8090"
set "HOST=127.0.0.1"
set "URL=http://%HOST%:%PORT%/webapps/quantum_exposure/dashboard.html?standalone=1"

cd /d "%SCRIPT_DIR%"

where git >nul 2>nul
if %ERRORLEVEL%==0 (
  echo Checking for dashboard updates from GitHub...
  git pull --ff-only
  if not %ERRORLEVEL%==0 (
    echo Warning: git pull failed. Launching with local files.
  )
)

echo Starting Quantum Exposure standalone server on %URL%

where py >nul 2>nul
if %ERRORLEVEL%==0 (
  start "" "%URL%"
  py -3 -m http.server %PORT% --bind %HOST%
  goto :eof
)

where python >nul 2>nul
if %ERRORLEVEL%==0 (
  start "" "%URL%"
  python -m http.server %PORT% --bind %HOST%
  goto :eof
)

echo Python was not found. Install Python 3 and retry.
pause
