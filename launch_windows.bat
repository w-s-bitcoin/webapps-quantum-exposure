@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PORT=8090"
set "HOST=127.0.0.1"
set "URL=http://%HOST%:%PORT%/webapps/quantum_exposure/dashboard.html?standalone=1"

cd /d "%SCRIPT_DIR%"

set "AUTO_UPDATE_ENABLED=1"
if exist "%SCRIPT_DIR%\.standalone_prefs.json" (
  for /f %%A in ('powershell -NoProfile -Command "try { $p = Get-Content -Raw '.standalone_prefs.json' | ConvertFrom-Json; if ($null -ne $p.autoUpdateEnabled -and -not [bool]$p.autoUpdateEnabled) { '0' } else { '1' } } catch { '1' }"') do set "AUTO_UPDATE_ENABLED=%%A"
)

where git >nul 2>nul
if %ERRORLEVEL%==0 (
  if "%AUTO_UPDATE_ENABLED%"=="1" (
    echo Checking for dashboard updates from GitHub...
    git pull --ff-only
    if not %ERRORLEVEL%==0 (
      echo Warning: git pull failed. Launching with local files.
    )
  ) else (
    echo Auto update is off. Skipping git pull.
  )
)

echo Starting Quantum Exposure standalone server on %URL%

where py >nul 2>nul
if %ERRORLEVEL%==0 (
  start "" "%URL%"
  py -3 "%SCRIPT_DIR%standalone_server.py" --host %HOST% --port %PORT% --root "%SCRIPT_DIR%"
  goto :eof
)

where python >nul 2>nul
if %ERRORLEVEL%==0 (
  start "" "%URL%"
  python "%SCRIPT_DIR%standalone_server.py" --host %HOST% --port %PORT% --root "%SCRIPT_DIR%"
  goto :eof
)

echo Python was not found. Install Python 3 and retry.
pause
