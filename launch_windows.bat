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

if "%AUTO_UPDATE_ENABLED%"=="1" (
  where py >nul 2>nul
  if %ERRORLEVEL%==0 (
    py -3 "%SCRIPT_DIR%update_standalone_bundle.py"
    if not %ERRORLEVEL%==0 echo Warning: auto update failed. Launching with local files.
  ) else (
    where python >nul 2>nul
    if %ERRORLEVEL%==0 (
      python "%SCRIPT_DIR%update_standalone_bundle.py"
      if not %ERRORLEVEL%==0 echo Warning: auto update failed. Launching with local files.
    ) else (
      echo Auto update skipped: Python not found for updater.
    )
  )
) else (
  echo Auto update is off. Skipping update check.
)

echo Starting Quantum Exposure standalone server on %URL%

where py >nul 2>nul
if %ERRORLEVEL%==0 (
  start "" "%URL%"
  py -3 "%SCRIPT_DIR%standalone_server.py" --host %HOST% --port %PORT% --root "%SCRIPT_DIR:~0,-1%"
  goto :eof
)

where python >nul 2>nul
if %ERRORLEVEL%==0 (
  start "" "%URL%"
  python "%SCRIPT_DIR%standalone_server.py" --host %HOST% --port %PORT% --root "%SCRIPT_DIR:~0,-1%"
  goto :eof
)

echo Python was not found. Install Python 3 and retry.
pause
