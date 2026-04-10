$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Port = 8090
$HostAddr = "127.0.0.1"
$Url = "http://$HostAddr`:$Port/webapps/quantum_exposure/dashboard.html?standalone=1"

Set-Location $ScriptDir

$autoUpdateEnabled = $true
$prefsPath = Join-Path $ScriptDir ".standalone_prefs.json"
if (Test-Path $prefsPath) {
  try {
    $prefs = Get-Content -Raw $prefsPath | ConvertFrom-Json
    if ($null -ne $prefs.autoUpdateEnabled) {
      $autoUpdateEnabled = [bool]$prefs.autoUpdateEnabled
    }
  } catch {
    $autoUpdateEnabled = $true
  }
}

if ($autoUpdateEnabled) {
  if (Get-Command py -ErrorAction SilentlyContinue) {
    try {
      & py -3 "$ScriptDir\update_standalone_bundle.py"
    } catch {
      Write-Host "Warning: auto update failed. Launching with local files."
    }
  } elseif (Get-Command python -ErrorAction SilentlyContinue) {
    try {
      & python "$ScriptDir\update_standalone_bundle.py"
    } catch {
      Write-Host "Warning: auto update failed. Launching with local files."
    }
  } else {
    Write-Host "Auto update skipped: Python not found for updater."
  }
} else {
  Write-Host "Auto update is off. Skipping update check."
}

Write-Host "Starting Quantum Exposure standalone server on $Url"

$usePyLauncher = $false
$pythonExe = $null
if (Get-Command py -ErrorAction SilentlyContinue) {
  $usePyLauncher = $true
  $pythonExe = "py"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
  $pythonExe = "python"
}

if (-not $pythonExe) {
  Write-Host "Python was not found. Install Python 3 and retry."
  Read-Host "Press Enter to close"
  exit 1
}

Start-Process $Url | Out-Null
if ($usePyLauncher) {
  & $pythonExe -3 "$ScriptDir\standalone_server.py" --host $HostAddr --port $Port --root $ScriptDir
} else {
  & $pythonExe "$ScriptDir\standalone_server.py" --host $HostAddr --port $Port --root $ScriptDir
}
