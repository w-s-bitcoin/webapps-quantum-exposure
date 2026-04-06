$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Port = 8090
$HostAddr = "127.0.0.1"
$Url = "http://$HostAddr`:$Port/webapps/quantum_exposure/dashboard.html?standalone=1"

Set-Location $ScriptDir
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
  & $pythonExe -3 -m http.server $Port --bind $HostAddr
} else {
  & $pythonExe -m http.server $Port --bind $HostAddr
}
