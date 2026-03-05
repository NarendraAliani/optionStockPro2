param(
    [string]$Queue = "scan"
)

$ErrorActionPreference = "Stop"

$python = Join-Path $PSScriptRoot "..\\venv\\Scripts\\python.exe"
if (-not (Test-Path $python)) {
    Write-Host "Python venv not found at $python"
    exit 1
}

& $python -m rq worker -w rq.worker.SimpleWorker $Queue
