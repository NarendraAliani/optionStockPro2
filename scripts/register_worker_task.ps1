param(
    [string]$TaskName = "OptionStockPro-RQ-Worker",
    [string]$Queue = "scan"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root "venv\\Scripts\\python.exe"
$script = Join-Path $root "scripts\\start_rq_worker.ps1"

if (-not (Test-Path $python)) {
    Write-Host "Python venv not found at $python"
    exit 1
}
if (-not (Test-Path $script)) {
    Write-Host "Worker script not found at $script"
    exit 1
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$script`" -Queue `"$Queue`""
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "NT AUTHORITY\\SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
