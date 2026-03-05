$ErrorActionPreference = "Stop"

$service = Get-Service -Name redis -ErrorAction SilentlyContinue
if (-not $service) {
    Write-Host "Redis service not found. Ensure Redis is installed as a service."
    exit 1
}

Set-Service -Name redis -StartupType Automatic
if ($service.Status -ne "Running") {
    Start-Service -Name redis
}
Get-Service -Name redis | Format-Table -AutoSize
