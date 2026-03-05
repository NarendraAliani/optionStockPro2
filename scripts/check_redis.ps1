$ErrorActionPreference = "Stop"

$python = Join-Path $PSScriptRoot "..\\venv\\Scripts\\python.exe"
if (-not (Test-Path $python)) {
    Write-Host "Python venv not found at $python"
    exit 1
}

$code = @"
import redis
import sys

url = 'redis://localhost:6379/0'
try:
    r = redis.Redis.from_url(url)
    print('PING:', r.ping())
except Exception as exc:
    print('ERROR:', exc)
    sys.exit(1)
"@

& $python -c $code
