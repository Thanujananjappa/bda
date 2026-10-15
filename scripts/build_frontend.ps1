$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location (Join-Path $Root "frontend")

if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Error "Node.js/npm required. Install from https://nodejs.org/"
    exit 1
}

npm install
npm run build
Write-Host "Frontend built -> frontend/dist" -ForegroundColor Green
