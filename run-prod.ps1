# run-prod.ps1
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
$env:APP_ENV = "PROD"
Write-Host "[PS] APP_ENV=$($env:APP_ENV)"
python -u .\cpb.py
