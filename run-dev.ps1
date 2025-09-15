# DEV
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
$env:APP_ENV = "DEV"
Write-Host "[PS] APP_ENV=$($env:APP_ENV)"
python -u .\cpb.py
