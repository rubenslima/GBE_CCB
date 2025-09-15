# HML
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
$env:APP_ENV = "HML"
Write-Host "[PS] APP_ENV=$($env:APP_ENV)"
python -u .\cpb.py
