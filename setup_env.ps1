# PowerShell environment setup script for Windows/VSCode.
# Usage:
#   .\setup_env.ps1           # Full setup (Spark + Polars)
#   .\setup_env.ps1 -Polars   # Polars-only (no PySpark/Java)

param(
    [switch]$Polars
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$VenvDir = Join-Path $ScriptDir ".venv"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$VenvPip = Join-Path $VenvDir "Scripts\pip.exe"

# Create venv if needed
if (-Not (Test-Path $VenvDir)) {
    Write-Host "Creating venv at $VenvDir..." -ForegroundColor Cyan
    & python -m venv $VenvDir
} else {
    Write-Host "Venv already exists at $VenvDir" -ForegroundColor Green
}

# Upgrade pip
Write-Host "`nUpgrading pip..." -ForegroundColor Cyan
& $VenvPython -m pip install --upgrade pip setuptools wheel -q

if ($Polars) {
    Write-Host "`nInstalling Polars-only dependencies (no PySpark)..." -ForegroundColor Cyan
    & $VenvPython -m pip install "pyarrow>=14.0" "polars>=1.0" "deltalake>=0.18" "pytest>=7.0" "pytest-timeout>=2.2" -q
} else {
    $ReqFile = Join-Path $ScriptDir "requirements-dev.txt"
    Write-Host "`nInstalling all dependencies from requirements-dev.txt..." -ForegroundColor Cyan
    & $VenvPython -m pip install -r $ReqFile -q
}

# Install project editable
Write-Host "`nInstalling recon package (editable)..." -ForegroundColor Cyan
& $VenvPython -m pip install -e $ScriptDir -q

# Instructions
Write-Host ""
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host "Environment ready!" -ForegroundColor Green
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host ""
Write-Host "  Activate:  .venv\Scripts\activate"

if ($Polars) {
    Write-Host '  Run tests: pytest tests/ -m "not spark" -v'
    Write-Host ""
    Write-Host "  NOTE: Spark tests excluded (-Polars mode)." -ForegroundColor Yellow
    Write-Host "  To include Spark tests, re-run: .\setup_env.ps1"
} else {
    Write-Host "  Run tests: pytest tests/ -v"
}
Write-Host ""
