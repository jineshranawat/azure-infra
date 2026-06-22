# Azure ETL lab — one-command orchestrator (Windows)
# Full lab (default): Class-1 + ADF + Purview + Databricks + verify + cost compare
#   .\orchestrate.ps1 --skip-setup
# Class-1 only:  .\orchestrate.ps1 --class1-only --skip-setup
# First-time VM:   .\orchestrate.ps1 --install-cli
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$py = $null
foreach ($c in @('py', 'python', 'python3')) {
    if (Get-Command $c -ErrorAction SilentlyContinue) {
        $py = $c
        break
    }
}
if (-not $py) { throw 'Python 3.11+ required.' }

& $py scripts/orchestrate.py @args
exit $LASTEXITCODE
