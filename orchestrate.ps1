# Internal helper — called by orchestrate.cmd via -ExecutionPolicy Bypass.
# Learners should use:  orchestrate.cmd
#Requires -Version 5.1
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$py = $null
foreach ($c in @('py', 'python', 'python3')) {
    if (Get-Command $c -ErrorAction SilentlyContinue) {
        $py = $c
        break
    }
}
if (-not $py) {
    Write-Host 'ERROR: Python 3.11+ required. Install from https://www.python.org/downloads/' -ForegroundColor Red
    Write-Host '       Tick "Add python.exe to PATH" during install.' -ForegroundColor Yellow
    exit 1
}

& $py scripts/orchestrate.py @args
exit $LASTEXITCODE
