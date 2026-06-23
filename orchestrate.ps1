# Internal helper — called by orchestrate.cmd via -ExecutionPolicy Bypass.
# Learners should use:  orchestrate.cmd
#Requires -Version 5.1
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$py = $null
foreach ($c in @('py', 'python', 'python3')) {
    $cmd = Get-Command $c -ErrorAction SilentlyContinue
    if (-not $cmd) { continue }
    & $cmd.Source -c "import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)" 2>$null
    if ($LASTEXITCODE -eq 0) {
        $py = $cmd.Source
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
