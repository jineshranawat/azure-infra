#Requires -Version 5.1
<#
.SYNOPSIS
    Windows setup for Bank of England Azure ETL - Class 1 Landing Zone.

.DESCRIPTION
    Installs Azure CLI (if missing), prepares a Python virtual environment,
    copies .env from the template, and prints the next manual steps (az login).

.EXAMPLE
    # Run from the repository root (recommended):
    powershell -ExecutionPolicy Bypass -File .\scripts\setup-windows.ps1

.EXAMPLE
    # Skip Azure CLI install (already installed):
    .\scripts\setup-windows.ps1 -SkipAzureCliInstall
#>
[CmdletBinding()]
param(
    [switch] $SkipAzureCliInstall,
    [switch] $SkipPythonSetup
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Step([string] $Message) {
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Write-Ok([string] $Message) {
    Write-Host "    OK: $Message" -ForegroundColor Green
}

function Write-Warn([string] $Message) {
    Write-Host "    WARN: $Message" -ForegroundColor Yellow
}

function Test-Command([string] $Name) {
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Refresh-SessionPath {
    $machinePath = [Environment]::GetEnvironmentVariable('Path', 'Machine')
    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    if ($machinePath -or $userPath) {
        $env:Path = @($machinePath, $userPath) -join ';'
    }
}

function Find-AzureCli {
    Refresh-SessionPath
    if (Test-Command 'az') {
        return $true
    }

    $defaultAz = Join-Path $env:ProgramFiles 'Microsoft SDKs\Azure\CLI2\wbin\az.cmd'
    if (Test-Path $defaultAz) {
        $azDir = Split-Path $defaultAz -Parent
        $env:Path = "$azDir;$env:Path"
        return (Test-Command 'az')
    }

    return $false
}

function Install-AzureCli {
    if (-not (Test-Command 'winget')) {
        throw @"
winget is not available. Install Azure CLI manually, then re-run this script with -SkipAzureCliInstall:
  https://learn.microsoft.com/cli/azure/install-azure-cli-windows
"@
    }

    Write-Step 'Installing Azure CLI via winget (Microsoft.AzureCLI)...'
    & winget install -e --id Microsoft.AzureCLI `
        --accept-source-agreements `
        --accept-package-agreements

    if ($LASTEXITCODE -ne 0) {
        throw "winget install failed (exit $LASTEXITCODE). Try running PowerShell as Administrator."
    }

    if (-not (Find-AzureCli)) {
        Write-Warn 'Azure CLI was installed but is not on PATH in this session.'
        Write-Warn 'Close and reopen your terminal, then run: az login'
        return $false
    }

    return $true
}

function Ensure-DotEnv([string] $RepoRoot) {
    $example = Join-Path $RepoRoot '.env.example'
    $dotEnv = Join-Path $RepoRoot '.env'

    if (-not (Test-Path $example)) {
        throw "Missing template: $example"
    }

    if (Test-Path $dotEnv) {
        Write-Ok '.env already exists (not overwritten)'
        return
    }

    Copy-Item $example $dotEnv
    Write-Ok 'Created .env from .env.example - edit AZURE_SUBSCRIPTION_ID, LEARNER, OWNER_EMAIL'
}

function Ensure-PythonVenv([string] $RepoRoot) {
    $venvDir = Join-Path $RepoRoot '.venv'
    $python = $null

    if (Test-Command 'py') {
        $python = 'py'
    } elseif (Test-Command 'python') {
        $python = 'python'
    } else {
        throw @"
Python 3.11+ not found. Install from https://www.python.org/downloads/
and ensure 'Add python.exe to PATH' is checked during setup.
"@
    }

    $versionOutput = & $python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    $parts = $versionOutput.Trim().Split('.')
    $major = [int] $parts[0]
    $minor = [int] $parts[1]
    if ($major -lt 3 -or ($major -eq 3 -and $minor -lt 11)) {
        throw "Python 3.11+ required; found $versionOutput"
    }
    Write-Ok "Python $versionOutput"

    if (-not (Test-Path (Join-Path $venvDir 'Scripts\python.exe'))) {
        Write-Step 'Creating virtual environment (.venv)...'
        & $python -m venv $venvDir
        Write-Ok 'Virtual environment created'
    } else {
        Write-Ok 'Virtual environment already exists'
    }

    $venvPython = Join-Path $venvDir 'Scripts\python.exe'
    Write-Step 'Installing Python dependencies...'
    & $venvPython -m pip install --upgrade pip --quiet
    & $venvPython -m pip install -r (Join-Path $RepoRoot 'requirements.txt') --quiet
    Write-Ok 'requirements.txt installed into .venv'
}

# --- main ---
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
Push-Location $repoRoot

try {
    Write-Host ''
    Write-Host 'Azure ETL Class 1 - Windows setup' -ForegroundColor White
    Write-Host "Repository: $repoRoot"

    $azReady = Find-AzureCli
    if ($azReady) {
        $azVersion = (& az version | ConvertFrom-Json).'azure-cli'
        Write-Ok "Azure CLI already installed ($azVersion)"
    } elseif (-not $SkipAzureCliInstall) {
        $azReady = Install-AzureCli
        if ($azReady) {
            $azVersion = (& az version | ConvertFrom-Json).'azure-cli'
            Write-Ok "Azure CLI installed ($azVersion)"
        }
    } else {
        Write-Warn 'Azure CLI not found (-SkipAzureCliInstall). Install manually before deploy.'
    }

    if (-not $SkipPythonSetup) {
        Ensure-PythonVenv -RepoRoot $repoRoot
    }

    Ensure-DotEnv -RepoRoot $repoRoot

    Write-Step 'Next steps (manual)'
    Write-Host @'

1. Edit .env in the repo root:
     AZURE_SUBSCRIPTION_ID=<your-subscription-guid>
     LEARNER=<2-10 lowercase alphanumeric id>
     OWNER_EMAIL=<your-email>

2. Sign in to Azure (browser opens):
     az login

3. Select your subscription (if you have more than one):
     az account set --subscription <your-subscription-guid>

4. Deploy - choose one path:
     Full lab:  orchestrate.cmd
     Class-1:   orchestrate.cmd --class1-only
     Python: .\.venv\Scripts\python.exe scripts\provision.py --subscription-id <guid>

5. Verify:
     .\.venv\Scripts\python.exe scripts\verify_cost.py --resource-group rg-<learner>-class1

'@ -ForegroundColor Gray

    if (-not $azReady) {
        Write-Warn 'Restart your terminal so "az" is on PATH, then run: az login'
    }

    Write-Host 'Setup complete.' -ForegroundColor Green
}
finally {
    Pop-Location
}
