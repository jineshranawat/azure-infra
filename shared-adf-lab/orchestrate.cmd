@echo off
REM Shared ADF lab — SQL (westus) + 10 teaching pipelines + Databricks triggers
REM Prerequisite: provision-shared.cmd (shared eastus estate)
REM
REM   orchestrate.cmd                      deploy everything (idempotent)
REM   orchestrate.cmd --setup-databricks-integration   secrets + Databricks Job + manifest
REM   orchestrate.cmd --run-pipeline pl_07_databricks_notebook
REM   orchestrate.cmd --warm-cluster-only --warm-cluster-minutes 140
setlocal
cd /d "%~dp0"
set PYTHONUNBUFFERED=1

echo Shared ADF lab — FinLedger class estate
echo.
echo PowerShell: use .\orchestrate.cmd  (not bare orchestrate.cmd)
echo.

if not exist "..\.venv\Scripts\python.exe" (
  echo [One-time setup] Creating Python environment at repo root...
  cd ..
  python -m venv .venv
  if errorlevel 1 (
    echo ERROR: Python 3.11+ required from python.org
    exit /b 1
  )
  ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -q -r requirements.txt
  cd shared-adf-lab
) else (
  REM Force ADF SDK 9.x — quiet pin often leaves v10 installed and phase 3 crashes on ForEach
  echo Ensuring azure-mgmt-datafactory 9.3.0 ^(not v10^)...
  "..\.venv\Scripts\python.exe" -m pip install --disable-pip-version-check --force-reinstall -q "azure-mgmt-datafactory==9.3.0"
  if errorlevel 1 (
    echo ERROR: could not install azure-mgmt-datafactory 9.3.0
    exit /b 1
  )
)

"..\.venv\Scripts\python.exe" scripts\run_shared_adf.py %*
exit /b %ERRORLEVEL%
