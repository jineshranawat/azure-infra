@echo off
REM Shared ADF lab — SQL (westus) + 10 teaching pipelines + Databricks triggers
REM Prerequisite: provision-shared.cmd (shared eastus estate)
REM
REM   orchestrate.cmd                      deploy everything (idempotent)
REM   orchestrate.cmd --skip-sql           pipelines only (no SQL westus)
REM   orchestrate.cmd --run-pipeline pl_07_databricks_notebook
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
)

"..\.venv\Scripts\python.exe" scripts\run_shared_adf.py %*
exit /b %ERRORLEVEL%
