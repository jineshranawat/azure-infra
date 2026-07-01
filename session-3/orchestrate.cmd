@echo off
REM Session 3 — Databricks lakehouse transformation (2-hour lab)
REM Prerequisite: repo root orchestrate.cmd (Class-1 + Databricks workspace deployed)
REM
REM   orchestrate.cmd                  prep bronze + RBAC + print Databricks paths
REM   orchestrate.cmd --setup-secrets  create finledger scope + secrets from .env
REM   orchestrate.cmd --verify-storage check silver/gold outputs after notebook run
setlocal
cd /d "%~dp0"
set PYTHONUNBUFFERED=1

echo.
echo Session 3 - Databricks transformation lab
echo.

if not exist "..\.venv\Scripts\python.exe" (
  echo [One-time setup] Creating Python environment at repo root ^(about 1-2 minutes^)...
  echo                  Deploy Class-1 first if you have not: cd .. ^& orchestrate.cmd
  echo.
  cd ..
  python -m venv .venv
  if errorlevel 1 (
    echo ERROR: Python 3.11+ required. Install from python.org and tick Add to PATH.
    exit /b 1
  )
  echo [One-time setup] Installing Azure SDK packages...
  ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -r requirements.txt
  if errorlevel 1 (
    echo ERROR: pip install failed. Check network and re-run orchestrate.cmd
    exit /b 1
  )
  echo [One-time setup] Installing Databricks CLI...
  ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -q databricks-cli>=0.18.0
  cd session-3
  echo [One-time setup] Ready.
  echo.
) else (
  echo [Setup] Ensuring Azure SDK packages are installed...
  "..\.venv\Scripts\python.exe" -m pip install --disable-pip-version-check -q -r ..\requirements.txt
  if errorlevel 1 (
    echo ERROR: pip install failed. Check network and re-run orchestrate.cmd
    exit /b 1
  )
  echo [Setup] Installing Databricks CLI for --setup-secrets...
  "..\.venv\Scripts\python.exe" -m pip install --disable-pip-version-check -q databricks-cli>=0.18.0
)

"..\.venv\Scripts\python.exe" scripts\run_session3.py %*
exit /b %ERRORLEVEL%
