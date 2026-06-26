@echo off
REM ============================================================================
REM  Bank of England Azure ETL Class-1 — SINGLE Windows entry point
REM ============================================================================
REM  Prerequisites: Python 3.11+ on PATH (tick "Add to PATH" at install)
REM
REM  FIRST TIME (clean machine):
REM    orchestrate.cmd --install-cli
REM    -> creates .env — edit subscription, learner, email — run same command again
REM
REM  FULL LAB (default, safe to re-run):
REM    orchestrate.cmd
REM
REM  DAY 1 only (landing zone):
REM    orchestrate.cmd --class1-only
REM
REM  FAST DATABRICKS eastus2 (async delete old UK workspace + create — skips ADF/Purview/verify):
REM    orchestrate.cmd --databricks-eastus2-only
REM
REM  PLATFORMS (ADF + async Databricks eastus2 migration — same as above for DBX cleanup):
REM    orchestrate.cmd --platforms-only
REM
REM  TEARDOWN:
REM    orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes
REM ============================================================================
setlocal
cd /d "%~dp0"

if /I "%~1"=="teardown" (
  shift
  if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" scripts\teardown.py %*
    exit /b !ERRORLEVEL!
  )
  python scripts\teardown.py %*
  exit /b %ERRORLEVEL%
)

if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" scripts\orchestrate.py %*
  exit /b %ERRORLEVEL%
)

REM Before .venv exists: use PowerShell helper (Bypass policy — learner never edits policy)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0orchestrate.ps1" %*
exit /b %ERRORLEVEL%
