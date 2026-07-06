@echo off
REM Deploy bronze sample data, FinLedger secrets, and master PySpark notebook to shared Databricks.
REM Prerequisite: provision-shared.cmd completed; .env has DATABRICKS_TOKEN + STORAGE_ACCOUNT_KEY
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  python -m venv .venv
  ".venv\Scripts\python.exe" -m pip install -q -r requirements.txt
)
".venv\Scripts\python.exe" scripts\deploy_shared_lab.py %*
exit /b %ERRORLEVEL%
