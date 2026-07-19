@echo off
REM Deploy bronze sample data, FinLedger secrets, and master PySpark notebook to shared Databricks.
REM Auth (in order):
REM   1) Valid DATABRICKS_TOKEN in .env (PAT) — optional
REM   2) Else Azure AD token from `az login` (preferred for students — no PAT required)
REM Host: DATABRICKS_HOST in .env OR auto from workspace dbw-shared-qgr7mj
REM Storage: STORAGE_ACCOUNT_KEY in .env OR auto from `az storage account keys list`
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  python -m venv .venv
  ".venv\Scripts\python.exe" -m pip install -q -r requirements.txt
)
".venv\Scripts\python.exe" scripts\deploy_shared_lab.py %*
exit /b %ERRORLEVEL%
