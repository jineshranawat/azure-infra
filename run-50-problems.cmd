@echo off
REM Run 50_data_engineering_problems on shared Databricks (starts cluster if needed).
REM Prerequisite: .env has DATABRICKS_HOST + DATABRICKS_TOKEN; secrets via session-3\orchestrate.cmd --setup-secrets
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo ERROR: Run provision-shared.cmd or session-3\orchestrate.cmd first to create .venv
  exit /b 1
)
".venv\Scripts\python.exe" scripts\run_50_problems_notebook.py %*
exit /b %ERRORLEVEL%
