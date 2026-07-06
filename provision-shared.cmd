@echo off
REM Shared student estate — one RG in eastus for all learners.
REM   provision-shared.cmd
REM   provision-shared.cmd --owner-email trainer@example.com
REM Prerequisite: az login (or service principal in .env)
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [One-time setup] Creating Python environment...
  python -m venv .venv
  if errorlevel 1 (
    echo ERROR: Python 3.11+ required from python.org
    exit /b 1
  )
  ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -q -r requirements.txt
)

".venv\Scripts\python.exe" scripts\provision_shared.py %*
exit /b %ERRORLEVEL%
