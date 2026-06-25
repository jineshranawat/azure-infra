@echo off
REM Session 2 — ADF orchestration & bronze ingestion (2-hour lab)
REM Prerequisite: repo root orchestrate.cmd (Class-1 + ADF deployed)
REM
REM   orchestrate.cmd                  deploy + upload + watermark (fast classroom run)
REM   orchestrate.cmd --morning-check  also query ADF pipeline run history
REM   orchestrate.cmd --run-pipeline   also trigger ADF copy activity
setlocal
cd /d "%~dp0"
set PYTHONUNBUFFERED=1

echo.
echo Session 2 - ADF ingestion lab
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
  cd session-2
  echo [One-time setup] Ready.
  echo.
)

"..\.venv\Scripts\python.exe" scripts\run_session2.py %*
exit /b %ERRORLEVEL%
