@echo off
REM Day 6 — Python for data engineers (Session 6, 2-hour lab)
REM Prerequisite: Sessions 1-5 complete; repo root .env configured
REM
REM   orchestrate.cmd              local Python demos + unit tests + notebook guide
REM   orchestrate.cmd --verbose    more log detail
setlocal
cd /d "%~dp0"
set PYTHONUNBUFFERED=1

echo.
echo Day 6 - Python for data engineers
echo.

if not exist "..\.venv\Scripts\python.exe" (
  echo [One-time setup] Creating Python environment at repo root ^(about 1-2 minutes^)...
  echo.
  cd ..
  python -m venv .venv
  if errorlevel 1 (
    echo ERROR: Python 3.11+ required. Install from python.org and tick Add to PATH.
    exit /b 1
  )
  echo [One-time setup] Installing packages...
  ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -r requirements.txt
  if errorlevel 1 (
    echo ERROR: pip install failed. Check network and re-run orchestrate.cmd
    exit /b 1
  )
  cd day6
  echo [One-time setup] Ready.
  echo.
) else (
  "..\.venv\Scripts\python.exe" -m pip install --disable-pip-version-check -q -r ..\requirements.txt
)

"..\.venv\Scripts\python.exe" scripts\run_day6.py %*
exit /b %ERRORLEVEL%
