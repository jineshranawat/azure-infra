@echo off
REM Day 8 — PySpark transformations (Session 8)
REM   orchestrate.cmd              venv recap + pandas/polars + PySpark theory + transforms + tests
REM   orchestrate.cmd --skip-spark   skip local Spark if Java not installed
setlocal
cd /d "%~dp0"
set PYTHONUNBUFFERED=1

echo.
echo Day 8 - PySpark transformations
echo.

if not exist "..\.venv\Scripts\python.exe" (
  echo [One-time setup] Creating Python environment at repo root...
  cd ..
  python -m venv .venv
  if errorlevel 1 exit /b 1
  ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -r requirements.txt
  cd day8
)

"..\.venv\Scripts\python.exe" -m pip install --disable-pip-version-check -q -r requirements.txt
"..\.venv\Scripts\python.exe" scripts\run_day8.py %*
exit /b %ERRORLEVEL%
