@echo off
REM Day 7 — Python essentials + pandas/polars + storage + local Spark (Session 7)
REM   orchestrate.cmd                    full lab + notebook kernel setup
REM   orchestrate.cmd --skip-spark         skip PySpark if Java not installed
REM   orchestrate.cmd --setup-notebook     only Jupyter kernel (for .ipynb in Cursor)
setlocal
cd /d "%~dp0"
set PYTHONUNBUFFERED=1

echo.
echo Day 7 - Python essentials and read the lake
echo.

if not exist "..\.venv\Scripts\python.exe" (
  echo [One-time setup] Creating Python environment at repo root...
  cd ..
  python -m venv .venv
  if errorlevel 1 (
    echo ERROR: Python 3.11+ required from python.org — tick "Add to PATH"
    exit /b 1
  )
  ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -r requirements.txt
  cd day7
)

"..\.venv\Scripts\python.exe" -m pip install --disable-pip-version-check -q -r requirements.txt

if "%~1"=="--setup-notebook" (
  "..\.venv\Scripts\python.exe" scripts\setup_notebook_kernel.py
  exit /b %ERRORLEVEL%
)

"..\.venv\Scripts\python.exe" scripts\setup_notebook_kernel.py
if errorlevel 1 (
  echo WARNING: notebook kernel setup had issues — see messages above
)

"..\.venv\Scripts\python.exe" scripts\run_day7.py %*
exit /b %ERRORLEVEL%
