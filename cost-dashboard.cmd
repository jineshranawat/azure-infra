@echo off
REM ============================================================================
REM  FinLedger Cost Dashboard — works on ANY student Windows machine
REM ============================================================================
REM  Prerequisites: Python 3.10+ on PATH  OR  this repo's .venv
REM                 Azure CLI logged in:  az login
REM                 (OR .env with AZURE_TENANT_ID / CLIENT_ID / SECRET / SUBSCRIPTION_ID)
REM
REM  FIRST RUN / RE-RUN (idempotent):
REM    cost-dashboard.cmd
REM    cost-dashboard.cmd --open
REM    cost-dashboard.cmd --days 30 --open
REM
REM  Output:
REM    docs\cost-dashboard-out\index.html   ← open in browser
REM    docs\cost-dashboard-out\*.csv
REM ============================================================================
setlocal EnableExtensions
cd /d "%~dp0"

set "PY="
if exist ".venv\Scripts\python.exe" set "PY=.venv\Scripts\python.exe"
if not defined PY (
  where python >nul 2>&1 && set "PY=python"
)
if not defined PY (
  echo ERROR: Python not found. Install Python 3.10+ OR run orchestrate.cmd once to create .venv
  exit /b 1
)

echo.
echo === FinLedger Cost Dashboard ===
echo Python: %PY%
echo.

"%PY%" -m pip install -q azure-identity azure-mgmt-costmanagement azure-mgmt-datafactory azure-mgmt-resource 2>nul
"%PY%" scripts\cost_dashboard.py --open %*
set ERR=%ERRORLEVEL%

echo.
if %ERR%==0 (
  echo Done. Dashboard: docs\cost-dashboard-out\index.html
) else (
  echo Failed with exit code %ERR%. Tip: run  az login   then retry.
)
exit /b %ERR%
