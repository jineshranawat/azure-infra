@echo off
REM Medallion + SQL metadata + Purview governance — CI/CD release (Windows)
REM Builds Day 9 notebooks, deploys to Databricks + ADF governance, optional job run.

setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo Creating venv...
  python -m venv .venv
  .venv\Scripts\python.exe -m pip install -q -r requirements.txt
)

echo.
echo === Medallion + Governance Release ===
echo.

.venv\Scripts\python.exe scripts\release_medallion_governance.py %*

endlocal
