@echo off
REM =============================================================================
REM FinLedger PURVIEW DEMO — deploy governance pipelines if missing, then run
REM =============================================================================
REM Pipelines live in ADF folder: 13-governance-purview
REM   pl_gov_01 … pl_gov_06  (there is NO pipeline literally named "Purview")
REM Docs: docs\purview-teach-demo.html
REM =============================================================================
setlocal EnableExtensions
cd /d "%~dp0"

set "PY=.venv\Scripts\python.exe"
if not exist "%PY%" (
  echo Creating .venv ...
  python -m venv .venv
  "%PY%" -m pip install --disable-pip-version-check -q -r requirements.txt
)

echo.
echo ========================================================================
echo  PURVIEW DEMO — FinLedger shared class
echo  Detail HTML: docs\purview-teach-demo.html
echo ========================================================================
echo.
echo  WHERE THE PIPELINES ARE (ADF Studio)
echo    Factory : adf-shared-qgr7mj
echo    Author  -^> Pipelines -^> folder 13-governance-purview
echo    Names   : pl_gov_01 … pl_gov_06
echo    Demo    : pl_gov_06_master_medallion_governance
echo.
echo  If that folder is missing: this script will re-deploy ADF lab (skip SQL).
echo.

echo ------------------------------------------------------------------------
echo  [1/2] Ensuring governance pipelines are deployed (idempotent)...
echo ------------------------------------------------------------------------
call shared-adf-lab\orchestrate.cmd --skip-sql --skip-notebook
if errorlevel 1 (
  echo WARN ADF deploy had issues — trying run anyway if pipelines already exist.
)

echo.
echo ------------------------------------------------------------------------
echo  [2/2] Running master demo: pl_gov_06_master_medallion_governance
echo ------------------------------------------------------------------------
call shared-adf-lab\orchestrate.cmd --run-only --run-pipeline pl_gov_06_master_medallion_governance
set "RC=%ERRORLEVEL%"

echo.
echo ========================================================================
echo  NEXT CLICKS
echo ========================================================================
echo  ADF Studio:
echo    Author -^> Pipelines -^> 13-governance-purview -^> pl_gov_06
echo    Monitor -^> confirm Succeeded + child runs 01-05
echo.
echo  Purview (classic — New portal OFF):
echo    Search catalog: sample_transactions   OR   stsharedqgr7mj
echo    Open asset -^> Lineage tab  (wait 5-30 min after first run)
echo.
echo  Full detail: docs\purview-teach-demo.html
echo ========================================================================
exit /b %RC%
