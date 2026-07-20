@echo off
REM =============================================================================
REM FinLedger PURVIEW DEMO — one ready component for class
REM =============================================================================
REM What it does:
REM   1) Reminds trainer/learner of Purview teach points
REM   2) Runs the ready governance pipeline pl_gov_06 (bronze→silver→gold + discovery)
REM   3) Prints exactly what to click in the classic Purview portal
REM
REM Re-run safe. Requires: az login, shared estate, ADF lab already deployed (phase 3).
REM Docs: docs\purview-teach-demo.html
REM       shared-adf-lab\docs\purview_portal_search_guide.md
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
echo  Teach page: docs\purview-teach-demo.html
echo ========================================================================
echo.
echo  WHAT YOU ARE DEMOING
echo    Purview = Google for your data estate ^(catalog + lineage^)
echo    Demo component = ADF pipeline pl_gov_06_master_medallion_governance
echo.
echo  BEFORE YOU CLICK RUN ^(trainer checklist^)
echo    1. Classic Purview portal — turn OFF "New Microsoft Purview portal"
echo    2. ADF Manage -^> Microsoft Purview = Connected
echo    3. Data Curator on root collection for MI: adf-shared-qgr7mj
echo.
echo  Search terms AFTER the run ^(classic catalog search^):
echo    stsharedqgr7mj
echo    sample_transactions
echo    loaded
echo    cleaned
echo    aggregates
echo.
echo  Open Lineage tab on an asset — ADF appears as a PROCESS node.
echo  Do NOT search for factory name "adf-shared-qgr7mj" as a catalog asset.
echo.
echo ------------------------------------------------------------------------
echo  Running ready demo pipeline: pl_gov_06_master_medallion_governance
echo ------------------------------------------------------------------------
echo.

call shared-adf-lab\orchestrate.cmd --run-only --run-pipeline pl_gov_06_master_medallion_governance
set "RC=%ERRORLEVEL%"

echo.
echo ========================================================================
echo  PURVIEW DEMO — NEXT CLICKS ^(5-30 min for lineage sync^)
echo ========================================================================
echo  1. Azure Portal -^> search pviewrohan4hnv7s -^> Open Purview Studio
echo  2. Toggle OFF "New Microsoft Purview portal" ^(classic^)
echo  3. Data catalog -^> Search: sample_transactions  OR  stsharedqgr7mj
echo  4. Open asset -^> Lineage tab -^> show bronze -^> silver -^> gold edges
echo  5. ADF Studio Monitor -^> pl_gov_06 Succeeded ^(same story^)
echo.
echo  Full teach page: docs\purview-teach-demo.html
echo  Portal guide:    shared-adf-lab\docs\purview_portal_search_guide.md
echo ========================================================================
exit /b %RC%
