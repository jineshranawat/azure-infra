@echo off
REM =============================================================================
REM FinLedger Genie lab — create Genie Agent from UC catalog + question orchestrator
REM =============================================================================
REM ONE command:
REM   genie-lab.cmd           create agent + ask full suite
REM   genie-lab.cmd create    create/update agent only
REM   genie-lab.cmd ask       ask suite only (needs prior create)
REM Requires: DATABRICKS_TOKEN in .env, SQL warehouse, Partner AI enabled
REM Docs: docs\databricks-genie-agents-teach.html
REM Cost: starts SQL warehouse (auto-stop). Warns — cheapest warehouse preferred.
REM =============================================================================
setlocal EnableExtensions
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

set "PY=.venv\Scripts\python.exe"
if not exist "%PY%" (
  echo Creating .venv ...
  python -m venv .venv
  if errorlevel 1 (
    echo ERROR: Python not found.
    exit /b 1
  )
)

set "PHASE=%~1"
if "%PHASE%"=="" set "PHASE=all"
if /I "%PHASE%"=="create" set "PHASE=create"
if /I "%PHASE%"=="ask" set "PHASE=ask"
if /I "%PHASE%"=="all" set "PHASE=all"

echo.
echo ========================================================================
echo  GENIE LAB — catalog tables -^> Genie Agent -^> question orchestrator
echo  Phase: %PHASE%
echo  COST: will START SQL warehouse (serverless/pro). Auto-stop when idle.
echo ========================================================================
echo.

echo ------------------------------------------------------------------------
echo  [1/2] Ensure deps ...
echo ------------------------------------------------------------------------
"%PY%" -m pip install --disable-pip-version-check -q -r requirements.txt
if errorlevel 1 (
  echo ERROR: pip install failed
  exit /b 1
)

echo.
echo ------------------------------------------------------------------------
echo  [2/2] Run Genie lab ...
echo ------------------------------------------------------------------------
"%PY%" shared-adf-lab\genie-lab\run_all.py --phase %PHASE%
set "RC=%ERRORLEVEL%"

echo.
echo ========================================================================
echo  OPEN
echo ========================================================================
echo  Teach HTML : docs\databricks-genie-agents-teach.html
echo  Results    : shared-adf-lab\genie-lab\out\
echo  Agent UI   : see genie_agent_state.json ui_url
echo ========================================================================
if exist "%~dp0shared-adf-lab\genie-lab\out\orchestrator_results.md" (
  start "" "%~dp0shared-adf-lab\genie-lab\out\orchestrator_results.md"
)
exit /b %RC%
