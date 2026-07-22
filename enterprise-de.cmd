@echo off
REM =============================================================================
REM FinLedger ENTERPRISE DE PLAYBOOK — everyday data-engineering end-to-end demos
REM =============================================================================
REM ONE command runs the scenarios every engineer uses:
REM   UC01 Incident notify (Jira+attach+email)
REM   UC02 Data quality gate
REM   UC03 Incremental watermark
REM   UC04 Quarantine / dead-letter
REM   UC05 Config-driven ForEach entities
REM   UC06 Run audit trail
REM Docs: docs\enterprise-de-playbook.html
REM Usage:
REM   enterprise-de.cmd           run ALL
REM   enterprise-de.cmd 2         run UC02 only
REM   enterprise-de.cmd all
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
    echo ERROR: Python not found. Install Python 3.11+ and retry.
    exit /b 1
  )
)

set "UC=%~1"
if "%UC%"=="" set "UC=all"

echo.
echo ========================================================================
echo  ENTERPRISE DE PLAYBOOK
echo  Detail HTML: docs\enterprise-de-playbook.html
echo  Scenario   : %UC%
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
echo  [2/2] Run scenario(s) ...
echo ------------------------------------------------------------------------
"%PY%" shared-adf-lab\enterprise-de\run_all.py --uc %UC%
set "RC=%ERRORLEVEL%"

echo.
echo ========================================================================
echo  OPEN
echo ========================================================================
echo  Playbook HTML : docs\enterprise-de-playbook.html
echo  Results folder: shared-adf-lab\enterprise-de\out\
echo  Notify-only   : enterprise-notify.cmd
echo ========================================================================
start "" "%~dp0docs\enterprise-de-playbook.html"
exit /b %RC%
