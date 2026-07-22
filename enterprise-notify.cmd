@echo off
REM =============================================================================
REM FinLedger ENTERPRISE NOTIFY — local Jira + mail + incident flow
REM =============================================================================
REM Shows: ADF-style failure → raise Jira ticket → attach run log → send email
REM Docs: docs\enterprise-jira-email-demo.html
REM Re-run: enterprise-notify.cmd   (idempotent — reuses local services)
REM =============================================================================
setlocal EnableExtensions
cd /d "%~dp0"

set "PY=.venv\Scripts\python.exe"
if not exist "%PY%" (
  echo Creating .venv ...
  python -m venv .venv
  if errorlevel 1 (
    echo ERROR: Python not found. Install Python 3.11+ and retry.
    exit /b 1
  )
)

echo.
echo ========================================================================
echo  ENTERPRISE NOTIFY — Jira ticket + attach + email
echo  Detail HTML: docs\enterprise-jira-email-demo.html
echo ========================================================================
echo.

echo ------------------------------------------------------------------------
echo  [1/3] Ensure deps (fastapi / uvicorn / httpx)...
echo ------------------------------------------------------------------------
"%PY%" -m pip install --disable-pip-version-check -q -r requirements.txt
if errorlevel 1 (
  echo ERROR: pip install failed
  exit /b 1
)

echo.
echo ------------------------------------------------------------------------
echo  [2/3] Start local Jira mock :18080 + mail sink :18081 ...
echo ------------------------------------------------------------------------
"%PY%" shared-adf-lab\enterprise-notify\start_services.py
if errorlevel 1 (
  echo ERROR: could not start local services
  exit /b 1
)

echo.
echo ------------------------------------------------------------------------
echo  [3/3] Run incident flow (failure → Jira → attach → email)...
echo ------------------------------------------------------------------------
"%PY%" shared-adf-lab\enterprise-notify\client\run_incident_flow.py --force-fail true
set "RC=%ERRORLEVEL%"

echo.
echo ========================================================================
echo  OPEN THESE IN YOUR BROWSER
echo ========================================================================
echo  Jira board : http://127.0.0.1:18080/
echo  Mail inbox : http://127.0.0.1:18081/
echo  Teach HTML : docs\enterprise-jira-email-demo.html
echo.
echo  Opening teach HTML (flow + complete code overview)...
start "" "%~dp0docs\enterprise-jira-email-demo.html"
echo.
echo  NOTE: This one command is the full visible demo (Jira + mail).
echo  Optional ADF Studio later: shared-adf-lab\orchestrate.cmd --skip-sql
echo ========================================================================
exit /b %RC%
