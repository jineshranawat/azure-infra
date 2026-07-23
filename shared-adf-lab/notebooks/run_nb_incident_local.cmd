@echo off
REM Run nb_incident_jira_email.py on this Windows laptop (not Databricks).
REM Starts local Jira :18080 + mail :18081 if needed, then runs the notebook.
setlocal EnableExtensions
cd /d "%~dp0\..\.."

set "PY=%~dp0\..\..\.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"

echo.
echo ========================================================================
echo  LOCAL notebook run: nb_incident_jira_email.py
echo  Jira : http://127.0.0.1:18080/
echo  Mail : http://127.0.0.1:18081/
echo ========================================================================
echo.

"%PY%" shared-adf-lab\enterprise-notify\start_services.py
if errorlevel 1 (
  echo ERROR: could not start local Jira/mail mocks
  exit /b 1
)

echo.
"%PY%" -u shared-adf-lab\notebooks\nb_incident_jira_email.py
exit /b %ERRORLEVEL%
