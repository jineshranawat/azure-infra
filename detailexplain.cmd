@echo off
REM =============================================================================
REM Open shared-adf-lab\detailexplain.html (RTT Pipeline Explainer) in the browser
REM Serves over localhost so Mermaid ES modules load (file:// often blocks them).
REM =============================================================================
setlocal EnableExtensions
cd /d "%~dp0"

set "HTML=%~dp0shared-adf-lab\detailexplain.html"
if not exist "%HTML%" (
  echo ERROR: missing %HTML%
  exit /b 1
)

set "PY=.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"

echo.
echo ========================================================================
echo  RTT Pipeline Explainer
echo  File : shared-adf-lab\detailexplain.html
echo  URL  : http://127.0.0.1:8766/detailexplain.html
echo ========================================================================
echo.
echo Starting local server on port 8766 (Ctrl+C to stop)...
echo.

start "" "http://127.0.0.1:8766/detailexplain.html"
"%PY%" -m http.server 8766 --directory "%~dp0shared-adf-lab"
exit /b %ERRORLEVEL%
