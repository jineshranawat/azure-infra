@echo off
REM =============================================================================
REM Open docs\palantirflow.html (RTT Command Centre) in the browser
REM Serves over localhost so React/Babel CDNs and the app load reliably.
REM =============================================================================
setlocal EnableExtensions
cd /d "%~dp0"

set "HTML=%~dp0docs\palantirflow.html"
if not exist "%HTML%" (
  echo ERROR: missing %HTML%
  exit /b 1
)

set "PY=.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"

echo.
echo ========================================================================
echo  Palantir / RTT Command Centre
echo  File : docs\palantirflow.html
echo  URL  : http://127.0.0.1:8765/palantirflow.html
echo ========================================================================
echo.
echo Starting local server on port 8765 (Ctrl+C to stop)...
echo.

start "" "http://127.0.0.1:8765/palantirflow.html"
"%PY%" -m http.server 8765 --directory "%~dp0docs"
exit /b %ERRORLEVEL%
