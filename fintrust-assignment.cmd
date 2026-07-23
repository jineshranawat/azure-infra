@echo off
REM Open FinTrust retail banking DE capstone assignment (HTML)
setlocal EnableExtensions
cd /d "%~dp0"
echo FinTrust assignment: docs\retail-banking-de-assignment.html
echo Full MD           : docs\RETAIL-BANKING-DE-ASSIGNMENT.md
if not exist "%~dp0docs\retail-banking-de-assignment.html" (
  echo ERROR: HTML missing
  exit /b 1
)
start "" "%~dp0docs\retail-banking-de-assignment.html"
exit /b 0
