@echo off
REM =============================================================================
REM FinLedger — open Databricks Genie 40h+ teach HTML
REM =============================================================================
REM HTML: docs\databricks-genie-agents-teach.html
REM MD  : docs\DATABRICKS-GENIE-AGENTS.md
REM Live lab (create+ask): genie-lab.cmd
REM Class Genie room:
REM   https://adb-7405613791235979.19.azuredatabricks.net/genie/rooms/01f185e11fc4152bad1eca7e4bbefa05
REM =============================================================================
setlocal EnableExtensions
cd /d "%~dp0"

echo.
echo ========================================================================
echo  DATABRICKS GENIE — 40h+ curriculum (why / what / how)
echo ========================================================================
echo  HTML : docs\databricks-genie-agents-teach.html
echo  MD   : docs\DATABRICKS-GENIE-AGENTS.md
echo  Lab  : genie-lab.cmd
echo.
echo  Workspace : https://adb-7405613791235979.19.azuredatabricks.net
echo  Genie     : https://adb-7405613791235979.19.azuredatabricks.net/genie
echo  Class room: https://adb-7405613791235979.19.azuredatabricks.net/genie/rooms/01f185e11fc4152bad1eca7e4bbefa05
echo ========================================================================
echo.

if not exist "%~dp0docs\databricks-genie-agents-teach.html" (
  echo ERROR: teach HTML missing.
  exit /b 1
)

start "" "%~dp0docs\databricks-genie-agents-teach.html"
exit /b 0
