@echo off
REM Day 9+ — Delta lakehouse notebooks (Sessions 9–15)
REM   orchestrate.cmd              build all 7 notebooks locally
REM   orchestrate.cmd --deploy     build + upload to shared Databricks
setlocal
cd /d "%~dp0"
set PYTHONUNBUFFERED=1

echo.
echo Day 9+ - Sessions 9-15 notebooks (silver, Delta, MERGE, gold, orchestration)
echo.

if not exist "..\.venv\Scripts\python.exe" (
  echo [Setup] Creating Python environment at repo root...
  cd ..
  python -m venv .venv
  if errorlevel 1 exit /b 1
  cd day9
)

"..\.venv\Scripts\python.exe" scripts\build_all_notebooks.py
if errorlevel 1 exit /b 1

if /i "%~1"=="--deploy" (
  cd ..
  call deploy-shared-lab.cmd
  exit /b %ERRORLEVEL%
)

echo.
echo Built notebooks in day9\notebooks\
echo Deploy: orchestrate.cmd --deploy   OR   deploy-shared-lab.cmd from repo root
exit /b 0
