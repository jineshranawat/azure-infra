@echo off
REM =============================================================================
REM FinLedger RELEASE HUB — check-in then incremental release (ADF / Databricks)
REM Doc: docs\CICD-INCREMENTAL-RELEASE.md
REM =============================================================================
REM   release.cmd                 Show menu
REM   release.cmd databricks      Notebooks only (no ADF SQL rebuild)
REM   release.cmd adf             ADF pipelines only (--skip-sql)
REM   release.cmd adf-full        ADF + SQL (rarer)
REM   release.cmd medallion       Full CI/CD: build + DBX + ADF + optional job
REM   release.cmd medallion-deploy  Same but --skip-run (safe classroom default)
REM   release.cmd verify          Print what to check in Portal / Studio
REM =============================================================================
setlocal EnableExtensions
cd /d "%~dp0"

set "ACTION=%~1"
if "%ACTION%"=="" goto menu
if /I "%ACTION%"=="help" goto menu
if /I "%ACTION%"=="--help" goto menu
if /I "%ACTION%"=="-h" goto menu
if /I "%ACTION%"=="databricks" goto rel_dbx
if /I "%ACTION%"=="dbx" goto rel_dbx
if /I "%ACTION%"=="adf" goto rel_adf
if /I "%ACTION%"=="adf-full" goto rel_adf_full
if /I "%ACTION%"=="medallion" goto rel_med
if /I "%ACTION%"=="medallion-deploy" goto rel_med_deploy
if /I "%ACTION%"=="verify" goto rel_verify
echo Unknown: %ACTION%
echo Run: release.cmd
exit /b 2

:menu
echo.
echo ========================================================================
echo  FINLEDGER CI/CD RELEASE HUB
echo  Guide: docs\CICD-INCREMENTAL-RELEASE.md
echo ========================================================================
echo.
echo  After git push, pick the SMALLEST release that matches your change:
echo.
echo    release.cmd databricks         Day 8/9 notebooks -^> Databricks
echo    release.cmd adf                ADF pipelines/LS (skip SQL)
echo    release.cmd adf-full           ADF + SQL westus (infra-ish)
echo    release.cmd medallion-deploy   Build + DBX + ADF, NO job run
echo    release.cmd medallion          Full: build + deploy + job
echo    release.cmd verify             Checklist (Portal / Studio)
echo.
echo  Typical student flow:
echo    1. git add / commit / push
echo    2. release.cmd medallion-deploy
echo    3. release.cmd verify
echo.
exit /b 0

:ensure_venv
if exist ".venv\Scripts\python.exe" goto :eof
python -m venv .venv
.venv\Scripts\python.exe -m pip install -q -r requirements.txt
goto :eof

:rel_dbx
call :ensure_venv
echo.
echo === RELEASE: Databricks notebooks (incremental overwrite) ===
echo.
call day9\orchestrate.cmd --deploy
if errorlevel 1 exit /b %ERRORLEVEL%
call deploy-shared-lab.cmd
echo.
echo Done. Open Databricks /Shared/day9/ and refresh.
exit /b %ERRORLEVEL%

:rel_adf
call :ensure_venv
echo.
echo === RELEASE: ADF only (incremental, --skip-sql) ===
echo.
call shared-adf-lab\orchestrate.cmd --skip-sql
echo.
echo Done. Open ADF Studio - pipelines updated in place.
exit /b %ERRORLEVEL%

:rel_adf_full
call :ensure_venv
echo.
echo === RELEASE: ADF + SQL (use when SQL linked service / DB changed) ===
echo.
call deploy-shared-adf-lab.cmd
echo.
echo Done.
exit /b %ERRORLEVEL%

:rel_med_deploy
call :ensure_venv
echo.
echo === RELEASE: medallion CI/CD deploy-only (--skip-run) ===
echo.
call release-medallion-governance.cmd --skip-run
exit /b %ERRORLEVEL%

:rel_med
call :ensure_venv
echo.
echo === RELEASE: medallion CI/CD FULL (includes job - costs DBUs) ===
echo.
call release-medallion-governance.cmd
exit /b %ERRORLEVEL%

:rel_verify
echo.
echo === VERIFY after release ===
echo.
echo Databricks:
echo   Workspace -^> /Shared/day9/  (notebooks overwritten)
echo   Jobs -^> finledger-medallion-governance (if medallion ran)
echo.
echo ADF:
echo   Studio -^> Author -^> Pipelines (pl_* / pl_gov_*)
echo   Monitor -^> Pipeline runs
echo.
echo Storage:
echo   Portal -^> stshared... -^> bronze / silver / gold / audit
echo.
echo Full checklist: docs\CICD-INCREMENTAL-RELEASE.md section E
echo.
exit /b 0

endlocal
