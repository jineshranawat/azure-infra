@echo off
REM Session 2 — ADF orchestration & bronze ingestion (2-hour lab)
REM Prerequisite: repo root orchestrate.cmd (Class-1 + ADF deployed)
REM
REM   orchestrate.cmd                  deploy + upload + watermark + morning check
REM   orchestrate.cmd --run-pipeline   also trigger ADF copy activity
cd /d "%~dp0"

if exist "..\.venv\Scripts\python.exe" (
  "..\.venv\Scripts\python.exe" scripts\run_session2.py %*
  exit /b %ERRORLEVEL%
)

REM Bootstrap parent venv if missing
cd ..
call orchestrate.cmd --skip-setup
cd session-2
"..\.venv\Scripts\python.exe" scripts\run_session2.py %*
exit /b %ERRORLEVEL%
