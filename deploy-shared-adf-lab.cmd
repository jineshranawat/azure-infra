@echo off
REM Deploy shared ADF lab artefacts (SQL westus + pipelines + Databricks notebook)
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
  echo Ensuring azure-mgmt-datafactory 9.3.0 for phase 3 / ADF lab...
  ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check --force-reinstall -q "azure-mgmt-datafactory==9.3.0"
)

call shared-adf-lab\orchestrate.cmd %*
exit /b %ERRORLEVEL%
