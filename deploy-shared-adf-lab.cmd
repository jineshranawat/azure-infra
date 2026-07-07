@echo off
REM Deploy shared ADF lab artefacts (SQL westus + pipelines + Databricks notebook)
setlocal
cd /d "%~dp0"
call shared-adf-lab\orchestrate.cmd %*
exit /b %ERRORLEVEL%
