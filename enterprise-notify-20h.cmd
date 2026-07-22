@echo off
REM Open the 20-hour enterprise notify curriculum (ADF + Databricks) in the default browser.
setlocal EnableExtensions
cd /d "%~dp0"
start "" "%~dp0docs\enterprise-notify-20h.html"
echo Opened docs\enterprise-notify-20h.html
echo Quick demo: docs\enterprise-jira-email-demo.html
echo Local proof: enterprise-notify.cmd
exit /b 0
