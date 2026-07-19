@echo off
REM Shared student estate via Terraform (twin of provision-shared.cmd / shared-eastus.bicep)
REM   provision-shared-tf.cmd
REM   provision-shared-tf.cmd --plan-only
REM   provision-shared-tf.cmd --auto-approve
REM Docs: docs\BICEP-TERRAFORM-SHARED-ESTATE.md
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [One-time setup] Creating Python environment...
  python -m venv .venv
  if errorlevel 1 (
    echo ERROR: Python 3.10+ required
    exit /b 1
  )
  ".venv\Scripts\python.exe" -m pip install --disable-pip-version-check -q -r requirements.txt
)

".venv\Scripts\python.exe" scripts\provision_shared_tf.py %*
exit /b %ERRORLEVEL%
