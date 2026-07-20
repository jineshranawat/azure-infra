@echo off
REM =============================================================================
REM FinLedger INFRA WALKTHROUGH - one entry that LINKS every child infra script
REM =============================================================================
REM Students: read docs\INFRA-WALKTHROUGH-20H.md first, then run phases with trainer.
REM Trainer:  infra-walkthrough.cmd --list
REM           infra-walkthrough.cmd --phase 0
REM           infra-walkthrough.cmd --phase 3
REM           infra-walkthrough.cmd --all-safe   (skips teardown / never deletes RG)
REM
REM NEVER points teardown at rg-shared-class1. Teardown is NOT offered here.
REM Re-run safe: every child uses ARM Incremental / overwrite / check-before-create.
REM =============================================================================
setlocal EnableExtensions
cd /d "%~dp0"

set "PY=.venv\Scripts\python.exe"
set "PHASE="
set "MODE=list"

:parse
if "%~1"=="" goto after_parse
if /I "%~1"=="--list"      set "MODE=list" & shift & goto parse
if /I "%~1"=="--help"      set "MODE=help" & shift & goto parse
if /I "%~1"=="-h"          set "MODE=help" & shift & goto parse
if /I "%~1"=="--all-safe"  set "MODE=allsafe" & shift & goto parse
if /I "%~1"=="--phase" (
  set "MODE=phase"
  set "PHASE=%~2"
  shift & shift & goto parse
)
echo Unknown argument: %~1
echo Run: infra-walkthrough.cmd --help
exit /b 2

:after_parse

if /I "%MODE%"=="help" goto show_help
if /I "%MODE%"=="list" goto show_list
if /I "%MODE%"=="allsafe" goto run_all_safe
if /I "%MODE%"=="phase" goto run_phase
goto show_list

:show_help
echo.
echo FinLedger Infra Walkthrough - links ALL child infra / DevOps scripts
echo Doc: docs\INFRA-WALKTHROUGH-20H.md
echo.
echo Usage:
echo   infra-walkthrough.cmd --list          Show all phases + child commands
echo   infra-walkthrough.cmd --phase N       Run one phase (0-12, or 1t = Terraform)
echo   infra-walkthrough.cmd --all-safe      Run phases 0-11 in order (NO teardown)
echo   infra-walkthrough.cmd --help
echo.
echo Phase cheat sheet:
echo   0  Bootstrap (venv + Azure CLI hint)
echo   1  Shared infra Bicep          - provision-shared.cmd
echo  1t  Shared infra Terraform      - provision-shared-tf.cmd  (twin of Bicep)
echo   2  Lab assets to Databricks    - deploy-shared-lab.cmd
echo   3  Shared ADF + SQL            - deploy-shared-adf-lab.cmd
echo   4  ADF to Databricks link      - shared-adf-lab\orchestrate.cmd --setup-databricks-integration
echo   5  Day 6 Python DE             - day6\orchestrate.cmd
echo   6  Day 7 Storage/lake read     - day7\orchestrate.cmd
echo   7  Day 8 PySpark               - day8\orchestrate.cmd
echo   8  Day 9 build+deploy          - day9\orchestrate.cmd --deploy
echo   9  Medallion CI/CD release     - release-medallion-governance.cmd
echo  10  Cost dashboard (laptop)     - cost-dashboard.cmd
echo  11  Optional labs (EH/cost NB)  - python deploy_* helpers
echo  12  Per-learner Class-1 (alt)   - orchestrate.cmd  [personal RG only]
echo.
echo Incremental release story: docs\INFRA-WALKTHROUGH-20H.md section D
exit /b 0

:show_list
echo.
echo ========================================================================
echo  FINLEDGER INFRA WALKTHROUGH - phase map (child scripts)
echo  Full teaching text: docs\INFRA-WALKTHROUGH-20H.md
echo ========================================================================
echo.
echo  [0]  Bootstrap PC
echo       Child:  .venv + pip  (auto) ; az login reminder
echo       Analogy: unlock the toolbox before building the house
echo.
echo  [1]  Shared Azure estate (eastus) — BICEP
echo       Child:  provision-shared.cmd
echo               - scripts\provision_shared.py
echo               - infra\shared-eastus.bicep
echo       Creates: rg-shared-class1, KV, ADLS, ADF, Databricks
echo.
echo  [1t] Shared Azure estate (eastus) — TERRAFORM twin (same case)
echo       Child:  provision-shared-tf.cmd
echo               - scripts\provision_shared_tf.py
echo               - infra\terraform\shared-eastus\main.tf
echo       Docs:   docs\BICEP-TERRAFORM-SHARED-ESTATE.md
echo       Tip:    --plan-only first; name_hash=qgr7mj matches live Bicep names
echo.
echo  [2]  Put lab assets into Databricks
echo       Child:  deploy-shared-lab.cmd
echo               - scripts\deploy_shared_lab.py
echo       Does: bronze CSV, finledger secrets, Day 8/9 notebooks
echo.
echo  [3]  Shared ADF lab + SQL (westus)
echo       Child:  deploy-shared-adf-lab.cmd
echo               - shared-adf-lab\orchestrate.cmd
echo       Does: pipelines, linked services, SQL Basic finledger
echo.
echo  [4]  ADF calls Databricks (integration)
echo       Child:  shared-adf-lab\orchestrate.cmd --setup-databricks-integration
echo.
echo  [5]  Day 6 - Python for DE
echo       Child:  day6\orchestrate.cmd
echo.
echo  [6]  Day 7 - Storage + lake read
echo       Child:  day7\orchestrate.cmd
echo.
echo  [7]  Day 8 - PySpark transforms
echo       Child:  day8\orchestrate.cmd
echo.
echo  [8]  Day 9 - lakehouse write layer notebooks
echo       Child:  day9\orchestrate.cmd --deploy
echo.
echo  [9]  DevOps release - medallion + Purview governance
echo       Child:  release-medallion-governance.cmd
echo               - scripts\release_medallion_governance.py
echo       Incremental: build - deploy notebooks - ADF --skip-sql - job
echo.
echo [10]  Cost / ops visibility
echo       Child:  cost-dashboard.cmd --open
echo               python scripts\ensure_cost_lab_secrets.py
echo               python scripts\deploy_cost_management_lab.py
echo.
echo [11]  Optional stretch labs
echo       Child:  python scripts\ensure_eventhub_lab.py
echo               python scripts\deploy_eventhub_finledger_lab.py
echo               python scripts\deploy_system_tables_lab.py
echo               python scripts\deploy_perf_databricks_adf_lab.py
echo               python scripts\deploy_spark_dag_problems_lab.py
echo               run-50-problems.cmd
echo.
echo [12]  ALTERNATE - personal Class-1 RG (uksouth) - NOT shared estate
echo       Child:  orchestrate.cmd
echo       WARNING: do NOT teardown rg-shared-class1
echo.
echo ========================================================================
echo  Run one phase:  infra-walkthrough.cmd --phase 1
echo  Run safe path:  infra-walkthrough.cmd --all-safe
echo ========================================================================
exit /b 0

:ensure_venv
if exist "%PY%" goto :eof
echo [Phase 0] Creating .venv ...
python -m venv .venv
if errorlevel 1 (
  echo ERROR: Python 3.10+ required from python.org
  exit /b 1
)
"%PY%" -m pip install --disable-pip-version-check -q -r requirements.txt
goto :eof

:banner
echo.
echo ------------------------------------------------------------------------
echo  PHASE %~1 - %~2
echo  Doc: docs\INFRA-WALKTHROUGH-20H.md
echo ------------------------------------------------------------------------
echo.
goto :eof

:run_phase
call :ensure_venv
if errorlevel 1 exit /b 1
if "%PHASE%"=="0" goto p0
if "%PHASE%"=="1" goto p1
if /I "%PHASE%"=="1t" goto p1t
if "%PHASE%"=="2" goto p2
if "%PHASE%"=="3" goto p3
if "%PHASE%"=="4" goto p4
if "%PHASE%"=="5" goto p5
if "%PHASE%"=="6" goto p6
if "%PHASE%"=="7" goto p7
if "%PHASE%"=="8" goto p8
if "%PHASE%"=="9" goto p9
if "%PHASE%"=="10" goto p10
if "%PHASE%"=="11" goto p11
if "%PHASE%"=="12" goto p12
echo Unknown phase "%PHASE%". Use 0-12 or 1t. Run --list
exit /b 2

:run_all_safe
call :ensure_venv
if errorlevel 1 exit /b 1
echo.
echo === ALL-SAFE: phases 0-11 (NO personal Class-1, NO teardown) ===
echo Trainer should pause between phases for teaching.
echo.
call :p0
if errorlevel 1 exit /b 1
call :p1
if errorlevel 1 exit /b 1
call :p2
if errorlevel 1 exit /b 1
call :p3
if errorlevel 1 exit /b 1
call :p4
if errorlevel 1 exit /b 1
call :p5
if errorlevel 1 exit /b 1
call :p6
if errorlevel 1 exit /b 1
call :p7
if errorlevel 1 exit /b 1
call :p8
if errorlevel 1 exit /b 1
call :p9
if errorlevel 1 exit /b 1
call :p10
if errorlevel 1 exit /b 1
call :p11
echo.
echo === ALL-SAFE COMPLETE ===
echo Next teaching: open docs\INFRA-WALKTHROUGH-20H.md section E (portal verify)
exit /b 0

:p0
call :banner 0 "Bootstrap - unlock the toolbox"
echo Reminder: az login  (or SPN in .env)
echo Reminder: .env must have AZURE_SUBSCRIPTION_ID, DATABRICKS_HOST/TOKEN for later phases
where az >nul 2>&1
if errorlevel 1 (
  echo Azure CLI not found - run: orchestrate.cmd --install-cli
) else (
  echo Azure CLI: OK
)
"%PY%" -c "import azure.identity,sys; print('Python SDK: OK', sys.version.split()[0])"
exit /b %ERRORLEVEL%

:p1
call :banner 1 "Shared estate Bicep - build the house frame"
REM OWNER_EMAIL / CLASS_OWNER_EMAIL are loaded from .env by provision_shared.py
call provision-shared.cmd
exit /b %ERRORLEVEL%

:p1t
call :banner 1t "Shared estate Terraform twin - same house, HashiCorp blueprint"
echo Docs: docs\BICEP-TERRAFORM-SHARED-ESTATE.md
echo Plan first (safe): provision-shared-tf.cmd --plan-only
call provision-shared-tf.cmd --auto-approve
exit /b %ERRORLEVEL%

:p2
call :banner 2 "Deploy lab assets - furniture into the house"
echo Auth: uses DATABRICKS_TOKEN from .env if valid; otherwise Azure AD token from az login.
echo Host/storage key auto-detected when omitted. Invalid/expired PAT falls back automatically.
call deploy-shared-lab.cmd
exit /b %ERRORLEVEL%

:p3
call :banner 3 "Shared ADF + SQL - hire the kitchen manager + ledger"
call deploy-shared-adf-lab.cmd
exit /b %ERRORLEVEL%

:p4
call :banner 4 "ADF to Databricks integration - manager calls the chefs"
call shared-adf-lab\orchestrate.cmd --setup-databricks-integration
exit /b %ERRORLEVEL%

:p5
call :banner 5 "Day 6 - Python knife skills"
call day6\orchestrate.cmd
exit /b %ERRORLEVEL%

:p6
call :banner 6 "Day 7 - Storage walk-in fridge + read path"
call day7\orchestrate.cmd
exit /b %ERRORLEVEL%

:p7
call :banner 7 "Day 8 - PySpark cooking techniques"
call day8\orchestrate.cmd
exit /b %ERRORLEVEL%

:p8
call :banner 8 "Day 9 - lakehouse write layer notebooks"
call day9\orchestrate.cmd --deploy
exit /b %ERRORLEVEL%

:p9
call :banner 9 "DevOps release - incremental medallion + governance"
echo Classroom default: --skip-run (deploy notebooks + ADF; no DBU job submit).
echo To also submit the Databricks job: release-medallion-governance.cmd
echo Other flags: --skip-deploy  --run-only
call release-medallion-governance.cmd --skip-run
exit /b %ERRORLEVEL%

:p10
call :banner 10 "Cost and ops - read the utility meters"
call cost-dashboard.cmd
if exist "%PY%" (
  echo Ensuring finledger cost secrets + deploying cost notebook...
  "%PY%" scripts\ensure_cost_lab_secrets.py
  if errorlevel 1 echo WARN ensure_cost_lab_secrets failed — continuing ^(dashboard already OK^)
  "%PY%" scripts\deploy_cost_management_lab.py
  if errorlevel 1 echo WARN deploy_cost_management_lab failed — open docs\cost-dashboard-out\index.html
)
exit /b 0

:p11
call :banner 11 "Optional stretch labs (soft - continue on failure)"
echo Event Hub lab...
"%PY%" scripts\ensure_eventhub_lab.py
if errorlevel 1 echo SKIP/WARN ensure_eventhub_lab
"%PY%" scripts\deploy_eventhub_finledger_lab.py
if errorlevel 1 echo SKIP/WARN deploy_eventhub
echo System tables notebook (needs UC grants)...
"%PY%" scripts\deploy_system_tables_lab.py
if errorlevel 1 echo SKIP/WARN system_tables
echo Perf + DAG labs...
"%PY%" scripts\deploy_perf_databricks_adf_lab.py
if errorlevel 1 echo SKIP/WARN perf lab
"%PY%" scripts\deploy_spark_dag_problems_lab.py
if errorlevel 1 echo SKIP/WARN dag lab
echo.
echo Optional: run-50-problems.cmd  (submits cluster job - costs DBUs)
exit /b 0

:p12
call :banner 12 "ALTERNATE personal Class-1 (uksouth) - separate estate"
echo This creates rg-LESSONS learner RG - NOT the shared class estate.
echo Press Ctrl+C to cancel, or continue...
timeout /t 8
call orchestrate.cmd
exit /b %ERRORLEVEL%

endlocal
