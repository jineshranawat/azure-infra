# Shared ADF Lab — Azure Data Factory (easy concepts, all pipelines as code)

**Goal:** One shared Data Factory (`adf-shared-*` in **eastus**) with **12 small pipelines** that teach every ADF building block — including **Databricks notebook triggers**, **Azure SQL in westus** (cross-region linked service), and **SQL metadata-driven orchestration**.

**Single entry point (Windows):**

```text
cd d:\azure\shared-adf-lab
.\orchestrate.cmd
```

PowerShell requires `.\` before local scripts (not plain `orchestrate.cmd`).

Or from repo root:

```text
deploy-shared-adf-lab.cmd
```

---

## A. WHAT & WHY — components in plain language

### Azure Data Factory (ADF)

**What:** ADF is the **orchestrator** — a cloud scheduler that runs activities (copy files, call Databricks, read SQL) on a timetable or when you click **Trigger now**.

**Why:** FinLedger nightly loads should not depend on someone clicking Run in a notebook. ADF chains steps, passes parameters (`run_id`), and shows success/failure in one **Monitor** blade.

**Analogy:** ADF is the **conductor**; Databricks notebooks are the **musicians**; ADLS is the **sheet music storage**.

### Linked service

**What:** A **saved connection** — “how ADF talks to storage / SQL / Databricks”.

**Why:** You define credentials once; every pipeline activity reuses the name (e.g. `ls_adls_finledger`).

### Dataset

**What:** A **pointer to data** — not the data itself. “CSV in `bronze/incoming/...`” or “table `dbo.dim_channel`”.

**Why:** Copy activities need source + sink **datasets**; changing path means editing the dataset, not every pipeline.

### Pipeline

**What:** A **recipe** — ordered **activities** (Copy, If, ForEach, Databricks Notebook, …).

**Why:** One pipeline = one business process (e.g. “copy bronze, then run silver notebook”).

### Integration Runtime (IR)

**What:** The **engine** that executes activities. Default **Azure IR** runs in Azure’s network (no VM to manage).

**Why:** Copy from SQL westus → ADLS eastus uses Azure IR; self-hosted IR is only for on-prem (not this lab).

### Azure SQL (westus) — region exception

**What:** A small **Azure SQL Database** (`finledger`, **Basic** tier) in **westus**.

**Why we use westus:** Teaches **cross-region** ADF patterns (SQL in westus, lake in eastus) — common in global banks. Documented exception to the UK-only learner estate rule; **shared class only**.

**Cost:** Basic tier + auto-pause not on Basic — keep lab short; tear down when course ends.

### Databricks notebook activity

**What:** ADF starts a **job cluster**, runs a notebook path (e.g. `/Shared/shared-adf/nb_adf_hello`), passes **base parameters**, returns when notebook exits.

**Why:** Transformations stay in PySpark; ADF only **schedules** them.

---

## B. Architecture (shared class)

```text
                    ┌─────────────────────────────────────┐
                    │  Azure Data Factory (eastus)        │
                    │  adf-shared-{hash}                  │
                    └──────────┬──────────────────────────┘
           Copy / Metadata    │    Databricks Notebook
                    ┌──────────┴──────────┐
                    ▼                     ▼
         ┌──────────────────┐   ┌─────────────────────┐
         │ ADLS Gen2        │   │ Databricks          │
         │ stshared{hash}   │   │ dbw-shared-{hash}   │
         │ eastus           │   │ eastus              │
         │ bronze/silver/   │   │ nb_adf_hello        │
         │ gold/audit       │   └─────────────────────┘
         └────────┬─────────┘
                  │ Copy SQL ↔ lake
                  ▼
         ┌──────────────────┐
         │ Azure SQL          │
         │ sql-shared-{hash}  │
         │ westus · Basic     │
         └──────────────────┘
```

---

## C. The 12 pipelines (what each teaches)

| Pipeline | ADF concept | What it does |
|----------|-------------|--------------|
| `pl_01_bronze_copy` | **Copy** activity | `bronze/incoming` → `bronze/loaded` (CSV) |
| `pl_02_get_metadata` | **Get Metadata** | List files in incoming folder |
| `pl_03_if_then_copy` | **If Condition** | Copy only if folder has files |
| `pl_04_foreach_dates` | **ForEach** | Loop over array of run dates |
| `pl_05_lookup_watermark` | **Lookup** + **Set Variable** | Read `audit/watermark.json` |
| `pl_06_wait_then_copy` | **Wait** | Pause 5s then copy (throttle pattern) |
| `pl_07_databricks_notebook` | **Databricks Notebook** | Run `nb_adf_hello` with `run_id` |
| `pl_08_sql_to_lake` | **Copy** SQL → ADLS | Export `dim_channel` to `audit/sql_export/` |
| `pl_09_lake_to_sql` | **Copy** ADLS → SQL | Load CSV into `stg_channel_from_lake` |
| `pl_10_parent_chain` | **Execute Pipeline** | Child copy → then Databricks notebook |
| `pl_11_filename_validate_route` | **Filter** + **If Condition** | Validate file name in incoming → copy + Databricks, or reject |
| `pl_12_sql_metadata_orchestrate` | **Lookup (SQL)** + **If Condition** + **Stored Procedure** | Read `dbo.adf_job_metadata` → copy + Databricks if READY; else SKIPPED; write status back to SQL |

### Advanced pipelines (`pl_13`–`pl_26`) — see `docs/adf_folders_dataflows_advanced_catalog.md`

| Pipeline | Concept |
|----------|---------|
| `pl_13_df_bronze_cleanse` | Execute Mapping Data Flow (derive + select → silver) |
| `pl_14_df_channel_aggregate` | Execute Data Flow (aggregate by channel) |
| `pl_15_delete_rejected` | Delete activity |
| `pl_16_web_ping` | Web activity (REST) |
| `pl_17_switch_channel` | Switch activity |
| `pl_18_until_file_ready` | Until loop (poll folder) |
| `pl_19_append_audit_log` | Append Variable |
| `pl_20_validate_incoming` | Validation activity |
| `pl_21_fail_on_bad_param` | Fail activity |
| `pl_22_sql_script_count` | SQL Script activity |

### Incremental CDC & Power Query (`pl_23`–`pl_26`) — see `docs/adf_folders_dataflows_advanced_catalog.md` Part G

| Pipeline | Concept |
|----------|---------|
| `pl_23_incremental_watermark_copy` | **Incremental copy** — Lookup `cdc_watermark.json`, copy only files modified after watermark |
| `pl_24_sql_change_tracking_copy` | **SQL CDC** — change tracking + `CHANGETABLE` → `audit/sql_cdc_export/` |
| `pl_25_power_query_returns` | **Power Query parity** — mapping flow `df_03` aggregates returns by channel (Studio: `pq_01`) |
| `pl_26_join_returns_txn` | **Join** data flow — returns inner-joined to bronze transactions |

### Databricks integration lab (`pl_db_01`–`pl_db_10`) — see `docs/databricks_integration_catalog.md`

Ten pipelines in folder **`11-expression-builder`** (shared with all expression labs) teaching **expression builder** + **Databricks Notebook** patterns (no data flows):

| Pipeline | Teaches |
|----------|---------|
| `pl_db_01_static_notebook` | Static base parameters |
| `pl_db_02_pipeline_param_expressions` | `@pipeline().parameters.*`, `@concat` |
| `pl_db_03_system_expressions` | `@pipeline().RunId`, `@utcnow()`, `@guid()` |
| `pl_db_04_metadata_to_notebook` | `@activity(...).output` from Get Metadata |
| `pl_db_05_copy_then_notebook` | Copy → notebook on lake path |
| `pl_db_06_if_then_notebook` | If Condition gates notebook |
| `pl_db_07_foreach_channels` | ForEach + `@item()` per channel |
| `pl_db_08_lookup_watermark_notebook` | Lookup JSON → notebook params |
| `pl_db_09_wait_then_notebook` | Wait then notebook (cluster pattern) |
| `pl_db_10_child_pipeline_notebook` | Execute Pipeline → child notebook |

**Trainer guide (UI + expressions):** `docs/databricks_integration_catalog.md`

### Expression builder lab (`pl_ex_01`–`pl_ex_11`) — see `docs/expression_builder_catalog.md`

**All expression + Databricks expression pipelines live in ADF folder `11-expression-builder`.**

| Pipeline | Teaches |
|----------|---------|
| `pl_ex_01_pipeline_param_variables` | `@pipeline().parameters.*` |
| `pl_ex_02_system_context_variables` | `@pipeline().RunId`, `@utcnow()`, `@guid()` |
| `pl_ex_03_concat_string_functions` | `@concat`, `@toUpper`, `@variables` |
| `pl_ex_04_metadata_output_expressions` | `@activity().output`, `@length`, `@coalesce` |
| `pl_ex_05_lookup_json_expressions` | Lookup `firstRow` |
| `pl_ex_06_if_equals_compare` | `@equals` + If Condition |
| `pl_ex_07_foreach_item_expressions` | `@item()` + Append Variable |
| `pl_ex_08_switch_route_expressions` | Switch on parameter |
| `pl_ex_09_logic_and_or_not` | `@and`, `@not`, `@greater` |
| `pl_ex_10_child_pipeline_expressions` | Execute Pipeline + expression child params |
| `pl_ex_11_debug_print_variables` | Print/debug globals + params + variables (Monitor) |
| `pl_ex_12_dataset_foreach_item` | `@dataset().folder_path` + `@item().incoming` in ForEach |
| `pl_ex_13_user_properties_monitor` | User properties on activities for Monitor filtering |

**User properties guide:** `docs/adf_user_properties_guide.md`

**Debug print step-by-step:** `docs/debug_print_variables_guide.md`  
**Dataset + ForEach `@item()` guide:** `docs/dataset_foreach_item_guide.md`

**Master expression guide (all concepts + UI steps):** `docs/expression_builder_catalog.md`

### Trigger lab (`pl_trig_01`–`pl_trig_02`) — `docs/adf_triggers_guide.md`

**ADF folder:** `12-triggers` | **Code:** `scripts/adf_triggers.py`

| Pipeline | Trigger | Type | FinLedger scenario |
|----------|---------|------|-------------------|
| `pl_trig_01_tumbling_window_bronze` | `tr_tumbling_hourly_bronze` | **Tumbling window** (hourly) | Batch copy files modified in each hour window |
| `pl_trig_02_storage_event_copy` | `tr_storage_bronze_incoming_csv` | **Storage event** (BlobCreated) | Copy when partner drops `.csv` in incoming |

Triggers deploy **Stopped** — trainer **Start** in ADF Studio → **Publish** before demo.

### Complete ADF + Databricks integration — `docs/adf_databricks_complete_integration.md`

**Folder:** `04-integration-databricks-complete` | **Infra:** `scripts/databricks_infra.py`

| Pipeline | Pattern |
|----------|---------|
| `pl_db_11_databricks_job_preview` | **Databricks Job activity (Preview)** |
| `pl_db_12_end_to_end_integration` | Copy → master notebook → Delta silver |
| `pl_db_13_copy_notebook_job_chain` | Copy → notebook → Job (Preview) chain |

**One-time bootstrap:**

```cmd
cd shared-adf-lab
.\orchestrate.cmd --setup-databricks-integration
```

Pipelines are organized in ADF folders `01-core-copy` … `12-triggers` (deployed automatically).

**Code that defines them:** `scripts/adf_pipelines.py` (read top to bottom — linked services, datasets, then each pipeline).

**Trainer deep-dive guide:** `docs/pipeline_code_walkthrough.md` — **full ADF Studio manual** for pipelines `pl_01`–`pl_12`.

**Folders, data flows & advanced activities:** `docs/adf_folders_dataflows_advanced_catalog.md` — where to create folders in Studio, `pl_13`–`pl_26`, mapping + Power Query data flows, incremental CDC, and **12×12 scenario catalog**.

---

## D. Quick start (one command)

### Prerequisites

1. `az login`
2. `.env` at repo root:

```env
LEARNER=shared
AZURE_SUBSCRIPTION_ID=a64c0dd2-3a31-4604-bde3-3d40c7d5e8be
OWNER_EMAIL=you@example.com
DATABRICKS_HOST=https://adb-XXXX.azuredatabricks.net
DATABRICKS_TOKEN=dapiXXXXXXXX
DATABRICKS_CLUSTER_ID=0703-105931-31juyffm
```

3. Shared estate deployed: `provision-shared.cmd`

### Deploy (re-run command)

```text
deploy-shared-adf-lab.cmd
```

This runs:

1. `azure_sql.py` — deploy SQL westus (Bicep `infra/shared-sql-westus.bicep`)
2. `adf_rbac.py` — ADF managed identity → storage + Databricks
3. `adf_pipelines.py` — linked services, datasets, core pipelines
4. `adf_advanced.py` + `adf_databricks.py` — advanced + Databricks teaching pipelines
5. Upload bronze samples + `audit/watermark.json`
6. Import Databricks notebooks (`nb_adf_hello`, `nb_adf_params_echo`, `nb_adf_bronze_stats`)

### Trigger a pipeline manually

```text
cd shared-adf-lab
.\orchestrate.cmd --run-pipeline pl_07_databricks_notebook
```

```cmd
cd shared-adf-lab
.\orchestrate.cmd --warm-cluster-only
.\orchestrate.cmd --run-pipeline pl_db_01_static_notebook
```

```cmd
cd shared-adf-lab
.\orchestrate.cmd --run-pipeline pl_12_sql_metadata_orchestrate
```

In **ADF Studio** → Trigger now → set `job_id` = `job-02` to demo the skip path (`HOLD` → SQL status `SKIPPED`).

Or in **ADF Studio** → Author → open pipeline → **Trigger now** → Monitor.

### Students cloning the repo

1. `az login` and create `.env` from `.env.example` (`LEARNER=shared`, subscription, Databricks vars).
2. From repo root: `provision-shared.cmd` (shared eastus estate).
3. `cd shared-adf-lab` → `.\orchestrate.cmd` (**full deploy** — seeds SQL + publishes all pipelines).
4. Trigger `pl_12` only after step 3. Using `--run-only` before a deploy leaves **stale** pipeline JSON in ADF and causes SQL error **111** on `pl_12`.

---

## E. Step-by-step — what the code does

### Step 1 — Config (`scripts/_config.py`)

- Loads `.env`; enforces `LEARNER=shared`
- `ADF_LOCATION=eastus`, `SQL_LOCATION=westus` (documented exception)

### Step 2 — Discover (`scripts/discover.py`)

- Lists `rg-shared-class1` → finds `stshared*`, `adf-shared-*`, `dbw-shared-*`, `kv-shared-*`

### Step 3 — SQL westus (`scripts/azure_sql.py` + `infra/shared-sql-westus.bicep`)

| Bicep resource | Why |
|----------------|-----|
| `Microsoft.Sql/servers` | Logical server in **westus** |
| `firewallRules AllowAllAzureIPs` | Lets ADF Azure IR connect |
| `Microsoft.Sql/servers/databases` | `finledger` database, **Basic** SKU |
| Key Vault secret `sql-admin-password` | Password not in git |

### Step 4 — RBAC (`scripts/adf_rbac.py`)

- ADF **system-assigned managed identity** gets:
  - **Storage Blob Data Contributor** on `stshared*`
  - **Contributor** on Databricks workspace (for notebook activities)

### Step 5 — Linked services (`adf_pipelines.py` → `deploy_linked_services`)

| Name | Type | Points to |
|------|------|-----------|
| `ls_adls_finledger` | AzureBlobFS | `https://stshared*.dfs.core.windows.net` |
| `ls_databricks_shared` | AzureDatabricks | Existing shared all-purpose cluster (`DATABRICKS_CLUSTER_ID`) |
| `ls_azure_sql_westus` | AzureSqlDatabase | `sql-shared-*.database.windows.net` |

### Step 6 — Pipelines (`adf_pipelines.py` → `deploy_all_pipelines`)

Each `PipelineResource` is **create_or_update** — safe to re-run.

### Step 7 — Databricks hello notebook (`notebooks/nb_adf_hello.py`)

- Reads widgets `run_id`, `triggered_by`
- Prints proof line for learners
- `dbutils.notebook.exit(JSON)` — ADF sees success

---

## F. Manual verification (ADF Studio)

1. Open URL printed at end of deploy (or Portal → Data factories → `adf-shared-*` → **Open Studio**).
2. **Manage** → Linked services → confirm 3 linked services.
3. **Author** → filter pipelines `pl_0*` → open `pl_01_bronze_copy` → **Trigger now**:
   - `incoming_folder` = `incoming/run=session3-lab`
   - `loaded_folder` = `loaded/run=session3-lab`
4. **Monitor** → wait for **Succeeded**.
5. Repeat for `pl_07_databricks_notebook` (`run_id` = `session3-lab`) — allow 5–15 min for cluster start.

---

## G. Idempotency — four re-run scenarios

| Scenario | Safe? | What happens |
|----------|-------|--------------|
| Fresh machine, estate not provisioned | Run `provision-shared.cmd` first | Bicep creates RG + ADF + storage |
| Re-run after full success | **Yes** | `orchestrate.cmd` logs "already present — skip" |
| Re-run after partial failure | **Yes** | Incremental Bicep + SDK create_or_update resume |
| After teardown | Re-run `provision-shared.cmd` then `deploy-shared-adf-lab.cmd` | Recreates estate |

**Re-run command:** `deploy-shared-adf-lab.cmd`

---

## H. Teardown (optional)

```text
az group delete -n rg-shared-class1 --yes --no-wait
```

Removes ADF, storage, Databricks, SQL westus, Key Vault in one RG.

---

## I. File map

| File | Purpose |
|------|---------|
| `orchestrate.cmd` | Windows entry point |
| `scripts/run_shared_adf.py` | Orchestrates all steps |
| `scripts/adf_pipelines.py` | **All pipeline definitions** |
| `scripts/azure_sql.py` | SQL westus deploy + seed |
| `infra/shared-sql-westus.bicep` | SQL server + Basic DB |
| `notebooks/nb_adf_hello.py` | Databricks target for ADF |

---

## J. Troubleshooting

| Error | Fix |
|-------|-----|
| `LEARNER=shared` required | Set in `.env` |
| ADF copy 403 | Re-run deploy (RBAC); wait 2 min for IAM |
| Databricks activity failed (`Error code 3204`, identity lacks permission) | Cause: ADF managed identity can trigger ARM but cannot run notebook ACL in Databricks workspace. Fix: set `DATABRICKS_HOST` + `DATABRICKS_TOKEN` in `.env`, then re-run `deploy-shared-adf-lab.cmd` so linked service uses PAT token. |
| Databricks `SkuNotAvailable` / `CLOUD_PROVIDER_RESOURCE_STOCKOUT` | ADF was creating a new `Standard_DS3_v2` job cluster. Fix: set `DATABRICKS_CLUSTER_ID` to the shared all-purpose cluster (`0703-105931-31juyffm`) and re-run deploy. Orchestrator auto-starts the cluster before notebook pipelines. |
| SQL firewall | Bicep adds `AllowAllAzureIPs`; re-deploy SQL bicep |
| `pyodbc` seed skipped | Optional: `pip install pyodbc` + ODBC Driver 18 |

---

## K. Run all pipelines until success

Use this when you want automatic pass/fail checks across all concept pipelines:

```text
cd d:\azure\shared-adf-lab
.\orchestrate.cmd --run-all-until-success --max-rounds 3
```

What it does:

1. Triggers all pipelines (or all non-SQL pipelines with `--skip-sql`)
2. Polls ADF run status (`Succeeded` / `Failed` / `Cancelled`)
3. Retries only failed pipelines in next round
4. Stops when all succeed or max rounds reached

---

## L. Relation to course syllabus

- **Session 19** — ADF orchestration overview  
- **Sessions 20–21** — 50 Databricks problems  
- **This lab** — hands-on ADF concepts + Databricks + SQL cross-region  

See also: `docs/data-engineering-course.html` → section **50 Problems lab**.
