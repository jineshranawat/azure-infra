# Shared ADF Lab — Azure Data Factory (easy concepts, all pipelines as code)

**Goal:** One shared Data Factory (`adf-shared-*` in **eastus**) with **10 small pipelines** that teach every ADF building block — including **Databricks notebook triggers** and **Azure SQL in westus** (cross-region linked service).

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

## C. The 10 pipelines (what each teaches)

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

**Code that defines them:** `scripts/adf_pipelines.py` (read top to bottom — linked services, datasets, then each pipeline).

**Trainer deep-dive guide:** `docs/pipeline_code_walkthrough.md` — **full ADF Studio manual**: every linked service field, dataset property, activity setting, pipeline parameter, Trigger now JSON, and minute-by-minute class flow for all 10 pipelines.

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
3. `adf_pipelines.py` — linked services, datasets, 10 pipelines
4. Upload `audit/watermark.json`
5. Import Databricks notebook `nb_adf_hello`

### Trigger a pipeline manually

```text
cd shared-adf-lab
.\orchestrate.cmd --run-pipeline pl_07_databricks_notebook
```

Or in **ADF Studio** → Author → open pipeline → **Trigger now** → Monitor.

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
