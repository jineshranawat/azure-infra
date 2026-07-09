# Shared ADF Lab — Complete Pipeline Manual (ADF Studio + Code)

**Purpose:** Anyone reading this document should be able to **build all 10 pipelines manually in ADF Studio** — every linked service field, dataset property, activity setting, pipeline parameter, expression, and **Trigger now** value is listed here.

**Code source of truth:** `shared-adf-lab/scripts/adf_pipelines.py`  
**Deploy command:** `deploy-shared-adf-lab.cmd` or `shared-adf-lab\orchestrate.cmd`

---

## How to use this document

| Section | What you get |
|---------|----------------|
| **Part 0** | Shared estate names (replace `qgr7mj` with your hash if different) |
| **Part 1** | Linked services — every field in the ADF UI |
| **Part 2** | Datasets — every path, parameter, format option |
| **Part 3** | Pipelines 01–10 — activity-by-activity manual steps |
| **Part 4** | Trigger-now parameter JSON for each pipeline |
| **Part 5** | ADF expression cheat sheet |
| **Part 6** | Classroom minute-by-minute flow |

**ADF Studio navigation (same for every object):**

1. Portal → Data factories → `adf-shared-qgr7mj` → **Open Studio**
2. **Manage** (toolbox icon) → Linked services / Datasets
3. **Author** (pencil) → Pipelines → `+` New pipeline
4. **Monitor** → Pipeline runs → click run → **Output** / **Error** for debugging

---

## Part 0 — Shared estate (concrete values for this class)

| Object | Value |
|--------|--------|
| Resource group | `rg-shared-class1` |
| Region (ADF + lake + Databricks) | `eastus` |
| Data Factory | `adf-shared-qgr7mj` |
| Storage account | `stsharedqgr7mj` |
| ADLS DFS URL | `https://stsharedqgr7mj.dfs.core.windows.net` |
| Databricks workspace | `dbw-shared-qgr7mj` |
| Azure SQL server | `sql-shared-qgr7mj.database.windows.net` |
| SQL database | `finledger` |
| SQL region | `westus` (documented exception) |
| Shared Databricks cluster ID | `0703-105931-31juyffm` |
| Notebook path | `/Shared/shared-adf/nb_adf_hello` |
| Bronze incoming folder | `incoming/run=session3-lab` |
| Bronze loaded folder | `loaded/run=session3-lab` |
| Sample file name | `sample_transactions.csv` |

**Integration Runtime:** Default **Auto Resolve Integration Runtime** (Azure IR) — no self-hosted IR in this lab.

---

## Part 1 — Linked services (create in Manage → Linked services)

Create these **before** datasets and pipelines. Test connection after each save.

---

### 1.1 `ls_adls_finledger` — Azure Data Lake Storage Gen2

| ADF UI field | Value | Why |
|--------------|-------|-----|
| **Name** | `ls_adls_finledger` | Referenced by all lake datasets |
| **Type** | Azure Data Lake Storage Gen2 | Hierarchical namespace (real folders) |
| **Connect via** | AutoResolveIntegrationRuntime | Runs in Azure network |
| **Authentication** | **Managed identity** | ADF system-assigned identity; no keys in ADF |
| **Account selection method** | From Azure subscription | Pick from dropdown |
| **Storage account name** | `stsharedqgr7mj` | Shared class lake |
| **URL** (if shown) | `https://stsharedqgr7mj.dfs.core.windows.net` | Must be `.dfs.` not `.blob.` |
| **Description** | `ADLS Gen2 — shared FinLedger lake (eastus)` | Optional |

**RBAC required:** ADF managed identity → **Storage Blob Data Contributor** on `stsharedqgr7mj` (done by `adf_rbac.py`).

**Python equivalent:**

```python
AzureBlobFSLinkedService(
    url="https://stsharedqgr7mj.dfs.core.windows.net",
    description="ADLS Gen2 — shared FinLedger lake (eastus)",
)
```

---

### 1.2 `ls_databricks_shared` — Azure Databricks

| ADF UI field | Value | Why |
|--------------|-------|-----|
| **Name** | `ls_databricks_shared` | Used by `pl_07` and `pl_10` notebook activities |
| **Type** | Azure Databricks | External compute |
| **Databricks workspace URL** | `https://adb-7405613791235979.19.azuredatabricks.net` (your workspace URL) | From `.env` `DATABRICKS_HOST` |
| **Authentication** | **Access Token** (classroom) or **Managed Service Identity** | PAT is more reliable for notebook ACL |
| **Access token** | From `.env` `DATABRICKS_TOKEN` | Secure string; never commit |
| **Select cluster** | **Existing interactive cluster** | Avoids new VM stockouts in `eastus` |
| **Cluster ID** | `0703-105931-31juyffm` | Same cluster as `run-50-problems.cmd` |
| **Description** | `Shared class Databricks — existing cluster` | Optional |

**Do NOT** configure "New job cluster" with `Standard_DS3_v2` for class — `eastus` often returns `SkuNotAvailable`.

**Python equivalent:**

```python
AzureDatabricksLinkedService(
    domain="https://adb-....azuredatabricks.net",
    existing_cluster_id="0703-105931-31juyffm",
    access_token=SecureString(value="dapi..."),  # or authentication="MSI"
)
```

**Before running notebook pipelines:** Start the shared cluster in Databricks **Compute** (or let `orchestrate.cmd` warm it up).

---

### 1.3 `ls_azure_sql_westus` — Azure SQL Database

| ADF UI field | Value | Why |
|--------------|-------|-----|
| **Name** | `ls_azure_sql_westus` | SQL source/sink for `pl_08`, `pl_09` |
| **Type** | Azure SQL Database | Relational sink/source |
| **Connect via** | AutoResolveIntegrationRuntime | Cross-region: SQL westus → ADF eastus |
| **Server name** | `sql-shared-qgr7mj.database.windows.net` | Logical server FQDN |
| **Database name** | `finledger` | Target database |
| **Authentication** | SQL authentication | Lab uses SQL login from Key Vault |
| **User name** | `finledgeradmin` | From Bicep output |
| **Password** | From Key Vault `sql-admin-password` | Not in git |
| **Encrypt** | True | Required for Azure SQL |
| **Trust server certificate** | False | Production-safe default |
| **Description** | `Azure SQL westus — sql-shared-qgr7mj` | Optional |

**Firewall:** Bicep rule `AllowAllAzureIPs` lets Azure services connect.

---

## Part 2 — Datasets (create in Manage → Datasets)

Datasets are **pointers** to data locations. They do not contain data.

---

### 2.1 `ds_bronze_incoming_csv` — Delimited text (parameterized folder)

| Field | Value |
|-------|--------|
| **Name** | `ds_bronze_incoming_csv` |
| **Type** | DelimitedText |
| **Linked service** | `ls_adls_finledger` |
| **File path type** | Wildcard file path or file path in directory |
| **File system** | `bronze` |
| **Directory** | `@dataset().folder_path` (parameter) |
| **File name** | `sample_transactions.csv` |
| **Column delimiter** | `,` (comma) |
| **First row as header** | **True** |
| **Quote character** | `"` (default) |
| **Escape character** | `\` (default) |
| **Null value** | (empty) |
| **Encoding** | UTF-8 (default) |
| **Compression** | None |

**Dataset parameter:**

| Parameter name | Type | Default (optional) |
|----------------|------|------------------|
| `folder_path` | String | — |

**Resolves at runtime to:**  
`abfss://bronze@stsharedqgr7mj.dfs.core.windows.net/incoming/run=session3-lab/sample_transactions.csv`

---

### 2.2 `ds_bronze_loaded_csv` — Delimited text (parameterized folder)

Same as `ds_bronze_incoming_csv` except used as **copy sink** (loaded zone).

| Field | Value |
|-------|--------|
| **Name** | `ds_bronze_loaded_csv` |
| **Directory parameter** | `folder_path` → e.g. `loaded/run=session3-lab` |
| All other fields | Same as 2.1 |

---

### 2.3 `ds_bronze_incoming_folder` — Folder for Get Metadata

**Critical:** Get Metadata on `childItems` needs a **folder** dataset, not a file dataset.

| Field | Value |
|-------|--------|
| **Name** | `ds_bronze_incoming_folder` |
| **Type** | Json (format type is flexible; location matters) |
| **Linked service** | `ls_adls_finledger` |
| **File system** | `bronze` |
| **Directory** | `@dataset().folder_path` |
| **File name** | *(leave empty — folder only)* |

**Dataset parameter:** `folder_path` (String)

---

### 2.4 `ds_audit_watermark_json` — JSON control file

| Field | Value |
|-------|--------|
| **Name** | `ds_audit_watermark_json` |
| **Type** | Json |
| **Linked service** | `ls_adls_finledger` |
| **File system** | `audit` |
| **Directory** | *(root of container)* |
| **File name** | `watermark.json` |

**File content (seeded by deploy):**

```json
{
  "last_run": "session3-lab",
  "last_successful_watermark": "2026-07-01"
}
```

---

### 2.5 `ds_audit_sql_export_csv` — SQL export landing file

| Field | Value |
|-------|--------|
| **Name** | `ds_audit_sql_export_csv` |
| **Type** | DelimitedText |
| **Linked service** | `ls_adls_finledger` |
| **File system** | `audit` |
| **Directory** | `sql_export` |
| **File name** | `dim_channel.csv` |
| **First row as header** | True |
| **Column delimiter** | `,` |

**Expected columns after `pl_08`:** `channel_code`, `channel_name`

---

### 2.6 `ds_sql_dim_channel` — Azure SQL table (source)

| Field | Value |
|-------|--------|
| **Name** | `ds_sql_dim_channel` |
| **Type** | Azure SqlTable |
| **Linked service** | `ls_azure_sql_westus` |
| **Table name** | `dbo.dim_channel` |

*Note: `pl_08` uses a SQL query override in the Copy activity, so the physical table may not exist in lab — query supplies rows.*

---

### 2.7 `ds_sql_stg_channel` — Azure SQL table (sink)

| Field | Value |
|-------|--------|
| **Name** | `ds_sql_stg_channel` |
| **Type** | Azure SqlTable |
| **Linked service** | `ls_azure_sql_westus` |
| **Table name** | `dbo.stg_channel_from_lake` |

**Table schema (created by `pl_09` pre-copy script if missing):**

| Column | SQL type |
|--------|----------|
| `channel_code` | `NVARCHAR(32)` |
| `channel_name` | `NVARCHAR(64)` |

---

## Part 3 — Pipelines (Author → Pipelines)

For each pipeline: **Settings** tab → add **Parameters**; canvas → drag activities; wire **green success** arrows for dependencies.

---

### Pipeline 01 — `pl_01_bronze_copy`

**Concept:** Copy CSV from bronze **incoming** → bronze **loaded**.

#### Pipeline parameters

| Name | Type | Default | Required at trigger |
|------|------|---------|---------------------|
| `incoming_folder` | String | — | Yes |
| `loaded_folder` | String | — | Yes |

#### Activity: `CopyBronze` (Copy data)

| Setting | Value |
|---------|--------|
| **Name** | `CopyBronze` |
| **Source dataset** | `ds_bronze_incoming_csv` |
| **Source dataset parameter** | `folder_path` = `@pipeline().parameters.incoming_folder` |
| **Sink dataset** | `ds_bronze_loaded_csv` |
| **Sink dataset parameter** | `folder_path` = `@pipeline().parameters.loaded_folder` |
| **Source tab — Format** | Delimited text |
| **Source — Recursive** | True |
| **Sink tab — Format** | Delimited text |
| **Sink — Copy behavior** | **Merge files** |
| **Enable staging** | Off |
| **Fault tolerance** | Default |
| **Settings — Retry** | 1 |
| **Settings — Retry interval (sec)** | 30 |
| **Integration Runtime** | AutoResolveIntegrationRuntime |

**Manual Trigger now parameters:**

```json
{
  "incoming_folder": "incoming/run=session3-lab",
  "loaded_folder": "loaded/run=session3-lab"
}
```

**Success check:** Monitor → Succeeded; Storage Explorer → file exists under `bronze/loaded/run=session3-lab/`.

---

### Pipeline 02 — `pl_02_get_metadata`

**Concept:** List files in incoming folder (no copy).

#### Pipeline parameters

| Name | Type | Default |
|------|------|---------|
| `incoming_folder` | String | `incoming/run=session3-lab` |

#### Activity: `ListBronzeIncoming` (Get Metadata)

| Setting | Value |
|---------|--------|
| **Name** | `ListBronzeIncoming` |
| **Dataset** | `ds_bronze_incoming_folder` |
| **Dataset parameter** | `folder_path` = `@pipeline().parameters.incoming_folder` |
| **Field list** | `childItems`, `itemName`, `itemType` |
| **Store settings — Recursive** | True |
| **Format settings** | (default for dataset) |

**Manual Trigger now:**

```json
{
  "incoming_folder": "incoming/run=session3-lab"
}
```

**Output to inspect:** Activity output → `childItems` array (should list `sample_transactions.csv`).

**Common failure:** Using file dataset instead of folder dataset → *"Cannot navigate through the provided data source"*.

---

### Pipeline 03 — `pl_03_if_then_copy`

**Concept:** Get metadata → **If** folder has files → copy; else skip.

#### Pipeline parameters

| Name | Type | Default |
|------|------|---------|
| `incoming_folder` | String | `incoming/run=session3-lab` |
| `loaded_folder` | String | — |

#### Activity 1: `CheckFolder` (Get Metadata)

Same settings as `pl_02` but name = `CheckFolder`, field list = **`childItems` only**.

#### Activity 2: `IfFilesExist` (If Condition)

| Setting | Value |
|---------|--------|
| **Name** | `IfFilesExist` |
| **Depends on** | `CheckFolder` → **Succeeded** |
| **Expression** | `@greater(length(activity('CheckFolder').output.childItems), 0)` |
| **If True activities** | `CopyIfFound` (copy activity) |
| **If False activities** | *(empty — pipeline still succeeds)* |

#### Activity inside If True: `CopyIfFound` (Copy data)

Identical to `pl_01` `CopyBronze` but activity name = `CopyIfFound`.

**Manual Trigger now:**

```json
{
  "incoming_folder": "incoming/run=session3-lab",
  "loaded_folder": "loaded/run=session3-lab"
}
```

---

### Pipeline 04 — `pl_04_foreach_dates`

**Concept:** Loop over an array of `{incoming, loaded}` pairs.

#### Pipeline parameters

| Name | Type | Example value at trigger |
|------|------|--------------------------|
| `run_dates` | **Array** | See JSON below |

#### Activity: `ForEachRunDate` (ForEach)

| Setting | Value |
|---------|--------|
| **Name** | `ForEachRunDate` |
| **Sequential** | **True** (one date at a time) |
| **Items** | `@pipeline().parameters.run_dates` |
| **Activities inside** | `CopyEachDate` (Copy) |

#### Inner activity: `CopyEachDate`

| Setting | Value |
|---------|--------|
| Source `folder_path` | `@item().incoming` |
| Sink `folder_path` | `@item().loaded` |

**Manual Trigger now:**

```json
{
  "run_dates": [
    {
      "incoming": "incoming/run=session3-lab",
      "loaded": "loaded/run=session3-lab"
    }
  ]
}
```

*For backfill demo, add more objects to the array.*

---

### Pipeline 05 — `pl_05_lookup_watermark`

**Concept:** Read JSON watermark → store `last_run` in pipeline variable.

#### Pipeline variables (Settings → Variables)

| Name | Type |
|------|------|
| `last_successful_run` | String |

#### Activity 1: `ReadWatermark` (Lookup)

| Setting | Value |
|---------|--------|
| **Name** | `ReadWatermark` |
| **Source dataset** | `ds_audit_watermark_json` |
| **Source format** | JSON |
| **First row only** | **True** |
| **Dataset parameters** | None |

#### Activity 2: `StoreLastRun` (Set variable)

| Setting | Value |
|---------|--------|
| **Name** | `StoreLastRun` |
| **Depends on** | `ReadWatermark` → Succeeded |
| **Variable name** | `last_successful_run` |
| **Value** | `@activity('ReadWatermark').output.firstRow.last_run` |

**Manual Trigger now:** `{}` (no parameters)

**Verify:** Pipeline run output → variables → `last_successful_run` = `session3-lab`.

---

### Pipeline 06 — `pl_06_wait_then_copy`

**Concept:** Wait 5 seconds → copy (throttle / ordering pattern).

#### Pipeline parameters

Same as `pl_01`: `incoming_folder`, `loaded_folder`.

#### Activity 1: `WaitBeforeCopy` (Wait)

| Setting | Value |
|---------|--------|
| **Name** | `WaitBeforeCopy` |
| **Wait time (seconds)** | **5** |

#### Activity 2: `CopyAfterWait` (Copy)

| Setting | Value |
|---------|--------|
| **Depends on** | `WaitBeforeCopy` → Succeeded |
| Copy settings | Same as `pl_01` |

**Manual Trigger now:** Same JSON as `pl_01`.

---

### Pipeline 07 — `pl_07_databricks_notebook`

**Concept:** ADF triggers Databricks notebook on **existing shared cluster**.

#### Pipeline parameters

| Name | Type | Default |
|------|------|---------|
| `run_id` | String | `session3-lab` |

#### Activity: `RunHelloNotebook` (Databricks notebook)

| Setting | Value |
|---------|--------|
| **Name** | `RunHelloNotebook` |
| **Linked service** | `ls_databricks_shared` |
| **Notebook path** | `/Shared/shared-adf/nb_adf_hello` |
| **Base parameters** | See table below |
| **Timeout** | `0.01:00:00` (1 hour) |
| **Retry** | 1 |

**Base parameters (notebook widgets):**

| Widget name | Value in ADF |
|-------------|--------------|
| `run_id` | `@pipeline().parameters.run_id` |
| `triggered_by` | `adf-pl_07` (literal string) |

**Manual Trigger now:**

```json
{
  "run_id": "session3-lab"
}
```

**Prerequisites:**

1. Notebook imported at `/Shared/shared-adf/nb_adf_hello` (deploy does this)
2. Shared cluster `0703-105931-31juyffm` is **Running**
3. `DATABRICKS_TOKEN` in linked service

**Success:** Notebook prints `ADF NOTEBOOK ACTIVITY — SUCCESS PATH` and exits with JSON `{"status":"ok",...}`.

---

### Pipeline 08 — `pl_08_sql_to_lake`

**Concept:** SQL (westus) → CSV on lake (eastus) — cross-region copy.

#### Pipeline parameters

None.

#### Activity: `SqlToLake` (Copy data)

| Setting | Value |
|---------|--------|
| **Name** | `SqlToLake` |
| **Source dataset** | `ds_sql_dim_channel` |
| **Sink dataset** | `ds_audit_sql_export_csv` |
| **Source — Use query** | **Yes** |
| **SQL query** | See below |
| **Sink — Format** | Delimited text |
| **Sink — Copy behavior** | Merge files |
| **Sink — First row as header** | True (from dataset) |

**SQL query (source override):**

```sql
SELECT 'card' AS channel_code, 'Card payments' AS channel_name
UNION ALL SELECT 'wire', 'Wire transfer'
UNION ALL SELECT 'UNKNOWN', 'Unknown channel'
```

**Manual Trigger now:** `{}`

**Success check:** File `audit/sql_export/dim_channel.csv` with 3 rows.

---

### Pipeline 09 — `pl_09_lake_to_sql`

**Concept:** Lake CSV (`dim_channel.csv` from `pl_08`) → SQL staging table.

**Important:** Source must be **`ds_audit_sql_export_csv`** (columns `channel_code`, `channel_name`) — **not** `sample_transactions.csv` (which has `transaction_id` and causes `SqlInvalidColumnName`).

#### Pipeline parameters

None (fixed paths on datasets).

#### Activity: `LakeToSql` (Copy data)

| Setting | Value |
|---------|--------|
| **Name** | `LakeToSql` |
| **Source dataset** | `ds_audit_sql_export_csv` |
| **Sink dataset** | `ds_sql_stg_channel` |
| **Source — Recursive** | True |
| **Sink — Pre-copy script** | See below |
| **Table option** | Auto create table **off** (script creates if missing) |
| **Write behavior** | Insert (default) |

**Pre-copy script (Sink tab):**

```sql
IF OBJECT_ID('dbo.stg_channel_from_lake', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.stg_channel_from_lake (
        channel_code NVARCHAR(32),
        channel_name NVARCHAR(64)
    );
END
```

**Recommended run order:** Run **`pl_08` first**, then **`pl_09`**.

**Manual Trigger now:** `{}`

---

### Pipeline 10 — `pl_10_parent_chain`

**Concept:** Parent pipeline = **Execute child pipeline** → **Databricks notebook** (full E2E chain).

#### Pipeline parameters

| Name | Type | Default |
|------|------|---------|
| `incoming_folder` | String | — |
| `loaded_folder` | String | — |
| `run_id` | String | `session3-lab` |

#### Activity 1: `ChildCopy` (Execute Pipeline)

| Setting | Value |
|---------|--------|
| **Name** | `ChildCopy` |
| **Invoked pipeline** | `pl_01_bronze_copy` |
| **Wait on completion** | **True** |
| **Parameters passed to child** | See below |

**Child parameter mapping:**

| Child parameter | Parent expression |
|-----------------|-------------------|
| `incoming_folder` | `@pipeline().parameters.incoming_folder` |
| `loaded_folder` | `@pipeline().parameters.loaded_folder` |

#### Activity 2: `ChildNotebook` (Databricks notebook)

| Setting | Value |
|---------|--------|
| **Name** | `ChildNotebook` |
| **Depends on** | `ChildCopy` → **Succeeded** |
| **Linked service** | `ls_databricks_shared` |
| **Notebook path** | `/Shared/shared-adf/nb_adf_hello` |
| `run_id` | `@pipeline().parameters.run_id` |
| `triggered_by` | `adf-pl_10-chain` (literal) |

**Manual Trigger now:**

```json
{
  "incoming_folder": "incoming/run=session3-lab",
  "loaded_folder": "loaded/run=session3-lab",
  "run_id": "session3-lab"
}
```

**Monitor tip:** Expand parent run → see **child** `pl_01_bronze_copy` run nested under `ChildCopy`.

---

## Part 4 — Quick reference: all Trigger now payloads

| Pipeline | Trigger JSON |
|----------|----------------|
| `pl_01_bronze_copy` | `{"incoming_folder":"incoming/run=session3-lab","loaded_folder":"loaded/run=session3-lab"}` |
| `pl_02_get_metadata` | `{"incoming_folder":"incoming/run=session3-lab"}` |
| `pl_03_if_then_copy` | Same as pl_01 |
| `pl_04_foreach_dates` | `{"run_dates":[{"incoming":"incoming/run=session3-lab","loaded":"loaded/run=session3-lab"}]}` |
| `pl_05_lookup_watermark` | `{}` |
| `pl_06_wait_then_copy` | Same as pl_01 |
| `pl_07_databricks_notebook` | `{"run_id":"session3-lab"}` |
| `pl_08_sql_to_lake` | `{}` |
| `pl_09_lake_to_sql` | `{}` (run after pl_08) |
| `pl_10_parent_chain` | `{"incoming_folder":"incoming/run=session3-lab","loaded_folder":"loaded/run=session3-lab","run_id":"session3-lab"}` |

**CLI equivalent:**

```text
cd shared-adf-lab
.\orchestrate.cmd --run-pipeline pl_01_bronze_copy
.\orchestrate.cmd --run-all-until-success --max-rounds 3
```

---

## Part 5 — ADF expression cheat sheet (used in this lab)

| Expression | Meaning |
|------------|---------|
| `@pipeline().parameters.incoming_folder` | Pipeline parameter at trigger time |
| `@dataset().folder_path` | Dataset parameter (set from activity) |
| `@item().incoming` | Current item inside **ForEach** |
| `@activity('CheckFolder').output.childItems` | Output of a named activity |
| `@greater(length(...), 0)` | True if array has elements |
| `@activity('ReadWatermark').output.firstRow.last_run` | JSON field from Lookup |

**Rules:**

- Expressions always start with `@`
- Activity names in expressions are **case-sensitive** and must match the activity **Name** on canvas
- Use **Expression** tab (not string literal) when binding parameters in ADF UI

---

## Part 6 — Activity dependency diagram (all pipelines)

```text
pl_01:  [CopyBronze]

pl_02:  [ListBronzeIncoming]

pl_03:  [CheckFolder] ──► [IfFilesExist]
                              └─ True ─► [CopyIfFound]

pl_04:  [ForEachRunDate]
            └─► [CopyEachDate] (per item)

pl_05:  [ReadWatermark] ──► [StoreLastRun]

pl_06:  [WaitBeforeCopy] ──► [CopyAfterWait]

pl_07:  [RunHelloNotebook]

pl_08:  [SqlToLake]

pl_09:  [LakeToSql]

pl_10:  [ChildCopy] ──► [ChildNotebook]
           (executes pl_01)
```

---

## Part 7 — Troubleshooting (symptom → fix)

| Symptom | Pipeline | Fix |
|---------|----------|-----|
| Cannot navigate data source | pl_02, pl_03 | Use **folder** dataset `ds_bronze_incoming_folder`, not CSV file dataset |
| `incoming_folder` empty | pl_02 | Pass parameter at trigger; default only applies in code deploy |
| `SkuNotAvailable` / DS3_v2 stockout | pl_07, pl_10 | Linked service → **existing cluster** `0703-105931-31juyffm`, not new job cluster |
| Notebook not found / 404 | pl_07, pl_10 | Create folder `/Shared/shared-adf`, import `nb_adf_hello` |
| `transaction_id` column error | pl_09 | Source must be `ds_audit_sql_export_csv`; run **pl_08** first |
| SQL table invalid object | pl_08 | Lab uses SQL **query** in copy source (no physical `dim_channel` required) |
| Copy 403 to ADLS | Any copy | Re-run deploy for RBAC; wait 2 min for IAM propagation |

---

## Part 8 — Classroom flow (minute-by-minute, ~60 min)

| Minutes | Topic | Pipeline | Action |
|---------|-------|----------|--------|
| 0–5 | ADF model: LS, dataset, pipeline, IR | — | Show Manage blade |
| 5–15 | Copy activity | pl_01 | Trigger + Monitor + Storage Explorer |
| 15–22 | Get Metadata | pl_02 | Show `childItems` output |
| 22–30 | If Condition | pl_03 | Explain expression builder |
| 30–38 | ForEach | pl_04 | Show array parameter JSON |
| 38–45 | Lookup + Variable | pl_05 | Show variable in run details |
| 45–48 | Wait | pl_06 | Explain throttle pattern |
| 48–55 | Databricks notebook | pl_07 | Start cluster first; show notebook output |
| 55–58 | SQL ↔ Lake | pl_08 → pl_09 | Cross-region; column schema match |
| 58–60 | Execute Pipeline | pl_10 | Parent/child monitor view |

---

## Part 9 — Code map (Python SDK → ADF object)

| Python function | Creates |
|-----------------|---------|
| `deploy_linked_services()` | `ls_adls_finledger`, `ls_databricks_shared`, `ls_azure_sql_westus` |
| `_deploy_datasets()` | All `ds_*` datasets |
| `_copy_bronze_activity()` | Reusable Copy activity for bronze paths |
| `deploy_all_pipelines()` | All `pl_01` … `pl_10` |
| `trigger_pipeline()` | Starts a run (same as Trigger now) |
| `wait_for_pipeline_run()` | Polls Monitor until Succeeded/Failed |

**Idempotency:** `create_or_update` on every object — safe to run `orchestrate.cmd` repeatedly.

---

## Part 10 — Related files

| File | Purpose |
|------|---------|
| `scripts/adf_pipelines.py` | Full SDK definitions |
| `scripts/run_shared_adf.py` | Deploy orchestration + trigger helpers |
| `notebooks/nb_adf_hello.py` | Databricks target notebook |
| `README.md` | Lab quick start |
| `infra/shared-sql-westus.bicep` | SQL server + database |

*Last aligned with code: shared ADF lab — existing Databricks cluster, folder metadata datasets, pl_09 source = `ds_audit_sql_export_csv`.*
