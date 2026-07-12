# ADF + Databricks Integration — Trainer Catalog (10 pipelines, no data flows)

**Purpose:** Ten pipelines in ADF folder **`11-expression-builder`** (see also `docs/expression_builder_catalog.md` for the full expression language + all grouped pipelines).

**Code:** `scripts/adf_databricks.py`  
**Notebooks:** `notebooks/nb_adf_hello.py`, `nb_adf_params_echo.py`, `nb_adf_bronze_stats.py`  
**Deploy:** `shared-adf-lab\orchestrate.cmd`  
**Folder:** `11-expression-builder` (same folder as `pl_ex_*` expression labs)

---

## Part A — WHAT & WHY

### Azure Data Factory expressions

**What:** Dynamic values in pipeline parameters, activity inputs, and Databricks **base parameters** — written as `@function()` strings.

**Why:** FinLedger pipelines must pass `run_id`, file paths, and branch logic without hard-coding. Expressions read **pipeline parameters**, **system variables**, and **prior activity outputs**.

**Analogy:** Expressions are **mail-merge fields** — the pipeline template stays the same; each run fills in different values.

### Databricks Notebook activity

**What:** ADF calls a notebook path on a linked Databricks workspace and passes **base parameters** (widgets).

**Why:** Heavy transforms (Spark, ML) run on Databricks; ADF only **schedules** and **chains** steps.

**This lab:** Uses **existing all-purpose cluster** (`DATABRICKS_CLUSTER_ID` in `.env`) — avoids job-cluster stockouts in `eastus`.

---

## Part B — Expression builder reference (teach these first)

Open any activity field → click **Add dynamic content** (or type `@`).

| Category | Expression | Example in this lab |
|----------|------------|---------------------|
| Pipeline param | `@pipeline().parameters.run_id` | `pl_db_02` |
| Pipeline RunId | `@pipeline().RunId` | `pl_db_03` |
| Factory name | `@pipeline().DataFactory` | `pl_db_03` |
| Trigger name | `@pipeline().TriggerName` | `pl_db_03` (via concat) |
| Activity output | `@activity('GetIncomingMetadata').output.childItems` | `pl_db_04` |
| Lookup first row | `@activity('ReadWatermark').output.firstRow.last_run` | `pl_db_08` |
| Variable | `@variables('file_count')` | `pl_db_06` |
| ForEach item | `@item()` | `pl_db_07` |
| String concat | `@concat('adf-', pipeline().parameters.lab_tag)` | `pl_db_02` |
| Cast to string | `@string(length(...))` | `pl_db_04`, `pl_db_06` |
| Cast to int | `@int(variables('file_count'))` | `pl_db_06` |
| UTC timestamp | `@utcnow()` | `pl_db_03` |
| New GUID | `@guid()` | `pl_db_03` |
| Coalesce null | `@coalesce(activity('X').output.itemName, 'unknown')` | `pl_db_04` |
| Compare | `@greater(int(variables('file_count')), 0)` | `pl_db_06` |
| Equals | `@equals(pipeline().parameters.channel, 'wire')` | (see `pl_17`) |
| Child pipeline param | Expression object on Execute Pipeline `parameters` | `pl_db_10` |

### Expression rules (classroom)

1. Every dynamic field starts with **`@`**.
2. Activity names in `@activity('Name')` must match the activity **Name** on canvas exactly.
3. Databricks **base parameters** accept `@` expressions — they become notebook **widgets**.
4. Use **Debug** on one pipeline before **Trigger now** on all ten.
5. **Warm cluster** before first notebook run: `.\orchestrate.cmd --warm-cluster-only`

---

## Part C — Linked service & notebooks (UI once)

### C.1 `ls_databricks_shared` (already deployed by code)

| Step | ADF Studio |
|------|------------|
| 1 | **Manage** → **Linked services** → `ls_databricks_shared` |
| 2 | **Type** | Azure Databricks |
| 3 | **Account selection** | Enter workspace URL / select workspace |
| 4 | **Select cluster** | **Existing interactive cluster** → paste cluster ID from `.env` |
| 5 | **Authentication** | Access token (PAT) or MSI |

> **Do not** pick "New job cluster" for class — `Standard_DS3_v2` is often unavailable in `eastus`.

### C.2 Import notebooks (automated)

`orchestrate.cmd` imports:

| Workspace path | Used by |
|----------------|---------|
| `/Shared/shared-adf/nb_adf_hello` | `pl_db_01`, `pl_db_09`, `pl_07` |
| `/Shared/shared-adf/nb_adf_params_echo` | `pl_db_02`–`04`, `06`–`08`, `10` |
| `/Shared/shared-adf/nb_adf_bronze_stats` | `pl_db_05` |

**Manual UI import:** Databricks → **Workspace** → **Shared** → **Create** → **Import** → upload `.py` source files from `shared-adf-lab/notebooks/`.

### C.3 Databricks Notebook activity (common UI steps)

| Step | Action |
|------|--------|
| 1 | **Author** → open pipeline → **Activities** → **Databricks** → **Notebook** |
| 2 | **Name** | e.g. `RunParamExpressions` |
| 3 | **Databricks linked service** | `ls_databricks_shared` |
| 4 | **Notebook path** | `/Shared/shared-adf/nb_adf_params_echo` |
| 5 | **Base parameters** | Click **+ New** → name must match `dbutils.widgets` in notebook |
| 6 | Value | Static text **or** click **Add dynamic content** → paste `@expression` |
| 7 | **Settings** | Timeout 1 hour, Retry 1 (classroom) |

---

## Part D — The 10 Databricks pipelines (scenario catalog)

| # | Pipeline | ADF concepts | Expression focus |
|---|----------|--------------|------------------|
| 1 | `pl_db_01_static_notebook` | Basic notebook | **Static** base parameters (no `@`) |
| 2 | `pl_db_02_pipeline_param_expressions` | Pipeline parameters | `@pipeline().parameters.*`, `@concat` |
| 3 | `pl_db_03_system_expressions` | System context | `@pipeline().RunId`, `@utcnow()`, `@guid()` |
| 4 | `pl_db_04_metadata_to_notebook` | Get Metadata → Notebook | `@activity(...).output`, `@length`, `@coalesce` |
| 5 | `pl_db_05_copy_then_notebook` | Copy → Notebook | `@concat` path; notebook reads **abfss** |
| 6 | `pl_db_06_if_then_notebook` | If + Set Variable | `@greater`, `@int`, `@variables` |
| 7 | `pl_db_07_foreach_channels` | ForEach + Notebook | `@item()` per channel |
| 8 | `pl_db_08_lookup_watermark_notebook` | Lookup JSON → Notebook | `@activity('ReadWatermark').output.firstRow.*` |
| 9 | `pl_db_09_wait_then_notebook` | Wait → Notebook | Cluster warm-up / ordering pattern |
| 10 | `pl_db_10_child_pipeline_notebook` | Execute Pipeline | Parent passes expressions to child params |

Helper child (not taught standalone): `pl_db_10_child_notebook_only`

---

## Part E — UI walkthrough per pipeline

### `pl_db_01_static_notebook` — static parameters

**Teaches:** Simplest path — prove ADF → Databricks works.

**UI steps:**

1. **Author** → folder `04-integration-databricks` → **+** → **Pipeline** → `pl_db_01_static_notebook`
2. Drag **Databricks Notebook** → name `RunStaticNotebook`
3. Notebook path: `/Shared/shared-adf/nb_adf_hello`
4. Base parameters (all **literal** strings):

   | Name | Value |
   |------|-------|
   | `run_id` | `session3-lab` |
   | `triggered_by` | `adf-pl_db_01-static` |
   | `source_file` | `sample_transactions.csv` |

5. **Debug** → Monitor → open Databricks run link → see printed parameters

**Verify:** Notebook stdout shows three parameters; run status **Succeeded**.

---

### `pl_db_02_pipeline_param_expressions` — pipeline parameters

**Teaches:** `@pipeline().parameters.*` and `@concat`.

**Pipeline parameters (Parameters tab):**

| Name | Type | Default |
|------|------|---------|
| `run_id` | String | `session3-lab` |
| `lab_tag` | String | `db02-params` |
| `expected_file` | String | `sample_transactions.csv` |

**Notebook base parameters (dynamic):**

| Name | Expression |
|------|------------|
| `run_id` | `@pipeline().parameters.run_id` |
| `triggered_by` | `@concat('adf-', pipeline().parameters.lab_tag)` |
| `source_file` | `@pipeline().parameters.expected_file` |
| `lab_tag` | `@pipeline().parameters.lab_tag` |

**Trigger now:** Change `lab_tag` to your name → notebook shows `triggered_by: adf-<yourname>`.

---

### `pl_db_03_system_expressions` — RunId, utcnow, guid

**Teaches:** Built-in pipeline functions (unique per run).

| Widget | Expression |
|--------|------------|
| `run_id` | `@pipeline().RunId` |
| `triggered_by` | `@concat('factory=', pipeline().DataFactory)` |
| `source_file` | `@concat('utc=', utcnow())` |
| `lab_tag` | `@guid()` |

**Class demo:** Run twice — `run_id` and `lab_tag` change every time.

---

### `pl_db_04_metadata_to_notebook` — activity output

**Teaches:** Chain **Get Metadata** before notebook; read `childItems` length.

**Activities:**

1. **Get Metadata** `GetIncomingMetadata`
   - Dataset: `ds_bronze_incoming_folder`
   - Folder path: `@pipeline().parameters.incoming_folder`
   - Field list: `childItems`, `itemName`
2. **Databricks Notebook** `RunMetadataExpressions` (depends on Get Metadata)
   - `triggered_by`: `@concat('childCount=', string(length(activity('GetIncomingMetadata').output.childItems)))`
   - `source_file`: `@coalesce(activity('GetIncomingMetadata').output.itemName, 'unknown')`

**Verify:** `childCount` > 0 when `incoming/run=session3-lab` has sample CSV.

---

### `pl_db_05_copy_then_notebook` — ETL handoff

**Teaches:** **Copy** lands file; notebook processes lake path (classic bronze pattern).

**Flow:** `CopyBronzeForNotebook` → `RunBronzeStats`

**Key expression (notebook `bronze_path`):**

```text
@concat(pipeline().parameters.loaded_folder, '/', pipeline().parameters.file_name)
```

**Notebook:** `nb_adf_bronze_stats` reads `abfss://bronze@<storage>.dfs.core.windows.net/<bronze_path>` and returns row count.

**Verify:** Notebook exit JSON includes `"row_count": 5` (sample file).

---

### `pl_db_06_if_then_notebook` — conditional notebook

**Teaches:** Only run Databricks when folder has files.

**Flow:**

1. Get Metadata → Set Variable `file_count`
2. If `@greater(int(variables('file_count')), 0)` → notebook
3. Else → Set Variable `run_status` = `skipped-no-files`

**Demo empty folder:** Change `incoming_folder` to empty path → skip path runs (no Databricks charge).

---

### `pl_db_07_foreach_channels` — loop + notebook

**Teaches:** `@item()` inside **ForEach**.

**Pipeline parameter `channels` (Array):**

```json
["card", "wire", "fps"]
```

**Inside ForEach — notebook `triggered_by`:** `@item()`  
**source_file:** `@concat('channel=', item())`

**Verify:** Monitor shows **3** notebook runs (sequential) — one per channel.

---

### `pl_db_08_lookup_watermark_notebook` — Lookup JSON

**Teaches:** Read `audit/watermark.json` → pass values to notebook.

**Lookup** `ReadWatermark` on `ds_audit_watermark_json`

| Widget | Expression |
|--------|------------|
| `triggered_by` | `@concat('lastRun=', activity('ReadWatermark').output.firstRow.last_run)` |
| `source_file` | `@activity('ReadWatermark').output.firstRow.last_successful_watermark` |

---

### `pl_db_09_wait_then_notebook` — Wait then run

**Teaches:** **Wait** 15s before notebook (cluster spin-up / ordering).

**Flow:** `WaitForCluster` (15 sec) → `RunAfterNotebook`

**Class note:** Combine with `orchestrate.cmd --warm-cluster-only` for reliable first run.

---

### `pl_db_10_child_pipeline_notebook` — Execute Pipeline

**Teaches:** Parent orchestrates; child runs notebook (separation of concerns).

**Parent** `pl_db_10_child_pipeline_notebook`:

- Activity: **Execute Pipeline** `ExecuteChildNotebook`
- Invoked pipeline: `pl_db_10_child_notebook_only`
- Parameters (expression):

  | Child param | Parent value |
  |-------------|--------------|
  | `run_id` | `@pipeline().parameters.run_id` |
  | `lab_tag` | `@concat('parent→child-', pipeline().parameters.lab_tag)` |

**Child** `pl_db_10_child_notebook_only`: single notebook activity.

**Verify:** Child notebook shows `triggered_by: child-parent→child-db10-parent`.

---

## Part F — ADF parameter → Databricks widget mapping

| ADF base parameter name | Notebook widget | Required |
|-------------------------|-----------------|----------|
| `run_id` | `dbutils.widgets.text("run_id", ...)` | Yes |
| `triggered_by` | `dbutils.widgets.text("triggered_by", ...)` | Yes |
| `source_file` | `dbutils.widgets.text("source_file", ...)` | Optional |
| `lab_tag` | `dbutils.widgets.text("lab_tag", ...)` | `nb_adf_params_echo` only |
| `bronze_path` | `dbutils.widgets.text("bronze_path", ...)` | `nb_adf_bronze_stats` only |
| `storage_account` | `dbutils.widgets.text("storage_account", ...)` | Optional (falls back to secret) |

**Names must match exactly** — ADF does not rename parameters.

---

## Part G — Quick run commands

```cmd
cd shared-adf-lab
.\orchestrate.cmd --warm-cluster-only
.\orchestrate.cmd --run-pipeline pl_db_01_static_notebook
.\orchestrate.cmd --run-pipeline pl_db_02_pipeline_param_expressions
.\orchestrate.cmd --run-from-pipeline pl_db_01_static_notebook
```

| Pipeline | Trigger params | Expected |
|----------|----------------|----------|
| `pl_db_01` | (none) | Notebook prints static params |
| `pl_db_02` | optional `lab_tag` | `triggered_by` = `adf-<lab_tag>` |
| `pl_db_03` | (none) | New GUID each run |
| `pl_db_04` | `incoming_folder` | `childCount` > 0 |
| `pl_db_05` | defaults | `row_count` in notebook output |
| `pl_db_06` | `incoming_folder` | Notebook or skip |
| `pl_db_07` | `channels` array | 3 notebook runs |
| `pl_db_08` | (none) | Watermark values in output |
| `pl_db_09` | (none) | Success after 15s wait |
| `pl_db_10` | `lab_tag` | Child receives parent expression |

---

## Part H — Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| Cluster not found | Wrong `DATABRICKS_CLUSTER_ID` | Fix `.env`, re-run orchestrate |
| Notebook not found | Import skipped | Set `DATABRICKS_TOKEN`, run `orchestrate.cmd` (not `--skip-notebook`) |
| `Activity ... not found` in expression | Typo in `@activity('Name')` | Match activity **Name** on canvas |
| `SkuNotAvailable` | Job cluster selected | Use **existing cluster** in linked service |
| Parameter empty in notebook | Widget name mismatch | Align ADF base parameter ↔ `dbutils.widgets` |
| Copy OK, notebook 403 on abfss | RBAC | Run `orchestrate.cmd` (RBAC script grants ADF MSI on storage) |
| ForEach runs 0 times | `channels` not Array type | Pipeline parameter type must be **Array** |

---

## Part I — How this fits the full lab

| Already in core lab | New `pl_db_*` module |
|---------------------|----------------------|
| `pl_07` one notebook | `pl_db_01`–`03` expression depth |
| `pl_10` copy + notebook | `pl_db_05` explicit copy→notebook |
| `pl_11` filter + notebook | `pl_db_06` if-condition gating |
| `pl_12` SQL metadata + notebook | `pl_db_08` lookup pattern |

**No mapping data flows** in any `pl_db_*` pipeline — pure orchestration + Databricks.

*Last aligned with code: `adf_databricks.py` — 10 teaching pipelines + 1 child helper, 3 notebooks.*
