# ADF + Databricks — Complete Integration (infra + notebooks + Job Preview)

**Purpose:** End-to-end ADF orchestration of Databricks — **Notebook activity**, **Job activity (Preview)**, secret scope, Delta silver write, and idempotent **infra scripts**.

**ADF folder:** `04-integration-databricks-complete`  
**Infra script:** `scripts/databricks_infra.py`  
**Orchestrate:** `shared-adf-lab\orchestrate.cmd`

---

## Part A — WHAT & WHY

| Piece | What | Why |
|-------|------|-----|
| **Linked service** `ls_databricks_shared` | ADF → Databricks connection | Reuse PAT + existing cluster |
| **Notebook activity** | Run `/Shared/shared-adf/nb_*` with parameters | Classic FinLedger silver transforms |
| **Job activity (Preview)** | Trigger Databricks **Job** by `job_id` | Microsoft recommended path for workflows, serverless-ready |
| **Secret scope `finledger`** | `storage-account` + `storage-key` | Notebooks read ADLS without hard-coded keys |
| **Infra script** | Secrets + Job create + `audit/adf_databricks_job.json` | One command bootstraps integration |

---

## Part B — One-time infra bootstrap (Windows)

### Prerequisites in repo-root `.env`

```env
LEARNER=shared
AZURE_SUBSCRIPTION_ID=...
DATABRICKS_HOST=https://adb-XXXX.azuredatabricks.net
DATABRICKS_TOKEN=dapiXXXXXXXX
DATABRICKS_CLUSTER_ID=0703-105931-31juyffm
STORAGE_ACCOUNT_KEY=<storage account key1>
```

### Commands (idempotent)

```cmd
cd d:\azure\shared-adf-lab
.\orchestrate.cmd --setup-databricks-integration
.\orchestrate.cmd --warm-cluster-only
.\orchestrate.cmd
```

| Step | What it does |
|------|----------------|
| `--setup-databricks-integration` | Secret scope `finledger` + Databricks Job `adf-finledger-integration-job` + `audit/adf_databricks_job.json` |
| `orchestrate.cmd` (full) | ADF pipelines, notebooks import, RBAC, bronze samples |
| `--warm-cluster-only` | Start shared all-purpose cluster |

**Infra-only (no ADF deploy):**

```cmd
.\orchestrate.cmd --run-only --setup-databricks-integration
```

---

## Part C — Notebooks (workspace paths)

| File | Workspace path | Role |
|------|----------------|------|
| `nb_adf_hello.py` | `/Shared/shared-adf/nb_adf_hello` | Hello / params demo |
| `nb_adf_params_echo.py` | `/Shared/shared-adf/nb_adf_params_echo` | Expression lab echo |
| `nb_adf_bronze_stats.py` | `/Shared/shared-adf/nb_adf_bronze_stats` | CSV row-count stats |
| **`nb_adf_integration_complete.py`** | `/Shared/shared-adf/nb_adf_integration_complete` | **Master** — bronze CSV → Delta silver |
| **`nb_adf_job_task.py`** | `/Shared/shared-adf/nb_adf_job_task` | **Job Preview** task notebook |

### Master notebook widgets (ADF base parameters)

| Widget | ADF expression example |
|--------|------------------------|
| `run_id` | `@pipeline().parameters.run_id` |
| `triggered_by` | `adf-pl_db_12-end-to-end` |
| `bronze_path` | `@concat(loaded_folder, '/', file_name)` |
| `silver_path` | `cleaned/run=session3-lab/adf_integration_delta` |
| `storage_account` | `stsharedqgr7mj` or empty (uses secret scope) |

---

## Part D — Pipelines in `04-integration-databricks-complete`

| Pipeline | Pattern |
|----------|---------|
| `pl_07_databricks_notebook` | Basic notebook (core lab) |
| `pl_10_parent_chain` | Execute Pipeline → copy → notebook |
| **`pl_db_11_databricks_job_preview`** | Lookup `adf_databricks_job.json` → **Databricks Job (Preview)** |
| **`pl_db_12_end_to_end_integration`** | Copy bronze → **master notebook** → Delta silver |
| **`pl_db_13_copy_notebook_job_chain`** | Copy → stats notebook → **Job (Preview)** |

### `pl_db_11` — Databricks Job activity (Preview) UI steps

1. **Author** → folder `04-integration-databricks-complete` → open `pl_db_11_databricks_job_preview`
2. Activity 1: **Lookup** `ReadDatabricksJobManifest` on dataset `ds_audit_databricks_job_json`
3. Activity 2: **Databricks** → **Job (Preview)** `RunDatabricksJobPreview`
4. **Settings:**
   - Linked service: `ls_databricks_shared`
   - **Job ID:** `@string(activity('ReadDatabricksJobManifest').output.firstRow.job_id)`
   - **Job parameters:**

     | Key | Value |
     |-----|-------|
     | `run_id` | `@pipeline().parameters.run_id` |
     | `triggered_by` | `@concat('adf-job-preview-', pipeline().parameters.lab_tag)` |
     | `job_note` | `@pipeline().parameters.job_note` |

5. **Trigger now** → Monitor → open Databricks job run link

### `pl_db_12` — Complete integration UI steps

1. Open `pl_db_12_end_to_end_integration`
2. **Copy** `CopyBronzeForIntegration` — incoming → loaded CSV
3. **Databricks Notebook** `RunCompleteIntegration`
   - Path: `/Shared/shared-adf/nb_adf_integration_complete`
   - Dynamic `bronze_path`, `silver_path`, `run_id`
4. **Verify:** `silver/cleaned/run=session3-lab/adf_integration_delta/` Delta files on ADLS

### `pl_db_13` — Full chain (Copy + Notebook + Job Preview)

```text
CopyBronzeForChain → RunNotebookInChain → ReadJobManifestInChain → RunJobPreviewInChain
```

Teaches sequential handoff: land file → validate in notebook → trigger Job workflow.

---

## Part E — Infra script internals (`databricks_infra.py`)

| Function | Idempotent behaviour |
|----------|---------------------|
| `setup_finledger_secrets()` | Scope `finledger` + storage account/key |
| `ensure_integration_job()` | Reuse job named `adf-finledger-integration-job` or create |
| `upload_job_manifest()` | `audit/adf_databricks_job.json` with `job_id` |
| `setup_complete_integration()` | All three in sequence |

**Job manifest JSON:**

```json
{
  "job_id": 123456789,
  "job_name": "adf-finledger-integration-job",
  "notebook_path": "/Shared/shared-adf/nb_adf_job_task",
  "integration": "adf-databricks-job-preview"
}
```

---

## Part F — Integration patterns covered

| Pattern | Pipeline | Microsoft guidance |
|---------|----------|-------------------|
| Notebook + static params | `pl_db_01`, `pl_07` | Legacy (still valid for teaching) |
| Notebook + dynamic expressions | `pl_db_02`–`08` | Parameterized ETL |
| Copy → Notebook | `pl_db_05`, `pl_db_12` | Lake handoff |
| **Job activity (Preview)** | `pl_db_11`, `pl_db_13` | **Recommended** for new frameworks |
| Execute Pipeline child | `pl_db_10`, `pl_10` | Modular orchestration |
| Secret scope abfss | All integration notebooks | Production-like auth |
| Delta silver write | `nb_adf_integration_complete` | Medallion bronze→silver |

---

## Part G — Run commands

```cmd
cd shared-adf-lab
.\orchestrate.cmd --setup-databricks-integration
.\orchestrate.cmd --warm-cluster-only
.\orchestrate.cmd --run-pipeline pl_db_12_end_to_end_integration
.\orchestrate.cmd --run-pipeline pl_db_11_databricks_job_preview
.\orchestrate.cmd --run-pipeline pl_db_13_copy_notebook_job_chain
```

---

## Part H — Troubleshooting

| Issue | Fix |
|-------|-----|
| Job activity: invalid job id | Run `--setup-databricks-integration` |
| Lookup `adf_databricks_job.json` fails | Same — manifest missing on audit container |
| Notebook 403 on abfss | Run `--setup-databricks-integration` (secret scope) |
| `STORAGE_ACCOUNT_KEY` missing | Add to `.env` from portal Access keys |
| Databricks CLI not found | `pip install databricks-cli` in repo venv |
| Job parameters not received | Job task `base_parameters` keys must match notebook widgets |
| Delta write fails | Cluster needs Delta runtime; check `silver_path` container exists |

---

## Part I — Related docs

| Doc | Content |
|-----|---------|
| `docs/expression_builder_catalog.md` | All `@` expressions + `11-expression-builder` folder |
| `docs/databricks_integration_catalog.md` | `pl_db_01`–`10` teaching catalog |
| `docs/pipeline_code_walkthrough.md` | Core `pl_07` linked service UI |

*Last aligned: `databricks_infra.py`, `adf_databricks.py` — complete integration + Job Preview.*
