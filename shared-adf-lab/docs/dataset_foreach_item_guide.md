# Dataset `@dataset()` + ForEach `@item()` — Step-by-Step Guide

**Pipeline:** `pl_ex_12_dataset_foreach_item`  
**ADF folder:** `11-expression-builder`  
**Dataset used:** `ds_bronze_incoming_folder` (parameter `folder_path` → `@dataset().folder_path` in dataset definition)

---

## Part A — Two layers (WHAT & WHY)

| Layer | Where | Expression | Who sets the value |
|-------|--------|------------|-------------------|
| **1. Dataset definition** | Author → Datasets → `ds_bronze_incoming_folder` | `@dataset().folder_path` | Factory template — “slot” on the dataset |
| **2a. Pipeline (once)** | Activity `ProbeMainFolder` | `@pipeline().parameters.default_incoming` | Debug / trigger parameters |
| **2b. ForEach (per item)** | Activity `GetFolderMetadata` | `@item().incoming` | Current element of array `folder_jobs` |

**Analogy:** The dataset is a mail envelope with a blank address line (`@dataset().folder_path`). The pipeline writes the address either from a form field (`@pipeline().parameters…`) or from each row in a list (`@item().incoming`).

---

## Part B — Dataset definition (Layer 1)

Open **Author → Datasets → `ds_bronze_incoming_folder`**:

| Field | Value |
|-------|--------|
| File system | `bronze` |
| Folder path | `@dataset().folder_path` (Expression) |
| Parameter | `folder_path` (String) |

Code reference: `scripts/adf_pipelines.py` → `_deploy_datasets`.

---

## Part C — Pipeline walkthrough

```
ProbeMainFolder          ← dataset folder_path = @pipeline().parameters.default_incoming
    ↓
SetBaselineFileCount     ← stores file count from ProbeMainFolder
    ↓
ForEachFolderJobs        ← loops @pipeline().parameters.folder_jobs
    ├── GetFolderMetadata    ← dataset folder_path = @item().incoming
    └── LogItemResult        ← AppendVariable: label + path + file count
```

### Parameters (Debug dialog)

| Parameter | Example value |
|-----------|----------------|
| `default_incoming` | `incoming/run=session3-lab` |
| `folder_jobs` | Array (see below) |

**`folder_jobs` JSON (paste in Debug):**

```json
[
  { "incoming": "incoming/run=session3-lab", "label": "main-incoming" },
  { "incoming": "incoming/run=session3-lab/rejected", "label": "rejected-decoys" }
]
```

---

## Part D — ADF Studio steps (learner)

### Step 1 — Inspect the dataset

| # | Action |
|---|--------|
| 1 | **Author** → **Datasets** → `ds_bronze_incoming_folder` |
| 2 | **Parameters** tab → `folder_path` (String) |
| 3 | **Connection** → Folder path → **Add dynamic content** → `@dataset().folder_path` |

### Step 2 — Open the pipeline

| # | Action |
|---|--------|
| 1 | **Pipelines** → `11-expression-builder` → `pl_ex_12_dataset_foreach_item` |
| 2 | Click **`ProbeMainFolder`** → Source dataset → **Parameters** |
| 3 | `folder_path` = `@pipeline().parameters.default_incoming` |

### Step 3 — ForEach + `@item()`

| # | Action |
|---|--------|
| 1 | Click **`ForEachFolderJobs`** → Settings → Items = `@pipeline().parameters.folder_jobs` |
| 2 | Open inner activity **`GetFolderMetadata`** |
| 3 | Dataset parameter `folder_path` = `@item().incoming` |
| 4 | Open **`LogItemResult`** → Value = expression using `item().label` and `item().incoming` |

### Step 4 — Debug and read output

| # | Action |
|---|--------|
| 1 | **Debug** → set parameters above → **OK** |
| 2 | **Monitor** → open run → **`LogItemResult`** (second iteration) |
| 3 | **Output** → `foreach_log` array, e.g. `main-incoming | @dataset folder_path←item().incoming=incoming/run=session3-lab | files=3` |

Also check **`SetBaselineFileCount`** output for `baseline_file_count` from the pipeline-parameter path.

---

## Part E — Expression reference

```
# Dataset definition (on the dataset object)
@dataset().folder_path

# Pipeline activity — single path (before ForEach)
@pipeline().parameters.default_incoming

# ForEach — current array element
@item()                    # whole object
@item().incoming           # property → passed to dataset parameter
@item().label              # used in Append variable log line

# Read metadata from activity inside ForEach
@activity('GetFolderMetadata').output.childItems
@length(activity('GetFolderMetadata').output.childItems)
```

---

## Part F — Related pipelines

| Pipeline | `@dataset()` via | `@item()` via |
|----------|------------------|---------------|
| **`pl_ex_12_dataset_foreach_item`** | pipeline param + ForEach | `folder_jobs[].incoming` |
| **`pl_04_foreach_dates`** | Copy datasets | `@item().incoming` / `@item().loaded` |
| **`pl_db_05_copy_then_notebook`** | `folder_path` + `file_name` | — (no ForEach) |
| **`pl_ex_04_metadata_output_expressions`** | `@pipeline().parameters.incoming_folder` | — |

**Deploy / run:**

```cmd
cd d:\azure\shared-adf-lab
.\orchestrate.cmd
.\orchestrate.cmd --run-pipeline pl_ex_12_dataset_foreach_item
```
