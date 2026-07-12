# Debug / Print Variables in ADF — Step-by-Step Guide

**Pipeline:** `pl_ex_11_debug_print_variables`  
**ADF folder:** `11-expression-builder`  
**Deploy:** `shared-adf-lab\orchestrate.cmd`

---

## Part A — WHAT & WHY

### There is no `print()` in ADF

Azure Data Factory does not have a console. When you debug, you **capture values into pipeline variables** and read them from **Monitor** (or optionally surface them in a **Fail** activity message).

| Concept | Expression | Scope |
|---------|------------|-------|
| **Global parameter** | `@pipeline().globalParameters.g_lab_environment` | Whole factory — set once under **Manage → Global parameters** |
| **Pipeline parameter** | `@pipeline().parameters.run_id` | One pipeline — set at trigger / Debug |
| **Pipeline variable** | `@variables('v_debug_line')` | One run — set with **Set variable** / **Append variable** activities |
| **System context** | `@pipeline().RunId`, `@pipeline().DataFactory` | Built-in per run |

**Analogy:** Global parameters = company-wide settings; pipeline parameters = form fields on one workflow; variables = sticky notes you write during the run.

---

## Part B — Deploy (trainer, once)

```cmd
cd d:\azure\shared-adf-lab
.\orchestrate.cmd
```

This publishes:

| Global parameter | Default value |
|------------------|---------------|
| `g_lab_environment` | `shared-class1` |
| `g_trainer_tag` | `finledger-adf-lab` |

---

## Part C — Run in ADF Studio (learner steps)

### Step 1 — Open the pipeline

| # | Action |
|---|--------|
| 1 | Portal → **Data factories** → `adf-shared-qgr7mj` → **Open Azure Data Factory Studio** |
| 2 | Left menu **Author** (pencil icon) |
| 3 | Expand **Pipelines** → folder **`11-expression-builder`** |
| 4 | Click **`pl_ex_11_debug_print_variables`** |

### Step 2 — Inspect parameters & variables (design time)

| # | Action |
|---|--------|
| 1 | Bottom panel → tab **Parameters** — see `run_id`, `show_debug_fail` |
| 2 | Tab **Variables** — see `v_global_env`, `v_debug_line`, `debug_steps`, etc. |
| 3 | Click activity **`CaptureGlobalEnvironment`** → **Variables** tab → value uses **Add dynamic content**: `@string(pipeline().globalParameters.g_lab_environment)` |
| 4 | Click **`BuildDebugLine`** → note the `@concat(...)` expression combining globals, params, and other variables |

### Step 3 — Debug (print to Monitor)

| # | Action |
|---|--------|
| 1 | Click **Debug** (top toolbar) |
| 2 | Parameters dialog: leave `run_id` = `session3-lab`, `show_debug_fail` = **false** |
| 3 | Click **OK** — wait for green checkmarks |
| 4 | Left menu **Monitor** → **Pipeline runs** → open the latest run |
| 5 | Click activity **`BuildDebugLine`** |
| 6 | Tab **Output** (or **JSON**) — find `v_debug_line` value, e.g. `DEBUG | global_env=shared-class1 | global_tag=finledger-adf-lab | run_id=session3-lab | factory=adf-shared-qgr7mj | pipeline_run=<guid>` |

That output **is** your printed debug line.

### Step 4 — View the array variable

| # | Action |
|---|--------|
| 1 | Same run → click **`AppendDebugStep`** |
| 2 | **Output** shows `debug_steps` array with one element (the same debug string) |

### Step 5 — Optional: “loud print” via Fail message

Use when you want the error banner to show the full string (teaching only).

| # | Action |
|---|--------|
| 1 | **Author** → `pl_ex_11_debug_print_variables` → **Debug** |
| 2 | Set `show_debug_fail` = **true** |
| 3 | Run fails on **`PrintViaFailMessage`** — open that activity in Monitor |
| 4 | **Error** tab shows `DEBUG-PRINT` and the full `@variables('v_debug_line')` text |

> Normal pipelines should **not** use Fail for logging — use Monitor Output. This branch exists only to show where the message appears.

---

## Part D — Build the same pipeline by hand (UI recipe)

Follow this order if students create from scratch:

### D.1 Global parameters (factory level)

| # | Action |
|---|--------|
| 1 | **Manage** (toolbox icon) → **Global parameters** → **+ New** |
| 2 | Name `g_lab_environment`, Type **String**, Default `shared-class1` → **OK** |
| 3 | **+ New** → Name `g_trainer_tag`, Type **String**, Default `finledger-adf-lab` → **OK** |
| 4 | **Publish all** |

### D.2 Pipeline shell

| # | Action |
|---|--------|
| 1 | **Author** → **+** → **Pipeline** |
| 2 | Name `pl_ex_11_debug_print_variables` |
| 3 | **Parameters** → **+ New**: `run_id` (String, default `session3-lab`) |
| 4 | **+ New**: `show_debug_fail` (Bool, default false) |
| 5 | **Variables** → add String variables: `v_global_env`, `v_global_tag`, `v_run_id`, `v_factory`, `v_pipeline_run_id`, `v_debug_line`, `v_status` |
| 6 | **+ New** Array variable: `debug_steps` |

### D.3 Set variable activities (chain left → right)

For each activity: drag **Set variable** from **Activities** → **General**.

**Activity `CaptureGlobalEnvironment`**

| Field | Value |
|-------|-------|
| Variable name | `v_global_env` |
| Value type | **Expression** |
| Value | `@string(pipeline().globalParameters.g_lab_environment)` |

**Activity `CaptureGlobalTrainerTag`** (depends on previous)

| Field | Value |
|-------|-------|
| Variable name | `v_global_tag` |
| Value | `@string(pipeline().globalParameters.g_trainer_tag)` |

**Activity `CaptureRunIdParam`**

| Field | Value |
|-------|-------|
| Variable name | `v_run_id` |
| Value | `@pipeline().parameters.run_id` |

**Activity `CaptureFactoryName`**

| Field | Value |
|-------|-------|
| Variable name | `v_factory` |
| Value | `@pipeline().DataFactory` |

**Activity `CapturePipelineRunId`**

| Field | Value |
|-------|-------|
| Variable name | `v_pipeline_run_id` |
| Value | `@pipeline().RunId` |

**Activity `BuildDebugLine`**

| Field | Value |
|-------|-------|
| Variable name | `v_debug_line` |
| Value | Click **Add dynamic content** → **Functions** → `concat` and build: |

```
@concat(
  'DEBUG | global_env=', variables('v_global_env'),
  ' | global_tag=', variables('v_global_tag'),
  ' | run_id=', variables('v_run_id'),
  ' | factory=', variables('v_factory'),
  ' | pipeline_run=', variables('v_pipeline_run_id')
)
```

### D.4 Append variable (array log)

| # | Action |
|---|--------|
| 1 | Drag **Append variable** |
| 2 | Name `AppendDebugStep` |
| 3 | Variable name `debug_steps` |
| 4 | Value (Expression) `@variables('v_debug_line')` |

### D.5 If Condition — optional Fail “print”

| # | Action |
|---|--------|
| 1 | Drag **If Condition** → name `IfShowDebugFail` |
| 2 | Expression: `@equals(pipeline().parameters.show_debug_fail, true)` |
| 3 | **True** branch → drag **Fail** → name `PrintViaFailMessage` |
| 4 | Fail **Message** (Expression): `@variables('v_debug_line')` |
| 5 | Fail **Error code**: `DEBUG-PRINT` |
| 6 | **False** branch → **Set variable** `MarkPrintedToMonitor` → `v_status` = `@string('see BuildDebugLine output in Monitor')` |

### D.6 Save & publish

| # | Action |
|---|--------|
| 1 | **Validate** (checkmark on canvas) |
| 2 | **Publish all** |
| 3 | **Debug** with `show_debug_fail` = false → read **BuildDebugLine** output |

---

## Part E — Expression cheat sheet (this pipeline)

```
@pipeline().globalParameters.g_lab_environment     ← global
@pipeline().parameters.run_id                      ← pipeline param
@variables('v_debug_line')                         ← variable set earlier
@pipeline().RunId                                  ← system
@concat('a=', variables('x'), ' b=', variables('y'))  ← build debug string
```

---

## Part F — Code reference

| File | What |
|------|------|
| `scripts/adf_expressions.py` | Pipeline `pl_ex_11_debug_print_variables` |
| `scripts/adf_global_parameters.py` | Deploys `g_lab_environment`, `g_trainer_tag` |
| `scripts/expression_folders.py` | Folder `11-expression-builder` |

**Re-run command:** `shared-adf-lab\orchestrate.cmd` (idempotent)
