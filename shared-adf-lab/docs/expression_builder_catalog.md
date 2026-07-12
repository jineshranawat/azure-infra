# ADF Expression Builder — Complete Trainer Guide

**Purpose:** One folder, one document — teach **every ADF expression concept** used in this lab with **click-by-click Studio UI** steps.

**ADF Studio folder:** `11-expression-builder` (all pipelines below appear here after deploy)  
**Code:** `scripts/adf_expressions.py` (pure expression labs), `scripts/adf_databricks.py` (Databricks + expressions), `scripts/expression_folders.py` (folder map)  
**Deploy:** `shared-adf-lab\orchestrate.cmd`

**Related:** Databricks notebook parameter mapping → `docs/databricks_integration_catalog.md`

---

## Part A — WHAT & WHY

### Expression builder

**What:** The **Add dynamic content** editor in ADF Studio — you write formulas starting with `@` that resolve at pipeline run time.

**Why:** FinLedger loads need dynamic paths (`incoming/run=2026-07-12`), branch logic (only run if files exist), and pass values between activities without hard-coding.

**Analogy:** Spreadsheet formulas — cell `=CONCAT(A1,"/",B1)` is like `@concat(pipeline().parameters.folder, '/', pipeline().parameters.file)`.

### How to open it (UI)

| Step | Action |
|------|--------|
| 1 | **Author** → open a pipeline → click an activity |
| 2 | Click any field that accepts dynamic values (parameters, variables, paths) |
| 3 | Click **Add dynamic content** below the field **or** type `@` in the field |
| 4 | Browse **System variables**, **Pipeline parameters**, **Activity outputs**, **Functions** |
| 5 | Click a function to insert; edit nested parts in the text box |
| 6 | **Save** pipeline → **Debug** or **Trigger now** to evaluate |

> **Tip:** Wrong activity name in `@activity('Name')` is the #1 classroom error — names are **case-sensitive** and must match the activity **Name** property exactly.

---

## Part B — Expression language reference (all concepts)

### B.1 Pipeline context

| Expression | Returns | Example pipeline |
|------------|---------|------------------|
| `@pipeline().parameters.<name>` | Trigger parameter value | `pl_ex_01`, `pl_db_02` |
| `@pipeline().RunId` | Unique run GUID | `pl_ex_02`, `pl_db_03` |
| `@pipeline().DataFactory` | Factory name | `pl_ex_02`, `pl_db_03` |
| `@pipeline().TriggerName` | Trigger that started run | `pl_db_03` |
| `@pipeline().TriggerTime` | Trigger timestamp | (use in custom params) |
| `@pipeline().GroupId` | Run group id | Parent/child runs |

### B.2 Activity outputs

| Expression | Returns | Example pipeline |
|------------|---------|------------------|
| `@activity('<Name>').output` | Full output object | `pl_ex_04` |
| `@activity('<Name>').output.childItems` | Metadata file list | `pl_ex_04`, `pl_ex_09` |
| `@activity('<Name>').output.itemName` | Metadata item name | `pl_ex_04` |
| `@activity('<Name>').output.firstRow.<col>` | Lookup first row column | `pl_ex_05`, `pl_db_08` |
| `@activity('<Name>').output.value` | Filter activity output | `pl_11` |
| `@activity('<Name>').status` | Activity status | Debugging only |

### B.3 Pipeline variables

| Expression | Returns | Example pipeline |
|------------|---------|------------------|
| `@variables('<name>')` | Variable set earlier in run | `pl_ex_06`, `pl_ex_09` |
| Set with **Set variable** activity + **Expression** type value | | `pl_ex_01`–`05` |

### B.3a Global parameters (factory-wide)

| Expression | Returns | Example pipeline |
|------------|---------|------------------|
| `@pipeline().globalParameters.<name>` | Factory global default | `pl_ex_11_debug_print_variables` |
| Set under **Manage → Global parameters** | Deployed by `adf_global_parameters.py` | `g_lab_environment`, `g_trainer_tag` |

**Debug / print variables:** full click-by-click guide → `docs/debug_print_variables_guide.md`

### B.4 ForEach context

| Expression | Returns | Example pipeline |
|------------|---------|------------------|
| `@item()` | Current array element | `pl_ex_07`, `pl_db_07` |
| `@item().<property>` | Property when item is object | `pl_04` (`incoming`, `loaded`) |

### B.5 String functions

| Function | Syntax | Example pipeline |
|----------|--------|------------------|
| `concat` | `@concat('a', 'b', variables('x'))` | `pl_ex_03`, `pl_db_02` |
| `string` | `@string(length(...))` | `pl_ex_04` |
| `toUpper` | `@toUpper(pipeline().parameters.channel)` | `pl_ex_03` |
| `toLower` | `@toLower(...)` | (same pattern) |
| `trim` | `@trim(...)` | Whitespace cleanup |
| `substring` | `@substring(pipeline().parameters.path, 0, 10)` | Path parsing |
| `replace` | `@replace(variables('path'), 'bronze', 'silver')` | Path rewrite |

### B.6 Conversion functions

| Function | Syntax | Example pipeline |
|----------|--------|------------------|
| `int` | `@int(variables('file_count'))` | `pl_ex_09` |
| `float` | `@float('12.5')` | Numeric compare |
| `bool` | `@bool('true')` | Parameter coercion |
| `json` | `@json(activity('Web').output.Response)` | REST parse |
| `string` | `@string(guid())` | Store GUID in variable |

### B.7 Collection functions

| Function | Syntax | Example pipeline |
|----------|--------|------------------|
| `length` | `@length(activity('GetMetadata').output.childItems)` | `pl_ex_04` |
| `createArray` | `@createArray('a','b')` | Inline arrays |
| `first` | `@first(variables('arr'))` | Array head |
| `union` | `@union(createArray('a'), createArray('b'))` | Merge arrays |

### B.8 Logical & comparison

| Function | Syntax | Example pipeline |
|----------|--------|------------------|
| `equals` | `@equals(variables('job_status'), 'READY')` | `pl_ex_06` |
| `greater` | `@greater(int(variables('file_count')), 0)` | `pl_ex_09` |
| `less` | `@less(...)` | Threshold checks |
| `and` | `@and(equals(...), greater(...))` | `pl_ex_09` |
| `or` | `@or(equals(a,'x'), equals(a,'y'))` | Multi-branch |
| `not` | `@not(equals(pipeline().parameters.force_empty, true))` | `pl_ex_09` |
| `if` | `@if(equals(status,'READY'), 'go', 'stop')` | Inline ternary |

### B.9 Date/time & misc

| Function | Syntax | Example pipeline |
|----------|--------|------------------|
| `utcnow` | `@utcnow()` | `pl_ex_02`, `pl_db_03` |
| `guid` | `@guid()` | `pl_ex_02`, `pl_db_03` |
| `coalesce` | `@coalesce(activity('X').output.itemName, 'none')` | `pl_ex_04` |
| `adddays` | `@adddays(utcnow(), -1)` | Watermark windows |
| `formatDateTime` | `@formatDateTime(utcnow(), 'yyyy-MM-dd')` | Partition paths |

### B.10 Child pipeline parameters

On **Execute Pipeline** activity, set child parameter **Value** to expression:

```json
{
  "value": "@concat('parent→child-', pipeline().parameters.lab_tag)",
  "type": "Expression"
}
```

Example: `pl_ex_10`, `pl_db_10`

### B.11 Databricks notebook base parameters

Same `@` syntax in **Databricks Notebook** activity → **Base parameters** → each value can be dynamic.

Example: `pl_db_02` — `@pipeline().parameters.run_id` becomes `dbutils.widgets.get("run_id")` in the notebook.

---

## Part C — Folder `11-expression-builder` — all pipelines

After `orchestrate.cmd`, refresh ADF Studio → expand **Pipelines** → **`11-expression-builder`**.

### C.1 Pure expression labs (`pl_ex_01`–`pl_ex_10`)

| Pipeline | Concepts taught |
|----------|-----------------|
| `pl_ex_01_pipeline_param_variables` | `@pipeline().parameters.*` → Set Variable |
| `pl_ex_02_system_context_variables` | `@pipeline().RunId`, `@utcnow()`, `@guid()` |
| `pl_ex_03_concat_string_functions` | `@concat`, nested `@variables`, `@toUpper` |
| `pl_ex_04_metadata_output_expressions` | `@activity().output`, `@length`, `@coalesce` |
| `pl_ex_05_lookup_json_expressions` | `@activity().output.firstRow.*` |
| `pl_ex_06_if_equals_compare` | `@equals`, If Condition + variables |
| `pl_ex_07_foreach_item_expressions` | `@item()`, `@concat` in Append Variable |
| `pl_ex_08_switch_route_expressions` | `@pipeline().parameters.route` on Switch |
| `pl_ex_09_logic_and_or_not` | `@and`, `@not`, `@greater`, `@int` |
| `pl_ex_10_child_pipeline_expressions` | Execute Pipeline + expression child params |
| `pl_ex_10_child_echo_params` | (child — called by pl_ex_10) |

### C.2 Databricks + expressions (`pl_db_01`–`pl_db_10`)

| Pipeline | Concepts taught |
|----------|-----------------|
| `pl_db_01_static_notebook` | Static params (baseline — no `@`) |
| `pl_db_02_pipeline_param_expressions` | Dynamic notebook params from pipeline |
| `pl_db_03_system_expressions` | RunId / utcnow / guid → notebook |
| `pl_db_04_metadata_to_notebook` | Get Metadata output → notebook |
| `pl_db_05_copy_then_notebook` | `@concat` lake path → notebook |
| `pl_db_06_if_then_notebook` | If gates notebook on file count |
| `pl_db_07_foreach_channels` | ForEach + `@item()` → notebook per channel |
| `pl_db_08_lookup_watermark_notebook` | Lookup → notebook params |
| `pl_db_09_wait_then_notebook` | Ordering before notebook |
| `pl_db_10_child_pipeline_notebook` | Parent Execute Pipeline + notebook child |

### C.3 Core lab pipelines (also in this folder)

| Pipeline | Expression highlight |
|----------|---------------------|
| `pl_03_if_then_copy` | If on metadata `childItems` length |
| `pl_04_foreach_dates` | `@item().incoming`, `@item().loaded` |
| `pl_05_lookup_watermark` | Lookup JSON → Set Variable |
| `pl_11_filename_validate_route` | Filter output → If → expressions |
| `pl_17_switch_channel` | Switch on `@pipeline().parameters.channel` |
| `pl_18_until_file_ready` | Until + `@variables('file_ready')` |
| `pl_19_append_audit_log` | Append Variable with `@string()` |
| `pl_21_fail_on_bad_param` | If on `@equals(pipeline().parameters.force_fail, true)` |
| `pl_23_incremental_watermark_copy` | Lookup + `@activity().output.firstRow` on Copy |

---

## Part D — UI walkthrough: build `pl_ex_01` from scratch

### D.1 Create folder (one-time)

1. **Author** → **Pipelines** header → **`⋯`** → **New folder** → `11-expression-builder`

### D.2 Create pipeline

1. **`⋯`** on `11-expression-builder` → **New pipeline** → name `pl_ex_01_pipeline_param_variables`
2. **Parameters** tab → **+ New**:

   | Name | Type | Default |
   |------|------|---------|
   | `run_id` | String | `session3-lab` |
   | `lab_tag` | String | `ex01-params` |

3. **Variables** tab → **+ New**:

   | Name | Type |
   |------|------|
   | `captured_run_id` | String |
   | `captured_lab_tag` | String |

### D.3 Set Variable activities

**Activity 1 — `CaptureRunId`**

1. Drag **Set variable** onto canvas
2. **Name:** `CaptureRunId`
3. **Variable name:** `captured_run_id`
4. **Value:** click field → **Add dynamic content** → **Pipeline parameters** → `run_id`  
   - Result: `@pipeline().parameters.run_id`

**Activity 2 — `CaptureLabTag`**

1. Drag **Set variable** → name `CaptureLabTag`
2. **Depends on:** `CaptureRunId` → **Succeeded**
3. **Variable name:** `captured_lab_tag`
4. **Value:** `@pipeline().parameters.lab_tag`

### D.4 Debug

1. **Debug** → open **Output** tab → each Set Variable shows resolved values
2. **Publish** (optional) → **Trigger now**

---

## Part E — UI walkthrough: expression on If Condition (`pl_ex_06`)

1. Open `pl_ex_06_if_equals_compare` (or build new)
2. **Set variable** `SetStatusFromParam` → `job_status` = `@pipeline().parameters.status`
3. Drag **If Condition** → name `IfStatusReady`
4. **Expression:** **Add dynamic content** → **Functions** → `equals`  
   - Or type: `@equals(variables('job_status'), 'READY')`
5. **True activities:** Set variable `branch_taken` = `ready-branch`
6. **False activities:** Set variable `branch_taken` = `hold-branch`
7. **Trigger now** with `status` = `READY` vs `HOLD` — show different branches

---

## Part F — UI walkthrough: ForEach + `@item()` (`pl_ex_07`)

1. Pipeline parameter `channels` → type **Array** → default `["card","wire","fps"]`
2. Pipeline variable `channel_audit` → type **Array**
3. Drag **ForEach** → **Items:** `@pipeline().parameters.channels`
4. Inside ForEach: **Append variable** → variable `channel_audit`
5. **Value:** `@concat('processed-', item())`
6. **Debug** → output shows array `["processed-card","processed-wire","processed-fps"]`

---

## Part G — UI walkthrough: Lookup + firstRow (`pl_ex_05`)

1. Drag **Lookup** → name `LookupWatermark`
2. **Source dataset:** `ds_audit_watermark_json`
3. **First row only:** checked
4. **Set variable** `last_run` =  
   `@activity('LookupWatermark').output.firstRow.last_run`
5. **Set variable** `watermark` =  
   `@activity('LookupWatermark').output.firstRow.last_successful_watermark`

---

## Part H — UI walkthrough: Databricks notebook dynamic param (`pl_db_02`)

1. Open `pl_db_02_pipeline_param_expressions`
2. Click **Databricks Notebook** activity
3. **Base parameters** → `triggered_by` → **Add dynamic content**:

   ```text
   @concat('adf-', pipeline().parameters.lab_tag)
   ```

4. **Debug** → open Databricks run → stdout shows `triggered_by: adf-db02-params`

---

## Part I — Suggested teaching order

```text
Day 1 — Language basics
  pl_ex_01 → pl_ex_02 → pl_ex_03

Day 2 — Activity outputs & control flow
  pl_ex_04 → pl_ex_05 → pl_ex_06 → pl_ex_08 → pl_ex_09

Day 3 — Iteration & orchestration
  pl_ex_07 → pl_ex_10 → pl_04 → pl_05

Day 4 — Databricks integration + expressions
  pl_db_01 → pl_db_02 → pl_db_03 → pl_db_04 → pl_db_05

Day 5 — Advanced patterns
  pl_db_06 → pl_db_07 → pl_db_08 → pl_db_10 → pl_11 → pl_23
```

---

## Part J — Quick run commands

```cmd
cd shared-adf-lab
.\orchestrate.cmd
.\orchestrate.cmd --run-pipeline pl_ex_01_pipeline_param_variables
.\orchestrate.cmd --run-from-pipeline pl_ex_01_pipeline_param_variables
.\orchestrate.cmd --warm-cluster-only
.\orchestrate.cmd --run-pipeline pl_db_02_pipeline_param_expressions
```

---

## Part K — Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Unable to parse expression` | Missing `@` or unbalanced `()` | Start with `@`; count parentheses |
| `Activity 'X' not found` | Wrong name in `@activity('X')` | Match activity **Name** on canvas |
| Expression shows literal `@pipeline()...` in notebook | Forgot dynamic content toggle | Re-enter via **Add dynamic content** |
| `firstRow` is null | Lookup returned 0 rows | Check dataset path / file exists |
| ForEach 0 iterations | Parameter not Array type | Parameters tab → type **Array** |
| Variable empty in If | Set Variable runs after If | Fix **Depends on** ordering |
| Pipelines not in folder | Stale deploy | Run full `.\orchestrate.cmd` (not `--run-only`) |

---

## Part L — Concept coverage map

| Expression concept | Pipeline(s) |
|--------------------|-------------|
| Pipeline parameters | `pl_ex_01`, `pl_db_02`, `pl_17` |
| System RunId / utcnow / guid | `pl_ex_02`, `pl_db_03` |
| concat / string | `pl_ex_03`, `pl_db_02`, `pl_db_05` |
| Activity output | `pl_ex_04`, `pl_db_04`, `pl_11` |
| Lookup firstRow | `pl_ex_05`, `pl_db_08`, `pl_05`, `pl_23` |
| variables() | `pl_ex_06`, `pl_ex_09`, `pl_18` |
| equals / if | `pl_ex_06`, `pl_03`, `pl_db_06` |
| greater / int | `pl_ex_09`, `pl_db_06` |
| and / or / not | `pl_ex_09` |
| item() / foreach | `pl_ex_07`, `pl_db_07`, `pl_04` |
| switch | `pl_ex_08`, `pl_17` |
| coalesce | `pl_ex_04`, `pl_db_04` |
| append variable | `pl_ex_07`, `pl_19` |
| child pipeline params | `pl_ex_10`, `pl_db_10` |
| Databricks base params | `pl_db_01`–`pl_db_10` |

*Last aligned with code: `adf_expressions.py`, `adf_databricks.py`, `expression_folders.py` — folder `11-expression-builder`.*
