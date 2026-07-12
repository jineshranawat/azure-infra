# ADF User Properties — When, Why & How (FinLedger Example)

**Pipeline:** `pl_ex_13_user_properties_monitor`  
**ADF folder:** `11-expression-builder`  
**Deploy:** `shared-adf-lab\orchestrate.cmd`

---

## Part A — WHAT are user properties?

**User properties** are **custom name/value tags on an activity** that appear in **Monitor** after each run.

| Concept | Static? | Where defined | Shows in Monitor? |
|---------|---------|---------------|-------------------|
| **Annotations** | Yes | Pipeline / dataset object | No (organize in Author only) |
| **Pipeline parameters** | No | Pipeline | In run input, not as filter columns |
| **User properties** | No (can be expressions) | **Each activity** | **Yes — filterable columns** |

**Analogy:** Annotations are sticky labels on a folder in the filing cabinet. User properties are **barcodes scanned at each processing step** — operations can search “show me all activities where `job_name=finledger-bronze-load` failed.”

---

## Part B — WHEN to use user properties

Use them when you need **operations / support / monitoring**, not business logic:

| Scenario | User property example |
|----------|----------------------|
| Filter failed copies by source table | `source_path` = `@concat(...)` |
| Track which FinLedger job step ran | `job_name` = `finledger-bronze-load` |
| Distinguish ForEach iterations in Monitor | `channel` = `@item()` |
| Tag environment (dev/test/prod) | `environment` = `@pipeline().globalParameters.g_lab_environment` |
| Correlate to pipeline run | `pipeline_run_id` = `@pipeline().RunId` |
| Phase of ETL (discover / transform / load) | `phase` = `discover` / `load` |

**Do NOT use user properties for:**

- Passing data between activities → use **variables**, **activity outputs**, or **parameters**
- Controlling if/switch logic → use **expressions** on the activity itself
- Replacing logging tables → they are **monitor metadata only** (max **5 per activity**)

---

## Part C — FinLedger scenario (why this lab pipeline exists)

**Problem:** FinLedger runs `pl_ex_13` with 3 ForEach channel tags + 1 copy. In Monitor you see generic activity names (`CopyBronzeWithTags`, `TagChannelStep`) — hard to know **which job**, **which folder**, **which channel** failed.

**Solution:** Attach user properties so Monitor grid shows:

| Activity | User properties (examples) |
|----------|---------------------------|
| `ListIncomingWithTags` | `phase=discover`, `job_name`, `source_folder`, `environment`, `run_id` |
| `TagChannelStep` (inside ForEach) | `phase=foreach-tag`, `channel=@item()`, `job_name`, `run_id` |
| `CopyBronzeWithTags` | `phase=load`, `source_path`, `destination_path`, `pipeline_run_id` |

Support filters: “show all `phase=load` failures for `job_name=finledger-bronze-load`.”

---

## Part D — Pipeline walkthrough

```
ListIncomingWithTags     ← user props: discover phase, folders, environment
    ↓
ForEachChannelTags       ← items: ["card","wire","fps"]
    └── TagChannelStep   ← user prop channel = @item()  (different per iteration!)
    ↓
CopyBronzeWithTags       ← user props: source_path, destination_path, load phase
```

### Debug parameters

| Parameter | Example |
|-----------|---------|
| `run_id` | `session3-lab` |
| `job_name` | `finledger-bronze-load` |
| `incoming_folder` | `incoming/run=session3-lab` |
| `loaded_folder` | `loaded/run=session3-lab` |
| `channels` | `["card","wire","fps"]` |

---

## Part E — ADF Studio steps (add user properties by hand)

### Step 1 — Open activity

| # | Action |
|---|--------|
| 1 | **Author** → `pl_ex_13_user_properties_monitor` |
| 2 | Click activity **`CopyBronzeWithTags`** |
| 3 | Tab **User properties** (next to General / Settings) |

### Step 2 — Add a property

| # | Action |
|---|--------|
| 1 | Click **+ New** |
| 2 | **Name** = `job_name` |
| 3 | **Value** = type expression manually (no expression builder button): |

```
@pipeline().parameters.job_name
```

> **Note:** User property values use the same `@...` expression language. Type directly in the Value box.

### Step 3 — Recommended properties for Copy activities

| Name | Value expression |
|------|------------------|
| `phase` | `@string('load')` |
| `job_name` | `@pipeline().parameters.job_name` |
| `source_path` | `@concat(pipeline().parameters.incoming_folder, '/sample_transactions.csv')` |
| `destination_path` | `@concat(pipeline().parameters.loaded_folder, '/sample_transactions.csv')` |
| `pipeline_run_id` | `@pipeline().RunId` |

### Step 4 — ForEach iteration tagging

On inner activity **`TagChannelStep`**:

| Name | Value |
|------|--------|
| `channel` | `@item()` |
| `job_name` | `@pipeline().parameters.job_name` |

Each ForEach iteration writes a **different** `channel` value in Monitor.

---

## Part F — View in Monitor (the payoff)

| # | Action |
|---|--------|
| 1 | **Debug** pipeline with parameters above |
| 2 | **Monitor** → **Pipeline runs** → open run |
| 3 | Activity list → click **Columns** / **Add column** |
| 4 | Select user properties: `job_name`, `channel`, `source_path`, `phase` |
| 5 | Grid now filters/sorts by your custom tags |

You can also filter failed activities by `job_name` without opening JSON output.

---

## Part G — User properties vs other metadata

| Feature | Purpose | Dynamic? | Monitor columns |
|---------|---------|----------|-----------------|
| **Annotations** | Group pipelines in Author | Static tags | No |
| **Global parameters** | Factory-wide config | Static default | No |
| **Pipeline parameters** | Input per run | Yes | Run input only |
| **Variables** | In-run state | Yes | Activity output |
| **User properties** | **Ops / monitoring labels** | Yes | **Yes** |

---

## Part H — Expression cheat sheet

```
@pipeline().parameters.job_name
@pipeline().parameters.incoming_folder
@pipeline().globalParameters.g_lab_environment
@pipeline().RunId
@item()                                    ← inside ForEach
@concat(pipeline().parameters.incoming_folder, '/sample_transactions.csv')
@string('load')
```

---

## Part I — Run commands

```cmd
cd d:\azure\shared-adf-lab
.\orchestrate.cmd
.\orchestrate.cmd --run-pipeline pl_ex_13_user_properties_monitor
```

**Code:** `scripts/adf_expressions.py` → `pl_ex_13_user_properties_monitor`
