# ADF Triggers — Tumbling Window & Storage Event (Detailed Scenarios)

**ADF folder:** `12-triggers`  
**Deploy:** `shared-adf-lab\orchestrate.cmd`  
**Code:** `scripts/adf_triggers.py`

---

## Part A — WHAT & WHY (plain language)

### Manual trigger vs automated trigger

| Mode | Who starts the pipeline | Example in this lab |
|------|-------------------------|---------------------|
| **Debug / Trigger now** | You click a button | Any pipeline in Studio |
| **Tumbling window trigger** | ADF clock — fixed time slices | `tr_tumbling_hourly_bronze` |
| **Storage event trigger** | Azure Storage — file lands in lake | `tr_storage_bronze_incoming_csv` |

**Analogy**

- **Tumbling window** = a bus that leaves every hour on the hour. Each trip handles only passengers who arrived during that hour (files modified in that window).
- **Storage event** = a doorbell. When a new `.csv` file appears in `bronze/incoming/`, ADF runs immediately.

---

## Part B — FinLedger scenario (why we use each)

### Scenario 1 — Tumbling window (scheduled batch)

**Business need:** The bank receives transaction files throughout the day, but downstream SQL reporting only needs an **hourly batch** — not a run on every single file drop.

**Flow:**

1. Every hour ADF opens a new **window** (e.g. 09:00–10:00 UTC).
2. Trigger passes `windowStart` and `windowEnd` into `pl_trig_01_tumbling_window_bronze`.
3. Copy activity reads `sample_transactions.csv` only if it was **modified inside that window**.
4. Loaded path: `bronze/loaded/run=session3-lab/`.

**When to use:** Predictable schedule, backfill-friendly, cost control (not event-per-file).

### Scenario 2 — Storage event (event-driven)

**Business need:** A partner uploads `sample_transactions.csv` to ADLS at unpredictable times. FinLedger must **react within minutes** when the file appears.

**Flow:**

1. Partner (or lab script) uploads to `bronze/incoming/run=session3-lab/*.csv`.
2. Azure Storage emits `Microsoft.Storage.BlobCreated`.
3. ADF trigger `tr_storage_bronze_incoming_csv` fires.
4. Pipeline `pl_trig_02_storage_event_copy` logs the event metadata and copies the arrived file to `loaded/`.

**When to use:** Near-real-time ingest, file arrival is the signal.

---

## Part C — What was deployed

### Pipelines

| Pipeline | Trigger | Purpose |
|----------|---------|---------|
| `pl_trig_01_tumbling_window_bronze` | `tr_tumbling_hourly_bronze` | Hourly window copy (modified-time filter) |
| `pl_trig_02_storage_event_copy` | `tr_storage_bronze_incoming_csv` | Copy on blob created |

### Triggers (deployed **Stopped** — trainer starts in Studio)

| Trigger | Type | Schedule / event |
|---------|------|------------------|
| `tr_tumbling_hourly_bronze` | **Tumbling window** | Every **1 hour**, from 2026-01-01 |
| `tr_storage_bronze_incoming_csv` | **Blob events** | `BlobCreated` under `/bronze/incoming/run=session3-lab/*.csv` |

> Triggers are **Stopped** after deploy so the class estate does not run hourly jobs until you enable them.

---

## Part D — Tumbling window (step-by-step in ADF Studio)

### D.1 Open the trigger

| # | Action |
|---|--------|
| 1 | **Author** → **Triggers** (left under Factory Resources) |
| 2 | Open **`tr_tumbling_hourly_bronze`** |
| 3 | Type = **Tumbling window** |

### D.2 Trigger settings

| Setting | Lab value | Meaning |
|---------|-----------|---------|
| Frequency | Hourly | Window size unit |
| Interval | 1 | Every 1 hour |
| Start date | 2026-01-01 | When scheduling begins |
| End date | 2099-12-31 | Far-future end |
| Max concurrency | 1 | One window at a time (teaching) |

### D.3 Parameters passed to pipeline (expressions)

| Pipeline parameter | Trigger expression |
|------------------|-------------------|
| `window_start` | `@trigger().outputs.windowStartTime` |
| `window_end` | `@trigger().outputs.windowEndTime` |
| `incoming_folder` | `incoming/run=session3-lab` |
| `loaded_folder` | `loaded/run=session3-lab` |

### D.4 Pipeline activities

| Activity | What it teaches |
|----------|-----------------|
| `CaptureWindowRange` | Logs window into variable `window_audit_line` |
| `CopyFilesInWindow` | Copy with `modified_datetime_start/end` = window params |

### D.5 Test without waiting an hour

| # | Action |
|---|--------|
| 1 | Open pipeline **`pl_trig_01_tumbling_window_bronze`** |
| 2 | **Debug** with manual parameters: |
| | `window_start` = `2026-07-12T00:00:00Z` |
| | `window_end` = `2026-07-12T01:00:00Z` |
| 3 | **Monitor** → `CaptureWindowRange` → Output |

### D.6 Enable the trigger (trainer)

| # | Action |
|---|--------|
| 1 | **Triggers** → `tr_tumbling_hourly_bronze` → toggle **Start** |
| 2 | **Publish all** |
| 3 | Next hour boundary → pipeline runs automatically |

**CLI manual test:**

```cmd
cd d:\azure\shared-adf-lab
.\orchestrate.cmd --run-pipeline pl_trig_01_tumbling_window_bronze
```

---

## Part E — Storage event trigger (step-by-step)

### E.1 Open the trigger

| # | Action |
|---|--------|
| 1 | **Author** → **Triggers** → **`tr_storage_bronze_incoming_csv`** |
| 2 | Type = **Storage events** |

### E.2 Trigger settings

| Setting | Lab value |
|---------|-----------|
| Storage account | `stsharedqgr7mj` (auto from scope) |
| Event | **Blob created** |
| Blob path begins with | `/bronze/incoming/run=session3-lab/` |
| Blob path ends with | `.csv` |
| Ignore empty blobs | Yes |

### E.3 Parameters from event (expressions)

| Pipeline parameter | Trigger expression |
|------------------|-------------------|
| `trigger_file_name` | `@trigger().outputs.body.fileName` |
| `trigger_folder_path` | `@trigger().outputs.body.folderPath` |
| `incoming_folder` | `incoming/run=session3-lab` |
| `loaded_folder` | `loaded/run=session3-lab` |

### E.4 FinLedger test scenario

| # | Action |
|---|--------|
| 1 | **Start** trigger `tr_storage_bronze_incoming_csv` → **Publish all** |
| 2 | Upload a file (simulates partner drop): |

```cmd
cd d:\azure\shared-adf-lab
.\orchestrate.cmd
```

(Re-uploads `sample_transactions.csv` to incoming — or upload any `.csv` via Storage Explorer.)

| 3 | **Monitor** → **Trigger runs** → open `tr_storage_bronze_incoming_csv` run |
| 4 | Open `pl_trig_02_storage_event_copy` → `LogStorageEvent` → see `event_audit_line` |

**CLI manual test (no event):**

```cmd
.\orchestrate.cmd --run-pipeline pl_trig_02_storage_event_copy
```

---

## Part F — Expression reference

### Tumbling window (trigger → pipeline)

```
@trigger().outputs.windowStartTime
@trigger().outputs.windowEndTime
@pipeline().parameters.window_start
@pipeline().parameters.window_end
```

### Storage event (trigger → pipeline)

```
@trigger().outputs.body.fileName
@trigger().outputs.body.folderPath
@pipeline().parameters.trigger_file_name
```

### Dataset (inside pipeline — same as other labs)

```
@dataset().folder_path          ← on dataset definition
@pipeline().parameters.incoming_folder   ← activity passes value
```

---

## Part G — Compare trigger types

| | Tumbling window | Storage event |
|--|-----------------|---------------|
| **Driver** | Clock | File landing |
| **Latency** | Up to 1 hour (hourly) | Seconds |
| **Parameters** | Window start/end | File name, folder path |
| **Cost** | Predictable schedule | Per event (+ pipeline run) |
| **Backfill** | Built-in (rerun windows) | Re-upload or replay event |
| **FinLedger use** | Hourly regulatory batch | Partner file arrival |

---

## Part H — Troubleshooting

| Issue | Fix |
|-------|-----|
| Trigger won't start | **Publish all** after deploy |
| Storage trigger never fires | Confirm path `/bronze/incoming/run=session3-lab/` and `.csv` suffix |
| Event Grid permission | ADF needs event subscription on storage — Studio creates on Start |
| Tumbling run empty copy | File `last modified` outside window — widen window in Debug test |
| Hourly runs cost concern | Keep trigger **Stopped** until class demo |

---

## Part I — Re-run & idempotency

```cmd
cd d:\azure\shared-adf-lab
.\orchestrate.cmd
```

Safe to re-run: pipelines and triggers update in place; triggers remain **Stopped** after deploy.

**Code:** `scripts/adf_triggers.py`  
**Pipelines:** `pl_trig_01_tumbling_window_bronze`, `pl_trig_02_storage_event_copy`  
**Triggers:** `tr_tumbling_hourly_bronze`, `tr_storage_bronze_incoming_csv`
