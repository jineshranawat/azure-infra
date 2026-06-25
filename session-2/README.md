# Session 2 — Azure Data Factory: Orchestration & Ingestion

**Duration:** 2 hours practical | **Source:** [azure.html](../azure.html) Day 2 (Hours 17–24, compressed)

**Prerequisite:** Class-1 + ADF deployed from repo root (`orchestrate.cmd`).

**Navigation:** [COVERAGE-MAP.md](../COVERAGE-MAP.md) — steps 10–12 | **Classroom guide (start here):** [SESSION2-STUDENT-GUIDE.md](SESSION2-STUDENT-GUIDE.md) | **Portal steps:** [MANUAL-LAB.md](MANUAL-LAB.md) | **20h ADF course:** [adf-course/README.md](adf-course/README.md)

## A. What & why (read first)

### Azure Data Factory (ADF)

**What:** Cloud orchestration for moving and transforming data between systems.  
**Why:** Repeatable ingestion — one pipeline serves many feeds via parameters.  
**Analogy:** A train timetable — schedules when data moves; engines idle until a run starts.  
**Code:** `infra/platform-services.bicep` (factory) + `scripts/adf_pipeline.py` (pipeline as code).

### Linked service

**What:** Connection definition from ADF to ADLS Gen2.  
**Why:** Pipelines reference linked services, not raw connection strings.  
**Code:** Bicep `AdlsBronzeLinkedService` + SDK update in `adf_pipeline.py`.

### Dataset & copy activity

**What:** Dataset = shape/location of data; copy activity = move from source dataset to sink.  
**Why:** Parametrised paths mean one pipeline copies any run folder.  
**Code:** `ds_bronze_incoming_csv`, `ds_bronze_loaded_csv`, `pl_bronze_copy`.

### Bronze loader (Python)

**What:** Uploads synthetic CSV to `bronze/incoming/` before ADF copies to `bronze/loaded/`.  
**Why:** Operators ingest via code — idempotent, logged, testable (Day 1 Hour 13 + Day 2 Hour 18).  
**Code:** `scripts/bronze_loader.py`.

### Watermark control file

**What:** JSON in `bronze/_control/watermark.json` tracking last successful run.  
**Why:** Foundation for incremental loads — high-watermark pattern (Day 2 Hour 20).  
**Code:** `scripts/watermark_store.py`.

### Morning check

**What:** Python script listing ADF pipeline runs in the last 24 hours.  
**Why:** Answer "did last night's loads succeed?" without opening the portal (Hour 23).  
**Code:** `scripts/morning_check.py`.

---

## B. How to run (Windows)

**Students:** follow [SESSION2-STUDENT-GUIDE.md](SESSION2-STUDENT-GUIDE.md) in class (Blocks 0–5).

```text
cd session-2
orchestrate.cmd
```

Optional flags:

```text
orchestrate.cmd --morning-check    # query ADF run history (Block 5 / Hour 23)
orchestrate.cmd --run-pipeline     # trigger ADF copy (small activity-run cost)
```

**First run:** creates the repo-root `.venv` if missing (1–2 min pip install) — does **not** re-run the full Class-1 deploy.

**Re-run (idempotent):** same command — skips ADF deploy when artefacts exist, RBAC check-before-create, upload overwrite.

**Manual portal steps (read → do → verify):** [MANUAL-LAB.md](MANUAL-LAB.md) — Storage upload, ADF linked service, datasets, pipeline trigger, and verification tables.

---

## C. Two-hour schedule

| Time | Block | Activity | Portal |
|------|-------|----------|--------|
| 0:00–0:20 | **1 — ADF anatomy** | §A then §D — find RG → Studio + linked service | ADF → Manage |
| 0:20–0:50 | **2 — Bronze ingest** | §B upload **or** `orchestrate.cmd` + §C verify | Storage → bronze |
| 0:50–1:20 | **3 — Pipeline as code** | §G build **or** §E verify (not both) | ADF → datasets + pipeline |
| 1:20–1:45 | **4 — Operate** | [MANUAL-LAB §F](MANUAL-LAB.md#f-manual-adf--trigger-pipeline-run-15-min--block-4) Trigger now + Monitor | ADF → Monitor |
| 1:45–2:00 | **5 — Checkpoint** | [MANUAL-LAB §I](MANUAL-LAB.md#i-end-to-end-verification-checklist--block-5) full checklist | Cost Management |

Trainer detail: [GUIDE.md](GUIDE.md) | **Portal handout:** [MANUAL-LAB.md](MANUAL-LAB.md)

---

## D. Deliverables checklist

Use [MANUAL-LAB.md §I](MANUAL-LAB.md#i-end-to-end-verification-checklist--block-5) for portal verification tables.

**MANUAL-LAB section order:** Block 1 = **§A then §D** (sequential steps). Block 2 = **§B** (portal) or **§C** (after script). Block 3 = **§G** or **§E** (pick one). See [MANUAL-LAB section map](MANUAL-LAB.md#section-map-read-in-order--letters-are-not-interchangeable).

- [ ] `orchestrate.cmd` exits 0
- [ ] `bronze/incoming/transactions/<run_id>/sample_transactions.csv` exists
- [ ] ADF pipeline `pl_bronze_copy` visible in studio
- [ ] `bronze/_control/watermark.json` updated
- [ ] Morning check prints run history (or notes no runs yet)
- [ ] Optional: pipeline run **Succeeded** after `--run-pipeline`

---

## E. Files

| Path | Purpose |
|------|---------|
| `orchestrate.cmd` | Session 2 entry point |
| `scripts/run_session2.py` | Phase orchestrator |
| `scripts/adf_pipeline.py` | Deploy linked service, datasets, pipeline |
| `scripts/bronze_loader.py` | Upload synthetic feed |
| `scripts/watermark_store.py` | Incremental control file |
| `scripts/morning_check.py` | Pipeline run report |
| `data/sample_transactions.csv` | Synthetic banking feed |
| `SESSION2-STUDENT-GUIDE.md` | **Classroom handout — start here** (agenda, ADF UI, steps) |
| `MANUAL-LAB.md` | Portal micro-steps and verify tables |

---

## F. Cost note

ADF factory idle ≈ £0. `--run-pipeline` triggers copy activity runs (free-tier allowance on training subs). **Do not** create self-hosted integration runtimes or mapping data flows in this session.

---

## G. Failures & workarounds

| Symptom | Fix |
|---------|-----|
| ADF not found | Run `..\orchestrate.cmd` (full lab) first |
| Storage upload 403 | Re-run root `orchestrate.cmd` (RBAC); wait 2 min for propagation |
| Pipeline run failed | Check ADF MI has *Storage Blob Data Contributor* — re-run session `orchestrate.cmd` |
| No runs in morning check | Expected until `--run-pipeline`; use `orchestrate.cmd --morning-check` |
| Import errors | Re-run `orchestrate.cmd` (auto-creates `.venv` + pip install) |
| Script appears hung | First run loads Azure SDK (~30s); ensure `az login` via repo root `orchestrate.cmd` |
| `az datafactory` timeout | Fixed in latest `adf_rbac.py` — uses SDK; `git pull` and re-run |

Use Cursor / VS Code AI chat to explain errors — paste terminal output, never secrets. See [README §1](../README.md#when-to-use-cursor--vs-code-ai-chat).
