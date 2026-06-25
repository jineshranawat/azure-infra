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

**Normal classroom:** trainer runs `orchestrate.cmd` before class; students **verify in portal** — see guide **START HERE** section.

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

**Default classroom = script pre-run; students verify + trigger in the portal.**

| Time | Block | Students do in portal | Portal |
|------|-------|-----------------------|--------|
| 0:00–0:20 | **1 — ADF anatomy** | [lab-a](MANUAL-LAB.md#lab-a) then [lab-d](MANUAL-LAB.md#lab-d) — find RG → Studio + linked service | ADF → Manage |
| 0:20–0:50 | **2 — Bronze ingest** | [lab-c](MANUAL-LAB.md#lab-c) verify the file + watermark the script landed | Storage → bronze |
| 0:50–1:20 | **3 — Pipeline** | [lab-e](MANUAL-LAB.md#lab-e) inspect `pl_bronze_copy` (do not rebuild) | ADF → Author |
| 1:20–1:45 | **4 — Operate** | [lab-f](MANUAL-LAB.md#lab-f) **Trigger now** + Monitor | ADF → Monitor |
| 1:45–2:00 | **5 — Checkpoint** | [lab-h](MANUAL-LAB.md#lab-h) run history + [lab-i](MANUAL-LAB.md#lab-i) checklist | Cost Management |

No-script fallback: Block 2 → [lab-b](MANUAL-LAB.md#lab-b), Block 3 → [lab-g](MANUAL-LAB.md#lab-g).

Trainer detail: [GUIDE.md](GUIDE.md) | **Portal handout:** [MANUAL-LAB.md](MANUAL-LAB.md)

---

## D. Deliverables checklist

Use [MANUAL-LAB lab-i](MANUAL-LAB.md#lab-i) for portal verification tables.

**MANUAL-LAB section order (jump links `lab-a` … `lab-i`):** Block 1 = [lab-a](MANUAL-LAB.md#lab-a) then [lab-d](MANUAL-LAB.md#lab-d) (sequential). Block 2 = [lab-c](MANUAL-LAB.md#lab-c) (verify script) or [lab-b](MANUAL-LAB.md#lab-b) (portal upload). Block 3 = [lab-e](MANUAL-LAB.md#lab-e) (verify) or [lab-g](MANUAL-LAB.md#lab-g) (build by hand).

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
