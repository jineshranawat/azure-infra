# Session 2 — Trainer guide (2 hours)

Maps to [azure.html](../azure.html) **Day 2: Azure Data Factory** (Hours 17–24, compressed).

**Learner handout:** [SESSION2-STUDENT-GUIDE.md](SESSION2-STUDENT-GUIDE.md) — students work **in the portal**; script is pre-run homework.  
**Portal detail:** [MANUAL-LAB.md](MANUAL-LAB.md) — [jump links](MANUAL-LAB.md#jump-links) (`lab-a` … `lab-i`)

---

## The one picture (read this first)

### Before class — YOU run the script (once per learner)

```text
cd session-2
orchestrate.cmd
orchestrate.cmd --run-pipeline
```

| Phase | Script | What students will find in Azure |
|---|---|---|
| 1 | `adf_rbac.py` | ADF managed identity → storage RBAC |
| 2 | `adf_pipeline.py` | `pl_bronze_copy`, datasets, linked service |
| 3 | `bronze_loader.py` | CSV in `bronze/incoming/transactions/<run_id>/` |
| 5 | `watermark_store.py` | `bronze/_control/watermark.json` |
| 4 (optional) | `--run-pipeline` | One **Succeeded** run in Monitor (demo) |

**Students do not need to run the terminal in class** unless troubleshooting.

### In class — STUDENTS work in the portal

| Block | Time | Trainer does | Students do |
|---|---|---|---|
| **1** | 20 min | Whiteboard FinLedger; open Bicep ADF bit | [lab-a](MANUAL-LAB.md#lab-a) → [lab-d](MANUAL-LAB.md#lab-d) |
| **2** | 30 min | Ask: "Where did the file land?" | [lab-c](MANUAL-LAB.md#lab-c) verify upload + watermark |
| **3** | 30 min | Show `adf_pipeline.py` on screen (optional) | [lab-e](MANUAL-LAB.md#lab-e) inspect `pl_bronze_copy` |
| **4** | 25 min | **They** click Trigger now | [lab-f](MANUAL-LAB.md#lab-f) + Monitor |
| **5** | 15 min | Cost + case study wrap | [lab-h](MANUAL-LAB.md#lab-h) + [lab-i](MANUAL-LAB.md#lab-i) |

**Talking point (Block 2):** TXN-10003 — £50k **pending** wire — fraud / silver-layer discussion.

---

## Pre-class checklist (trainer)

- [ ] Class-1 deployed (`orchestrate.cmd` at repo root)
- [ ] Each learner: `session-2\orchestrate.cmd` exit 0
- [ ] Optional: `--run-pipeline` so Monitor already has a run
- [ ] Students have [SESSION2-STUDENT-GUIDE.md](SESSION2-STUDENT-GUIDE.md) open
- [ ] Portal login works on classroom machines

---

## Block 1 — ADF anatomy (20 min)

**Objective:** ADF vocabulary — linked service, dataset, pipeline, IR, MSI.

| Min | Trainer | Portal |
|-----|---------|--------|
| 5 | FinLedger story: morning file → bronze → ADF → loaded | — |
| 5 | `infra/platform-services.bicep` — factory + linked service | RG → Data Factory |
| 10 | Live demo: Studio panes | ADF → Manage → Linked services |

**Students:** [lab-a](MANUAL-LAB.md#lab-a) then [lab-d](MANUAL-LAB.md#lab-d) — tick verify tables.

**Talking point:** Azure IR only — no self-hosted VM cost.

---

## Block 2 — Bronze verify (30 min)

**Objective:** Students **find** what the script uploaded — not upload again.

**Students:** [lab-c](MANUAL-LAB.md#lab-c) — incoming path, 5-row preview, watermark JSON.

| Min | Trainer |
|-----|---------|
| 5 | "Script ran last night — prove the file is there" |
| 15 | Walk Storage → bronze with one learner screen-shared |
| 10 | Discuss `TXN-10003` pending — production relevance |

**If script did not run:** students use [lab-b](MANUAL-LAB.md#lab-b) OR you run `orchestrate.cmd` live.

---

## Block 3 — Pipeline inspect (30 min)

**Objective:** Map `adf_pipeline.py` to Author blades — **do not rebuild** in portal.

**Students:** [lab-e](MANUAL-LAB.md#lab-e) — datasets, `pl_bronze_copy`, parameters, JSON view.

| Min | Trainer |
|-----|---------|
| 10 | Screen-share `scripts/adf_pipeline.py` — `create_or_update` |
| 15 | Students open same objects in Author |
| 5 | Pipeline as code = change control |

**Portal-only cohort:** [lab-g](MANUAL-LAB.md#lab-g) instead (30 min hand-build).

---

## Block 4 — Operate (25 min)

**Objective:** Every student triggers **their own** pipeline run and reads Monitor.

**Students:** [lab-f](MANUAL-LAB.md#lab-f) — **Trigger now** with their `run_id` parameters.

| Min | Trainer |
|-----|---------|
| 5 | Demo Trigger now once |
| 15 | Students trigger; you circulate |
| 5 | Verify loaded path — 5 rows |

**If 403:** RBAC propagation — wait 2 min, [lab-f3](MANUAL-LAB.md#lab-f3).

---

## Block 5 — Checkpoint (15 min)

| Min | Do |
|-----|-----|
| 5 | Monitor run history — [lab-h](MANUAL-LAB.md#lab-h) |
| 5 | Cost Management — ADF idle ≈ £0 |
| 5 | [lab-i](MANUAL-LAB.md#lab-i) checklist — all tick before leave |

**FinLedger wrap:** "Tomorrow's incremental load reads `watermark.json` — Module 1 course."

---

## Script reference (when students ask "what did it do?")

| File | One line |
|---|---|
| `run_session2.py` | Orchestrates phases 1–6 |
| `adf_rbac.py` | ADF MI → Storage Blob Data Contributor (SDK, not `az`) |
| `adf_pipeline.py` | Deploys linked service, datasets, `pl_bronze_copy` |
| `bronze_loader.py` | Uploads `sample_transactions.csv` with run_id path |
| `watermark_store.py` | Writes `_control/watermark.json` |
| `morning_check.py` | Lists runs (`--morning-check`) |

---

## Portal walk order (end of session)

1. ADF → **Author** → `pl_bronze_copy`
2. ADF → **Monitor** → student's run **Succeeded**
3. Storage → **bronze** → incoming / loaded / `_control`
4. Cost Management → `rg-<learner>-class1`

---

## AI chat prompt (learners)

```text
Session 2 ADF lab — pipeline run failed in Monitor:
[paste error from activity output]
LEARNER=xxx, RG=rg-xxx-class1, run_id=xxx (no secrets)
Which portal blade should I check?
```
