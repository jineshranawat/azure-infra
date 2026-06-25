# Session 2 — Trainer guide (2 hours)

Maps to [azure.html](../azure.html) **Day 2: Azure Data Factory** (Hours 17–24, compressed).

**Prerequisite:** Learners completed Class-1 / Session 1 (`orchestrate.cmd` at repo root).

**Learner handout:** [SESSION2-STUDENT-GUIDE.md](SESSION2-STUDENT-GUIDE.md) — **Path M (portal)** or **Path S (script)**; students bored with code should use **Path M** only.  
**Portal detail:** [MANUAL-LAB.md](MANUAL-LAB.md)

---

## Pre-class (5 min)

- [ ] Confirm `orchestrate.cmd` succeeded at repo root (ADF + storage in `rg-<learner>-class1`)
- [ ] Learners `cd session-2`
- [ ] Explain: Session 2 adds **ingestion orchestration** on top of the medallion lake

---

## Block 1 — ADF anatomy (20 min)

**Objective:** Speak ADF fluently — pipelines, activities, datasets, linked services.

| Min | Do | Portal |
|-----|-----|--------|
| 5 | Whiteboard: source → ADF → bronze (link to Day 1 lake) | — |
| 10 | Open `infra/platform-services.bicep` ADF section | RG → Data Factory |
| 5 | Show linked service `AdlsBronzeLinkedService` | ADF → Manage → Linked services |

**Learners do:** [MANUAL-LAB §A + §D](MANUAL-LAB.md) — find resources, test linked service, tick verify boxes.

**Talking point:** Integration runtime types — we use Azure IR only (no self-hosted = no VM cost).

---

## Block 2 — Bronze ingest lab (30 min)

**Objective:** Move data into bronze via Python; run-stamped paths.

```text
orchestrate.cmd
```

| Min | Do | Portal |
|-----|-----|--------|
| 5 | Run command; watch phases 1–3 | — |
| 10 | Open `scripts/bronze_loader.py` — path logic | Storage → bronze → incoming |
| 10 | Learners find `incoming/transactions/<run_id>/` | Upload blade |
| 5 | Discuss idempotency — re-run same command | Same paths overwritten |

**Exercise:** Re-run `orchestrate.cmd` — confirm no duplicate folders (same run_id pattern).

**Learners do:** [MANUAL-LAB §C](MANUAL-LAB.md#c-verify-storage-after-orchestratecmd-10-min) — verify incoming path + watermark in portal.

---

## Block 3 — Pipelines as code (30 min)

**Objective:** Define ADF artefacts via SDK, not portal clicks.

| Min | Do | Portal |
|-----|-----|--------|
| 10 | Walk `scripts/adf_pipeline.py` top to bottom | — |
| 10 | Re-run `orchestrate.cmd` — `create_or_update` idempotent | ADF → Author → pipeline `pl_bronze_copy` |
| 10 | Show datasets `ds_bronze_incoming_csv`, `ds_bronze_loaded_csv` | ADF → Datasets |

**Talking point:** Pipeline change = code commit + review (BoE change control).

**Learners do:** [MANUAL-LAB §E](MANUAL-LAB.md#e-adf--datasets--pipeline-15-min) — open datasets and pipeline in Author; complete verify table.

**Optional advanced:** [MANUAL-LAB §G](MANUAL-LAB.md#g-manual-adf--build-copy-pipeline-by-hand-optional-30-min) — build pipeline entirely in portal.

---

## Block 4 — Operate & monitor (25 min)

**Objective:** Trigger a run; read history from Python.

```text
orchestrate.cmd --run-pipeline
```

| Min | Do | Portal |
|-----|-----|--------|
| 5 | Run with `--run-pipeline` | ADF → Monitor |
| 10 | Open `scripts/morning_check.py` | Compare to portal runs |
| 10 | Verify `bronze/loaded/run=<id>/` after success | Storage → loaded |

**If pipeline fails:** RBAC propagation — wait 2 min, re-run. See [MANUAL-LAB §F3](MANUAL-LAB.md#f3-if-run-failed--common-fixes).

**Learners do:** [MANUAL-LAB §F + §H](MANUAL-LAB.md#f-manual-adf--trigger-pipeline-run-15-min) — Trigger now in studio OR `--run-pipeline`; compare Monitor to morning check.

---

## Block 5 — Cost & checkpoint (15 min)

| Min | Do |
|-----|-----|
| 5 | ADF pricing: factory free, pay per activity run |
| 5 | Show `bronze/_control/watermark.json` |
| 5 | Deliverables — [MANUAL-LAB §I](MANUAL-LAB.md#i-end-to-end-verification-checklist) full portal checklist |

---

## Portal walk order (after lab)

1. ADF → **Author** → pipeline `pl_bronze_copy`
2. ADF → **Monitor** → pipeline runs
3. Storage → **bronze** → incoming / loaded / _control
4. Cost Management → filter `rg-<learner>-class1`

---

## NFR tags this session

| NFR | How exercised |
|-----|----------------|
| Integrity | Watermark + copy validation discussion |
| Observability | Morning check script |
| Cost governance | ADF idle vs run cost |
| Auditability | Run-stamped paths, pipeline as code |

---

## AI chat prompt (learners)

```text
Session 2 ADF lab — error after orchestrate.cmd --run-pipeline:
[paste last 10 lines]
LEARNER=xxx, RG=rg-xxx-class1 (no secrets)
Which script and portal blade should I check?
```
