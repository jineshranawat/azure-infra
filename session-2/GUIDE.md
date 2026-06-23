# Session 2 — Trainer guide (2 hours)

Maps to [azure.html](../azure.html) **Day 2: Azure Data Factory** (Hours 17–24, compressed).

**Prerequisite:** Learners completed Class-1 / Session 1 (`orchestrate.cmd` at repo root).

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

---

## Block 3 — Pipelines as code (30 min)

**Objective:** Define ADF artefacts via SDK, not portal clicks.

| Min | Do | Portal |
|-----|-----|--------|
| 10 | Walk `scripts/adf_pipeline.py` top to bottom | — |
| 10 | Re-run `orchestrate.cmd` — `create_or_update` idempotent | ADF → Author → pipeline `pl_bronze_copy` |
| 10 | Show datasets `ds_bronze_incoming_csv`, `ds_bronze_loaded_csv` | ADF → Datasets |

**Talking point:** Pipeline change = code commit + review (BoE change control).

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

**If pipeline fails:** RBAC propagation — wait 2 min, re-run. Check dead-letter concept (Hour 22) as discussion only.

---

## Block 5 — Cost & checkpoint (15 min)

| Min | Do |
|-----|-----|
| 5 | ADF pricing: factory free, pay per activity run |
| 5 | Show `bronze/_control/watermark.json` |
| 5 | Deliverables checklist from [README.md](README.md) |

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
