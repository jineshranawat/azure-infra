# Trainer guide — ADF course (20+ hours)

**Audience:** Instructors facilitating [FinLedger UK](CASE-STUDY.md) hands-on labs.  
**Learner handout pattern:** Every lesson uses **Read → Do → Verify** (same as [Session 2 MANUAL-LAB.md](../MANUAL-LAB.md)).

---

## How learners use this course

| Step | What | Where |
|---|---|---|
| 1. Read | Concept + why before clicking | Lesson **Why this matters** |
| 2. Do | Atomic portal steps | Lesson **Part A** |
| 3. Verify | Tick boxes — storage, Monitor, row counts | Lesson **Part D** + [VERIFICATION-CHECKLIST.md](docs/VERIFICATION-CHECKLIST.md) |
| 4. Code twin | JSON + Python/REST | Parts **B** and **C** |
| 5. Data | Upload sample files | [`data/`](data/README.md) per module |

**Microsoft Learn:** Each lesson cites `source_url` in [manifest.json](manifest.json). Trainer should skim the official tutorial before class — learners follow the expanded click-level version.

---

## Pre-course setup (trainer)

- [ ] Learners completed [SETUP.md](SETUP.md) — subscription, `uksouth`, naming
- [ ] Repo cloned; path to `session-2/adf-course/data/` confirmed
- [ ] Optional: Class-1 `orchestrate.cmd` already run — map names in CASE-STUDY
- [ ] Budget alert set (£25 MTD recommended)
- [ ] Trainer has portal access to demo subscription for screen share

**Single entry point (Session 2 automation):** `session-2/orchestrate.cmd` — not required for full 20h course but bridges Class-1 learners.

---

## Facilitation model

### Read → Do → Verify (every lesson)

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ READ concept│ ──► │ DO Part A   │ ──► │ VERIFY Part D│
│ + case study│     │ upload data │     │ + checklist  │
└─────────────┘     └─────────────┘     └─────────────┘
```

**Trainer script:** "Before you click, tell me what artifact we're creating. After the run, show me the Monitor blade and the blob path."

### Verification vs validation

| | Verification | Validation |
|---|---|---|
| Question | Did it run without error? | Is the data correct for FinLedger? |
| Where | ADF Monitor, activity status | Storage Preview, row counts, business rules |
| Example | Copy activity green | 12 rows in `transactions_daily`, no duplicate keys |

---

## Module facilitation notes

### Module 0 — Foundations (~150 min)

| Lesson | Trainer focus | Sample data | Primary verify |
|---|---|---|---|
| 00-00 | Mental model whiteboard | None | Learner names 6 ADF components |
| 00-01 | ADLS Gen2 HNS on | `data/module-00-foundations/upload_manifest.txt` | Container `bronze`, HNS enabled |
| 00-02 | Factory V2, UK South | None | Factory blade **Succeeded** |
| 00-03 | Studio tour — every icon | None | Learner finds Author/Manage/Monitor |
| 00-04 | Linked service vs dataset | None | Oral quiz |
| 00-05 | MSI + test connection | None | Green **Connection successful** |

**MS Learn:** [Introduction](https://learn.microsoft.com/en-us/azure/data-factory/introduction), [Quickstart create factory](https://learn.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory)

---

### Module 1 — Copy & ingest (~250 min)

**Story beat:** Land FinLedger daily transactions and master data into bronze.

| Lesson | Activity type | Data file(s) | Verify |
|---|---|---|---|
| 01-01 | Copy Data **wizard** | `transactions_daily.csv` | File in `bronze/loaded/transactions/` |
| 01-02 | **Copy** activity manual | `customers.csv` | Pipeline JSON matches UI |
| 01-03 | Parameters on datasets | `products.csv` | One pipeline, parametrised path |
| 01-04 | Self-hosted IR (demo/sim) | — | IR status **Running** (or documented skip) |
| 01-05 | **If** + retry policies | inject bad path | Failure branch sends alert / second path |
| 01-06 | S3 → ADLS (or emulator) | partner feed sample | Rows landed |
| 01-07 | Watermark incremental | `incremental/customers_batch*.csv` | `_control/watermark.json` updated |
| 01-08 | Change tracking intro | SQL sample (Azure SQL optional) | Only delta rows copied |

**Trainer tip:** Start each lesson by uploading the module's file to `bronze/incoming/...` *before* opening ADF Studio — learners see the full ingest story.

---

### Module 2 — Mapping data flows (~210 min)

**Story beat:** Cleanse messy transactions; wrangle returns into silver.

| Lesson | Activity type | Data file(s) | Verify |
|---|---|---|---|
| 02-01 | **Execute Data Flow** debug | `transactions_messy.csv` | Debug cluster stopped after session |
| 02-02 | Source → transform → sink | `transactions_daily.csv` | Parquet/CSV in `silver/transactions/` |
| 02-03 | Derived column, drift | `transactions_messy.csv` | Invalid `amount_gbp` filtered |
| 02-04 | **Delta** sink | silver output | `_delta_log` folder exists |
| 02-05 | **Power Query** wrangling | `returns_raw.csv` | Normalised reasons in silver |
| 02-06 | Partition by `value_date` | silver output | Multiple partition folders |

> ⚠️ WARNING: Data flows bill per vCore-hour — **mandatory tear-down** at end of each Module 2 lab block.

---

### Module 3 — Control flow (~180 min)

**Story beat:** Nightly orchestration driven by manifest file.

| Lesson | Activity type | Data file(s) | Verify |
|---|---|---|---|
| 03-01 | **Lookup**, **Set Variable**, **Web** | `pipeline_parameters.json` | Variable populated from JSON |
| 03-02 | **ForEach**, **If**, **Until**, **Switch** | `file_manifest.json` | Only `enabled:true` files copied |
| 03-03 | Dynamic expressions | parameters file | `@pipeline().parameters` resolves |
| 03-04 | **Triggers** (schedule + tumbling) | — | Next run time visible on trigger |
| 03-05 | Monitor + email / Logic App | — | Test email received |

**Lab script:** ForEach reads `file_manifest.json` from `bronze/incoming/manifests/` — learners upload from `data/module-03-control-flow/`.

---

### Module 4 — External compute (~150 min)

**Story beat:** Score high-value accounts in Databricks / Spark.

| Lesson | Activity type | Data file(s) | Verify |
|---|---|---|---|
| 04-01 | **Databricks Notebook** | `scoring_input.csv` | Notebook output in `gold/` |
| 04-02 | **HDInsight Spark** | `scoring_input.csv` | Spark job Succeeded |
| 04-03 | **HDInsight Hive** | silver tables | Hive table query returns rows |

> ⚠️ WARNING: Highest cost module — use smallest cluster SKUs; delete clusters in tear-down.

**MPN / quota:** If Databricks quota is 0, document manual workaround (portal preview only) — do not hard-fail the cohort.

---

### Module 5 — Networking & security (~130 min)

**Story beat:** Connect to on-prem SQL without public exposure.

| Lesson | Activity type | Data | Verify |
|---|---|---|---|
| 05-01 | Managed VNet concepts | — | Diagram oral check |
| 05-02 | Copy in managed VNet | `customers.csv` | Managed IR used |
| 05-03 | Managed private endpoint | SQL (lab DB) | Test connection green |
| 05-04 | **Key Vault** linked service | secret in KV | No secrets in pipeline JSON |

---

### Module 6 — Governance & CI/CD (~150 min)

| Lesson | Activity type | Data | Verify |
|---|---|---|---|
| 06-01 | Git publish | ADF repo | Commit visible in GitHub/DevOps |
| 06-02 | ARM deploy | `sample_arm_parameters.json` | Dev → prod parameterised |
| 06-03 | Purview lineage | pipelines | Lineage graph in Purview |
| 06-04 | SSIS IR | — | Package runs (or documented skip) |
| 06-05 | Cost alerts | — | Budget alert fired in test |

---

## Timing template (full-day workshop)

| Block | Duration | Module |
|---|---|---|
| Morning 1 | 2.5 h | 0 + 1 (01-01 – 01-03) |
| Morning 2 | 2.5 h | 1 (01-04 – 01-08) |
| Afternoon 1 | 2.5 h | 2 |
| Afternoon 2 | 2.5 h | 3 |
| Day 2 | 5 h | 4 + 5 |
| Day 3 | 3 h | 6 + Capstone |

Self-paced: follow manifest order at listed minute budgets.

---

## Common blockers (trainer cheat sheet)

| Symptom | Fix | Lesson |
|---|---|---|
| Connection test failed MSI | Storage RBAC **Storage Blob Data Contributor** for factory MI | 00-05 |
| Copy 403 | Wrong container or path — compare to CASE-STUDY paths | 01-* |
| IR not running | Start self-hosted IR service on VM | 01-04 |
| Data flow debug won't start | Region capacity — retry or smaller TTL | 02-* |
| ForEach zero iterations | Manifest path wrong — verify blob upload | 03-02 |
| Databricks link failed | PAT expired or workspace URL typo | 04-01 |

---

## Assessment rubric (optional)

| Criterion | Meets | Exceeds |
|---|---|---|
| Portal | Completes verify checklist | Explains blade without guide |
| JSON | Pastes valid pipeline from Part B | Commits to Git with naming convention |
| Code | Runs Part C script | Integrates into own automation |
| Case study | Lands FinLedger transactions | Capstone runs end-to-end unattended |

---

## Related repo docs

| Doc | Purpose |
|---|---|
| [CASE-STUDY.md](CASE-STUDY.md) | Business narrative + lake layout |
| [data/README.md](data/README.md) | Sample files + upload paths |
| [docs/VERIFICATION-CHECKLIST.md](docs/VERIFICATION-CHECKLIST.md) | Master learner tick-list |
| [session-2/GUIDE.md](../GUIDE.md) | 2-hour Session 2 trainer notes |
| [session-2/MANUAL-LAB.md](../MANUAL-LAB.md) | 2-hour portal handout |

---

## Generation status

Lessons are generated per [manifest.json](manifest.json). Trainer guide applies to all lessons — when a lesson file is `todo`, use this guide's module table + MS Learn `source_url` until the lesson body exists.
