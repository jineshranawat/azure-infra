# Azure Data Factory: Every Click, Every Pipeline

A self-paced, **20+ hour** course for working data engineers and analysts. Every UI interaction is written click-by-click. Every pipeline ships as runnable JSON. Every portal flow has a Python, REST, or ARM code twin.

**Location in repo:** `session-2/adf-course/` (companion to the 2-hour [Session 2 lab](../README.md)).

---

## How to use this course

1. Complete [SETUP.md](SETUP.md) once (subscription, naming, region guardrails).
2. Read [CASE-STUDY.md](CASE-STUDY.md) — **FinLedger UK** story used in every module.
3. Work lessons **in order** — each file ends with a link to the next.
4. **Read → Do → Verify:** follow Part A, then tick boxes in Part D and [docs/VERIFICATION-CHECKLIST.md](docs/VERIFICATION-CHECKLIST.md).
5. Upload sample files from [`data/`](data/README.md) before each module lab.
6. Look up terms in [GLOSSARY.md](GLOSSARY.md).
7. Generation progress is tracked in [manifest.json](manifest.json).

**Trainers:** [CLASS-PLAN.md](CLASS-PLAN.md) — run the 20+ hours as **12 portal-first classes** (start here to teach across sessions) · [TRAINER-GUIDE.md](TRAINER-GUIDE.md) — facilitation, activity types, timing.

**Generation status:** **Course complete** — 38/38 lessons + CAPSTONE. See Phase Final audit notes in [CAPSTONE.md](CAPSTONE.md).

---

## Prerequisites

| Skill / asset | Level |
|---|---|
| SQL | Comfortable writing SELECT, WHERE, JOIN |
| Python | Read scripts, set variables, run `pip install` |
| Azure | Subscription with Contributor access; no prior ADF experience assumed |
| Time | ~21 hours at listed budgets (reading + hands-on) |

---

## What makes this course different

| Standard | This course |
|---|---|
| "Create a linked service to Blob storage" | 6–9 atomic clicks with screen-change narration |
| Portal-only walkthrough | Matching JSON + Python/REST/ARM for every artifact |
| 10-minute overview | 1,800–2,800 words per 45-minute lesson budget |
| Tutorial paraphrase | Grounded on [Microsoft Learn ADF tutorials](https://learn.microsoft.com/en-us/azure/data-factory/data-factory-tutorials), expanded with missing detail |
| Disconnected labs | **FinLedger UK** case study — same lake, new activity type each module |
| No sample files | CSV/JSON in [`data/module-*/`](data/README.md) with row-count verification |

---

## FinLedger UK case study

One fictional UK retailer. You build their medallion lake (bronze → silver → gold) across all modules.

| Module | ADF activity focus | Sample data |
|---|---|---|
| 0 | Storage + factory setup | [`data/module-00-foundations/`](data/module-00-foundations/) |
| 1 | **Copy** activities | [`data/module-01-copy-ingest/`](data/module-01-copy-ingest/) |
| 2 | **Mapping data flows** | [`data/module-02-data-flows/`](data/module-02-data-flows/) |
| 3 | **Control flow** (Lookup, ForEach, If…) | [`data/module-03-control-flow/`](data/module-03-control-flow/) |
| 4 | **Databricks / Spark / Hive** | [`data/module-04-external-compute/`](data/module-04-external-compute/) |
| 5 | Managed VNet + Key Vault | — |
| 6 | Git, ARM, Purview | [`data/module-06-governance/`](data/module-06-governance/) |

Full narrative: [CASE-STUDY.md](CASE-STUDY.md) · Master checklist: [docs/VERIFICATION-CHECKLIST.md](docs/VERIFICATION-CHECKLIST.md)

---

## Course structure & time budgets

**Total: 1,260 minutes (21 hours)** across 37 lessons + capstone.

### Module 0 — Foundations & Environment (~150 min)

| ID | Lesson | Budget | File |
|---|---|---:|---|
| 00-00 | Overview & mental model of ADF | 15 min | [00-00-overview.md](module-00-foundations/00-00-overview.md) |
| 00-01 | Create ADLS Gen2 storage account, containers, hierarchical namespace | 30 min | [00-01-create-storage-adls-gen2.md](module-00-foundations/00-01-create-storage-adls-gen2.md) |
| 00-02 | Create the Data Factory instance | 20 min | [00-02-create-data-factory.md](module-00-foundations/00-02-create-data-factory.md) |
| 00-03 | ADF Studio tour — every pane, every icon | 30 min | [00-03-studio-tour-every-pane.md](module-00-foundations/00-03-studio-tour-every-pane.md) |
| 00-04 | Linked services & Integration Runtime concepts | 25 min | [00-04-linked-services-and-integration-runtime.md](module-00-foundations/00-04-linked-services-and-integration-runtime.md) |
| 00-05 | Link ADF to storage, click-by-click | 30 min | [00-05-link-adf-to-storage-step-by-step.md](module-00-foundations/00-05-link-adf-to-storage-step-by-step.md) |

### Module 1 — Copy & Ingest (~250 min)

| ID | Lesson | Budget | File |
|---|---|---:|---|
| 01-01 | Copy Data tool (guided wizard, end to end) | 35 min | [01-01-copy-data-tool.md](module-01-copy-ingest/01-01-copy-data-tool.md) |
| 01-02 | Copy activity built manually in a pipeline | 35 min | [01-02-copy-activity-manual-pipeline.md](module-01-copy-ingest/01-02-copy-activity-manual-pipeline.md) |
| 01-03 | Datasets, linked services, parameters deep dive | 30 min | [01-03-datasets-linked-services-parameters.md](module-01-copy-ingest/01-03-datasets-linked-services-parameters.md) |
| 01-04 | On-prem → cloud with self-hosted IR | 35 min | [01-04-on-prem-to-cloud-self-hosted-ir.md](module-01-copy-ingest/01-04-on-prem-to-cloud-self-hosted-ir.md) |
| 01-05 | Conditional execution, retries & error handling | 30 min | [01-05-conditional-execution-error-handling.md](module-01-copy-ingest/01-05-conditional-execution-error-handling.md) |
| 01-06 | Amazon S3 → ADLS Gen2 | 25 min | [01-06-amazon-s3-to-adls-gen2.md](module-01-copy-ingest/01-06-amazon-s3-to-adls-gen2.md) |
| 01-07 | Incremental copy patterns (overview + watermark) | 30 min | [01-07-incremental-copy-patterns.md](module-01-copy-ingest/01-07-incremental-copy-patterns.md) |
| 01-08 | Change tracking, multiple tables, CDC intro | 30 min | [01-08-change-tracking-multiple-tables-cdc.md](module-01-copy-ingest/01-08-change-tracking-multiple-tables-cdc.md) |

### Module 2 — Mapping Data Flows (~210 min)

| ID | Lesson | Budget | File |
|---|---|---:|---|
| 02-01 | Data flow fundamentals, debug, the canvas | 30 min | [02-01-data-flow-fundamentals-debug-canvas.md](module-02-data-flows/02-01-data-flow-fundamentals-debug-canvas.md) |
| 02-02 | Code-free transformation at scale | 40 min | [02-02-code-free-transformation-at-scale.md](module-02-data-flows/02-02-code-free-transformation-at-scale.md) |
| 02-03 | Expressions, schema drift, derived columns | 35 min | [02-03-expressions-schema-drift-derived-columns.md](module-02-data-flows/02-03-expressions-schema-drift-derived-columns.md) |
| 02-04 | Delta Lake transformations | 35 min | [02-04-delta-lake-transformations.md](module-02-data-flows/02-04-delta-lake-transformations.md) |
| 02-05 | Power Query (wrangling) data flow | 30 min | [02-05-power-query-wrangling-data-flow.md](module-02-data-flows/02-05-power-query-wrangling-data-flow.md) |
| 02-06 | Writing to the lake — partitioning & best practices | 40 min | [02-06-writing-to-lake-partitioning-best-practices.md](module-02-data-flows/02-06-writing-to-lake-partitioning-best-practices.md) |

### Module 3 — Control Flow & Orchestration (~180 min)

| ID | Lesson | Budget | File |
|---|---|---:|---|
| 03-01 | Pipeline activities catalogue | 30 min | [03-01-pipeline-activities-catalogue.md](module-03-control-flow-orchestration/03-01-pipeline-activities-catalogue.md) |
| 03-02 | Control flow: ForEach, If, Until, Switch | 40 min | [03-02-control-flow-foreach-if-until-switch.md](module-03-control-flow-orchestration/03-02-control-flow-foreach-if-until-switch.md) |
| 03-03 | Parameters, variables, expressions & dynamic content | 35 min | [03-03-parameters-variables-expressions-dynamic-content.md](module-03-control-flow-orchestration/03-03-parameters-variables-expressions-dynamic-content.md) |
| 03-04 | Triggers: schedule, tumbling window, event, custom | 40 min | [03-04-triggers-schedule-tumbling-event-custom.md](module-03-control-flow-orchestration/03-04-triggers-schedule-tumbling-event-custom.md) |
| 03-05 | Monitoring + email notifications | 35 min | [03-05-monitoring-email-notifications.md](module-03-control-flow-orchestration/03-05-monitoring-email-notifications.md) |

### Module 4 — External Compute (~150 min)

| ID | Lesson | Budget | File |
|---|---|---:|---|
| 04-01 | Databricks notebook activity from ADF | 50 min | [04-01-databricks-notebook-activity.md](module-04-external-compute/04-01-databricks-notebook-activity.md) |
| 04-02 | HDInsight Spark transformation | 50 min | [04-02-hdinsight-spark-transformation.md](module-04-external-compute/04-02-hdinsight-spark-transformation.md) |
| 04-03 | Hive transformation | 50 min | [04-03-hive-transformation.md](module-04-external-compute/04-03-hive-transformation.md) |

### Module 5 — Networking & Security (~130 min)

| ID | Lesson | Budget | File |
|---|---|---:|---|
| 05-01 | Managed VNet & private endpoints concepts | 30 min | [05-01-managed-vnet-private-endpoints-concepts.md](module-05-networking-security/05-01-managed-vnet-private-endpoints-concepts.md) |
| 05-02 | Copy pipeline inside a managed VNet | 35 min | [05-02-copy-pipeline-managed-vnet.md](module-05-networking-security/05-02-copy-pipeline-managed-vnet.md) |
| 05-03 | On-prem SQL via managed private endpoint | 35 min | [05-03-on-prem-sql-managed-private-endpoint.md](module-05-networking-security/05-03-on-prem-sql-managed-private-endpoint.md) |
| 05-04 | Key Vault, managed identity, RBAC for ADF | 30 min | [05-04-key-vault-managed-identity-rbac.md](module-05-networking-security/05-04-key-vault-managed-identity-rbac.md) |

### Module 6 — Governance, CI/CD & Operations (~150 min)

| ID | Lesson | Budget | File |
|---|---|---:|---|
| 06-01 | Git integration (Azure DevOps / GitHub) for ADF | 30 min | [06-01-git-integration-azure-devops-github.md](module-06-governance-cicd-ops/06-01-git-integration-azure-devops-github.md) |
| 06-02 | ARM templates & CI/CD across dev/test/prod | 40 min | [06-02-arm-templates-cicd-dev-test-prod.md](module-06-governance-cicd-ops/06-02-arm-templates-cicd-dev-test-prod.md) |
| 06-03 | Push lineage to Microsoft Purview | 30 min | [06-03-push-lineage-to-purview.md](module-06-governance-cicd-ops/06-03-push-lineage-to-purview.md) |
| 06-04 | SSIS integration runtime (lift & shift) | 30 min | [06-04-ssis-integration-runtime.md](module-06-governance-cicd-ops/06-04-ssis-integration-runtime.md) |
| 06-05 | Cost monitoring, alerts & operational runbook | 20 min | [06-05-cost-monitoring-alerts-runbook.md](module-06-governance-cicd-ops/06-05-cost-monitoring-alerts-runbook.md) |

### Capstone (~40 min reading + open-ended build)

| ID | Lesson | Budget | File |
|---|---|---:|---|
| CAPSTONE | End-to-end project tying every module together | 40 min | [CAPSTONE.md](CAPSTONE.md) |

---

## Per-lesson anatomy

Every lesson follows the same skeleton:

| Section | Contents |
|---|---|
| What you'll build | Concrete artifact at end of lesson |
| Why this matters | Concept before clicking |
| Key terms | First-appearance definitions → GLOSSARY |
| Architecture at a glance | Mermaid diagram |
| Part A | UI — atomic micro-steps |
| Part B | Complete JSON for every artifact |
| Part C | Python / REST / ARM code twin |
| Part D | Run, validate, read output |
| Common errors & fixes | ≥ 3 real issues |
| Cost & tear-down | Billable resource cleanup |
| Recap & self-check | Summary + questions |

---

## File tree

```text
adf-course/
├── README.md
├── TRAINER-GUIDE.md           ← instructors: read-do-verify facilitation
├── CASE-STUDY.md              ← FinLedger UK narrative
├── manifest.json
├── GLOSSARY.md
├── SETUP.md
├── docs/
│   └── VERIFICATION-CHECKLIST.md
├── data/                      ← sample CSV/JSON per module
│   ├── module-00-foundations/
│   ├── module-01-copy-ingest/
│   └── ...
├── assets/diagrams/
├── module-00-foundations/
├── module-01-copy-ingest/
├── module-02-data-flows/
├── module-03-control-flow-orchestration/
├── module-04-external-compute/
├── module-05-networking-security/
└── module-06-governance-cicd-ops/
```

---

## Microsoft Learn source index

All lessons ground on official tutorials: [ADF tutorials index](https://learn.microsoft.com/en-us/azure/data-factory/data-factory-tutorials). Per-lesson `source_url` values are in [manifest.json](manifest.json).

---

## Start here

**Students — open ONE doc in class:** → [STUDENT-GUIDE.md](STUDENT-GUIDE.md) (per class: what to **Open / Execute / Show**, theory beside practical).

**Teaching across multiple classes?** → [CLASS-PLAN.md](CLASS-PLAN.md) maps these 38 lessons into 12 portal-first classes (~24 h), each with a visual "wow" moment.

Self-paced:

1. [SETUP.md](SETUP.md) — naming, region, cost guardrails
2. [CASE-STUDY.md](CASE-STUDY.md) — business context
3. [00-00 · Overview](module-00-foundations/00-00-overview.md) — first lesson

---

## Progress

| Metric | Value |
|---|---|
| Lessons in manifest | 38 |
| Minutes target | 1,260 |
| Minutes completed | 1,260 |
| Lessons done | **38 / 38** ✓ |

Updated by the generation agent after each lesson.
