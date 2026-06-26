# Session 3 — link map (all correct paths)

Use this file when a link 404s. Paths are relative to **`session-3/`** unless noted.

---

## Start here

| Audience | Open this |
|----------|-----------|
| **Learner in class** | [SESSION3-STUDENT-GUIDE.md](SESSION3-STUDENT-GUIDE.md) |
| **Theory + graphs** | [UI-OVERVIEW.md](UI-OVERVIEW.md) |
| **Portal / Databricks clicks** | [MANUAL-LAB.md](MANUAL-LAB.md) |
| **Trainer** | [GUIDE.md](GUIDE.md) |
| **Programme index** | [../COVERAGE-MAP.md](../COVERAGE-MAP.md) |

---

## MANUAL-LAB jump links (anchors)

| Section | Link |
|---------|------|
| A — Find resources | [MANUAL-LAB.md#lab-a](MANUAL-LAB.md#lab-a) |
| B — UI tour | [MANUAL-LAB.md#lab-b](MANUAL-LAB.md#lab-b) |
| C — Create cluster | [MANUAL-LAB.md#lab-c](MANUAL-LAB.md#lab-c) |
| D — Import notebooks | [MANUAL-LAB.md#lab-d](MANUAL-LAB.md#lab-d) |
| E — Read bronze | [MANUAL-LAB.md#lab-e](MANUAL-LAB.md#lab-e) |
| F — Silver Delta | [MANUAL-LAB.md#lab-f](MANUAL-LAB.md#lab-f) |
| G — Gold Delta | [MANUAL-LAB.md#lab-g](MANUAL-LAB.md#lab-g) |
| H — Storage verify | [MANUAL-LAB.md#lab-h](MANUAL-LAB.md#lab-h) |
| I — Checklist | [MANUAL-LAB.md#lab-i](MANUAL-LAB.md#lab-i) |
| K — Troubleshooting | [MANUAL-LAB.md#lab-k](MANUAL-LAB.md#lab-k) |

---

## Notebooks (code)

| Notebook | File |
|----------|------|
| 01 Read bronze | [notebooks/nb_01_read_bronze.py](notebooks/nb_01_read_bronze.py) |
| 02 Bronze → silver | [notebooks/nb_02_bronze_to_silver.py](notebooks/nb_02_bronze_to_silver.py) |
| 03 Silver → gold | [notebooks/nb_03_silver_to_gold.py](notebooks/nb_03_silver_to_gold.py) |
| 04 End-to-end (ADF) | [notebooks/nb_04_end_to_end.py](notebooks/nb_04_end_to_end.py) |

---

## databricks-course modules

| Module | File |
|--------|------|
| 00 Overview | [databricks-course/module-00-foundations/00-00-overview.md](databricks-course/module-00-foundations/00-00-overview.md) |
| 01 Workspace UI | [databricks-course/module-01-workspace/01-01-workspace-tour.md](databricks-course/module-01-workspace/01-01-workspace-tour.md) |
| 02 PySpark | [databricks-course/module-02-pyspark/02-01-read-bronze-transform-silver.md](databricks-course/module-02-pyspark/02-01-read-bronze-transform-silver.md) |
| 03 Delta | [databricks-course/module-03-delta/03-01-delta-medallion.md](databricks-course/module-03-delta/03-01-delta-medallion.md) |
| 04 ADF orchestration | [databricks-course/module-04-adf-orchestration/04-01-adf-notebook-activity.md](databricks-course/module-04-adf-orchestration/04-01-adf-notebook-activity.md) |
| Case study | [databricks-course/CASE-STUDY.md](databricks-course/CASE-STUDY.md) |

---

## Other sessions & repo root

| Doc | Path from `session-3/` |
|-----|------------------------|
| Session 2 student guide | [../session-2/SESSION2-STUDENT-GUIDE.md](../session-2/SESSION2-STUDENT-GUIDE.md) |
| Session 2 manual lab | [../session-2/MANUAL-LAB.md](../session-2/MANUAL-LAB.md) |
| Session 2 ADF Databricks module | [../session-2/adf-course/module-04-external-compute/04-01-databricks-notebook-activity.md](../session-2/adf-course/module-04-external-compute/04-01-databricks-notebook-activity.md) |
| Curriculum | [../azure.html](../azure.html) |
| Class-1 README | [../README.md](../README.md) |

---

## Scripts

| Script | File |
|--------|------|
| Orchestrator | [scripts/run_session3.py](scripts/run_session3.py) |
| Bronze upload | [scripts/bronze_prep.py](scripts/bronze_prep.py) |
| RBAC (SDK, fast) | [scripts/databricks_rbac.py](scripts/databricks_rbac.py) |
| Verify Delta | [scripts/storage_verify.py](scripts/storage_verify.py) |
