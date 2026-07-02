# Day 7 — Storage concepts + read the lake with PySpark

**Duration:** 2 hours | **Course:** [docs/data-engineering-course.html](../docs/data-engineering-course.html) Session 7

**Prerequisite:** Day 6 + **Session 3** (`session-3\orchestrate.cmd --setup-secrets`, bronze CSV landed).

**Navigation:** [SESSION7-STUDENT-GUIDE.md](SESSION7-STUDENT-GUIDE.md) · [MANUAL-LAB.md](MANUAL-LAB.md) · [COLAB-JUPYTER-GUIDE.md](COLAB-JUPYTER-GUIDE.md) (Colab/online) · [GUIDE.md](GUIDE.md) · PySpark deep dive: [../day8/PYSPARK-REFERENCE.md](../day8/PYSPARK-REFERENCE.md)

---

## A. What & why (read first)

### One Python folder — no “intermediate” vs “advanced”

Learners often hunt for separate Python tracks. **FinLedger uses one `day7/` folder** with short notebook cells (3–4 lines) covering only what Spark notebooks need: venv, variables, dicts, functions, `pathlib`, pandas, Polars, and Spark imports.

**Analogy:** You do not learn every Python library — you learn the **keys that start the car** before driving PySpark.

### Virtual environment (local laptop)

**What:** `.venv` isolates `pip` packages for this repo.  
**Why:** `orchestrate.cmd` creates repo-root `.venv` and installs **PySpark** without touching global Python.  
**Code:** `orchestrate.cmd` → `scripts/run_day7.py`.

### pandas + Polars before Spark

**What:** both are DataFrame tools for local learning; pandas is common and Polars is speed-focused.  
**Why:** learners understand DataFrame concepts before cluster/distributed Spark complexity.  
**Code:** `scripts/pandas_polars_concepts.py`, `notebooks/nb_01_python_essentials.py`.

### ADLS Gen2 + medallion

**What:** Azure Data Lake Storage with real folders (HNS). Containers `bronze`, `silver`, `gold`.  
**Why:** Session 3 landed raw CSV in bronze; Session 7 **reads** it with `abfss://` paths.  
**Code:** `notebooks/nb_02_storage_medallion.py`, `scripts/path_builder.py`.

### Spark locally vs Databricks

**What:** PySpark needs **Java 11+** on your PC. Databricks already provides `spark`.  
**Why:** Local run proves the read path works before cloud — same `spark.read.csv` API.  
**Code:** `scripts/local_spark_read.py`.

### Lazy DataFrame vs actions

**What:** `.filter()` / `.select()` build a plan; `.count()` / `.show()` execute it.  
**Why:** Explains cluster work and cost — transformations are lazy until an action.  
**Code:** `notebooks/nb_03_spark_concepts.py`.

---

## B. How to run (Windows)

```text
cd d:\azure
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r day7\requirements.txt
cd day7
orchestrate.cmd
```

You can still use only one command from inside `day7\`: `orchestrate.cmd` (it auto-creates `.venv` if missing).

**What it does:**

1. Python essentials demo (`scripts/python_essentials.py`)
2. pandas + Polars concepts demo (`scripts/pandas_polars_concepts.py`)
3. Prints medallion `abfss://` paths (`scripts/storage_concepts.py`)
4. Local PySpark read of `data/sample_transactions.csv` (needs Java)
5. Unit tests on path builder
6. Prints Databricks notebook import list

**Re-run command:** `cd day7` → `orchestrate.cmd` (idempotent)

**No Java yet:** `orchestrate.cmd --skip-spark` (still runs Python + tests)

**Databricks:** Import `notebooks/nb_01` … `nb_04` — see [MANUAL-LAB.md](MANUAL-LAB.md).

---

## C. The re-run command & four scenarios

| Scenario | Command | Expected |
|----------|---------|----------|
| Fresh machine | `orchestrate.cmd` | Creates `.venv`, installs pyspark, runs demos |
| After success | `orchestrate.cmd` | Same output, no duplicate resources |
| Partial (no Java) | `orchestrate.cmd --skip-spark` | Python + tests OK; Spark skipped |
| After teardown | N/A — local only | Re-run anytime |

---

## D. Notebooks (theory → tiny code)

| File | Topic |
|------|-------|
| `nb_01_python_essentials.py` | venv theory, variables, dict, function, pathlib, pandas/Polars, Spark imports |
| `nb_02_storage_medallion.py` | ADLS, medallion, `abfss://` anatomy |
| `nb_03_spark_concepts.py` | **Full PySpark foundation** — SparkSession, lazy/actions, F.col, formats |
| `nb_04_read_bronze.py` | Read Session 3 bronze CSV + UC peek |

Cell 1 in cloud notebooks uses scope `finledger` (same as Session 3).

---

## E. File map

| File | Purpose |
|------|---------|
| `orchestrate.cmd` | Single entry point |
| `scripts/run_day7.py` | Orchestrator |
| `scripts/pandas_polars_concepts.py` | pandas/Polars vs Spark concept bridge |
| `scripts/local_spark_read.py` | Local SparkSession read |
| `scripts/path_builder.py` | Build bronze paths (tested) |
| `data/sample_transactions.csv` | Local CSV mirror |
| `tests/test_paths.py` | Path unit tests |
