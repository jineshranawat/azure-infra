# Day 8 — PySpark transformations you'll actually use

**Duration:** 2 hours | **Course:** [docs/data-engineering-course.html](../docs/data-engineering-course.html) Session 8

**Prerequisite:** Day 7 (`nb_03` + `nb_04`) + Session 3 secrets.

**Navigation:** [PYSPARK-REFERENCE.md](PYSPARK-REFERENCE.md) (full theory) · [SESSION8-STUDENT-GUIDE.md](SESSION8-STUDENT-GUIDE.md) · [MANUAL-LAB.md](MANUAL-LAB.md) · [GUIDE.md](GUIDE.md)

---

## A. What & why (read first)

### Learning order — same as Day 7

1. **venv** — `python -m venv .venv` at repo root  
2. **pip install** — `pandas`, `polars`, `pyspark` via `day8\requirements.txt`  
3. **pandas + Polars** — local DataFrame on `data/sample_transactions.csv`  
4. **PySpark foundation** — [PYSPARK-REFERENCE.md](PYSPARK-REFERENCE.md) + `nb_00_pyspark_foundation.py`  
5. **Eight transforms** — notebooks `nb_01` … `nb_04`

**Analogy:** venv = toolbox · pandas/Polars = practice on a workbench · PySpark = factory floor.

### PySpark mental model (summary)

| Concept | One sentence |
|---------|--------------|
| SparkSession | Front door — read, transform, write |
| DataFrame | Distributed lazy table |
| Transformation | Lazy step (select, filter, join…) |
| Action | Runs the plan (count, show, write) |
| `F.col` | Column expression — never loop rows |
| Partition | Data chunk on a worker; shuffles on groupBy/join |

Full detail: [PYSPARK-REFERENCE.md](PYSPARK-REFERENCE.md) (14 sections).

### Eight operations = 90% of batch work

| Operation | FinLedger use |
|-----------|----------------|
| `select` | Pick columns for silver |
| `filter` | Keep `posted` rows, drop bad amounts |
| `withColumn` + `cast` | `amount_gbp` string → `double` |
| `dropDuplicates` | One row per `transaction_id` |
| `join` | Enrich with reference data |
| `groupBy` + `agg` | Channel totals (gold preview) |
| `orderBy` | Rank channels by volume |
| `window` | Top-N per channel |

---

## B. How to run (Windows)

### Step 1 — Create venv (first time)

```text
cd d:\azure
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r day8\requirements.txt
```

### Step 2 — Run Day 8 script

```text
cd day8
orchestrate.cmd
```

**What it does:**

1. pandas + Polars read on sample CSV (`scripts/pandas_polars_concepts.py`)
2. Prints PySpark foundation theory (`scripts/pyspark_concepts.py`)
3. Local transform demos (`scripts/spark_transforms.py`) — needs Java
4. Unit tests
5. Notebook import list (start with **nb_00**)

**Re-run:** same command (idempotent).

**No Java:** `orchestrate.cmd --skip-spark` (pandas/Polars + theory + tests still run).

---

## C. Notebooks (read nb_00 first)

| File | Topic |
|------|-------|
| `nb_00_pyspark_foundation.py` | **Read first** — venv recap, module map, lazy/actions, SQL API |
| `nb_01_select_filter.py` | Column pick + row filter |
| `nb_02_withcolumn_cast.py` | Typed amount column |
| `nb_03_groupby_agg.py` | Channel summaries |
| `nb_04_join_window.py` | Dedupe, join, window rank |

Theory in markdown **before** each code cell. Cells are **3–4 lines max**.

---

## D. Re-run scenarios

| Scenario | Command |
|----------|---------|
| Fresh | `orchestrate.cmd` |
| Re-run after success | `orchestrate.cmd` |
| No Java | `orchestrate.cmd --skip-spark` |
| Full theory only | Read `PYSPARK-REFERENCE.md` |

---

## E. File map

| File | Purpose |
|------|---------|
| `PYSPARK-REFERENCE.md` | Complete PySpark theory (14 sections) |
| `scripts/pandas_polars_concepts.py` | Pre-Spark local DataFrame demo |
| `scripts/pyspark_concepts.py` | Printed foundation for terminal |
| `scripts/spark_transforms.py` | Local transform demos |
| `notebooks/nb_00_*.py` | Databricks foundation notebook |
