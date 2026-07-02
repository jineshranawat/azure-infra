# Day 7 — Student lab guide

**Session 7: Storage + read the lake with PySpark**

| | |
|---|---|
| **Duration** | 2 hours |
| **Prerequisite** | Day 6 + Session 3 secrets + bronze CSV |
| **Replace** | `<learner>` = your id from `.env` |

---

## Your goal today

*"I understand ADLS medallion paths, I created a venv locally, I ran PySpark on my laptop, and I read bronze from `abfss://` in Databricks."*

---

## Block 1 — Theory (20 min)

Trainer covers (you take notes):

1. **venv** — isolated Python toolbox on your PC
2. **pandas + Polars** — local DataFrame concepts before Spark
3. **ADLS Gen2** — folders in cloud storage (bronze/silver/gold)
4. **`abfss://`** — Spark path to a container
5. **Lazy DataFrame** — plan now, execute on `.count()` / `.show()`
6. **Full PySpark** — Day 7 `nb_03` (12 sections) + Day 8 [PYSPARK-REFERENCE.md](../day8/PYSPARK-REFERENCE.md)

---

## Block 2 — Local laptop (25 min)

### Step 0 — Create venv first (one time)

```text
cd d:\azure
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r day7\requirements.txt
```

### Step 1 — Java (one time for local Spark)

Install **Java 11+** from [adoptium.net](https://adoptium.net) if `java -version` fails.

### Step 2 — Run orchestrator script

```text
cd d:\azure\day7
orchestrate.cmd
```

No Java yet?

```text
orchestrate.cmd --skip-spark
```

### Step 3 — What you should see

- Python variables / dict / function demo
- pandas vs Polars concept lines
- Medallion path printed
- `Local Spark read OK` (if Java installed)
- `OK` from unit tests

---

## Block 3 — Databricks notebooks (55 min)

### Import (one file each)

1. `nb_01_python_essentials.py`
2. `nb_02_storage_medallion.py`
3. `nb_03_spark_concepts.py`
4. `nb_04_read_bronze.py`

### Before run

```text
cd d:\azure\session-3
orchestrate.cmd --setup-secrets
```

| Widget | Value |
|--------|-------|
| `run_id` | `session3-lab` |

### Order

Run notebooks **01 → 04**. Read markdown first, then run the small code cell below it.

---

## Checkpoint

- [ ] I explained venv to a partner
- [ ] I can compare pandas, Polars, and Spark
- [ ] I read an `abfss://bronze@...` path out loud
- [ ] Bronze row count matches Session 3
- [ ] Ready for Day 8 transforms
