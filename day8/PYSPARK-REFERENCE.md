# PySpark reference — FinLedger data engineering

**Use with:** Day 7 `nb_03_spark_concepts.py` + Day 8 `nb_00_pyspark_foundation.py` + transform notebooks.

Read theory here first; notebooks use **3–4 line code cells** only.

---

## 1. Learning order (do not skip)

```text
1. Create venv          python -m venv .venv
2. Install packages     pip install -r day7/requirements.txt  (or day8)
3. pandas + Polars      local DataFrames on sample_transactions.csv
4. PySpark concepts     SparkSession, lazy, schema, actions
5. Transforms           select, filter, withColumn, groupBy, join, window
6. Cloud                Databricks reads abfss:// bronze
```

---

## 2. Core objects

| Object | What it is | You use it to |
|--------|------------|---------------|
| **SparkSession** | Entry point to Spark | `read`, `sql`, `createDataFrame` |
| **DataFrame** | Distributed table | All transforms |
| **Column** | Expression (`F.col`) | `withColumn`, `filter`, `agg` |
| **Schema** | Names + types | `printSchema()`, cast |
| **Row** | One record | Avoid `.collect()` on big data |

On Databricks: `spark` already exists.  
On PC: `SparkSession.builder.appName("lab").master("local[*]").getOrCreate()`

---

## 3. pandas vs Polars vs PySpark

| | pandas | Polars | PySpark |
|---|--------|--------|---------|
| **Runs on** | Driver RAM | Driver RAM | Cluster executors |
| **Execution** | Eager (immediate) | Eager (optimized) | **Lazy** (plan first) |
| **Scale** | GB | GB | TB+ |
| **FinLedger use** | Local unit tests | Fast local probes | Production lake |

**Rule:** learn DataFrame thinking with pandas/Polars on your laptop; run FinLedger pipelines in PySpark on Databricks.

---

## 4. Lazy vs eager

**Lazy (transformations)** — Spark records steps, does not run yet:

- `select`, `filter`, `withColumn`, `drop`, `distinct`, `join`, `groupBy`, `orderBy`, `repartition`

**Eager (actions)** — triggers execution of the full plan:

- `show`, `count`, `collect`, `take`, `head`, `write`, `foreach`

**Why lazy?** Catalyst optimizer merges filters, picks efficient joins, pushes predicates to files.

---

## 5. Reading data

```python
# CSV bronze
df = spark.read.option("header", True).option("inferSchema", True).csv(path)

# Parquet silver (typed, columnar)
df = spark.read.parquet(path)

# Unity Catalog
df = spark.table("finledger.bronze.sample_transactions")
```

**abfss path:**

```text
abfss://bronze@<storage-account>.dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv
```

---

## 6. Column expressions (`functions as F`)

Always prefer **column API** over Python loops:

```python
from pyspark.sql import functions as F

df.select(F.col("transaction_id"), F.upper(F.col("channel")))
df.filter(F.col("amount") > 0)
df.withColumn("amount", F.col("amount_gbp").cast("double"))
df.groupBy("channel").agg(F.sum("amount").alias("total"))
```

---

## 7. The eight transforms (Day 8)

| # | API | Purpose | Notebook |
|---|-----|---------|----------|
| 1 | `select` | Pick columns | nb_01 |
| 2 | `filter` / `where` | Keep rows | nb_01 |
| 3 | `withColumn` + `cast` | Add/change column | nb_02 |
| 4 | `groupBy` + `agg` | Summarise | nb_03 |
| 5 | `orderBy` | Sort | nb_03 |
| 6 | `dropDuplicates` | Dedupe by key | nb_04 |
| 7 | `join` | Enrich | nb_04 |
| 8 | `window` + `row_number` | Rank within group | nb_04 |

---

## 8. Join types (one line each)

| how= | Keeps |
|------|-------|
| `inner` | Only matching keys both sides |
| `left` | All left rows + matches from right |
| `right` | All right + matches from left |
| `full` | Everything from both |

---

## 9. Window functions

```python
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.desc("amount"))
df.withColumn("rank", F.row_number().over(w))
```

- **partitionBy** — groups for the window (like GROUP BY keys but keeps all rows)
- **orderBy** — sort inside each partition
- **row_number** — unique 1,2,3…; **rank** — ties get same number

---

## 10. Writing data (actions)

```python
df.write.mode("overwrite").parquet(silver_path)
df.write.format("delta").mode("append").saveAsTable("finledger.silver.transactions")
```

`mode`: `append`, `overwrite`, `error`, `ignore`.

---

## 11. SQL vs DataFrame API

Same engine:

```python
df.createOrReplaceTempView("txn")
spark.sql("SELECT channel, COUNT(*) FROM txn GROUP BY channel").show()
```

Use whichever your team prefers — Catalyst optimises both.

---

## 12. Common mistakes

| Mistake | Fix |
|---------|-----|
| `.collect()` on millions of rows | `.show()` or write to lake |
| Compare columns with `==` alone | `F.col("a") == F.col("b")` |
| Forget cast on bronze CSV | `.cast("double")` then filter nulls |
| Many actions in a loop | One pipeline, one write |
| Hardcode storage keys | Session 3 scope `finledger` |

---

## 13. Local Windows setup

```text
cd d:\azure
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r day8\requirements.txt
```

Install Java 11+ from [adoptium.net](https://adoptium.net), then:

```text
cd day8
orchestrate.cmd
```

---

## 14. Databricks notebook order

1. `nb_00_pyspark_foundation.py` — read first  
2. `nb_01_select_filter.py`  
3. `nb_02_withcolumn_cast.py`  
4. `nb_03_groupby_agg.py`  
5. `nb_04_join_window.py`  

Prerequisite: `session-3\orchestrate.cmd --setup-secrets`
