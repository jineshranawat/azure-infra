# Databricks notebook source
# MAGIC %md
# MAGIC # Day 7 — PySpark concepts (complete foundation)
# MAGIC
# MAGIC **Read every markdown cell first.** Code cells are 3–4 lines only.
# MAGIC
# MAGIC **Learning path:** venv (PC) → pandas/Polars (nb_01) → **this notebook** → read bronze (nb_04).

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", _storage_key)
print("Secrets OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is Apache Spark?
# MAGIC
# MAGIC - **Distributed engine** — splits work across many machines (cluster).
# MAGIC - **In-memory** where possible — faster than row-by-row loops on one server.
# MAGIC - **PySpark** = Spark API in Python. You write Python; workers run JVM Spark.
# MAGIC
# MAGIC **FinLedger:** Databricks runs Spark for you. Your laptop uses `local[*]` for practice.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SparkSession — the front door
# MAGIC
# MAGIC | Where | How you get `spark` |
# MAGIC |-------|---------------------|
# MAGIC | Databricks | Already created — just use `spark` |
# MAGIC | Local PC | `SparkSession.builder.appName("day7").master("local[*]").getOrCreate()` |
# MAGIC
# MAGIC `SparkSession` knows how to **read**, **transform**, and **write** DataFrames.

# COMMAND ----------

print("Spark version:", spark.version)
print("App name:", spark.sparkContext.appName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. pandas vs Polars vs PySpark DataFrame
# MAGIC
# MAGIC | Tool | Data size | Execution | Best for |
# MAGIC |------|-----------|-----------|----------|
# MAGIC | **pandas** | Fits in RAM on one PC | Immediate (eager) | Quick local checks |
# MAGIC | **Polars** | Fits in RAM, very fast | Eager (optimized) | Local column ops |
# MAGIC | **PySpark** | Terabytes across cluster | **Lazy** until action | Lake + production |
# MAGIC
# MAGIC Same FinLedger CSV — three tools, three scales.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. What is a PySpark DataFrame?
# MAGIC
# MAGIC - **Table** with named columns and a **schema** (types).
# MAGIC - **Immutable** — every transform returns a *new* DataFrame.
# MAGIC - **Lazy** — calling `.filter()` does not run yet; Spark records the step.
# MAGIC - **Distributed** — rows live in **partitions** across executors.

# COMMAND ----------

dbutils.widgets.text("run_id", "session3-lab", "Run id")
run_id = dbutils.widgets.get("run_id").strip()

path = (
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"loaded/run={run_id}/sample_transactions.csv"
)

# COMMAND ----------

df = spark.read.option("header", True).csv(path)
print("Plan built — still lazy.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema — column names and types
# MAGIC
# MAGIC Bronze CSV often infers everything as **string**. Silver will **cast** to `double`, `date`, etc.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print("column names:", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Lazy transformations vs actions
# MAGIC
# MAGIC **Transformations (lazy)** — add to the plan:
# MAGIC - `select`, `filter`, `withColumn`, `drop`, `join`, `groupBy`, `orderBy`, `distinct`
# MAGIC
# MAGIC **Actions (run now)** — execute the whole plan:
# MAGIC - `count`, `show`, `collect`, `take`, `write`, `display`
# MAGIC
# MAGIC **Why lazy?** Spark can **optimise** the full pipeline (Catalyst optimizer) before touching data.

# COMMAND ----------

filtered = df.filter("status = 'posted'")
print("filter added to plan — still no cluster work.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Action — `.count()` runs the job

# COMMAND ----------

print("all rows:", df.count())
print("posted rows:", filtered.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Action — `.show()` prints a sample (driver only)

# COMMAND ----------

df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. `pyspark.sql.functions` (import as `F`)
# MAGIC
# MAGIC Use **column expressions**, not Python loops over rows:
# MAGIC - `F.col("amount_gbp")` — reference a column
# MAGIC - `F.col("x").cast("double")` — change type
# MAGIC - `F.sum("amount")`, `F.count("*")` — aggregates (Day 8)

# COMMAND ----------

from pyspark.sql import functions as F

sample = df.select(F.col("transaction_id"), F.col("channel"))
sample.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Reading data — formats you will see
# MAGIC
# MAGIC | Format | When |
# MAGIC |--------|------|
# MAGIC | **CSV** | Bronze landing (Session 3) |
# MAGIC | **Parquet** | Silver (columnar, typed) |
# MAGIC | **Delta** | ACID tables + time travel |
# MAGIC | **Unity Catalog** | `spark.table("finledger.bronze.sample_transactions")` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Partitions (one idea)
# MAGIC
# MAGIC Data is split into **partitions** on workers. Too few = slow; too many = overhead.
# MAGIC You rarely set this in class — but know `groupBy` and `join` **shuffle** data between partitions.

# COMMAND ----------

print("default shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Local install recap (your PC)
# MAGIC
# MAGIC 1. `cd d:\azure` → `python -m venv .venv`
# MAGIC 2. `.venv\Scripts\python.exe -m pip install -r day7\requirements.txt`
# MAGIC 3. Install **Java 11+** (adoptium.net)
# MAGIC 4. `cd day7` → `orchestrate.cmd` → `local_spark_read.py` runs Spark locally

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint — PySpark cleared?
# MAGIC - [ ] I can name driver vs executors (cluster runs work)
# MAGIC - [ ] I know `SparkSession` / `spark` is the entry point
# MAGIC - [ ] I can list 3 lazy transforms and 3 actions
# MAGIC - [ ] I know why bronze `amount_gbp` is string
# MAGIC - [ ] Next: **nb_04_read_bronze** then **Day 8** transforms
