# Databricks notebook source
# MAGIC %md
# MAGIC # Spark DAG Lab (simple) — problem → fix
# MAGIC
# MAGIC Short cells. Read the text, run the code, open **Spark UI** in another tab.
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | Path | `/Shared/day9/spark_dag_problems_lab` |
# MAGIC | Data | FinLedger `bronze` |
# MAGIC | Rule | One idea per cell |

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to learn
# MAGIC
# MAGIC 1. Read 5 lines of text  
# MAGIC 2. Run **problem** cell  
# MAGIC 3. Open **Compute → Spark UI → Jobs**  
# MAGIC 4. Run **fix** cell  
# MAGIC
# MAGIC ```text
# MAGIC filter/select  = narrow (easy)
# MAGIC groupBy/join   = wide (shuffle truck)
# MAGIC .count/.write  = action (starts the Job)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — load FinLedger

# COMMAND ----------

# =============================================================================
# CELL 0 — Bronze ingest (FinLedger shared estate)
# =============================================================================
# WHAT: Load sample_transactions.csv from abfss://bronze/...
# WHY:  Session 9+ writes silver/gold — we always start from the same bronze.
# HOW:  (1) RBAC  (2) account key on classic only  (3) driver SDK
# TIP:  Serverless rejects fs.azure.account.key — do NOT set it first.
# =============================================================================

SECRET_SCOPE = "finledger"  # Key Vault–backed scope created by deploy-shared-lab.cmd

# Storage account name — shared class estate (stsharedqgr7mj)
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()

# Storage key — classic clusters / driver SDK only (never first on Serverless)
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()

# Pipeline run folder — matches bronze/loaded/run=session3-lab/
RUN_ID = "session3-lab"


def _unset_account_key(account: str) -> None:
    """Clear poisoned Hadoop key config so later RBAC/DBFS paths can work."""
    conf_key = f"fs.azure.account.key.{account}.dfs.core.windows.net"
    try:
        spark.conf.unset(conf_key)
    except Exception:
        pass


def _read_bronze_transactions(spark, account: str, key: str, run_id: str):
    """Read bronze CSV; Serverless-safe order (RBAC → key → SDK)."""
    path = (
        f"abfss://bronze@{account}.dfs.core.windows.net"
        f"/loaded/run={run_id}/sample_transactions.csv"
    )

    # --- 1) RBAC / credential passthrough (do NOT set account key) ---
    try:
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "azure_rbac_passthrough"
    except Exception as exc1:
        print("  RBAC bronze read failed:", str(exc1)[:140])

    # --- 2) Classic cluster — storage account key (poisons Serverless — unset on fail) ---
    try:
        spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "storage_key_spark_conf"
    except Exception as exc2:
        print("  Account-key bronze read failed:", str(exc2)[:140])
        _unset_account_key(account)

    # --- 3) Driver SDK (small training CSV — OK for lab; no Spark ADLS conf) ---
    from io import StringIO
    import pandas as pd
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        import subprocess, sys
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "azure-storage-file-datalake", "-q"]
        )
        from azure.storage.filedatalake import DataLakeServiceClient

    rel = f"loaded/run={run_id}/sample_transactions.csv"
    client = DataLakeServiceClient(
        account_url=f"https://{account}.dfs.core.windows.net", credential=key
    )
    raw = (
        client.get_file_system_client("bronze")
        .get_file_client(rel)
        .download_file()
        .readall()
        .decode("utf-8")
    )
    return spark.createDataFrame(pd.read_csv(StringIO(raw))), path, "driver_sdk"


# Load bronze — result is a lazy DataFrame until you call an action (count, show, write)
bronze, BRONZE_PATH, BRONZE_AUTH_MODE = _read_bronze_transactions(
    spark, STORAGE_ACCOUNT, _storage_key, RUN_ID
)
df = bronze  # alias used in later cells

print("Storage     :", STORAGE_ACCOUNT)
print("Bronze path :", BRONZE_PATH)
print("Auth mode   :", BRONZE_AUTH_MODE)
print("Bronze rows :", bronze.count())  # ACTION — in-memory if driver_sdk

# COMMAND ----------

# SETUP — one typed table we reuse (no copies, no loops)
from pyspark.sql import functions as F
import time

df = (
    bronze
    .withColumn("amount", F.col("amount_gbp").cast("double"))
    .withColumn("channel", F.upper(F.trim(F.col("channel"))))
    .filter(F.col("amount").isNotNull())
)

print("rows =", df.count())
df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Lazy plan — nothing runs until an action
# MAGIC
# MAGIC **Idea:** `.select` / `.filter` only build a plan. `.count()` starts a Job.
# MAGIC
# MAGIC ```text
# MAGIC select → filter → groupBy → count   ← Job starts here
# MAGIC ```

# COMMAND ----------

# PROBLEM — plan only (no Job yet)
plan = df.select("channel", "amount").filter(F.col("amount") > 0)
print("Plan ready")

# ACTION — now Spark works
print("rows =", plan.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 1 — explain first, then one action

# COMMAND ----------

# FIX — look at plan, then aggregate once
plan.explain()
plan.groupBy("channel").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Wide vs narrow — shuffle
# MAGIC
# MAGIC **Narrow:** `filter`, `select` (no truck).  
# MAGIC **Wide:** `groupBy`, `join` (shuffle truck = `Exchange` in explain).
# MAGIC
# MAGIC ```text
# MAGIC filter ──► groupBy ──► Exchange (shuffle) ──► result
# MAGIC ```

# COMMAND ----------

# PROBLEM — groupBy causes shuffle
wide = df.groupBy("channel").sum("amount")
wide.explain()
wide.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 2 — filter before groupBy (less data to shuffle)

# COMMAND ----------

# FIX
df.filter(F.col("amount") > 10).groupBy("channel").sum("amount").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Too many partitions — tiny tasks
# MAGIC
# MAGIC **Smell:** `repartition(200)` on small data → many tiny tasks.

# COMMAND ----------

# PROBLEM
t0 = time.time()
print(df.repartition(200).count(), "ms", int((time.time() - t0) * 1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 3 — coalesce to fewer partitions

# COMMAND ----------

# FIX
t0 = time.time()
print(df.coalesce(4).count(), "ms", int((time.time() - t0) * 1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Too few partitions — fat tasks
# MAGIC
# MAGIC **Smell:** `coalesce(1)` → one cook does everything.

# COMMAND ----------

# PROBLEM
t0 = time.time()
print(df.coalesce(1).count(), "ms", int((time.time() - t0) * 1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 4 — repartition for balance

# COMMAND ----------

# FIX
t0 = time.time()
print(df.repartition(8, "channel").groupBy("channel").count().count(), "ms", int((time.time() - t0) * 1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Skew — one fat key
# MAGIC
# MAGIC **Smell:** most rows share one key → one slow task.
# MAGIC
# MAGIC ```text
# MAGIC HOT HOT HOT HOT  other other
# MAGIC       ▲
# MAGIC   one fat task
# MAGIC ```

# COMMAND ----------

# PROBLEM — make an artificial HOT key
skewed = df.withColumn(
    "k",
    F.when(F.rand() < 0.8, F.lit("HOT")).otherwise(F.col("channel")),
)
skewed.groupBy("k").count().orderBy(F.desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 5 — salt the HOT key, then re-aggregate

# COMMAND ----------

# FIX — split HOT into HOT_0 .. HOT_7
salted = skewed.withColumn(
    "k2",
    F.when(F.col("k") == "HOT", F.concat(F.lit("HOT_"), (F.rand() * 8).cast("int")))
    .otherwise(F.col("k")),
)
part = salted.groupBy("k2").count()
part.withColumn(
    "k",
    F.when(F.col("k2").startswith("HOT_"), F.lit("HOT")).otherwise(F.col("k2")),
).groupBy("k").sum("count").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Broadcast join (small table)
# MAGIC
# MAGIC **Smell:** shuffle-joining a tiny lookup table.  
# MAGIC **Fix:** `broadcast(small)`.
# MAGIC
# MAGIC ```text
# MAGIC tiny dim ──mail──► every worker
# MAGIC big fact ─────────► join local (no fact shuffle)
# MAGIC ```

# COMMAND ----------

# PROBLEM — join without broadcast
dim = spark.createDataFrame([("CARD", "Card"), ("WIRE", "Wire")], ["channel", "label"])
df.join(dim, "channel", "left").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 6 — broadcast the small side

# COMMAND ----------

# FIX
df.join(F.broadcast(dim), "channel", "left").explain()
df.join(F.broadcast(dim), "channel", "left").groupBy("label").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Never `collect` big data
# MAGIC
# MAGIC **Smell:** `collect()` / big `toPandas()` → driver OOM.  
# MAGIC **Fix:** aggregate in Spark; `display()` KPIs only.

# COMMAND ----------

# PROBLEM — only OK because we limit
rows = df.limit(10).collect()
print("collected", len(rows), "rows (never do this on full lake)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 7 — aggregate first

# COMMAND ----------

# FIX
kpi = df.groupBy("channel").agg(F.round(F.sum("amount"), 2).alias("gbp"))
display(kpi)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Cache when you reuse a DataFrame
# MAGIC
# MAGIC **Smell:** two `.count()` rebuild the same heavy plan twice.  
# MAGIC **Fix:** `.cache()` then `unpersist()`.

# COMMAND ----------

# PROBLEM — work twice
x = df.filter(F.col("amount") > 0)
print(x.count())
print(x.select("channel").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 8 — cache once

# COMMAND ----------

# FIX
x = df.filter(F.col("amount") > 0).cache()
print(x.count())
print(x.select("channel").distinct().count())
x.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9) Partition pruning
# MAGIC
# MAGIC **Smell:** partitioned table but no filter on partition column → full scan.  
# MAGIC **Fix:** filter `txn_date`.

# COMMAND ----------

# PROBLEM — write partitioned, read all
path = "dbfs:/tmp/dag_simple_part"
(
    df.select("transaction_id", "amount", "channel", F.to_date("value_date").alias("txn_date"))
    .write.format("delta").mode("overwrite").partitionBy("txn_date").save(path)
)
print("full scan", spark.read.format("delta").load(path).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 9 — filter the partition column

# COMMAND ----------

# FIX
one = spark.read.format("delta").load(path).filter(F.col("txn_date") == "2026-06-01")
one.explain()
print("pruned", one.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10) Accidental cross join
# MAGIC
# MAGIC **Smell:** join with no key → rows explode.  
# MAGIC **Fix:** always join ON a key; check counts.

# COMMAND ----------

# PROBLEM — 5 x 5 = 25
a = df.select("transaction_id").limit(5)
b = df.select(F.col("transaction_id").alias("id2")).limit(5)
print("cross rows", a.crossJoin(b).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 10 — join on key

# COMMAND ----------

# FIX
b2 = df.select("transaction_id", "amount").limit(5)
print("inner rows", a.join(b2, "transaction_id").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11) Python UDF is slow
# MAGIC
# MAGIC **Smell:** `@udf` row-by-row.  
# MAGIC **Fix:** use `F.when`.

# COMMAND ----------

# PROBLEM
from pyspark.sql.functions import udf

@udf("string")
def risk(x):
    return "high" if x and float(x) > 1000 else "ok"

df.withColumn("risk", risk(F.col("amount"))).groupBy("risk").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 11 — native when

# COMMAND ----------

# FIX
(
    df.withColumn("risk", F.when(F.col("amount") > 1000, "high").otherwise("ok"))
    .groupBy("risk").count().show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12) Spill risk — heavy agg on few partitions
# MAGIC
# MAGIC **Smell:** `collect_list` + `coalesce(2)` → memory pressure / spill in UI.  
# MAGIC **Fix:** only sum/count; more partitions.

# COMMAND ----------

# PROBLEM
(
    df.coalesce(2)
    .groupBy("channel")
    .agg(F.collect_list("transaction_id").alias("ids"))
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix 12 — light metrics + repartition

# COMMAND ----------

# FIX
(
    df.repartition(8, "channel")
    .groupBy("channel")
    .agg(F.count("*").alias("n"), F.round(F.sum("amount"), 2).alias("gbp"))
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cheat sheet
# MAGIC
# MAGIC | UI word | Meaning |
# MAGIC |---------|---------|
# MAGIC | Job | one action |
# MAGIC | Stage | work between shuffles |
# MAGIC | Task | one partition |
# MAGIC | Exchange | shuffle |
# MAGIC | BroadcastExchange | small table mailed out |
# MAGIC | Spill | used disk (bad) |
# MAGIC
# MAGIC **Also see:** `/Shared/day9/perf_databricks_adf_lab` for system tables + ADF.

# COMMAND ----------

print("DONE — open Spark UI and re-run any problem/fix pair")
dbutils.notebook.exit('{"lab":"spark_dag_problems_lab","status":"Succeeded"}')

# COMMAND ----------
