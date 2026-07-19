# Databricks notebook source

# MAGIC %md
# MAGIC # Session 13 — Medallion end-to-end in one batch run
# MAGIC
# MAGIC **Goal:** One function `run_medallion(run_date)` chains bronze → silver → gold — what ADF/Jobs will call later.

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

# =============================================================================
# CELL 1 — Medallion paths (where Session 9 writes land on ADLS)
# =============================================================================
# Container map on stsharedqgr7mj:
#   bronze  → raw CSV as landed
#   silver  → cleansed, typed Delta tables
#   gold    → business aggregates for analysts
#   audit   → quarantined bad rows (pipeline does NOT fail)
# =============================================================================

SILVER_PATH = (
    f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/transactions/run={RUN_ID}"       # one folder per pipeline run
)
QUARANTINE_PATH = (
    f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/quarantine/run={RUN_ID}"         # bad rows for ops review
)
GOLD_PATH = (
    f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/daily_channel_summary"             # channel totals (Session 13+)
)
GOLD_DAILY_PATH = (
    f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/daily_by_channel/run={RUN_ID}"   # partitioned by day (Session 14)
)

print("Silver     :", SILVER_PATH)
print("Quarantine :", QUARANTINE_PATH)
print("Gold       :", GOLD_PATH)

# COMMAND ----------

# PySpark column functions — always prefer F.col() over Python loops on rows
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.1 — Pipeline as a function

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC Wrap steps in `def run_medallion(run_date):` so Jobs/ADF pass **parameters** instead of copy-paste.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Production pipelines are scheduled, monitored, retried — they need one entry point.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC run_date
# MAGIC     │
# MAGIC     ▼
# MAGIC ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐
# MAGIC │ bronze │──►│ cast   │──►│ dedupe │──►│ silver │
# MAGIC └────────┘   └────────┘   └────────┘   └───┬────┘
# MAGIC                                              ▼
# MAGIC                                         ┌────────┐
# MAGIC                                         │  gold  │
# MAGIC                                         └────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Read bronze → cast → quarantine split → write silver → aggregate → write gold → return metrics.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **One recipe card:** head chef follows the same steps every night; only the delivery date changes.

# COMMAND ----------

print('We will define run_medallion(run_date) below')

# COMMAND ----------

def run_medallion(run_date: str) -> dict:
    """Bronze → silver (good) → gold channel totals. Returns row counts."""
    path = (
        f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
        f"/loaded/run={run_date}/sample_transactions.csv"
    )
    raw = spark.read.option("header", True).option("inferSchema", True).csv(path)
    typed = raw.withColumn("amount", F.col("amount_gbp").cast("double"))
    good = typed.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
    bad = typed.filter(F.col("amount").isNull() | (F.col("amount") <= 0))
    good = good.dropDuplicates(["transaction_id"])

    silver_run = (
        f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
        f"/transactions/run={run_date}"
    )
    quarantine_run = (
        f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net"
        f"/quarantine/run={run_date}"
    )
    good.write.format("delta").mode("overwrite").save(silver_run)
    if bad.count() > 0:
        bad.write.format("delta").mode("overwrite").save(quarantine_run)

    gold = good.groupBy("channel").agg(
        F.count("*").alias("txns"),
        F.sum("amount").alias("total_gbp"),
    )
    gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
    return {
        "bronze": raw.count(),
        "silver": good.count(),
        "quarantine": bad.count(),
        "gold_channels": gold.count(),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.2 — Run the pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC One action call — trainer demos full medallion.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC 1 Read bronze ──► 2 Cast ──► 3 Quarantine split
# MAGIC         │
# MAGIC         ▼
# MAGIC  4 Dedupe ──► 5 Join ──► 6 groupBy ──► 7 Gold report
# MAGIC ```

# COMMAND ----------

metrics = run_medallion(RUN_ID)
print(metrics)

# COMMAND ----------

spark.read.format("delta").load(GOLD_PATH).orderBy(F.desc("total_gbp")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session 13 checklist

# COMMAND ----------

# MAGIC %md
# MAGIC - [ ] `run_medallion` returns metrics
# MAGIC - [ ] Silver + gold paths written
# MAGIC - [ ] **Next:** Session 14 — gold reporting shapes

# COMMAND ----------
