# Databricks notebook source

# MAGIC %md
# MAGIC # Session 14 — Gold: aggregations & reporting tables
# MAGIC
# MAGIC **Goal:** Build **analyst-ready** tables — daily rollups, pivot, partitioned gold writes.

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
# MAGIC ### 14.0 — Load silver (from Session 13 or build now)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold builds on cleansed silver — read Delta if Session 13 already ran.

# COMMAND ----------

# =============================================================================
# Cast bronze strings → double, then split GOOD vs QUARANTINE
# =============================================================================
# FinLedger rule:
#   silver_good  → amount IS NOT NULL AND amount > 0
#   silver_bad   → amount IS NULL OR amount <= 0  (goes to audit container)
# =============================================================================

# Step 1 — CAST (lazy transformation — no data moved yet)
typed = df.withColumn(
    "amount",
    F.col("amount_gbp").cast("double"),  # "INVALID" text becomes null
)

# Step 2 — Demo bad row (watch-only lab: quarantine folder must not be empty)
# In production, bad rows arrive naturally from messy source files.
bad_row = spark.createDataFrame(
    [("TXN-BAD", "ACC-1", "INVALID", "2026-06-02", "wire", "pending")],
    "transaction_id string, account_id string, amount_gbp string, "
    "value_date string, channel string, status string",
)
bronze_demo = bronze.unionByName(bad_row, allowMissingColumns=True)
typed = bronze_demo.withColumn("amount", F.col("amount_gbp").cast("double"))

# Step 3 — SPLIT (two lazy filters — pipeline never fails on bad data)
silver_good = typed.filter(
    F.col("amount").isNotNull() & (F.col("amount") > 0)
)
silver_bad = typed.filter(
    F.col("amount").isNull() | (F.col("amount") <= 0)
)

# Step 4 — ACTION: count triggers cluster execution for both branches
print("good:", silver_good.count(), "| quarantine:", silver_bad.count())

# COMMAND ----------

(silver_good.dropDuplicates(["transaction_id"]).write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH))

# COMMAND ----------

silver = spark.read.format("delta").load(SILVER_PATH)
print("Silver rows:", silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.1 — Gold vs silver

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC **Silver** = row-level cleansed facts. **Gold** = business aggregates analysts query (sums, counts, KPIs).

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Analysts should not scan billions of silver rows for a dashboard — pre-aggregate in gold.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC Silver rows ──► groupBy channel+day ──► pivot (optional)
# MAGIC                         │
# MAGIC                         ▼
# MAGIC                  partitionBy day ──► gold Delta table
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC `groupBy` + `agg` then `write` partitioned Delta to `abfss://gold/...`.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Supermarket:** silver = every receipt line; gold = daily sales by aisle.

# COMMAND ----------

print('Gold = fewer rows, higher business value')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.2 — Channel rollup (already familiar)

# COMMAND ----------

# MAGIC %md
# MAGIC Same pattern as Day 8 Part 7 — now persisted.

# COMMAND ----------

by_channel = silver.groupBy("channel").agg(
    F.count("*").alias("txns"),
    F.sum("amount").alias("total_gbp"),
)
by_channel.orderBy(F.desc("total_gbp")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.3 — Daily rollup with date

# COMMAND ----------

# MAGIC %md
# MAGIC Add `value_date` as `day` for time-series reports.

# COMMAND ----------

daily = (
    silver.withColumn("day", F.col("value_date"))
    .groupBy("channel", "day")
    .agg(F.sum("amount").alias("total_gbp"))
)
daily.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.4 — Pivot (wide report)

# COMMAND ----------

# MAGIC %md
# MAGIC One row per day, columns per channel — easy for Excel/BI.

# COMMAND ----------

pivoted = daily.groupBy("day").pivot("channel").sum("total_gbp")
pivoted.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.5 — Partitioned gold write

# COMMAND ----------

# MAGIC %md
# MAGIC `partitionBy('day')` — one folder per day under gold (faster filters).

# COMMAND ----------

(daily.write.format("delta")
    .mode("overwrite")
    .partitionBy("day")
    .save(GOLD_DAILY_PATH))
print("Partitioned gold written:", GOLD_DAILY_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session 14 checklist

# COMMAND ----------

# MAGIC %md
# MAGIC - [ ] Channel + daily gold
# MAGIC - [ ] Pivot demo
# MAGIC - [ ] Partitioned write
# MAGIC - [ ] **Next:** Session 15 — SQL, catalog, Jobs, ADF

# COMMAND ----------
