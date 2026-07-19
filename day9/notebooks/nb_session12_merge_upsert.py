# Databricks notebook source

# MAGIC %md
# MAGIC # Session 12 — Batch incremental: MERGE upsert
# MAGIC
# MAGIC **Goal:** Load today's file and **merge** into silver — update existing keys, insert new ones. **Run twice = same row count** (idempotent).

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

# MAGIC %md
# MAGIC ### 12.1 — Why MERGE?

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC Full **overwrite** replaces the whole table. **MERGE** upserts by business key (`transaction_id`) — true incremental batch.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Daily files overlap yesterday's keys (amendments). Overwrite duplicates or loses history.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC Target Delta          Today's file         Result
# MAGIC ┌─────────────┐       ┌─────────────┐      ┌─────────────┐
# MAGIC │ yesterday   │       │ updates     │      │ MERGE ON    │
# MAGIC │ silver rows │──────►│ snapshot    │─────►│ txn_id      │
# MAGIC └─────────────┘       └─────────────┘      └──────┬──────┘
# MAGIC                                                   │
# MAGIC                               ┌───────────────────┴────────┐
# MAGIC                               ▼                            ▼
# MAGIC                        matched UPDATE              new INSERT
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Delta `merge` matches `ON` key → `whenMatchedUpdate`, `whenNotMatchedInsert`.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Contact list sync:** update existing people, add new numbers — don't delete the whole list.

# COMMAND ----------

print('MERGE = match on txn_id, upsert rows')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.2 — Create target silver table

# COMMAND ----------

# MAGIC %md
# MAGIC First load seeds the target (overwrite once).

# COMMAND ----------

(silver_good.dropDuplicates(["transaction_id"]).write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.3 — MERGE upsert

# COMMAND ----------

# MAGIC %md
# MAGIC Import `DeltaTable`, alias target `t` and source `s`, merge on `transaction_id`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC Day 1 file ──MERGE──►┐
# MAGIC                        ├──► Silver (one row per txn_id)
# MAGIC  Day 2 file ──MERGE──►┘
# MAGIC ```

# COMMAND ----------

from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, SILVER_PATH)
updates = silver_good.dropDuplicates(["transaction_id"])

(target.alias("t")
    .merge(updates.alias("s"), "t.transaction_id = s.transaction_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
print("MERGE complete — rows:", spark.read.format("delta").load(SILVER_PATH).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.4 — Idempotency demo

# COMMAND ----------

# MAGIC %md
# MAGIC Run the same MERGE again — row count must stay the same.

# COMMAND ----------

before = spark.read.format("delta").load(SILVER_PATH).count()
(target.alias("t")
    .merge(updates.alias("s"), "t.transaction_id = s.transaction_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
after = spark.read.format("delta").load(SILVER_PATH).count()
print("before:", before, "after:", after, "| idempotent:", before == after)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session 12 checklist

# COMMAND ----------

# MAGIC %md
# MAGIC - [ ] MERGE on transaction_id
# MAGIC - [ ] Re-run did not duplicate rows
# MAGIC - [ ] **Next:** Session 13 — full pipeline function

# COMMAND ----------
