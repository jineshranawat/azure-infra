# Databricks notebook source

# MAGIC %md
# MAGIC # Session 10 — Delta Lake basics (ACID, schema, read back)
# MAGIC
# MAGIC **Goal:** Understand **why** FinLedger stores silver/gold as **Delta**, not plain CSV or Parquet.

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
# MAGIC ### 10.1 — Why Delta Lake?

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC **Delta Lake** = Parquet files + a **transaction log** (`_delta_log`). Gives ACID, time travel, schema enforcement.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Plain Parquet folders have no single source of truth when two jobs write at once. Banks need **atomic** commits.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐       ┌─────────────────────┐
# MAGIC │  Parquet / CSV      │       │     Delta Lake      │
# MAGIC ├─────────────────────┤       ├─────────────────────┤
# MAGIC │ files only          │       │ Parquet files       │
# MAGIC │ no transaction log  │       │ + _delta_log        │
# MAGIC │ schema drift risk   │       │ ACID + time travel  │
# MAGIC └─────────────────────┘       └─────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Every write adds a JSON commit entry. Readers always see a consistent **version**.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Bank ledger book:** Parquet is the pages; Delta is the numbered ledger entries that say which pages are official.

# COMMAND ----------

print('Delta = Parquet + _delta_log')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 — Write silver as Delta

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC Use `.format('delta')` on write and read. Same path as Session 9 — now we understand the log underneath.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Delta is the default lakehouse format on Databricks — one API for batch and (later) streaming.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC my_table/
# MAGIC  ├── part-00001.parquet  ◄── data
# MAGIC  ├── part-00002.parquet
# MAGIC  └── _delta_log/
# MAGIC      ├── 000...000.json   ◄── version 0
# MAGIC      └── 000...001.json   ◄── version 1
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC `save(path)` creates `part-*.parquet` plus `_delta_log/`.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Git commits:** each write is a commit; the log is the history.

# COMMAND ----------

(silver_good.dropDuplicates(["transaction_id"]).write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 — Read Delta back

# COMMAND ----------

# MAGIC %md
# MAGIC Prove round-trip: `spark.read.format('delta').load(path)`.

# COMMAND ----------

back = spark.read.format("delta").load(SILVER_PATH)
back.printSchema()
back.show()

# COMMAND ----------

print("Row count:", back.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.4 — Write modes

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC | mode | Meaning |
# MAGIC |------|--------|
# MAGIC | `overwrite` | Replace this table snapshot (our batch default) |
# MAGIC | `append` | Add new files (use when keeping history) |
# MAGIC | `error` | Fail if path exists |
# MAGIC | `ignore` | Do nothing if exists |

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Wrong mode = duplicate data or accidental wipe. Batch snapshots usually use **overwrite** per partition folder.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌────────────┬────────────────────────────────────┐
# MAGIC │ overwrite  │ replace snapshot (batch default)   │
# MAGIC ├────────────┼────────────────────────────────────┤
# MAGIC │ append     │ add new files                      │
# MAGIC ├────────────┼────────────────────────────────────┤
# MAGIC │ error      │ fail if path exists                │
# MAGIC ├────────────┼────────────────────────────────────┤
# MAGIC │ ignore     │ skip if path exists                │
# MAGIC └────────────┴────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Mode is decided at **write** time — part of the action.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Notebook save:** 'Save' replaces vs 'Save As new file' — same idea.

# COMMAND ----------

print("FinLedger batch uses overwrite per run= folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.5 — Schema enforcement preview

# COMMAND ----------

# MAGIC %md
# MAGIC Delta can **reject** writes that don't match the table schema (production safety).

# COMMAND ----------

print("If you add a column with mergeSchema=True you can evolve — Session 11+")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session 10 checklist

# COMMAND ----------

# MAGIC %md
# MAGIC - [ ] Wrote and read Delta silver
# MAGIC - [ ] Can explain `_delta_log`
# MAGIC - [ ] **Next:** Session 11 — time travel

# COMMAND ----------
