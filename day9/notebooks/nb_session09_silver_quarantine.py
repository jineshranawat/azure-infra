# Databricks notebook source

# MAGIC %md
# MAGIC # Session 9 — Clean, validate & quarantine (FinLedger silver rules)
# MAGIC
# MAGIC **After Day 8:** you can *transform* in memory. **Today:** split good vs bad rows and **write** them to the lake.
# MAGIC
# MAGIC | Container | Path purpose |
# MAGIC |-----------|----------------|
# MAGIC | `bronze` | Raw CSV as landed |
# MAGIC | `silver` | Cleansed, typed transactions (Delta) |
# MAGIC | `audit` | Quarantined bad rows — pipeline keeps running |
# MAGIC
# MAGIC **Cell guide:** markdown = theory + diagrams · Python = commented steps you run on cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Session 9 in the FinLedger story
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC │   Day 8     │     │  Session 9  │     │ WRITE lake  │
# MAGIC │ transform   │────►│ cast+split  │────►│ silver+audit│
# MAGIC │ in memory   │     │ good / bad  │     │   Delta     │
# MAGIC └─────────────┘     └─────────────┘     └─────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADLS containers on shared storage
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────── stsharedqgr7mj ───────────────────┐
# MAGIC │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
# MAGIC │  │ bronze  │  │ silver  │  │  gold   │  │  audit  │ │
# MAGIC │  │ raw CSV │  │ Delta   │  │ reports │  │quarantine│ │
# MAGIC │  └─────────┘  └─────────┘  └─────────┘  └─────────┘ │
# MAGIC └──────────────────────────────────────────────────────┘
# MAGIC ```

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
# MAGIC ### 9.1 — Bridge from Day 8

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC Day 8 ended with `silver_good` and `silver_bad` **in memory**. Session 9 **persists** them to ADLS so other teams can query them tomorrow.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Banks must not lose bad rows (audit) and must not block the pipeline on one messy file.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
# MAGIC │   SOURCES    │     │    BRONZE    │     │    SILVER    │
# MAGIC │  CSV, APIs   │────►│  raw as land │────►│ cast, dedupe │
# MAGIC └──────────────┘     └──────┬───────┘     └──────┬───────┘
# MAGIC                             │                    │
# MAGIC                             │              ┌─────▼──────┐
# MAGIC                             │              │    GOLD    │
# MAGIC                             │              │ aggregates │
# MAGIC                             │              └────────────┘
# MAGIC                             │
# MAGIC                      ┌──────▼───────┐
# MAGIC                      │  QUARANTINE  │
# MAGIC                      │ audit/bad    │
# MAGIC                      └──────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Cast → filter → **write** is an **action**. Two writes: silver + quarantine.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Airport security:** good bags fly; suspicious bags go to a side room for inspection — the flight still departs.

# COMMAND ----------

print("Day 8 = kitchen prep (transform in memory).")
print("Session 9 = pack boxes and ship to warehouse (write to ADLS).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 — Cast before quality rules

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC Bronze stores `amount_gbp` as **text**. Cast to `double` so comparisons like `> 0` work.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Invalid text (`INVALID`) becomes **null** after cast — those rows need quarantine.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐
# MAGIC │ Bronze CSV      │
# MAGIC │ amount_gbp TEXT │
# MAGIC └────────┬────────┘
# MAGIC          │  .cast("double")
# MAGIC          ▼
# MAGIC ┌─────────────────┐
# MAGIC │ amount DOUBLE   │
# MAGIC └────────┬────────┘
# MAGIC     ┌────┴────┐
# MAGIC     ▼         ▼
# MAGIC ┌────────┐ ┌────────────┐
# MAGIC │ SILVER │ │ QUARANTINE │
# MAGIC │ good   │ │ null/<=0   │
# MAGIC └────────┘ └────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC `withColumn` + `cast` is lazy until an action (`count`, `write`).

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Weighing luggage:** text label 'heavy' means nothing until you put it on the scale (cast to number).

# COMMAND ----------

# Preview cast — see string vs double side by side
typed_preview = df.withColumn("amount", F.col("amount_gbp").cast("double"))
typed_preview.select("transaction_id", "amount_gbp", "amount").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cast outcomes — valid number vs null
# MAGIC
# MAGIC ```
# MAGIC amount_gbp          cast           result
# MAGIC ┌───────────┐    ┌──────────┐    ┌──────────────┐
# MAGIC │ "1250.50" │───►│ to double│───►│ 1250.50  OK  │
# MAGIC └───────────┘    └──────────┘    └──────────────┘
# MAGIC ┌───────────┐    ┌──────────┐    ┌──────────────┐
# MAGIC │ "INVALID" │───►│ to double│───►│ NULL → audit │
# MAGIC └───────────┘    └──────────┘    └──────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 — Quarantine pattern (dead-letter for data)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC **Quarantine** = isolate bad rows in `audit` container. The pipeline **succeeds**; ops fixes source data later.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Failing the whole job on one bad CSV costs money and blocks downstream gold reports.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐
# MAGIC          │  All rows    │
# MAGIC          └──────┬───────┘
# MAGIC                 │ quality rules
# MAGIC          ┌──────┴───────┐
# MAGIC          ▼              ▼
# MAGIC ┌─────────────┐  ┌─────────────┐
# MAGIC │ silver_good │  │ silver_bad  │
# MAGIC │ abfss://    │  │ abfss://    │
# MAGIC │ silver/...  │  │ audit/...   │
# MAGIC └─────────────┘  └─────────────┘
# MAGIC   pipeline OK      ops reviews
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Split with two `filter` branches: `silver_good` and `silver_bad`. Never `collect()` bad rows to driver only — write them.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Hospital triage:** stable patients to ward; critical cases to ICU — both are recorded, neither is thrown away.

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

silver_bad.show(truncate=False)

# COMMAND ----------

silver_good.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.4 — Write is an ACTION

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC `.write.format(...).save(path)` executes the full lazy chain and creates folders + files on ADLS.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Until you write, cleansed data exists only in the cluster memory for this session.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC LAZY (no cluster work yet)          ACTION (runs now)
# MAGIC ┌────────────────────────┐         ┌────────────────────────┐
# MAGIC │ filter                 │         │ .write.format("delta") │
# MAGIC │ select                 │   ───►  │ .save(abfss://...)     │
# MAGIC │ withColumn             │         │ folders on ADLS        │
# MAGIC └────────────────────────┘         └────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Spark writes parquet files + `_delta_log`. `mode('overwrite')` replaces this run's snapshot.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Saving a Word doc:** Ctrl+S actually hits disk — transforms alone are just typing.

# COMMAND ----------

print("Next: write silver_good to", SILVER_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write + verify sequence
# MAGIC
# MAGIC ```
# MAGIC Notebook          Spark cluster           ADLS silver
# MAGIC ┌─────────┐       ┌─────────────┐       ┌──────────────┐
# MAGIC │ write() │──────►│ executors   │──────►│ part-*.pq    │
# MAGIC └─────────┘       │ run tasks   │       │ _delta_log/  │
# MAGIC ┌─────────┐       └─────────────┘       └──────┬───────┘
# MAGIC │ read()  │◄─────────────────────────────────┘
# MAGIC └─────────┘       verify row count
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.5 — Write silver (good rows)

# COMMAND ----------

# MAGIC %md
# MAGIC First production write: cleansed rows land under `abfss://silver/.../transactions/run=<id>/`.

# COMMAND ----------

# =============================================================================
# WRITE silver — ACTION (data physically lands on ADLS)
# =============================================================================
# .format("delta")  → Parquet files + _delta_log (ACID) — see Session 10
# .mode("overwrite") → replace this run= folder snapshot (batch default)
# .save(path)       → ACTION — runs all lazy steps above, then writes
# =============================================================================

(
    silver_good.write
    .format("delta")       # Delta Lake format (not plain CSV)
    .mode("overwrite")     # idempotent re-run for same RUN_ID folder
    .save(SILVER_PATH)     # abfss://silver@.../transactions/run=session3-lab
)

print("Silver written to:", SILVER_PATH)

# COMMAND ----------

# =============================================================================
# VERIFY — read Delta back (proves round-trip to the lake)
# =============================================================================
back = spark.read.format("delta").load(SILVER_PATH)
print("Read back rows:", back.count())
back.select("transaction_id", "channel", "amount", "status").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.6 — Write quarantine (bad rows)

# COMMAND ----------

# MAGIC %md
# MAGIC Bad rows go to `abfss://audit/.../quarantine/run=<id>/` — separate container for governance.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────┐
# MAGIC │           Pipeline keeps running         │
# MAGIC └─────────────────┬────────────────────────┘
# MAGIC                   │
# MAGIC         ┌─────────┴─────────┐
# MAGIC         ▼                   ▼
# MAGIC ┌───────────────┐   ┌───────────────┐
# MAGIC │ Good → silver │   │ Bad → audit   │
# MAGIC └───────────────┘   └───────┬───────┘
# MAGIC                             ▼
# MAGIC                     ┌───────────────┐
# MAGIC                     │ Ops fixes src │
# MAGIC                     └───────────────┘
# MAGIC ```

# COMMAND ----------

# =============================================================================
# WRITE quarantine — bad rows to audit container (pipeline still succeeded)
# =============================================================================
# WHY separate container: governance team reviews audit/ without touching silver/
# =============================================================================

(
    silver_bad.write
    .format("delta")
    .mode("overwrite")
    .save(QUARANTINE_PATH)  # abfss://audit@.../quarantine/run=session3-lab
)

print("Quarantine written to:", QUARANTINE_PATH)
silver_bad.show(truncate=False)  # trainer: show TXN-BAD row landed here

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.7 — Trainer: verify in Azure Portal

# COMMAND ----------

# MAGIC %md
# MAGIC Open **Storage account** → **Containers**:
# MAGIC 1. `silver` → `transactions/run=session3-lab/` → `_delta_log/` + `part-*.parquet`
# MAGIC 2. `audit` → `quarantine/run=session3-lab/` → contains TXN-BAD row

# COMMAND ----------

# MAGIC %md
# MAGIC #### Where to click in Azure Portal
# MAGIC
# MAGIC ```
# MAGIC Portal check
# MAGIC ─────────────
# MAGIC Storage → stsharedqgr7mj → silver → transactions → run=session3-lab
# MAGIC Storage → stsharedqgr7mj → audit  → quarantine → run=session3-lab
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session 9 checklist

# COMMAND ----------

# MAGIC %md
# MAGIC - [ ] Cast `amount_gbp` → `amount`
# MAGIC - [ ] Split good vs quarantine
# MAGIC - [ ] Wrote silver to `abfss://silver/...`
# MAGIC - [ ] Wrote quarantine to `abfss://audit/...`
# MAGIC - [ ] Verified in Portal or `read.format('delta')`
# MAGIC - [ ] **Next:** Session 10 — why Delta beats raw Parquet

# COMMAND ----------
