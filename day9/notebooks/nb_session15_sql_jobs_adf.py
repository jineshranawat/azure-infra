# Databricks notebook source

# MAGIC %md
# MAGIC # Session 15 — SQL, Unity Catalog, Jobs & ADF (watch / demo)
# MAGIC
# MAGIC **Goal:** See how the **same lake** is queried with SQL, registered in **Unity Catalog**, scheduled as a **Job**, and orchestrated by **ADF**.
# MAGIC
# MAGIC *Concepts + short demo cells — trainer shows UI where noted.*

# COMMAND ----------

# =============================================================================
# CELL 0 — Bronze ingest (FinLedger shared estate)
# =============================================================================
# WHAT: Load sample_transactions.csv from abfss://bronze/...
# WHY:  Session 9+ writes silver/gold — we always start from the same bronze.
# HOW:  Try (1) storage key in spark.conf, (2) RBAC passthrough, (3) driver SDK.
# =============================================================================

SECRET_SCOPE = "finledger"  # Key Vault–backed scope created by deploy-shared-lab.cmd

# Storage account name — shared class estate (stsharedqgr7mj)
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()

# Storage key — used only when classic cluster allows spark.conf account keys
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()

# Pipeline run folder — matches bronze/loaded/run=session3-lab/
RUN_ID = "session3-lab"


def _read_bronze_transactions(spark, account: str, key: str, run_id: str):
    """Read bronze CSV; fallback chain for Serverless / Spark Connect."""
    # abfss = ADLS Gen2 hierarchical path (container @ account .dfs.core.windows.net / path)
    path = (
        f"abfss://bronze@{account}.dfs.core.windows.net"
        f"/loaded/run={run_id}/sample_transactions.csv"
    )

    # --- Auth method 1: classic cluster — set account key in Spark Hadoop config ---
    try:
        spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()  # tiny ACTION to prove read works
        return frame, path, "storage_key_spark_conf"
    except Exception:
        pass  # Serverless blocks spark.conf.set for account keys — try next method

    # --- Auth method 2: Azure credential passthrough (no key in notebook) ---
    try:
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "azure_rbac_passthrough"
    except Exception:
        pass

    # --- Auth method 3: driver SDK (small training CSV — OK for lab) ---
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
print("Bronze rows :", bronze.count())  # ACTION — triggers cluster read

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
# MAGIC ### 15.1 — PySpark vs SQL (same engine)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC DataFrame API and `%sql` compile to the **same Catalyst plan** — use what your team prefers.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC SQL analysts should not re-learn PySpark for dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐     ┌──────────────────┐
# MAGIC  │ PySpark API  │────►│                  │
# MAGIC  └──────────────┘     │    Catalyst      │──► executors
# MAGIC  ┌──────────────┐     │    optimizer     │
# MAGIC  │  %sql / SQL  │────►│                  │
# MAGIC  └──────────────┘     └──────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Register Delta path as a table, then `SELECT ... GROUP BY`.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Two keyboards, same computer:** QWERTY vs Dvorak — same CPU.

# COMMAND ----------

print('Catalyst optimizes both APIs')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.2 — Register Delta table (SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC External table points at our gold path.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS finledger_gold_channel
USING delta
LOCATION '{GOLD_PATH}'
""")

# COMMAND ----------

spark.sql('SELECT * FROM finledger_gold_channel ORDER BY total_gbp DESC').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.3 — Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC **Catalog.schema.table** naming; permissions (`GRANT SELECT`); lineage in UI.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Banks need one place to audit who can read PII and where tables came from.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC finledger (catalog)
# MAGIC  ├── bronze (schema)
# MAGIC  ├── silver (schema)
# MAGIC  └── gold (schema)
# MAGIC       └── channel_daily (table)
# MAGIC             └── GRANT SELECT → analysts
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Create catalog → schema → table or external location.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Library card catalog:** books (tables) organised by section (schema), borrow rules (grants).

# COMMAND ----------

print('UC: finledger.gold.channel_daily')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.4 — UC SQL (demo)

# COMMAND ----------

# MAGIC %md
# MAGIC If Unity Catalog enabled on workspace — trainer runs in Catalog UI.

# COMMAND ----------

-- %sql
-- CREATE CATALOG IF NOT EXISTS finledger;
-- CREATE SCHEMA IF NOT EXISTS finledger.gold;
-- GRANT SELECT ON TABLE finledger.gold.channel_daily TO `analysts`;
print("Trainer: show Catalog Explorer + lineage tab")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.5 — Databricks Jobs

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC A **Job** runs a notebook on a schedule with retries, alerts, and parameters (`run_date`).

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Interactive 'Run all' does not scale — production needs scheduling.

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Job → cluster → notebook → `dbutils.widgets.get('run_date')` → `run_medallion()`.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Alarm clock for the recipe:** same `run_medallion`, runs at 6am daily.

# COMMAND ----------

dbutils.widgets.text("run_date", RUN_ID)
run_date = dbutils.widgets.get("run_date")
print("Job would call run_medallion(", run_date, ")")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.6 — Azure Data Factory

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC **ADF** orchestrates: copy activities, dependencies, triggers. **Databricks notebook activity** runs our pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Enterprise batch often chains: land file → run Databricks → send email on failure.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────────┐
# MAGIC  │     ADF     │────►│ Databricks  │────►│  Notebook   │────►│ ADLS     │
# MAGIC  │  schedule   │     │    Job      │     │run_medallion│     │ Delta    │
# MAGIC  └─────────────┘     └─────────────┘     └─────────────┘     └──────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC ADF passes `run_date` into notebook base parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Conductor + orchestra:** ADF waves the baton; Databricks plays the music.

# COMMAND ----------

print('ADF pipeline: Trigger → DatabricksNotebook(run_medallion)')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.7 — Widgets + exit value

# COMMAND ----------

# MAGIC %md
# MAGIC How Jobs/ADF pass parameters and read success/failure.

# COMMAND ----------

dbutils.widgets.text("run_date", RUN_ID)
run_date = dbutils.widgets.get("run_date")
print("Job calls: run_medallion(", run_date, ")")
dbutils.notebook.exit("ok")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session 15 checklist

# COMMAND ----------

# MAGIC %md
# MAGIC - [ ] SQL query on gold Delta
# MAGIC - [ ] Saw Unity Catalog concept
# MAGIC - [ ] Understand Job vs interactive notebook
# MAGIC - [ ] Understand ADF → Databricks activity
# MAGIC - [ ] **Capstone preview:** new CSV → bronze → `run_medallion` → ADF trigger

# COMMAND ----------
