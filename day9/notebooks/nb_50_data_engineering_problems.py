# Databricks notebook source
# MAGIC %md
# MAGIC # 50 Data Engineering Problems — Runnable Solutions
# MAGIC
# MAGIC Read **each markdown cell** before the code — it explains the problem, solution, and use case.
# MAGIC Every code cell starts with a **PROBLEM / SOLUTION** banner.
# MAGIC
# MAGIC ## How to run
# MAGIC
# MAGIC 1. `session-3\orchestrate.cmd --setup-secrets` on Windows  
# MAGIC 2. Start cluster → attach → **Run all**  
# MAGIC 3. Or: `run-50-problems.cmd` from repo root
# MAGIC
# MAGIC ## Problem groups
# MAGIC - **A.** Data quality & cleansing (1–10)
# MAGIC - **B.** Scale & performance (11–15)
# MAGIC - **C.** Operations & reliability (16–20)
# MAGIC - **D.** Spark tuning (21–25)
# MAGIC - **E.** Cost & infrastructure (26–28)
# MAGIC - **F.** Security & governance (29–33)
# MAGIC - **G.** Analytics & dimensions (34–37)
# MAGIC - **H.** Integration & integrity (38–44)
# MAGIC - **I.** Optimization & lifecycle (45–47)
# MAGIC - **J.** Governance & ops (48–50)
# MAGIC
# MAGIC ## Full index
# MAGIC
# MAGIC | # | Problem | Solution |
# MAGIC |---|---------|----------|
# MAGIC | 01 | Duplicate Records | Deduplication |
# MAGIC | 02 | Late Arriving Data | Watermarking |
# MAGIC | 03 | CDC Processing | Change Data Feed |
# MAGIC | 04 | Data Skew | Salting |
# MAGIC | 05 | Small Files | OPTIMIZE |
# MAGIC | 06 | Schema Changes | Schema Evolution |
# MAGIC | 07 | Pipeline Failures | Dead Letter Queue (DLQ) |
# MAGIC | 08 | Data Quality Issues | Data Validation Framework |
# MAGIC | 09 | Missing Data / Nulls | Imputation / Default Values |
# MAGIC | 10 | Inconsistent Data Formats | Standardization |
# MAGIC | 11 | Large Data Volume | Partitioning |
# MAGIC | 12 | Slow Queries | Partition Pruning & Indexing |
# MAGIC | 13 | High Latency | Caching |
# MAGIC | 14 | Frequent Reprocessing | Incremental Processing |
# MAGIC | 15 | Unnecessary Full Loads | Incremental Loads |
# MAGIC | 16 | Manual Interventions | Automation |
# MAGIC | 17 | Monitoring Gaps | Logging & Alerting |
# MAGIC | 18 | Backfill Challenges | Batch Backfill Strategy |
# MAGIC | 19 | Data Loss | Checkpointing |
# MAGIC | 20 | Out-of-Order Events | Event Time Processing |
# MAGIC | 21 | Streaming Backlog | Trigger Optimization |
# MAGIC | 22 | Memory Pressure (OOM) | Right-sizing & Caching |
# MAGIC | 23 | Shuffle Bottlenecks | Reduce Shuffles / AQE |
# MAGIC | 24 | Slow Joins | Broadcast Join |
# MAGIC | 25 | Large Joins | Bucketing / Partitioning |
# MAGIC | 26 | High Cost | Cluster Optimization |
# MAGIC | 27 | Underutilized Clusters | Auto Scaling & Auto Termination |
# MAGIC | 28 | Cluster Startup Delays | Warm Pools / Cluster Pools |
# MAGIC | 29 | Security Issues | RBAC & Unity Catalog |
# MAGIC | 30 | Access Control Problems | Fine-Grained Permissions |
# MAGIC | 31 | Secrets Management | Secret Scopes / Key Vault |
# MAGIC | 32 | Data Lineage Gaps | Data Lineage Tools |
# MAGIC | 33 | Poor Documentation | Centralized Documentation |
# MAGIC | 34 | Inconsistent Metrics | Metric Standardization |
# MAGIC | 35 | Data Drift | Data Drift Detection |
# MAGIC | 36 | Dimension Changes (SCD) | SCD Type 2 |
# MAGIC | 37 | Reference Data Updates | Dimension Refresh Strategy |
# MAGIC | 38 | External API Failures | Retry Logic / Backoff |
# MAGIC | 39 | Rate Limiting | Throttling / Queuing |
# MAGIC | 40 | File Corruption | File Validation & Checksums |
# MAGIC | 41 | Inconsistent Timestamps | Time Standardization (UTC) |
# MAGIC | 42 | Time Zone Issues | Time Zone Normalization |
# MAGIC | 43 | Orphan Records | Referential Integrity Checks |
# MAGIC | 44 | Data Duplication Across Systems | Idempotent Processing |
# MAGIC | 45 | Slow ETL Jobs | Optimize Code & Resources |
# MAGIC | 46 | High I/O Costs | Data Compression |
# MAGIC | 47 | Data Retention Issues | Lifecycle Policies |
# MAGIC | 48 | Compliance & Governance | Data Governance Framework |
# MAGIC | 49 | Disaster Recovery | Backup & Restore Strategy |
# MAGIC | 50 | End-to-End Visibility | End-to-End Monitoring |

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 0 — Foundations (read this first)
# MAGIC
# MAGIC Before the 50 problems, understand **how data is stored** on the FinLedger lake. Every problem below assumes this stack.
# MAGIC
# MAGIC ## Timeline — when each format appears
# MAGIC
# MAGIC | Era | Format | Where in FinLedger | Why it exists |
# MAGIC |-----|--------|-------------------|---------------|
# MAGIC | Always | **CSV** | Bronze landing (`bronze/loaded/`) | Systems export spreadsheets; human-readable; cheap to land |
# MAGIC | 2013+ | **Parquet** | Silver files (inside Delta too) | Columnar analytics — read only columns you need |
# MAGIC | 2019+ | **Delta Lake** | Silver + Gold tables | Parquet **plus** transaction log = ACID, time travel, MERGE |
# MAGIC | Optional | **JSON** | APIs, semi-structured feeds | Nested data; flexible schema |
# MAGIC
# MAGIC ## What is a CSV file? (Bronze)
# MAGIC
# MAGIC - **What:** Plain text — one row per line, commas between columns.
# MAGIC - **Example:** `transaction_id,amount_gbp,channel` then `TXN-1,100.50,card`
# MAGIC - **When:** File lands from bank feed, ADF copy, or manual upload.
# MAGIC - **Problem:** No types (everything is text), no schema enforcement, slow for big analytics.
# MAGIC
# MAGIC ## What is Parquet? (Columnar storage)
# MAGIC
# MAGIC - **What:** **Parquet** is a **binary columnar** file format. Data is stored **by column**, not by row.
# MAGIC - **Born:** Open-sourced ~**2013** (Twitter + Cloudera) for Hadoop / Spark ecosystems.
# MAGIC - **Analogy:** Instead of filing whole receipts in date order (row store), Parquet files all amounts in one drawer, all channels in another — Spark reads only the drawers it needs.
# MAGIC - **When it enters FinLedger:**
# MAGIC   1. Bronze may stay CSV (raw).
# MAGIC   2. When you **write silver** with `.format("parquet")` or `.format("delta")`, the **data files** under the folder are Parquet.
# MAGIC   3. **OPTIMIZE** (Problem 5) compacts many small Parquet files into fewer larger ones.
# MAGIC - **Benefits:** Compression (Snappy/GZIP), predicate pushdown, fast aggregations.
# MAGIC - **Limits:** No built-in UPDATE/DELETE across files; no transaction log — if write fails mid-way, you can get corrupt partial folders.
# MAGIC
# MAGIC ## What is Delta Lake? (Parquet + transaction log)
# MAGIC
# MAGIC - **What:** **Delta** = **Parquet data files** + a **`_delta_log`** folder (JSON commit files listing which Parquet files belong to the table).
# MAGIC - **Born:** **Databricks, 2019** — “ACID on the data lake.”
# MAGIC - **When it enters FinLedger:**
# MAGIC   1. **Silver** cleansed tables (`silver/transactions`) — Delta recommended.
# MAGIC   2. **Gold** aggregates (`gold/daily_channel_summary`) — Delta for MERGE and reporting.
# MAGIC   3. Problems 3, 5, 6, 15, 36, 49 all use Delta features (CDC, OPTIMIZE, schema evolution, MERGE, SCD2, time travel).
# MAGIC - **What you see on disk:**
# MAGIC   ```
# MAGIC   prob05_optimized/
# MAGIC     _delta_log/          ← transaction log (who wrote what, when)
# MAGIC     part-00000-....parquet
# MAGIC     part-00001-....parquet
# MAGIC   ```
# MAGIC - **Benefits:** ACID writes, `MERGE`, `DESCRIBE HISTORY`, `RESTORE`, schema evolution, Change Data Feed.
# MAGIC
# MAGIC ## Medallion + formats (FinLedger)
# MAGIC
# MAGIC ```
# MAGIC BRONZE (CSV/raw)  →  SILVER (Delta/Parquet)  →  GOLD (Delta)
# MAGIC      land               cleanse + type            aggregate + serve
# MAGIC ```
# MAGIC
# MAGIC ## How to use this notebook in class
# MAGIC
# MAGIC 1. Read **Part 0** (this section) — 15 minutes.
# MAGIC 2. For each problem: read markdown (problem → solution → steps), then run code.
# MAGIC 3. In Catalog, open `main.demo_problems` (or ADLS paths) to see outputs.
# MAGIC
# MAGIC ---

# COMMAND ----------


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
# SETUP — writable Unity Catalog schema OR ADLS fallback
# =============================================================================
# WHAT: Find a schema you are allowed to write to (main.demo_problems preferred).
# WHY:  Shared workspace catalog dbw_shared_* often denies USE SCHEMA to learners.
# HOW:  Probe write permission; if denied, save Delta under DEMO_BASE on ADLS.
# =============================================================================
from pyspark.sql import functions as F

PREFERRED_CATALOG = "finledger"
FALLBACK_CATALOG = "main"
SCHEMA = "demo_problems"
DEMO_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/demo_50_problems"
CATALOG_ROOT = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_unity_catalog/{PREFERRED_CATALOG}"

dbutils.fs.mkdirs(DEMO_BASE)
dbutils.fs.mkdirs(CATALOG_ROOT)

UC_WRITE_ENABLED = False
WRITTEN_TABLES: list[str] = []


def _try_catalog_write(catalog: str) -> bool:
    """Return True if current user can CREATE TABLE in catalog.demo_problems."""
    full_schema = f"{catalog}.{SCHEMA}"
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    except Exception as exc:
        print(f"  CREATE SCHEMA {full_schema} failed:", str(exc)[:160])
        return False
    try:
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"USE SCHEMA {SCHEMA}")
    except Exception as exc:
        print(f"  USE SCHEMA {full_schema} failed:", str(exc)[:160])
        return False
    test_table = f"{full_schema}._write_probe"
    try:
        spark.createDataFrame([("ok",)], ["ping"]).write.format("delta").mode("overwrite").saveAsTable(
            test_table
        )
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")
        return True
    except Exception as exc:
        print(f"  Write probe on {full_schema} failed:", str(exc)[:160])
        return False


def _init_storage():
    global CATALOG, UC_TABLE, UC_WRITE_ENABLED
    names = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    print("Catalogs visible:", names)

    # Try finledger, then main — never blindly use dbw_* workspace catalog
    candidates = []
    for c in (PREFERRED_CATALOG, FALLBACK_CATALOG):
        if c in names:
            candidates.append(c)
    if PREFERRED_CATALOG not in names:
        try:
            spark.sql(
                f"CREATE CATALOG IF NOT EXISTS {PREFERRED_CATALOG} MANAGED LOCATION '{CATALOG_ROOT}'"
            )
            candidates.insert(0, PREFERRED_CATALOG)
        except Exception as exc:
            print("CREATE CATALOG finledger skipped:", str(exc)[:160])

    for catalog in candidates:
        print(f"Testing write access: {catalog}.{SCHEMA} ...")
        if _try_catalog_write(catalog):
            CATALOG = catalog
            UC_TABLE = f"{catalog}.{SCHEMA}"
            UC_WRITE_ENABLED = True
            print("UC write mode: ENABLED →", UC_TABLE)
            return

    CATALOG = "adls"
    UC_TABLE = SCHEMA
    UC_WRITE_ENABLED = False
    print("UC write mode: DISABLED (permission denied on all catalogs)")
    print("Tables will save as Delta under:", DEMO_BASE)


_init_storage()


def write_demo_table(df, table_name: str, mode: str = "overwrite") -> str:
    """Save demo output — Unity Catalog if permitted, else ADLS Delta path."""
    path = f"{DEMO_BASE}/{table_name}"
    if UC_WRITE_ENABLED:
        full_name = f"{UC_TABLE}.{table_name}"
        try:
            df.write.format("delta").mode(mode).saveAsTable(full_name)
            print("UC table :", full_name)
            WRITTEN_TABLES.append(full_name)
            return full_name
        except Exception as exc:
            err = str(exc)
            if "PERMISSION_DENIED" not in err and "UNAUTHORIZED" not in err:
                raise
            print("UC write denied — falling back to ADLS path")
    df.write.format("delta").mode(mode).save(path)
    print("Delta path:", path)
    WRITTEN_TABLES.append(path)
    return path


def read_demo_table(table_name: str):
    """Read demo table from UC or ADLS fallback path."""
    if UC_WRITE_ENABLED:
        try:
            return spark.table(f"{UC_TABLE}.{table_name}")
        except Exception:
            pass
    return spark.read.format("delta").load(f"{DEMO_BASE}/{table_name}")


def demo_table_ref(table_name: str) -> str:
    """SQL-safe name for COMMENT / DESCRIBE (UC only)."""
    return f"{UC_TABLE}.{table_name}" if UC_WRITE_ENABLED else f"delta.`{DEMO_BASE}/{table_name}`"


print("=" * 60)
print("SETUP COMPLETE")
print("  Catalog       :", CATALOG)
print("  Schema / mode :", UC_TABLE, "| UC writes:", UC_WRITE_ENABLED)
print("  ADLS base     :", DEMO_BASE)
print("  Bronze rows   :", bronze.count())
print("  Auth mode     :", BRONZE_AUTH_MODE)
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which problems teach which format?
# MAGIC
# MAGIC | Concept | Problems | Remember |
# MAGIC |---------|----------|----------|
# MAGIC | **CSV bronze** | 1, 7, 10, 14, 40 | Raw land; text; no ACID |
# MAGIC | **Parquet files** | 5, 11, 12, 25, 45, 46 | Columnar; inside Delta folders |
# MAGIC | **Delta Lake** | 3, 5, 6, 15, 19, 36, 44, 49 | Parquet + `_delta_log`; MERGE, OPTIMIZE, CDF |
# MAGIC | **Unity Catalog** | 29, 30, 31, 32, 33, 48 | Governance over Delta tables |
# MAGIC | **Streaming** | 2, 19, 20, 21 | Watermark, checkpoint, triggers |
# MAGIC | **Ops / cost** | 16, 17, 26, 27, 28, 50 | Jobs, metrics, clusters |

# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC # Part A — Data quality & cleansing (1–10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 01 — Duplicate Records
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Duplicate Records |
# MAGIC | **The solution** | **Deduplication** |
# MAGIC | **When to apply** | Before every silver write; after any pipeline re-run. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Nightly ADF copy runs twice after a retry — silver has duplicate `transaction_id` rows and channel totals are doubled.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Deduplication
# MAGIC
# MAGIC 1. **Land data** in bronze (often CSV) — may contain duplicate `transaction_id` if ADF re-runs.
# MAGIC 2. **Identify key** — business natural key (`transaction_id`), not row number.
# MAGIC 3. **Count before** — `duped.count()` so learners see the problem.
# MAGIC 4. **Deduplicate** — `dropDuplicates(["transaction_id"])` keeps first occurrence per key.
# MAGIC 5. **Write silver** — save as **Delta** (Parquet files + log) so downstream is idempotent.
# MAGIC 6. **Verify** — count after; duplicates removed.
# MAGIC
# MAGIC **Formats:** Read CSV bronze → write **Delta** silver.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Same business key appears more than once in a batch.
# MAGIC
# MAGIC **WHY:** Re-runs, upstream bugs, or append-only loads create duplicates.
# MAGIC
# MAGIC **HOW:** `dropDuplicates(['transaction_id'])` or `DISTINCT` on natural key before silver write.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Deduplication**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 01: Duplicate Records
# SOLUTION: Deduplication
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Duplicate Records breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Deduplication on FinLedger bronze data.
# OUTPUT: Table prob01_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

# Simulate duplicates on bronze (lab only — never mutate production bronze)
duped = bronze.union(bronze.limit(3))
before = duped.count()
deduped = duped.dropDuplicates(["transaction_id"])
after = deduped.count()

print(f"Rows before dedup : {before}")
print(f"Rows after dedup  : {after}")
print(f"Duplicates removed: {before - after}")

# Unity Catalog: register cleansed table
write_demo_table(deduped, "prob01_deduped", mode="overwrite")
print("UC table created:", f"{UC_TABLE}.prob01_deduped")
read_demo_table("prob01_deduped").show(3)

# COMMAND ----------

# Verify problem 01 output
try:
    _df = read_demo_table("prob01_deduped")
    print("VERIFY OK:", "prob01_deduped", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 02 — Late Arriving Data
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Late Arriving Data |
# MAGIC | **The solution** | **Watermarking** |
# MAGIC | **When to apply** | Streaming aggregations or windowed batch jobs with late events. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Mobile card payments sync hours later; a 6pm aggregation must not miss them or count them twice.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Watermarking (late data)
# MAGIC
# MAGIC 1. **Event time** — use `value_date` as when payment happened (not when file arrived).
# MAGIC 2. **Set watermark** — “accept events up to 48 hours late” (streaming: `.withWatermark()`).
# MAGIC 3. **Split** — on-time vs too-late rows.
# MAGIC 4. **Aggregate** only on-time in production; late goes to reconciliation.
# MAGIC
# MAGIC **Formats:** Applies to streaming and batch; stored result in **Delta**.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Events arrive after the window you already aggregated.
# MAGIC
# MAGIC **WHY:** Mobile/offline sources sync late; streaming windows need bounds.
# MAGIC
# MAGIC **HOW:** In Structured Streaming: `.withWatermark('event_time', '10 minutes')` — batch lab: filter rows where `event_time` within acceptable lateness window.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Watermarking**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 02: Late Arriving Data
# SOLUTION: Watermarking
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Late Arriving Data breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Watermarking on FinLedger bronze data.
# OUTPUT: Table prob02_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F
from datetime import timedelta

base = bronze.withColumn("event_time", F.to_timestamp(F.col("value_date")))
max_ts = base.agg(F.max("event_time")).collect()[0][0]
watermark_cutoff = max_ts - timedelta(hours=48)  # accept events up to 48h late

on_time = base.filter(F.col("event_time") >= F.lit(watermark_cutoff))
late = base.filter(F.col("event_time") < F.lit(watermark_cutoff))

print("Watermark cutoff (batch sim):", watermark_cutoff)
print("On-time rows :", on_time.count())
print("Too-late rows:", late.count())
print("Streaming equivalent: df.withWatermark('event_time', '48 hours')")

write_demo_table(on_time, "prob02_watermarked", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob02_watermarked")

# COMMAND ----------

# Verify problem 02 output
try:
    _df = read_demo_table("prob02_watermarked")
    print("VERIFY OK:", "prob02_watermarked", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 03 — CDC Processing
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | CDC Processing |
# MAGIC | **The solution** | **Change Data Feed** |
# MAGIC | **When to apply** | Source publishes inserts/updates/deletes and you need incremental downstream. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Core banking sends updates to existing payments; gold must apply only changes, not reload the full history.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Change Data Feed (CDC)
# MAGIC
# MAGIC 1. **Write Delta** with `delta.enableChangeDataFeed=true` — still **Parquet** underneath.
# MAGIC 2. **Change data** — `UPDATE` one row in Delta.
# MAGIC 3. **Read CDF** — `readChangeFeed` shows insert/update/delete rows.
# MAGIC 4. **Downstream** — gold consumes CDF instead of full table scan.
# MAGIC
# MAGIC **When Delta vs Parquet alone:** Need CDC → must use **Delta**, not plain Parquet.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Track inserts, updates, deletes on a Delta table.
# MAGIC
# MAGIC **WHY:** Downstream needs incremental CDC not full table scans.
# MAGIC
# MAGIC **HOW:** Enable `delta.enableChangeDataFeed=true` then `readChangeFeed`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Change Data Feed**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 03: CDC Processing
# SOLUTION: Change Data Feed
# ------------------------------------------------------------------------
# WHAT GOES WRONG: CDC Processing breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Change Data Feed on FinLedger bronze data.
# OUTPUT: Table prob03_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

cdc_path = f"{DEMO_BASE}/prob03_cdc"
bronze.limit(20).write.format("delta").mode("overwrite").option(
    "delta.enableChangeDataFeed", "true"
).save(cdc_path)

# Update one row to generate CDC events
tid = spark.read.format("delta").load(cdc_path).select("transaction_id").first()[0]
spark.sql(
    f"UPDATE delta.`{cdc_path}` SET amount_gbp = cast(amount_gbp as double) * 1.01 "
    f"WHERE transaction_id = '{tid}'"
)

cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .load(cdc_path)
)
print("Change Data Feed rows:", cdf.count())
cdf.groupBy("_change_type").count().show()

write_demo_table(cdf, "prob03_cdc_feed", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob03_cdc_feed")

# COMMAND ----------

# Verify problem 03 output
try:
    _df = read_demo_table("prob03_cdc_feed")
    print("VERIFY OK:", "prob03_cdc_feed", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 04 — Data Skew
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Data Skew |
# MAGIC | **The solution** | **Salting** |
# MAGIC | **When to apply** | `groupBy` or join on a skewed key (channel, country, status). |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** 90% of rows are `channel=card` — one Spark executor does all the work.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Salting (skew)
# MAGIC
# MAGIC 1. **Detect skew** — one `channel` has most rows.
# MAGIC 2. **Add salt** — random 0–4 so work splits across executors.
# MAGIC 3. **Partial aggregate** per (channel, salt).
# MAGIC 4. **Final aggregate** per channel.
# MAGIC
# MAGIC **Formats:** In-memory transform; output to **Delta**.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** One key has vastly more rows than others → one executor does all work.
# MAGIC
# MAGIC **WHY:** Hot keys (e.g. `channel='ONLINE'`) cause stragglers.
# MAGIC
# MAGIC **HOW:** Add random salt column, aggregate per salt, then roll up.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Salting**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 04: Data Skew
# SOLUTION: Salting
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Data Skew breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Salting on FinLedger bronze data.
# OUTPUT: Table prob04_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

salted = bronze.withColumn("salt", (F.rand() * 5).cast("int"))
skew_safe = (
    salted.groupBy("channel", "salt")
    .agg(F.sum("amount_gbp").alias("partial_gbp"))
    .groupBy("channel")
    .agg(F.sum("partial_gbp").alias("total_gbp"))
    .orderBy(F.desc("total_gbp"))
)
skew_safe.show()

write_demo_table(skew_safe, "prob04_salted_agg", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob04_salted_agg")

# COMMAND ----------

# Verify problem 04 output
try:
    _df = read_demo_table("prob04_salted_agg")
    print("VERIFY OK:", "prob04_salted_agg", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 05 — Small Files
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Small Files |
# MAGIC | **The solution** | **OPTIMIZE** |
# MAGIC | **When to apply** | Many small writes; after backfill loops; before heavy read jobs. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Hourly micro-batches land 500 tiny files in silver — morning query takes 20 min.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Small files → OPTIMIZE
# MAGIC
# MAGIC 1. **Problem** — many micro-batches create dozens of tiny **Parquet** files inside Delta.
# MAGIC 2. **Why slow** — Spark opens each file separately; metadata overhead.
# MAGIC 3. **Simulate** — append 5 small writes to `prob05_small_files/`.
# MAGIC 4. **OPTIMIZE** — run `OPTIMIZE` on the Delta table path to merge Parquet files inside the table.
# MAGIC 5. **Register** — `write_demo_table` saves optimized table for queries.
# MAGIC
# MAGIC **Key teaching point:** Delta **is** Parquet files; OPTIMIZE compacts those Parquet files. This is when **Parquet file layout** directly affects performance.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Many tiny Parquet/Delta files under one folder.
# MAGIC
# MAGIC **WHY:** Per-file overhead slows reads and increases metadata cost.
# MAGIC
# MAGIC **HOW:** `OPTIMIZE table` + optional `ZORDER BY` on filter columns.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **OPTIMIZE**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 05: Small Files
# SOLUTION: OPTIMIZE
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Small Files breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates OPTIMIZE on FinLedger bronze data.
# OUTPUT: Table prob05_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

small_path = f"{DEMO_BASE}/prob05_small_files"
for i in range(5):
    bronze.limit(10).write.format("delta").mode("append").save(small_path)

spark.sql(f"OPTIMIZE delta.`{small_path}`")
files_before = len(dbutils.fs.ls(small_path))  # rough folder listing
print("OPTIMIZE completed on", small_path)
print("Top-level entries after OPTIMIZE:", files_before)

write_demo_table(spark.read.format("delta").load(small_path), "prob05_optimized")
print("Saved optimized table as prob05_optimized")

# COMMAND ----------

# Verify problem 05 output
try:
    _df = read_demo_table("prob05_optimized")
    print("VERIFY OK:", "prob05_optimized", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 06 — Schema Changes
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Schema Changes |
# MAGIC | **The solution** | **Schema Evolution** |
# MAGIC | **When to apply** | Upstream schema evolves; new optional columns appear. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Source adds `source_system` column mid-sprint; pipeline must not fail.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Schema evolution
# MAGIC
# MAGIC 1. **First batch** — write Delta with columns A, B, C.
# MAGIC 2. **Second batch** — new column `source_system` arrives.
# MAGIC 3. **mergeSchema=true** — append adds column; old rows get null for new col.
# MAGIC 4. **No pipeline crash** — evolution is explicit and logged in `_delta_log`.
# MAGIC
# MAGIC **CSV vs Delta:** CSV has no schema enforcement; **Delta** tracks schema in the log.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Upstream adds a new column mid-pipeline.
# MAGIC
# MAGIC **WHY:** Rigid schemas break nightly loads.
# MAGIC
# MAGIC **HOW:** Delta `mergeSchema=true` on write; new cols appear as nullable.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Schema Evolution**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 06: Schema Changes
# SOLUTION: Schema Evolution
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Schema Changes breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Schema Evolution on FinLedger bronze data.
# OUTPUT: Table prob06_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

evo_path = f"{DEMO_BASE}/prob06_evo"
bronze.write.format("delta").mode("overwrite").save(evo_path)

# Batch 2 adds a new column
bronze.withColumn("source_system", F.lit("finledger")).write.format("delta").mode("append").option(
    "mergeSchema", "true"
).save(evo_path)

evolved = spark.read.format("delta").load(evo_path)
print("Columns after evolution:", evolved.columns)
write_demo_table(evolved, "prob06_evolved", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob06_evolved")

# COMMAND ----------

# Verify problem 06 output
try:
    _df = read_demo_table("prob06_evolved")
    print("VERIFY OK:", "prob06_evolved", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 07 — Pipeline Failures
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Pipeline Failures |
# MAGIC | **The solution** | **Dead Letter Queue (DLQ)** |
# MAGIC | **When to apply** | Bad rows must not fail the whole batch; ops reviews audit folder. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** 3 rows have `amount_gbp='N/A'` — quarantine them; post the other 10,000.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Dead Letter Queue (DLQ)
# MAGIC
# MAGIC 1. **Parse/cast** — `amount_gbp` to double.
# MAGIC 2. **Route valid** — rows that pass rules → silver **Delta**.
# MAGIC 3. **Route invalid** — bad rows → `audit/quarantine` path (still Delta or CSV for ops).
# MAGIC 4. **Pipeline succeeds** — DLQ holds bad rows for manual review.
# MAGIC
# MAGIC **Formats:** Bronze CSV → silver Delta + audit folder.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Bad rows should not crash the whole pipeline.
# MAGIC
# MAGIC **WHY:** One corrupt line should not block millions of good rows.
# MAGIC
# MAGIC **HOW:** Split valid vs invalid; write invalid to `audit/quarantine` DLQ path.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Dead Letter Queue (DLQ)**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 07: Pipeline Failures
# SOLUTION: Dead Letter Queue (DLQ)
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Pipeline Failures breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Dead Letter Queue (DLQ) on FinLedger bronze data.
# OUTPUT: Table prob07_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

typed = bronze.withColumn("amount_num", F.col("amount_gbp").cast("double"))
valid = typed.filter(F.col("amount_num").isNotNull() & (F.col("amount_num") > 0))
invalid = typed.filter(F.col("amount_num").isNull() | (F.col("amount_num") <= 0))

dlq_path = f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net/dlq/run={RUN_ID}/prob07"
write_demo_table(valid, "prob07_valid", mode="overwrite")
invalid.write.format("delta").mode("overwrite").save(dlq_path)

print("Valid rows  :", valid.count())
print("DLQ rows    :", invalid.count())
print("DLQ path    :", dlq_path)
print("UC table    :", f"{UC_TABLE}.prob07_valid")

# COMMAND ----------

# Verify problem 07 output
try:
    _df = read_demo_table("prob07_valid")
    print("VERIFY OK:", "prob07_valid", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 08 — Data Quality Issues
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Data Quality Issues |
# MAGIC | **The solution** | **Data Validation Framework** |
# MAGIC | **When to apply** | Every production load; metrics stored for auditors. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Compliance needs proof: null IDs = 0, negative amounts flagged before gold.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Data validation framework
# MAGIC
# MAGIC 1. **Define rules** — null IDs, negative amounts.
# MAGIC 2. **Compute metrics** — one-row summary DataFrame.
# MAGIC 3. **Filter passed** — only clean rows to silver.
# MAGIC 4. **Store metrics** — Delta table for audit trail.
# MAGIC
# MAGIC **Teaching:** Validation runs **before** gold; often on Delta silver.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Systematic checks on nulls, ranges, and referential rules.
# MAGIC
# MAGIC **WHY:** Catch issues before gold / regulatory reporting.
# MAGIC
# MAGIC **HOW:** Filter + aggregate DQ metrics; optional Great Expectations / DQX in production.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Data Validation Framework**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 08: Data Quality Issues
# SOLUTION: Data Validation Framework
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Data Quality Issues breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Data Validation Framework on FinLedger bronze data.
# OUTPUT: Table prob08_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

checks = bronze.select(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("transaction_id").isNull(), 1).otherwise(0)).alias("null_ids"),
    F.sum(F.when(F.col("amount_gbp").cast("double") < 0, 1).otherwise(0)).alias("negative_amounts"),
)
checks.show()

passed = bronze.filter(
    F.col("transaction_id").isNotNull() & (F.col("amount_gbp").cast("double") >= 0)
)
write_demo_table(passed, "prob08_dq_passed", mode="overwrite")
write_demo_table(checks, "prob08_dq_metrics", mode="overwrite")
print("UC tables: prob08_dq_passed, prob08_dq_metrics")

# COMMAND ----------

# Verify problem 08 output
try:
    _df = read_demo_table("prob08_dq_passed")
    print("VERIFY OK:", "prob08_dq_passed", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 09 — Missing Data / Nulls
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Missing Data / Nulls |
# MAGIC | **The solution** | **Imputation / Default Values** |
# MAGIC | **When to apply** | Nullable dimensions would break groupBy or joins. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** `channel` is null on wire transfers — default to `UNKNOWN` so rollups work.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Imputation / defaults
# MAGIC
# MAGIC 1. **Find nulls** — `channel` or `amount_gbp` missing.
# MAGIC 2. **coalesce / fillna** — `UNKNOWN`, `0.0`.
# MAGIC 3. **Document** — defaults are business decisions, not silent fixes.
# MAGIC 4. **Write cleansed** Delta silver.
# MAGIC
# MAGIC **Formats:** Transform in Spark; persist **Delta**.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Nullable columns break aggregations or ML features.
# MAGIC
# MAGIC **WHY:** Source systems omit optional fields.
# MAGIC
# MAGIC **HOW:** `coalesce`, `fillna`, or domain defaults (e.g. channel='UNKNOWN').
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Imputation / Default Values**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 09: Missing Data / Nulls
# SOLUTION: Imputation / Default Values
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Missing Data / Nulls breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Imputation / Default Values on FinLedger bronze data.
# OUTPUT: Table prob09_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

imputed = bronze.withColumn(
    "channel_clean",
    F.coalesce(F.col("channel"), F.lit("UNKNOWN")),
).withColumn(
    "amount_clean",
    F.coalesce(F.col("amount_gbp").cast("double"), F.lit(0.0)),
)
imputed.select("channel", "channel_clean", "amount_gbp", "amount_clean").show(5)
write_demo_table(imputed, "prob09_imputed", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob09_imputed")

# COMMAND ----------

# Verify problem 09 output
try:
    _df = read_demo_table("prob09_imputed")
    print("VERIFY OK:", "prob09_imputed", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 10 — Inconsistent Data Formats
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Inconsistent Data Formats |
# MAGIC | **The solution** | **Standardization** |
# MAGIC | **When to apply** | Mixed casing, whitespace, or date formats in bronze. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Source sends `card`, `CARD`, ` card ` — standardize before joining to dim.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Standardization
# MAGIC
# MAGIC 1. **trim + upper** on `channel`.
# MAGIC 2. **to_date** on `value_date`.
# MAGIC 3. **Compare** before/after in `.show()`.
# MAGIC 4. **Silver** stores canonical forms for joins.
# MAGIC
# MAGIC **Why:** CSV text varies; silver enforces types in **Delta schema**.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Same concept stored different ways (`uk`, `UK`, ` United Kingdom `).
# MAGIC
# MAGIC **WHY:** Breaks joins and group-bys.
# MAGIC
# MAGIC **HOW:** `trim`, `upper`, `regexp_replace` to canonical forms.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Standardization**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 10: Inconsistent Data Formats
# SOLUTION: Standardization
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Inconsistent Data Formats breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Standardization on FinLedger bronze data.
# OUTPUT: Table prob10_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

std = bronze.withColumn(
    "channel_std",
    F.upper(F.trim(F.col("channel"))),
).withColumn(
    "txn_date_std",
    F.to_date(F.col("value_date")),
)
std.select("channel", "channel_std", "value_date", "txn_date_std").show(5)
write_demo_table(std, "prob10_standardized", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob10_standardized")

# COMMAND ----------

# Verify problem 10 output
try:
    _df = read_demo_table("prob10_standardized")
    print("VERIFY OK:", "prob10_standardized", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part B — Scale & performance (11–15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 11 — Large Data Volume
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Large Data Volume |
# MAGIC | **The solution** | **Partitioning** |
# MAGIC | **When to apply** | Table grows beyond single-folder performance; partition by `value_date`. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** 5 years of transactions in one folder — filter one month still scans everything.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Partitioning (large volume)
# MAGIC
# MAGIC 1. **Choose key** — `value_date` (common for payments).
# MAGIC 2. **partitionBy** on write — folder per date: `txn_date=2026-06-01/`.
# MAGIC 3. **Each partition** holds Parquet files (inside Delta if format=delta).
# MAGIC 4. **Queries** filter one date → skip other folders.
# MAGIC
# MAGIC **When Parquet appears:** Partition folders contain Parquet (or Delta-managed Parquet).
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Billions of rows in one folder.
# MAGIC
# MAGIC **WHY:** Full scans are slow and expensive.
# MAGIC
# MAGIC **HOW:** `partitionBy('value_date')` on write.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Partitioning**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 11: Large Data Volume
# SOLUTION: Partitioning
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Large Data Volume breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Partitioning on FinLedger bronze data.
# OUTPUT: Table prob11_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

part_path = f"{DEMO_BASE}/prob11_partitioned"
bronze.withColumn("txn_date", F.to_date(F.col("value_date"))).write.format("delta").mode(
    "overwrite"
).partitionBy("txn_date").save(part_path)

parts = [r.name for r in dbutils.fs.ls(part_path) if r.name.startswith("txn_date=")]
print("Partition folders:", parts[:5], "...")
write_demo_table(spark.read.format("delta").load(part_path), "prob11_partitioned")
print("UC table:", f"{UC_TABLE}.prob11_partitioned")

# COMMAND ----------

# Verify problem 11 output
try:
    _df = read_demo_table("prob11_partitioned")
    print("VERIFY OK:", "prob11_partitioned", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 12 — Slow Queries
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Slow Queries |
# MAGIC | **The solution** | **Partition Pruning & Indexing** |
# MAGIC | **When to apply** | Designing queries and verifying `partitionFilters` in explain plan. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Analyst queries `WHERE value_date = '2026-06-01'` — must skip other months.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Partition pruning
# MAGIC
# MAGIC 1. **Load** partitioned table from Problem 11.
# MAGIC 2. **Filter** `WHERE txn_date = one day`.
# MAGIC 3. **explain()** — look for `PartitionFilters` in plan.
# MAGIC 4. **Teach:** pruning avoids reading all Parquet files.
# MAGIC
# MAGIC **Link:** Parquet + partitioning = fast scans.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Queries scan all data when only one day is needed.
# MAGIC
# MAGIC **WHY:** No partition filter in WHERE clause.
# MAGIC
# MAGIC **HOW:** Filter on partition column; Z-ORDER on high-cardinality filters.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Partition Pruning & Indexing**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 12: Slow Queries
# SOLUTION: Partition Pruning & Indexing
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Slow Queries breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Partition Pruning & Indexing on FinLedger bronze data.
# OUTPUT: Table prob12_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

tbl = read_demo_table("prob11_partitioned")
sample_date = tbl.select("txn_date").first()[0]
pruned = tbl.filter(F.col("txn_date") == sample_date)
print("Filter date:", sample_date)
print("Pruned rows:", pruned.count())
print("Physical plan (look for PartitionFilters):")
pruned.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 13 — High Latency
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | High Latency |
# MAGIC | **The solution** | **Caching** |
# MAGIC | **When to apply** | One DataFrame is reused for multiple actions in same job. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Same bronze→silver transform used in 5 cells — cache after first pass.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Caching
# MAGIC
# MAGIC 1. **Expensive transform** — `groupBy` once.
# MAGIC 2. **cache()** — store in executor memory after first action.
# MAGIC 3. **Reuse** — second action reads cache, not re-shuffle from **Delta/Parquet**.
# MAGIC 4. **unpersist()** — free memory when done.
# MAGIC
# MAGIC **Formats:** Cache is in Spark memory; source is still Delta on disk.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Same DataFrame recomputed many times in one notebook/job.
# MAGIC
# MAGIC **WHY:** Lazy evaluation re-reads from storage each action.
# MAGIC
# MAGIC **HOW:** `.cache()` or `.persist()` after expensive transform.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Caching**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 13: High Latency
# SOLUTION: Caching
# ------------------------------------------------------------------------
# WHAT GOES WRONG: High Latency breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Caching on FinLedger bronze data.
# OUTPUT: Table prob13_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

agg = bronze.groupBy("channel").agg(F.sum("amount_gbp").alias("total"))
agg.cache()
print("First action (populates cache):", agg.count())
print("Second action (from cache)     :", agg.count())
write_demo_table(agg, "prob13_cached_agg", mode="overwrite")
agg.unpersist()
print("UC table:", f"{UC_TABLE}.prob13_cached_agg")

# COMMAND ----------

# Verify problem 13 output
try:
    _df = read_demo_table("prob13_cached_agg")
    print("VERIFY OK:", "prob13_cached_agg", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 14 — Frequent Reprocessing
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Frequent Reprocessing |
# MAGIC | **The solution** | **Incremental Processing** |
# MAGIC | **When to apply** | Avoiding full bronze replay every night. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Nightly job only processes rows since last successful watermark date.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Incremental processing
# MAGIC
# MAGIC 1. **Watermark date** — last successful run (control table).
# MAGIC 2. **Filter bronze** — only `value_date >= watermark`.
# MAGIC 3. **Process subset** — not full history.
# MAGIC 4. **Update watermark** after success.
# MAGIC
# MAGIC **Formats:** Read bronze CSV or Delta; write **Delta** silver incrementally.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Re-running entire history every night.
# MAGIC
# MAGIC **WHY:** Wastes compute and extends SLAs.
# MAGIC
# MAGIC **HOW:** Track `last_processed_date` watermark; filter `WHERE date > watermark`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Incremental Processing**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 14: Frequent Reprocessing
# SOLUTION: Incremental Processing
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Frequent Reprocessing breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Incremental Processing on FinLedger bronze data.
# OUTPUT: Table prob14_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

watermark = "2024-01-01"  # in production: read from control table
incremental = bronze.filter(F.col("value_date") >= watermark)
print("Incremental rows since", watermark, ":", incremental.count())
write_demo_table(incremental, "prob14_incremental", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob14_incremental")

# COMMAND ----------

# Verify problem 14 output
try:
    _df = read_demo_table("prob14_incremental")
    print("VERIFY OK:", "prob14_incremental", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 15 — Unnecessary Full Loads
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Unnecessary Full Loads |
# MAGIC | **The solution** | **Incremental Loads** |
# MAGIC | **When to apply** | Incremental upserts on `transaction_id`. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Partner feed sends 200 updated amounts — MERGE into silver, not full replace.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Incremental MERGE (not full load)
# MAGIC
# MAGIC 1. **Target** — Delta table with 50 rows.
# MAGIC 2. **Updates** — 5 changed amounts from new batch.
# MAGIC 3. **MERGE** — match on `transaction_id`; update matched, insert new.
# MAGIC 4. **Avoid** — dropping and rewriting entire table (full load).
# MAGIC
# MAGIC **Delta feature:** `MERGE` requires **Delta**, not plain Parquet.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Full table extract when only deltas changed.
# MAGIC
# MAGIC **WHY:** Source DB and network cost scale with full exports.
# MAGIC
# MAGIC **HOW:** `MERGE` on key with CDC or `modified_at` column.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Incremental Loads**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 15: Unnecessary Full Loads
# SOLUTION: Incremental Loads
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Unnecessary Full Loads breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Incremental Loads on FinLedger bronze data.
# OUTPUT: Table prob15_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

target_path = f"{DEMO_BASE}/prob15_merge"
bronze.limit(50).write.format("delta").mode("overwrite").save(target_path)
updates = bronze.limit(5).withColumn("amount_gbp", F.col("amount_gbp").cast("double") * 1.05)

DeltaTable.forPath(spark, target_path).alias("t").merge(
    updates.alias("s"), "t.transaction_id = s.transaction_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

merged = spark.read.format("delta").load(target_path)
write_demo_table(merged, "prob15_merged", mode="overwrite")
print("MERGE complete — UC table:", f"{UC_TABLE}.prob15_merged")

# COMMAND ----------

# Verify problem 15 output
try:
    _df = read_demo_table("prob15_merged")
    print("VERIFY OK:", "prob15_merged", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part C — Operations & reliability (16–20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 16 — Manual Interventions
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Manual Interventions |
# MAGIC | **The solution** | **Automation** |
# MAGIC | **When to apply** | Moving from laptop demo to scheduled Databricks Job. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Trainer sets `run_id` widget; ADF passes same param — no manual path edits.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Automation (widgets / jobs)
# MAGIC
# MAGIC 1. **Widgets** — `run_id`, `env` as job parameters.
# MAGIC 2. **ADF / Job** passes same values every night.
# MAGIC 3. **No manual** path edits in notebook.
# MAGIC 4. **Log** run to Delta control table.
# MAGIC
# MAGIC **Ops:** Notebook → Databricks Job → ADF trigger.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Ops manually re-run failed steps.
# MAGIC
# MAGIC **WHY:** Not repeatable; error-prone at 2am.
# MAGIC
# MAGIC **HOW:** Databricks Jobs + ADF triggers + parameterized widgets.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Automation**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 16: Manual Interventions
# SOLUTION: Automation
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Manual Interventions breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Automation on FinLedger bronze data.
# OUTPUT: Table prob16_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

# Widgets = job parameters (set in Databricks Job UI or ADF notebook activity)
dbutils.widgets.text("run_id", RUN_ID, "Pipeline run id")
dbutils.widgets.text("env", "training", "Environment")

run_id_param = dbutils.widgets.get("run_id")
env_param = dbutils.widgets.get("env")
print("Automated job would receive:")
print("  run_id =", run_id_param)
print("  env    =", env_param)
print("Wire these to ADF / Databricks Jobs — no manual path edits.")

auto_log = spark.createDataFrame([(run_id_param, env_param, "scheduled")], ["run_id", "env", "trigger"])
write_demo_table(auto_log, "prob16_automation_log", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob16_automation_log")

# COMMAND ----------

# Verify problem 16 output
try:
    _df = read_demo_table("prob16_automation_log")
    print("VERIFY OK:", "prob16_automation_log", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 17 — Monitoring Gaps
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Monitoring Gaps |
# MAGIC | **The solution** | **Logging & Alerting** |
# MAGIC | **When to apply** | Every job; morning check script reads metrics table. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Row count and status logged to Delta — alert if bronze < expected.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Logging & alerting
# MAGIC
# MAGIC 1. **Structured log** — JSON with row counts, status.
# MAGIC 2. **Append** to `pipeline_metrics` Delta table.
# MAGIC 3. **Morning script** — read metrics; alert if threshold breached.
# MAGIC 4. **Link Problem 50** — end-to-end visibility.
# MAGIC
# MAGIC **Formats:** Metrics as small **Delta** tables.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Silent failures until users report wrong dashboards.
# MAGIC
# MAGIC **WHY:** No structured logs or row-count alerts.
# MAGIC
# MAGIC **HOW:** Log metrics to Delta control table; alert if `dq_fail_pct > threshold`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Logging & Alerting**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 17: Monitoring Gaps
# SOLUTION: Logging & Alerting
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Monitoring Gaps breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Logging & Alerting on FinLedger bronze data.
# OUTPUT: Table prob17_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F
import json

metrics = {
    "run_id": RUN_ID,
    "bronze_rows": bronze.count(),
    "status": "OK",
    "threshold_breached": False,
}
print("STRUCTURED_LOG:", json.dumps(metrics))

metrics_df = spark.createDataFrame([(metrics["run_id"], metrics["bronze_rows"], metrics["status"])],
                                   ["run_id", "row_count", "status"])
write_demo_table(metrics_df, "prob17_pipeline_metrics", mode="append")
print("UC table:", f"{UC_TABLE}.prob17_pipeline_metrics")

# COMMAND ----------

# Verify problem 17 output
try:
    _df = read_demo_table("prob17_pipeline_metrics")
    print("VERIFY OK:", "prob17_pipeline_metrics", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 18 — Backfill Challenges
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Backfill Challenges |
# MAGIC | **The solution** | **Batch Backfill Strategy** |
# MAGIC | **When to apply** | Backfill after bug fix; idempotent per-day writes. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Logic fix requires reprocessing June 1–7 only — loop dates, not full history.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Batch backfill
# MAGIC
# MAGIC 1. **List dates** to reprocess after bug fix.
# MAGIC 2. **Loop** each date — filter bronze, write silver partition.
# MAGIC 3. **Idempotent** per day — safe to re-run one date.
# MAGIC 4. **Avoid** one giant job for all history.
# MAGIC
# MAGIC **Formats:** Read bronze; write **Delta** per day partition.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Re-load historical dates after a logic fix.
# MAGIC
# MAGIC **WHY:** One big bang risks timeout.
# MAGIC
# MAGIC **HOW:** Loop over date partitions; idempotent write per day.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Batch Backfill Strategy**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 18: Backfill Challenges
# SOLUTION: Batch Backfill Strategy
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Backfill Challenges breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Batch Backfill Strategy on FinLedger bronze data.
# OUTPUT: Table prob18_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

dates = [r.value_date for r in bronze.select("value_date").distinct().limit(3).collect()]
backfill_rows = 0
for d in dates:
    day_df = bronze.filter(F.col("value_date") == d)
    n = day_df.count()
    backfill_rows += n
    print(f"Backfill {d}: {n} rows")
print("Total backfilled:", backfill_rows)

write_demo_table(bronze, "prob18_backfill_result", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob18_backfill_result")

# COMMAND ----------

# Verify problem 18 output
try:
    _df = read_demo_table("prob18_backfill_result")
    print("VERIFY OK:", "prob18_backfill_result", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 19 — Data Loss
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Data Loss |
# MAGIC | **The solution** | **Checkpointing** |
# MAGIC | **When to apply** | Structured Streaming; batch idempotency via MERGE keys. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Streaming job restarts — checkpoint on ADLS resumes offset, no duplicates.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Checkpointing
# MAGIC
# MAGIC 1. **Streaming** — checkpoint path on ADLS records offsets.
# MAGIC 2. **Restart** — resume from checkpoint, no duplicate read.
# MAGIC 3. **Batch analogy** — MERGE on keys for idempotency.
# MAGIC 4. **mkdirs** checkpoint folder in lab.
# MAGIC
# MAGIC **Formats:** Checkpoint is JSON metadata; data still **Delta**.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Streaming job restarts from scratch and duplicates or loses data.
# MAGIC
# MAGIC **WHY:** No durable offset store.
# MAGIC
# MAGIC **HOW:** Structured Streaming checkpoint location on ADLS; batch: idempotent MERGE keys.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Checkpointing**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 19: Data Loss
# SOLUTION: Checkpointing
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Data Loss breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Checkpointing on FinLedger bronze data.
# OUTPUT: Table prob19_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

checkpoint = f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/run={RUN_ID}/prob19"
dbutils.fs.mkdirs(checkpoint)
print("Checkpoint path (streaming):", checkpoint)
print("Batch idempotency: MERGE on transaction_id (see Problem 15)")

write_demo_table(bronze.select("transaction_id").dropDuplicates(), "prob19_idempotent_keys")
print("UC table:", f"{UC_TABLE}.prob19_idempotent_keys")

# COMMAND ----------

# Verify problem 19 output
try:
    _df = read_demo_table("prob19_idempotent_keys")
    print("VERIFY OK:", "prob19_idempotent_keys", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 20 — Out-of-Order Events
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Out-of-Order Events |
# MAGIC | **The solution** | **Event Time Processing** |
# MAGIC | **When to apply** | Out-of-order payments across time zones. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Events processed by arrival time skew totals — use `value_date` as event time.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Event-time processing
# MAGIC
# MAGIC 1. **Use** `value_date` as event time, not `current_timestamp()`.
# MAGIC 2. **Window** — `groupBy(window(event_ts, "7 days"))`.
# MAGIC 3. **Correct totals** when events arrive out of order.
# MAGIC 4. **Write** windowed aggregates to Delta gold.
# MAGIC
# MAGIC **Streaming:** `withWatermark` + event time windows.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Events arrive non-chronologically.
# MAGIC
# MAGIC **WHY:** Network delay across regions.
# MAGIC
# MAGIC **HOW:** Window on `event_time` not `current_timestamp()` (processing time).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Event Time Processing**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 20: Out-of-Order Events
# SOLUTION: Event Time Processing
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Out-of-Order Events breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Event Time Processing on FinLedger bronze data.
# OUTPUT: Table prob20_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

events = bronze.withColumn("event_ts", F.to_timestamp(F.col("value_date")))
by_event = events.groupBy(F.window(F.col("event_ts"), "7 days"), "channel").agg(
    F.count("*").alias("txn_count")
)
by_event.show(5, truncate=False)
write_demo_table(by_event, "prob20_event_time_windows", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob20_event_time_windows")

# COMMAND ----------

# Verify problem 20 output
try:
    _df = read_demo_table("prob20_event_time_windows")
    print("VERIFY OK:", "prob20_event_time_windows", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part D — Spark tuning (21–25)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 21 — Streaming Backlog
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Streaming Backlog |
# MAGIC | **The solution** | **Trigger Optimization** |
# MAGIC | **When to apply** | Streaming lag exceeds SLA. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Micro-batch backlog grows — switch trigger to `availableNow` to catch up.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Trigger optimization (streaming backlog)
# MAGIC
# MAGIC 1. **Backlog** — micro-batches fall behind.
# MAGIC 2. **Tune trigger** — `processingTime` vs `availableNow`.
# MAGIC 3. **Catch-up** — drain backlog in one shot when needed.
# MAGIC 4. **Document** trade-offs in Delta control table.
# MAGIC
# MAGIC **Lab:** Concept table in **Delta**.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Micro-batches pile up faster than processing.
# MAGIC
# MAGIC **WHY:** `processingTime` trigger too aggressive for volume.
# MAGIC
# MAGIC **HOW:** Tune trigger interval; use `AvailableNow` for catch-up batches.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Trigger Optimization**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 21: Streaming Backlog
# SOLUTION: Trigger Optimization
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Streaming Backlog breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Trigger Optimization on FinLedger bronze data.
# OUTPUT: Table prob21_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

print("Structured Streaming trigger options:")
print("  processingTime='30 seconds'  — steady micro-batches")
print("  availableNow=True            — drain backlog once (Databricks)")
print("  once=True                    — single batch for testing")
print("Lab uses batch; in production set trigger on streaming query.")

triggers = spark.createDataFrame([
    ("processingTime", "30 seconds", "steady load"),
    ("availableNow", "true", "catch-up backlog"),
], ["trigger_type", "value", "use_case"])
write_demo_table(triggers, "prob21_trigger_guide", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob21_trigger_guide")

# COMMAND ----------

# Verify problem 21 output
try:
    _df = read_demo_table("prob21_trigger_guide")
    print("VERIFY OK:", "prob21_trigger_guide", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 22 — Memory Pressure (OOM)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Memory Pressure (OOM) |
# MAGIC | **The solution** | **Right-sizing & Caching** |
# MAGIC | **When to apply** | OOM errors; reviewing notebook anti-patterns. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** `collect()` on millions of rows kills driver — aggregate first, `show(20)`.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: OOM / right-sizing
# MAGIC
# MAGIC 1. **Never collect()** millions of rows on driver.
# MAGIC 2. **aggregate** then small `.show()`.
# MAGIC 3. **Right-size** cluster memory in Compute UI.
# MAGIC 4. **spill** — if shuffle too large, add partitions or salt.
# MAGIC
# MAGIC **Formats:** Read **Delta/Parquet** in executors, not driver.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Driver or executor runs out of heap.
# MAGIC
# MAGIC **WHY:** `collect()` on huge data, too many wide transformations.
# MAGIC
# MAGIC **HOW:** Never collect large datasets; increase workers; spill-friendly joins.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Right-sizing & Caching**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 22: Memory Pressure (OOM)
# SOLUTION: Right-sizing & Caching
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Memory Pressure (OOM) breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Right-sizing & Caching on FinLedger bronze data.
# OUTPUT: Table prob22_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

# BAD:  bronze.collect()  # never on production volume
# GOOD: aggregate then take small sample
summary = bronze.agg(F.count("*").alias("n"), F.avg("amount_gbp").alias("avg_gbp"))
summary.show()
print("Executor memory: set in cluster UI (e.g. 8g standard)")
print("Driver memory  : avoid collect(); use show(20) or write")

write_demo_table(summary, "prob22_memory_safe_summary", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob22_memory_safe_summary")

# COMMAND ----------

# Verify problem 22 output
try:
    _df = read_demo_table("prob22_memory_safe_summary")
    print("VERIFY OK:", "prob22_memory_safe_summary", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 23 — Shuffle Bottlenecks
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Shuffle Bottlenecks |
# MAGIC | **The solution** | **Reduce Shuffles / AQE** |
# MAGIC | **When to apply** | Slow wide transformations; shuffle-heavy jobs. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Enable AQE so Spark coalesces partitions after skewed shuffle.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Shuffle + AQE
# MAGIC
# MAGIC 1. **Enable AQE** — `spark.sql.adaptive.enabled=true`.
# MAGIC 2. **Wide transform** — repartition + groupBy.
# MAGIC 3. **AQE coalesces** partitions after shuffle statistics.
# MAGIC 4. **Write** result to Delta.
# MAGIC
# MAGIC **Link:** Shuffle happens in memory; output Parquet via Delta.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Expensive shuffle before every wide join/groupBy.
# MAGIC
# MAGIC **WHY:** Default partition count wrong for skewed data.
# MAGIC
# MAGIC **HOW:** Enable Adaptive Query Execution (AQE); pre-filter; salting.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Reduce Shuffles / AQE**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 23: Shuffle Bottlenecks
# SOLUTION: Reduce Shuffles / AQE
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Shuffle Bottlenecks breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Reduce Shuffles / AQE on FinLedger bronze data.
# OUTPUT: Table prob23_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))

from pyspark.sql import functions as F
shuffled = bronze.repartition(4, "channel").groupBy("channel").count()
write_demo_table(shuffled, "prob23_aqe_agg", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob23_aqe_agg")

# COMMAND ----------

# Verify problem 23 output
try:
    _df = read_demo_table("prob23_aqe_agg")
    print("VERIFY OK:", "prob23_aqe_agg", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 24 — Slow Joins
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Slow Joins |
# MAGIC | **The solution** | **Broadcast Join** |
# MAGIC | **When to apply** | Dimension table fits in memory (<~10 MB). |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Join 1M facts to 5-row channel lookup — `broadcast(dim)`.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Broadcast join
# MAGIC
# MAGIC 1. **Large fact** — bronze transactions.
# MAGIC 2. **Small dim** — distinct channels (few rows).
# MAGIC 3. **broadcast(dim)** — copy small table to all executors.
# MAGIC 4. **Avoid shuffle** on join.
# MAGIC
# MAGIC **Formats:** Join in Spark; save joined **Delta** table.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Joining fact to small dimension scans both sides.
# MAGIC
# MAGIC **WHY:** Default sort-merge join shuffles both tables.
# MAGIC
# MAGIC **HOW:** `broadcast(dim_df)` when dimension < ~10MB.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Broadcast Join**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 24: Slow Joins
# SOLUTION: Broadcast Join
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Slow Joins breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Broadcast Join on FinLedger bronze data.
# OUTPUT: Table prob24_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

dim = bronze.select("channel").distinct().withColumn("tier", F.lit("standard"))
joined = bronze.join(F.broadcast(dim), "channel", "left")
joined.select("transaction_id", "channel", "tier").show(5)
write_demo_table(joined, "prob24_broadcast_join", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob24_broadcast_join")

# COMMAND ----------

# Verify problem 24 output
try:
    _df = read_demo_table("prob24_broadcast_join")
    print("VERIFY OK:", "prob24_broadcast_join", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 25 — Large Joins
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Large Joins |
# MAGIC | **The solution** | **Bucketing / Partitioning** |
# MAGIC | **When to apply** | Both sides of join are large. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Two huge fact tables on `channel` — co-partition or bucket to cut shuffle.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Large joins (bucketing / co-partition)
# MAGIC
# MAGIC 1. **Both sides large** — broadcast not possible.
# MAGIC 2. **partitionBy(channel)** — co-locate same channel in same folder.
# MAGIC 3. **Classic:** `bucketBy` on Hive tables.
# MAGIC 4. **Join** on partition key reduces shuffle.
# MAGIC
# MAGIC **Parquet:** Co-partitioned folders hold Parquet files.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Two huge tables joined on same key.
# MAGIC
# MAGIC **WHY:** Shuffle join does not scale.
# MAGIC
# MAGIC **HOW:** Bucket both tables on join key; co-locate partitions.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Bucketing / Partitioning**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 25: Large Joins
# SOLUTION: Bucketing / Partitioning
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Large Joins breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Bucketing / Partitioning on FinLedger bronze data.
# OUTPUT: Table prob25_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

bucket_path = f"{DEMO_BASE}/prob25_copartitioned"
# Co-partition on channel — same layout on both sides of join reduces shuffle
bronze.write.format("delta").mode("overwrite").partitionBy("channel").save(bucket_path)
print("Partitioned by channel at:", bucket_path)
print("Classic cluster + Hive: bucketBy(8, 'channel').sortBy('channel').saveAsTable(...)")
write_demo_table(bronze, "prob25_bucket_guide", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob25_bucket_guide")

# COMMAND ----------

# Verify problem 25 output
try:
    _df = read_demo_table("prob25_bucket_guide")
    print("VERIFY OK:", "prob25_bucket_guide", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part E — Cost & infrastructure (26–28)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 26 — High Cost
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | High Cost |
# MAGIC | **The solution** | **Cluster Optimization** |
# MAGIC | **When to apply** | Cost review; designing production scheduling. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Interactive cluster left on overnight — use job clusters + auto-terminate.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Cost control (cluster)
# MAGIC
# MAGIC 1. **Job clusters** — terminate after job.
# MAGIC 2. **Right SKU** — smallest node that doesn't OOM.
# MAGIC 3. **Photon** — if licensed, for SQL/ETL speed.
# MAGIC 4. **Document** tips in Delta table.
# MAGIC
# MAGIC **Not a file format** — infrastructure cost.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** DBU spend higher than needed.
# MAGIC
# MAGIC **WHY:** Oversized nodes, always-on clusters.
# MAGIC
# MAGIC **HOW:** Job clusters, right node type, photon where licensed.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Cluster Optimization**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 26: High Cost
# SOLUTION: Cluster Optimization
# ------------------------------------------------------------------------
# WHAT GOES WRONG: High Cost breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Cluster Optimization on FinLedger bronze data.
# OUTPUT: Table prob26_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

cost_tips = [
    ("Use Jobs cluster", "Terminate after job — not interactive 24/7"),
    ("Smallest node", "Standard_DS3_v2 for labs; scale up only if OOM"),
    ("Auto-terminate", "30 min idle on interactive clusters"),
    ("Serverless SQL", "For ad-hoc queries only when licensed"),
]
for tip, detail in cost_tips:
    print(f"  {tip}: {detail}")

write_demo_table(spark.createDataFrame(cost_tips, ["tip", "detail"]), "prob26_cost_tips")
print("UC table:", f"{UC_TABLE}.prob26_cost_tips")

# COMMAND ----------

# Verify problem 26 output
try:
    _df = read_demo_table("prob26_cost_tips")
    print("VERIFY OK:", "prob26_cost_tips", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 27 — Underutilized Clusters
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Underutilized Clusters |
# MAGIC | **The solution** | **Auto Scaling & Auto Termination** |
# MAGIC | **When to apply** | Variable load; preventing idle DBU burn. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Lab cluster autoscales 1–4 workers; terminates after 30 min idle.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Autoscale + auto-terminate
# MAGIC
# MAGIC 1. **min_workers=1, max_workers=4** — scale with load.
# MAGIC 2. **auto-terminate 30 min** — interactive clusters don't run overnight.
# MAGIC 3. **JSON config** stored for learners to copy to Compute UI.
# MAGIC
# MAGIC **Ops concept** — complements Delta storage cost (Problem 46).
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Cluster runs at 1 worker while paying for 8.
# MAGIC
# MAGIC **WHY:** Fixed size + no auto-terminate.
# MAGIC
# MAGIC **HOW:** `autoscale min_workers=1 max_workers=4`; terminate after 30 min idle.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Auto Scaling & Auto Termination**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 27: Underutilized Clusters
# SOLUTION: Auto Scaling & Auto Termination
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Underutilized Clusters breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Auto Scaling & Auto Termination on FinLedger bronze data.
# OUTPUT: Table prob27_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

cluster_policy = {
    "autoscale": {"min_workers": 1, "max_workers": 4},
    "autotermination_minutes": 30,
    "spark_version": "latest LTS",
}
import json
print("Recommended cluster JSON (set in Compute UI):")
print(json.dumps(cluster_policy, indent=2))

write_demo_table(spark.createDataFrame([(json.dumps(cluster_policy),)], ["cluster_config_json"]), "prob27_autoscale_config")
print("UC table:", f"{UC_TABLE}.prob27_autoscale_config")

# COMMAND ----------

# Verify problem 27 output
try:
    _df = read_demo_table("prob27_autoscale_config")
    print("VERIFY OK:", "prob27_autoscale_config", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 28 — Cluster Startup Delays
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Cluster Startup Delays |
# MAGIC | **The solution** | **Warm Pools / Cluster Pools** |
# MAGIC | **When to apply** | Sub-2-minute job start is required (trade-off: idle cost). |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** SLA job cannot wait 8 min cold start — instance pool keeps VMs warm.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Instance pools (warm VMs)
# MAGIC
# MAGIC 1. **Cold start** — new cluster waits for VMs.
# MAGIC 2. **Pool** — idle VMs ready.
# MAGIC 3. **Trade-off** — faster start vs idle VM cost.
# MAGIC 4. **Attach** pool in cluster policy.
# MAGIC
# MAGIC **When:** SLA-critical jobs only.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** 5–10 min wait for new cluster each job.
# MAGIC
# MAGIC **WHY:** Cold VM provisioning.
# MAGIC
# MAGIC **HOW:** Instance pool keeps VMs warm; policy attaches jobs to pool.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Warm Pools / Cluster Pools**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 28: Cluster Startup Delays
# SOLUTION: Warm Pools / Cluster Pools
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Cluster Startup Delays breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Warm Pools / Cluster Pools on FinLedger bronze data.
# OUTPUT: Table prob28_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

print("Instance pool (Azure Databricks admin):")
print("  1. Compute → Instance pools → Create")
print("  2. Min idle instances = 1 (warm VM ready)")
print("  3. Job cluster policy → use pool id")
print("Trade-off: warm VMs cost $ even when idle — use for SLA-critical jobs only.")

write_demo_table(spark.createDataFrame([("instance_pool", "reduces cold start", RUN_ID)], ["component", "benefit", "run_id"]), "prob28_pool_guide")
print("UC table:", f"{UC_TABLE}.prob28_pool_guide")

# COMMAND ----------

# Verify problem 28 output
try:
    _df = read_demo_table("prob28_pool_guide")
    print("VERIFY OK:", "prob28_pool_guide", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part F — Security & governance (29–33)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 29 — Security Issues
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Security Issues |
# MAGIC | **The solution** | **RBAC & Unity Catalog** |
# MAGIC | **When to apply** | Onboarding users; separating bronze/silver/gold access. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Only analysts with gold access see channel totals — UC GRANT on schema.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Unity Catalog RBAC
# MAGIC
# MAGIC 1. **Catalog** — `main` or `finledger`.
# MAGIC 2. **Schema** — `demo_problems`, `bronze`, `silver`, `gold`.
# MAGIC 3. **GRANT** USE SCHEMA, SELECT on tables.
# MAGIC 4. **Separate** who sees bronze PII vs gold aggregates.
# MAGIC
# MAGIC **UC** governs **Delta** tables in the lake.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Everyone can read PII gold tables.
# MAGIC
# MAGIC **WHY:** No central governance.
# MAGIC
# MAGIC **HOW:** Unity Catalog GRANT/REVOKE on catalog.schema.table.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **RBAC & Unity Catalog**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 29: Security Issues
# SOLUTION: RBAC & Unity Catalog
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Security Issues breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates RBAC & Unity Catalog on FinLedger bronze data.
# OUTPUT: Table prob29_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

print("Catalog :", CATALOG)
print("Schema  :", UC_TABLE)
print("Example GRANT (trainer runs as metastore admin):")
print(f"  GRANT USE SCHEMA ON SCHEMA {UC_TABLE} TO `account users`")
print(f"  GRANT SELECT ON TABLE {UC_TABLE}.prob01_deduped TO `account users`")

spark.sql(f"SHOW TABLES IN {UC_TABLE}").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 30 — Access Control Problems
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Access Control Problems |
# MAGIC | **The solution** | **Fine-Grained Permissions** |
# MAGIC | **When to apply** | PII columns must not appear in self-service SQL. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Analysts see aggregates only — masked view hides `transaction_id`.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Fine-grained access
# MAGIC
# MAGIC 1. **Masked gold** — aggregates only, no `transaction_id`.
# MAGIC 2. **Views** — `CREATE VIEW` with column subset.
# MAGIC 3. **GRANT** on view to analysts.
# MAGIC 4. **Write** masked table to Delta.
# MAGIC
# MAGIC **Governance** on top of **Delta** gold.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Analysts need channel totals but not transaction_id.
# MAGIC
# MAGIC **WHY:** Column-level PII restrictions.
# MAGIC
# MAGIC **HOW:** UC column masks, row filters, dynamic views.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Fine-Grained Permissions**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 30: Access Control Problems
# SOLUTION: Fine-Grained Permissions
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Access Control Problems breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Fine-Grained Permissions on FinLedger bronze data.
# OUTPUT: Table prob30_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

# Masked view: hide transaction_id — show only aggregates
masked = bronze.groupBy("channel").agg(
    F.count("*").alias("txn_count"),
    F.round(F.sum("amount_gbp"), 2).alias("total_gbp"),
)
write_demo_table(masked, "prob30_masked_gold", mode="overwrite")
print("UC table (safe for analysts):", f"{UC_TABLE}.prob30_masked_gold")
print("Production: CREATE VIEW ... + GRANT SELECT ON VIEW")

# COMMAND ----------

# Verify problem 30 output
try:
    _df = read_demo_table("prob30_masked_gold")
    print("VERIFY OK:", "prob30_masked_gold", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 31 — Secrets Management
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Secrets Management |
# MAGIC | **The solution** | **Secret Scopes / Key Vault** |
# MAGIC | **When to apply** | Once per laptop; `session-3\orchestrate.cmd --setup-secrets`. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Storage key in `.env` + scope `finledger` — notebooks never paste secrets.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Secret scopes
# MAGIC
# MAGIC 1. **Never** paste storage keys in notebook.
# MAGIC 2. **Scope `finledger`** — `storage-account`, `storage-key`.
# MAGIC 3. **Setup** — `session-3\orchestrate.cmd --setup-secrets` on Windows.
# MAGIC 4. **Read** — `dbutils.secrets.get()` in cell 0.
# MAGIC
# MAGIC **Enables** secure read of bronze **CSV** from ADLS.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Storage keys pasted in notebook cells.
# MAGIC
# MAGIC **WHY:** Git leak, shoulder surfing.
# MAGIC
# MAGIC **HOW:** `dbutils.secrets.get('finledger', 'storage-key')` — scope backed by Databricks (lab) or Key Vault (prod).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Secret Scopes / Key Vault**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 31: Secrets Management
# SOLUTION: Secret Scopes / Key Vault
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Secrets Management breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Secret Scopes / Key Vault on FinLedger bronze data.
# OUTPUT: Table prob31_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

acct = dbutils.secrets.get(scope="finledger", key="storage-account")
key_len = len(dbutils.secrets.get(scope="finledger", key="storage-key"))
print("Scope     : finledger")
print("Account   :", acct)
print("Key loaded: yes (" + str(key_len) + " chars — not printed)")
print("Production: --scope-backend-type AZURE_KEYVAULT on create-scope")

write_demo_table(spark.createDataFrame([("finledger", acct, key_len)], ["scope", "storage_account", "key_chars"]), "prob31_secrets_audit")
print("UC table:", f"{UC_TABLE}.prob31_secrets_audit")

# COMMAND ----------

# Verify problem 31 output
try:
    _df = read_demo_table("prob31_secrets_audit")
    print("VERIFY OK:", "prob31_secrets_audit", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 32 — Data Lineage Gaps
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Data Lineage Gaps |
# MAGIC | **The solution** | **Data Lineage Tools** |
# MAGIC | **When to apply** | Impact analysis before schema change; Purview registration. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Compliance asks which report uses `silver.transactions` — UC lineage tab.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Data lineage (explain clearly in class)
# MAGIC
# MAGIC **What is lineage?** A map of **data flow**: which tables/files feed which tables, notebooks, and dashboards.
# MAGIC
# MAGIC 1. **Upstream** — `prob01_deduped` (silver deduped transactions from Problem 01).
# MAGIC 2. **Transform** — take 10 rows and write a **downstream** table `prob32_lineage_demo`.
# MAGIC 3. **Register** — Unity Catalog records that `prob32` was created from `prob01` (when UC writes work).
# MAGIC 4. **View in UI** — Catalog Explorer → select `prob32_lineage_demo` → **Lineage** tab → see arrow from upstream.
# MAGIC 5. **Why it matters** — Compliance asks: "If we change `transaction_id` in silver, which gold reports break?" Lineage answers that without tribal knowledge.
# MAGIC 6. **Purview** — enterprise tool that scans ADLS + Databricks and shows lineage across ADF, Power BI, etc. (Session 24+).
# MAGIC
# MAGIC **Formats:** Lineage tracks **Delta** table relationships (Parquet files underneath). CSV bronze is usually tracked at the **path** level in Purview.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Lineage is a dependency map — which source tables feed which downstream tables and reports.
# MAGIC
# MAGIC **WHY:** Without it, nobody knows impact of a schema change; compliance cannot trace data to source.
# MAGIC
# MAGIC **HOW:** Create downstream from upstream (prob32 from prob01) → open Catalog **Lineage** tab → Purview for enterprise (Session 24+).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Data Lineage Tools**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 32: Data Lineage Gaps
# SOLUTION: Data Lineage Tools
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Data Lineage Gaps breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Data Lineage Tools on FinLedger bronze data.
# OUTPUT: Table prob32_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

# Lineage answers: "Which tables feed this report?" and "What breaks if I change this column?"
src_table = "prob01_deduped"
dst_table = "prob32_lineage_demo"
src = demo_table_ref(src_table)
write_demo_table(read_demo_table(src_table).limit(10), dst_table)
dst = demo_table_ref(dst_table)
print("LINEAGE DEMO")
print("  upstream (source) :", src)
print("  downstream (new)  :", dst)
print("  rows written      :", read_demo_table(dst_table).count())
print()
print("View lineage in Databricks:")
print("  Catalog ->", dst if UC_WRITE_ENABLED else dst_table, "-> Lineage tab")
print("  Upstream should show:", src_table, "(Problem 01 deduped output)")
print("Enterprise: Azure Purview scans ADLS and registers cross-system lineage (Session 24+)")
try:
    spark.sql(f"DESCRIBE EXTENDED {dst}").filter("col_name = 'Provider'").show(truncate=False)
except Exception as e:
    print("Extended metadata (UC only):", e)

# COMMAND ----------

# Verify problem 32 output
try:
    _df = read_demo_table("prob32_lineage_demo")
    print("VERIFY OK:", "prob32_lineage_demo", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 33 — Poor Documentation
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Poor Documentation |
# MAGIC | **The solution** | **Centralized Documentation** |
# MAGIC | **When to apply** | Every gold table; link to repo README. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** New engineer reads UC column comment on `transaction_id` — no Slack hunt.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Documentation (UC comments)
# MAGIC
# MAGIC 1. **COMMENT ON TABLE** — business description.
# MAGIC 2. **COMMENT ON COLUMN** — what `transaction_id` means.
# MAGIC 3. **Visible** in Catalog UI and `DESCRIBE EXTENDED`.
# MAGIC 4. **Skip** if no UC write permission (ADLS fallback mode).
# MAGIC
# MAGIC **Metadata** on **Delta** tables.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Column meanings live in someone's head.
# MAGIC
# MAGIC **WHY:** Onboarding slow; wrong joins.
# MAGIC
# MAGIC **HOW:** UC table/column COMMENTs; markdown in repo README.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Centralized Documentation**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 33: Poor Documentation
# SOLUTION: Centralized Documentation
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Poor Documentation breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Centralized Documentation on FinLedger bronze data.
# OUTPUT: Table prob33_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

if UC_WRITE_ENABLED:
    spark.sql(f"""
COMMENT ON TABLE {UC_TABLE}.prob01_deduped IS
'Deduplicated bronze transactions — Problem 01 demo. Owner: data-platform.'
""")
    spark.sql(f"""
COMMENT ON COLUMN {UC_TABLE}.prob01_deduped.transaction_id IS
'Natural business key — unique per payment'
""")
    print("Comments applied. View: DESCRIBE TABLE EXTENDED", f"{UC_TABLE}.prob01_deduped")
    spark.sql(f"DESCRIBE TABLE EXTENDED {UC_TABLE}.prob01_deduped").show(5, truncate=False)
else:
    print("SKIP: UC comments need write permission on schema")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part G — Analytics & dimensions (34–37)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 34 — Inconsistent Metrics
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Inconsistent Metrics |
# MAGIC | **The solution** | **Metric Standardization** |
# MAGIC | **When to apply** | Publishing gold layer; data dictionary alignment. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Team A uses `amount`, Team B `amount_gbp` — gold uses one canonical name.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Metric standardization
# MAGIC
# MAGIC 1. **One canonical name** — `metric_amount_gbp`.
# MAGIC 2. **Gold layer** enforces naming dictionary.
# MAGIC 3. **Avoid** `amount` vs `amt` vs `gbp` across teams.
# MAGIC 4. **Write** canonical **Delta** gold.
# MAGIC
# MAGIC **Semantic layer** on Parquet/Delta files.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** `revenue`, `amount`, `gbp_amt` mean the same thing.
# MAGIC
# MAGIC **WHY:** Different teams name columns differently.
# MAGIC
# MAGIC **HOW:** Gold layer canonical names + data dictionary.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Metric Standardization**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 34: Inconsistent Metrics
# SOLUTION: Metric Standardization
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Inconsistent Metrics breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Metric Standardization on FinLedger bronze data.
# OUTPUT: Table prob34_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

canonical = bronze.select(
    F.col("transaction_id"),
    F.col("amount_gbp").cast("double").alias("metric_amount_gbp"),  # canonical name
    F.col("channel").alias("dim_channel"),
)
write_demo_table(canonical, "prob34_canonical_metrics", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob34_canonical_metrics")
canonical.printSchema()

# COMMAND ----------

# Verify problem 34 output
try:
    _df = read_demo_table("prob34_canonical_metrics")
    print("VERIFY OK:", "prob34_canonical_metrics", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 35 — Data Drift
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Data Drift |
# MAGIC | **The solution** | **Data Drift Detection** |
# MAGIC | **When to apply** | Append run metrics; compare week-over-week. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Today's average payment 3× last week — drift profile flags upstream bug.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Data drift detection
# MAGIC
# MAGIC 1. **Profile** each run — row count, avg amount, nulls.
# MAGIC 2. **Append** to `drift_profile` Delta table.
# MAGIC 3. **Compare** week-over-week.
# MAGIC 4. **Alert** if avg shifts > 20%.
# MAGIC
# MAGIC **Quality** metadata in **Delta**.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Today's distribution differs from last week.
# MAGIC
# MAGIC **WHY:** Upstream bug or market shift.
# MAGIC
# MAGIC **HOW:** Compare null %, min/max, row counts run-over-run.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Data Drift Detection**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 35: Data Drift
# SOLUTION: Data Drift Detection
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Data Drift breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Data Drift Detection on FinLedger bronze data.
# OUTPUT: Table prob35_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

profile = bronze.agg(
    F.count("*").alias("row_count"),
    F.avg("amount_gbp").alias("avg_amount"),
    F.sum(F.when(F.col("channel").isNull(), 1).otherwise(0)).alias("null_channels"),
)
profile = profile.withColumn("run_id", F.lit(RUN_ID))
write_demo_table(profile, "prob35_drift_profile", mode="append")
print("Append profile each run; alert if avg_amount shifts > 20%")
profile.show()

# COMMAND ----------

# Verify problem 35 output
try:
    _df = read_demo_table("prob35_drift_profile")
    print("VERIFY OK:", "prob35_drift_profile", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 36 — Dimension Changes (SCD)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Dimension Changes (SCD) |
# MAGIC | **The solution** | **SCD Type 2** |
# MAGIC | **When to apply** | Point-in-time reporting on slowly changing dimensions. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Customer channel changes — SCD2 keeps history with `effective_from/to`.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: SCD Type 2
# MAGIC
# MAGIC 1. **Dimension changes** — channel renamed or reclassified.
# MAGIC 2. **Columns** — `effective_from`, `effective_to`, `is_current`.
# MAGIC 3. **MERGE** — close old row, insert new version.
# MAGIC 4. **Point-in-time** reports use date filter.
# MAGIC
# MAGIC **Classic warehouse pattern on Delta.**
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Customer address changes; need history.
# MAGIC
# MAGIC **WHY:** Point-in-time reporting.
# MAGIC
# MAGIC **HOW:** `effective_from`, `effective_to`, `is_current` + MERGE.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **SCD Type 2**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 36: Dimension Changes (SCD)
# SOLUTION: SCD Type 2
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Dimension Changes (SCD) breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates SCD Type 2 on FinLedger bronze data.
# OUTPUT: Table prob36_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

dim = bronze.select(
    F.col("channel").alias("channel_key"),
    F.col("channel").alias("channel_name"),
    F.current_date().alias("effective_from"),
    F.lit(None).cast("date").alias("effective_to"),
    F.lit(True).alias("is_current"),
).distinct()
write_demo_table(dim, "prob36_scd2_channel", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob36_scd2_channel")
dim.show()

# COMMAND ----------

# Verify problem 36 output
try:
    _df = read_demo_table("prob36_scd2_channel")
    print("VERIFY OK:", "prob36_scd2_channel", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 37 — Reference Data Updates
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Reference Data Updates |
# MAGIC | **The solution** | **Dimension Refresh Strategy** |
# MAGIC | **When to apply** | Reference data < 100k rows; daily refresh. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** FX rate table refreshed daily — small dim full overwrite.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Reference data refresh
# MAGIC
# MAGIC 1. **Small lookup** — channel reference table.
# MAGIC 2. **Full overwrite** daily if < 100k rows.
# MAGIC 3. **MERGE** if large slowly-changing ref.
# MAGIC 4. **Join** facts to latest ref in silver.
# MAGIC
# MAGIC **Ref dim as small **Delta** table.**
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Lookup table (FX rates, product codes) changes daily.
# MAGIC
# MAGIC **WHY:** Stale joins produce wrong amounts.
# MAGIC
# MAGIC **HOW:** Overwrite small dimension or MERGE on key.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Dimension Refresh Strategy**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 37: Reference Data Updates
# SOLUTION: Dimension Refresh Strategy
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Reference Data Updates breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Dimension Refresh Strategy on FinLedger bronze data.
# OUTPUT: Table prob37_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

ref = bronze.select("channel").distinct().withColumn("refreshed_at", F.current_timestamp())
write_demo_table(ref, "prob37_ref_channel", mode="overwrite")
print("Full overwrite OK for small reference dims (<100k rows)")
ref.show()

# COMMAND ----------

# Verify problem 37 output
try:
    _df = read_demo_table("prob37_ref_channel")
    print("VERIFY OK:", "prob37_ref_channel", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part H — Integration & integrity (38–44)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 38 — External API Failures
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | External API Failures |
# MAGIC | **The solution** | **Retry Logic / Backoff** |
# MAGIC | **When to apply** | Any external HTTP/REST ingest from notebook or driver. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Partner API timeout — retry 3× with exponential backoff before DLQ.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Retry with backoff
# MAGIC
# MAGIC 1. **API call fails** — transient network error.
# MAGIC 2. **Retry** 3 times with exponential sleep.
# MAGIC 3. **Fail** to DLQ after max attempts.
# MAGIC 4. **Log** success to Delta.
# MAGIC
# MAGIC **Driver-side** pattern before landing CSV bronze.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** REST call fails transiently.
# MAGIC
# MAGIC **WHY:** Network blips, 429 throttling.
# MAGIC
# MAGIC **HOW:** Exponential backoff retries (3–5 attempts).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Retry Logic / Backoff**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 38: External API Failures
# SOLUTION: Retry Logic / Backoff
# ------------------------------------------------------------------------
# WHAT GOES WRONG: External API Failures breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Retry Logic / Backoff on FinLedger bronze data.
# OUTPUT: Table prob38_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

import time
import random

def call_api_with_retry(fn, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as exc:
            wait = 2 ** attempt + random.random()
            print(f"Attempt {attempt} failed: {exc} — sleep {wait:.1f}s")
            if attempt == max_attempts:
                raise
            time.sleep(wait)

result = call_api_with_retry(lambda: bronze.count())
print("API-equivalent call succeeded, rows:", result)

write_demo_table(spark.createDataFrame([(result, "ok")], ["rows", "status"]), "prob38_retry_result")
print("UC table:", f"{UC_TABLE}.prob38_retry_result")

# COMMAND ----------

# Verify problem 38 output
try:
    _df = read_demo_table("prob38_retry_result")
    print("VERIFY OK:", "prob38_retry_result", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 39 — Rate Limiting
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Rate Limiting |
# MAGIC | **The solution** | **Throttling / Queuing** |
# MAGIC | **When to apply** | Calling external APIs from pipeline driver. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** API allows 100 req/min — throttle batches to avoid HTTP 429.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Rate limiting / throttle
# MAGIC
# MAGIC 1. **Partner API** — max 100 req/min.
# MAGIC 2. **Batch + sleep** between batches.
# MAGIC 3. **Respect** `Retry-After` header in production.
# MAGIC 4. **Log** throttle stats to Delta.
# MAGIC
# MAGIC **Ingest** pattern before bronze file exists.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** API returns HTTP 429 Too Many Requests.
# MAGIC
# MAGIC **WHY:** Exceeding provider quota.
# MAGIC
# MAGIC **HOW:** Sleep between calls; queue work; batch requests.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Throttling / Queuing**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 39: Rate Limiting
# SOLUTION: Throttling / Queuing
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Rate Limiting breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Throttling / Queuing on FinLedger bronze data.
# OUTPUT: Table prob39_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

import time

BATCH_SIZE = 50
total = bronze.count()
batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
for b in range(batches):
    print(f"Batch {b + 1}/{batches} — throttle sleep 0.3s (simulates API rate limit)")
    time.sleep(0.3)
print("Throttling prevents HTTP 429 from partner APIs")

write_demo_table(spark.createDataFrame([(total, BATCH_SIZE, batches)], ["total_rows", "batch_size", "batch_count"]), "prob39_throttle_log")
print("UC table:", f"{UC_TABLE}.prob39_throttle_log")

# COMMAND ----------

# Verify problem 39 output
try:
    _df = read_demo_table("prob39_throttle_log")
    print("VERIFY OK:", "prob39_throttle_log", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 40 — File Corruption
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | File Corruption |
# MAGIC | **The solution** | **File Validation & Checksums** |
# MAGIC | **When to apply** | File lands from partner SFTP; validate before parse. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Landing file truncated — checksum mismatch before bronze ingest.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: File integrity (checksum)
# MAGIC
# MAGIC 1. **Landing file** — partner sends SHA256 in manifest.
# MAGIC 2. **Compute hash** after upload (lab: row-count hash proxy).
# MAGIC 3. **Reject** ingest if mismatch — corrupt **CSV**.
# MAGIC 4. **Log** integrity row to Delta.
# MAGIC
# MAGIC **Before** parsing CSV to bronze.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Partial upload or bit rot in landing zone.
# MAGIC
# MAGIC **WHY:** Silent bad parses.
# MAGIC
# MAGIC **HOW:** MD5/SHA256 on landing; reject if mismatch.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **File Validation & Checksums**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 40: File Corruption
# SOLUTION: File Validation & Checksums
# ------------------------------------------------------------------------
# WHAT GOES WRONG: File Corruption breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates File Validation & Checksums on FinLedger bronze data.
# OUTPUT: Table prob40_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

import hashlib

row_count = bronze.count()
checksum = hashlib.sha256(f"{BRONZE_PATH}:{row_count}".encode()).hexdigest()[:16]
print("Bronze path  :", BRONZE_PATH)
print("Row count    :", row_count)
print("Integrity hash:", checksum)
print("Production: compare SHA256 from landing manifest before ingest")

write_demo_table(spark.createDataFrame([(BRONZE_PATH, row_count, checksum)], ["path", "rows", "checksum"]), "prob40_file_integrity")
print("UC table:", f"{UC_TABLE}.prob40_file_integrity")

# COMMAND ----------

# Verify problem 40 output
try:
    _df = read_demo_table("prob40_file_integrity")
    print("VERIFY OK:", "prob40_file_integrity", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 41 — Inconsistent Timestamps
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Inconsistent Timestamps |
# MAGIC | **The solution** | **Time Standardization (UTC)** |
# MAGIC | **When to apply** | Multi-region bank; regulatory timestamps. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** All `value_date` stored as UTC — reports consistent globally.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: UTC timestamps
# MAGIC
# MAGIC 1. **Store** all times in UTC in silver.
# MAGIC 2. **to_utc_timestamp** from `Europe/London` local.
# MAGIC 3. **Consistent** global reporting.
# MAGIC 4. **Write** UTC column to **Delta**.
# MAGIC
# MAGIC **Type enforcement** in Delta schema.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Mix of local times without timezone.
# MAGIC
# MAGIC **WHY:** DST and locale bugs in reports.
# MAGIC
# MAGIC **HOW:** Store UTC; convert at presentation layer.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Time Standardization (UTC)**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 41: Inconsistent Timestamps
# SOLUTION: Time Standardization (UTC)
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Inconsistent Timestamps breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Time Standardization (UTC) on FinLedger bronze data.
# OUTPUT: Table prob41_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

utc = bronze.withColumn(
    "txn_utc",
    F.to_utc_timestamp(F.to_timestamp(F.col("value_date")), "Europe/London"),
)
utc.select("value_date", "txn_utc").show(3)
write_demo_table(utc, "prob41_utc_timestamps", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob41_utc_timestamps")

# COMMAND ----------

# Verify problem 41 output
try:
    _df = read_demo_table("prob41_utc_timestamps")
    print("VERIFY OK:", "prob41_utc_timestamps", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 42 — Time Zone Issues
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Time Zone Issues |
# MAGIC | **The solution** | **Time Zone Normalization** |
# MAGIC | **When to apply** | Dashboards need local timezone; storage stays UTC. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Display London time to UK users from UTC storage.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Time zone normalization
# MAGIC
# MAGIC 1. **Read** UTC from Problem 41 table.
# MAGIC 2. **from_utc_timestamp** for London display.
# MAGIC 3. **Storage UTC, display local** — best practice.
# MAGIC 4. **Write** local display column to Delta.
# MAGIC
# MAGIC **Presentation** layer on **Delta** gold.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** London vs Mumbai vs New York same instant.
# MAGIC
# MAGIC **WHY:** Global bank operates across zones.
# MAGIC
# MAGIC **HOW:** `from_utc_timestamp` for display region.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Time Zone Normalization**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 42: Time Zone Issues
# SOLUTION: Time Zone Normalization
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Time Zone Issues breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Time Zone Normalization on FinLedger bronze data.
# OUTPUT: Table prob42_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

base = read_demo_table("prob41_utc_timestamps")
local = base.withColumn("txn_london", F.from_utc_timestamp(F.col("txn_utc"), "Europe/London"))
local.select("txn_utc", "txn_london").show(3)
write_demo_table(local, "prob42_tz_normalized", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob42_tz_normalized")

# COMMAND ----------

# Verify problem 42 output
try:
    _df = read_demo_table("prob42_tz_normalized")
    print("VERIFY OK:", "prob42_tz_normalized", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 43 — Orphan Records
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Orphan Records |
# MAGIC | **The solution** | **Referential Integrity Checks** |
# MAGIC | **When to apply** | FK validation before gold publish. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Payment references `channel` not in dim — left anti-join finds orphans.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Orphan records (referential integrity)
# MAGIC
# MAGIC 1. **Facts** — bronze transactions.
# MAGIC 2. **Dims** — valid channels.
# MAGIC 3. **left_anti join** — facts with no matching dim = orphans.
# MAGIC 4. **Quarantine** orphans; write valid to silver **Delta**.
# MAGIC
# MAGIC **Data quality** before gold.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Fact rows reference missing dimension keys.
# MAGIC
# MAGIC **WHY:** Late dimension load or bad FK.
# MAGIC
# MAGIC **HOW:** Left anti-join facts vs dims → quarantine orphans.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Referential Integrity Checks**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 43: Orphan Records
# SOLUTION: Referential Integrity Checks
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Orphan Records breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Referential Integrity Checks on FinLedger bronze data.
# OUTPUT: Table prob43_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

facts = bronze
dims = bronze.select("channel").distinct()
orphans = facts.join(dims, "channel", "left_anti")
print("Orphan rows (channel not in dim):", orphans.count())
valid = facts.join(dims, "channel", "inner")
write_demo_table(valid, "prob43_referential_valid", mode="overwrite")
write_demo_table(orphans, "prob43_orphans", mode="overwrite")
print("UC tables: prob43_referential_valid, prob43_orphans")

# COMMAND ----------

# Verify problem 43 output
try:
    _df = read_demo_table("prob43_referential_valid")
    print("VERIFY OK:", "prob43_referential_valid", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 44 — Data Duplication Across Systems
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Data Duplication Across Systems |
# MAGIC | **The solution** | **Idempotent Processing** |
# MAGIC | **When to apply** | Any retry-safe pipeline design. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** ADF retries pipeline — same `run_id` MERGE does not double-count.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Idempotent processing
# MAGIC
# MAGIC 1. **pipeline_run_id** column on every batch.
# MAGIC 2. **MERGE or overwrite** same run — ADF retry safe.
# MAGIC 3. **No duplicate** gold rows on retry.
# MAGIC 4. **Link** Problem 15 MERGE.
# MAGIC
# MAGIC **Reliability** on **Delta** writes.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Same batch loaded twice into gold.
# MAGIC
# MAGIC **WHY:** ADF retry without idempotency key.
# MAGIC
# MAGIC **HOW:** MERGE on natural key; deterministic run_id.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Idempotent Processing**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 44: Data Duplication Across Systems
# SOLUTION: Idempotent Processing
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Data Duplication Across Systems breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Idempotent Processing on FinLedger bronze data.
# OUTPUT: Table prob44_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

run_df = bronze.withColumn("pipeline_run_id", F.lit(RUN_ID))
write_demo_table(run_df, "prob44_idempotent", mode="overwrite")
# Re-run same RUN_ID → overwrite same table (idempotent for this demo)
print("Same RUN_ID re-run overwrites — production: MERGE on business key")
print("UC table:", f"{UC_TABLE}.prob44_idempotent")

# COMMAND ----------

# Verify problem 44 output
try:
    _df = read_demo_table("prob44_idempotent")
    print("VERIFY OK:", "prob44_idempotent", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part I — Optimization & lifecycle (45–47)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 45 — Slow ETL Jobs
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Slow ETL Jobs |
# MAGIC | **The solution** | **Optimize Code & Resources** |
# MAGIC | **When to apply** | Profiling slow notebooks; removing UDFs and collect(). |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** ETL selects only needed columns early — 40% faster silver job.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Optimize ETL code
# MAGIC
# MAGIC 1. **Select early** — only needed columns.
# MAGIC 2. **Filter early** — reduce shuffle size.
# MAGIC 3. **Native Spark** — avoid Python UDFs.
# MAGIC 4. **One write** at end to **Delta**.
# MAGIC
# MAGIC **Performance** reading **Parquet** via Delta.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Job takes 3 hours instead of 30 minutes.
# MAGIC
# MAGIC **WHY:** UDFs, collect, wide selects.
# MAGIC
# MAGIC **HOW:** Native Spark functions; column pruning; broadcast small dims.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Optimize Code & Resources**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 45: Slow ETL Jobs
# SOLUTION: Optimize Code & Resources
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Slow ETL Jobs breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Optimize Code & Resources on FinLedger bronze data.
# OUTPUT: Table prob45_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

optimized = (
    bronze.select("transaction_id", "channel", "amount_gbp")  # prune columns early
    .filter(F.col("amount_gbp").isNotNull())
    .groupBy("channel")
    .agg(F.sum("amount_gbp").alias("total"))
)
write_demo_table(optimized, "prob45_optimized_etl", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob45_optimized_etl")

# COMMAND ----------

# Verify problem 45 output
try:
    _df = read_demo_table("prob45_optimized_etl")
    print("VERIFY OK:", "prob45_optimized_etl", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 46 — High I/O Costs
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | High I/O Costs |
# MAGIC | **The solution** | **Data Compression** |
# MAGIC | **When to apply** | Storage bill review; writing large historical archives. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Delta Parquet snappy compression cuts ADLS scan cost.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Compression / I/O cost
# MAGIC
# MAGIC 1. **Delta** uses **Parquet** with Snappy by default.
# MAGIC 2. **Smaller files** — less ADLS scan cost.
# MAGIC 3. **compression** option on write (lab demo).
# MAGIC 4. **Compare** to raw CSV size on bronze.
# MAGIC
# MAGIC **When Parquet enters:** Every Delta write creates compressed Parquet files — this problem makes cost explicit.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Storage egress and scan costs dominate.
# MAGIC
# MAGIC **WHY:** Uncompressed CSV landing.
# MAGIC
# MAGIC **HOW:** Delta/Parquet with snappy or zstd compression.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Data Compression**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 46: High I/O Costs
# SOLUTION: Data Compression
# ------------------------------------------------------------------------
# WHAT GOES WRONG: High I/O Costs breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Data Compression on FinLedger bronze data.
# OUTPUT: Table prob46_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

comp_path = f"{DEMO_BASE}/prob46_compressed"
bronze.write.format("delta").option("compression", "snappy").mode("overwrite").save(comp_path)
print("Delta default: Parquet + snappy compression at", comp_path)
write_demo_table(spark.read.format("delta").load(comp_path), "prob46_compressed")
print("UC table:", f"{UC_TABLE}.prob46_compressed")

# COMMAND ----------

# Verify problem 46 output
try:
    _df = read_demo_table("prob46_compressed")
    print("VERIFY OK:", "prob46_compressed", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 47 — Data Retention Issues
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Data Retention Issues |
# MAGIC | **The solution** | **Lifecycle Policies** |
# MAGIC | **When to apply** | ADLS lifecycle policy design; Delta VACUUM after retention. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Bronze raw kept 90 days then cool tier — GDPR + cost.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Lifecycle / retention
# MAGIC
# MAGIC 1. **Bronze CSV** — move to cool tier after 90 days (ADLS lifecycle).
# MAGIC 2. **Audit** — delete after 365 days per policy.
# MAGIC 3. **Delta VACUUM** — remove old files no longer referenced by log.
# MAGIC 4. **Legal hold** check before VACUUM.
# MAGIC
# MAGIC **Storage policy** on CSV + **Delta** folders.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Bronze kept forever → cost and GDPR risk.
# MAGIC
# MAGIC **WHY:** No TTL on raw layers.
# MAGIC
# MAGIC **HOW:** ADLS lifecycle move to cool/archive; Delta `VACUUM` after retention.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Lifecycle Policies**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 47: Data Retention Issues
# SOLUTION: Lifecycle Policies
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Data Retention Issues breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Lifecycle Policies on FinLedger bronze data.
# OUTPUT: Table prob47_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

print("ADLS lifecycle (Azure Portal → Storage → Lifecycle management):")
print("  bronze/loaded/* → cool after 90 days")
print("  audit/quarantine/* → delete after 365 days")
print("Delta: VACUUM table RETAIN 168 HOURS (7 days) — after legal hold check")

write_demo_table(spark.createDataFrame([("bronze", 90, "cool"), ("audit", 365, "delete")], ["layer", "days", "action"]), "prob47_retention_policy")
print("UC table:", f"{UC_TABLE}.prob47_retention_policy")

# COMMAND ----------

# Verify problem 47 output
try:
    _df = read_demo_table("prob47_retention_policy")
    print("VERIFY OK:", "prob47_retention_policy", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part J — Governance & ops (48–50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 48 — Compliance & Governance
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Compliance & Governance |
# MAGIC | **The solution** | **Data Governance Framework** |
# MAGIC | **When to apply** | Governance sprint; audit prep. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** UC tags `classification=internal` on payments table for Purview.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Governance tags
# MAGIC
# MAGIC 1. **UC tags** — `domain=payments`, `classification=internal`.
# MAGIC 2. **Purview scan** — classify columns.
# MAGIC 3. **SHOW TBLPROPERTIES** — verify tags.
# MAGIC 4. **Compliance** reporting from **Delta** metadata.
# MAGIC
# MAGIC **Governance** on catalog-registered Delta tables.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** No owner, classification, or audit trail.
# MAGIC
# MAGIC **WHY:** Regulatory (GDPR, PCI) requires traceability.
# MAGIC
# MAGIC **HOW:** UC tags + Purview classification (Session 24).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Data Governance Framework**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 48: Compliance & Governance
# SOLUTION: Data Governance Framework
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Compliance & Governance breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Data Governance Framework on FinLedger bronze data.
# OUTPUT: Table prob48_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

if UC_WRITE_ENABLED:
    spark.sql(f"""
ALTER TABLE {UC_TABLE}.prob01_deduped SET TAGS ('domain' = 'payments', 'classification' = 'internal')
""")
    print("UC tags set on prob01_deduped")
    spark.sql(f"SHOW TBLPROPERTIES {UC_TABLE}.prob01_deduped").show(10, truncate=False)
else:
    print("SKIP: UC tags need write permission")
print("Purview: scan ADLS + register lineage (see GOVERNANCE-DEPLOY.md)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 49 — Disaster Recovery
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | Disaster Recovery |
# MAGIC | **The solution** | **Backup & Restore Strategy** |
# MAGIC | **When to apply** | Operational recovery; DR drill. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** Analyst runs bad DELETE — `RESTORE TABLE` to version before mistake.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: Disaster recovery (time travel)
# MAGIC
# MAGIC 1. **DESCRIBE HISTORY** — list versions of Delta table.
# MAGIC 2. **Bad MERGE** — note version before mistake.
# MAGIC 3. **RESTORE** or `versionAsOf` read.
# MAGIC 4. **DR** — geo-redundant ADLS + Delta history.
# MAGIC
# MAGIC **Delta-only:** time travel needs `_delta_log`, not plain Parquet.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Accidental DROP TABLE or bad MERGE.
# MAGIC
# MAGIC **WHY:** Human error happens.
# MAGIC
# MAGIC **HOW:** Delta time travel RESTORE; geo-redundant ADLS.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **Backup & Restore Strategy**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 49: Disaster Recovery
# SOLUTION: Backup & Restore Strategy
# ------------------------------------------------------------------------
# WHAT GOES WRONG: Disaster Recovery breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates Backup & Restore Strategy on FinLedger bronze data.
# OUTPUT: Table prob49_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F

dr_path = f"{DEMO_BASE}/prob49_dr"
bronze.limit(30).write.format("delta").mode("overwrite").save(dr_path)
history = spark.sql(f"DESCRIBE HISTORY delta.`{dr_path}`")
history.show(3, truncate=False)
print("RESTORE TABLE ... TO VERSION AS OF <n>  — or restore path via clone")
write_demo_table(bronze.limit(30), "prob49_dr_backup", mode="overwrite")
print("UC table:", f"{UC_TABLE}.prob49_dr_backup")

# COMMAND ----------

# Verify problem 49 output
try:
    _df = read_demo_table("prob49_dr_backup")
    print("VERIFY OK:", "prob49_dr_backup", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem 50 — End-to-End Visibility
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **The problem** | End-to-End Visibility |
# MAGIC | **The solution** | **End-to-End Monitoring** |
# MAGIC | **When to apply** | End of this notebook; template for daily ops check. |
# MAGIC
# MAGIC ### FinLedger use case
# MAGIC
# MAGIC **FinLedger example:** One table shows bronze rows, UC tables created, job status — morning dashboard.
# MAGIC
# MAGIC ### Trainer: explain step-by-step
# MAGIC
# MAGIC ### Step-by-step: End-to-end monitoring
# MAGIC
# MAGIC 1. **Stages** — bronze ingest, tables created, notebook complete.
# MAGIC 2. **Metrics row** per stage in `prob50_e2e_monitoring`.
# MAGIC 3. **List** all tables/paths written this run.
# MAGIC 4. **Morning dashboard** — one query on Delta control tables.
# MAGIC
# MAGIC **Ties all 50 problems** — formats, Delta, UC, ops.
# MAGIC
# MAGIC ### Quick reference (WHAT / WHY / HOW)
# MAGIC
# MAGIC **WHAT:** Cannot trace bronze → gold → dashboard latency.
# MAGIC
# MAGIC **WHY:** Siloed logs per tool.
# MAGIC
# MAGIC **HOW:** Unified metrics table + ADF Monitor + Databricks job runs.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Code demonstrates **End-to-End Monitoring**. Read the banner comments, then execute.

# COMMAND ----------

# ========================================================================
# PROBLEM 50: End-to-End Visibility
# SOLUTION: End-to-End Monitoring
# ------------------------------------------------------------------------
# WHAT GOES WRONG: End-to-End Visibility breaks or slows the pipeline.
# WHAT THIS CODE DOES: Demonstrates End-to-End Monitoring on FinLedger bronze data.
# OUTPUT: Table prob50_* in Unity Catalog or ADLS (see write_demo_table log).
# ========================================================================

from pyspark.sql import functions as F
import json

tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {UC_TABLE}").collect()]
e2e = spark.createDataFrame([
    (RUN_ID, "bronze_ingest", bronze.count(), "OK"),
    (RUN_ID, "uc_tables_created", len(tables), "OK"),
    (RUN_ID, "notebook", "50_problems", "COMPLETE"),
], ["run_id", "stage", "metric_value", "status"])

write_demo_table(e2e, "prob50_e2e_monitoring", mode="overwrite")
print("E2E summary:")
e2e.show()
print("UC tables in schema:", len(tables))
for t in sorted(tables)[:15]:
    print(" ", t)
if len(tables) > 15:
    print(" ... and", len(tables) - 15, "more")

# COMMAND ----------

# Verify problem 50 output
try:
    _df = read_demo_table("prob50_e2e_monitoring")
    print("VERIFY OK:", "prob50_e2e_monitoring", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final — all tables created this run

# COMMAND ----------

if UC_WRITE_ENABLED:
    display(spark.sql(f"SHOW TABLES IN {UC_TABLE}"))
    for row in sorted(spark.sql(f"SHOW TABLES IN {UC_TABLE}").collect(), key=lambda r: r.tableName):
        try:
            n = spark.table(f"{UC_TABLE}.{row.tableName}").count()
            print(f"  {UC_TABLE}.{row.tableName:42s} {n:>8} rows")
        except Exception as exc:
            print(f"  {UC_TABLE}.{row.tableName:42s}  error: {exc}")
else:
    print("ADLS Delta folders under", DEMO_BASE)
    for item in sorted(dbutils.fs.ls(DEMO_BASE)):
        print(" ", item.path)
print("-" * 70)
print("Written this run:", len(WRITTEN_TABLES), "tables/paths")
print("NOTEBOOK COMPLETE — 50 problems demonstrated.")

# COMMAND ----------
