# Databricks notebook source
# MAGIC %md
# MAGIC # FinLedger Databricks — Teach-All Topics
# MAGIC
# MAGIC A **large teaching notebook** (same spirit as the 50-problems lab):
# MAGIC **50 topics** — complicated industry problems explained the **easy** way with **analogies** + **commented code**.
# MAGIC
# MAGIC **Audience:** Trainers walking Sessions 8–19 / 27–30, or learners revising Spark + Delta + Events + Jobs.
# MAGIC
# MAGIC ## How to run
# MAGIC
# MAGIC 1. Prefer **classic** cluster; Serverless works via RBAC → SDK → DBFS fallback (account keys blocked)  
# MAGIC 2. `deploy-shared-lab.cmd` once  
# MAGIC 3. Open `/Shared/day9/dbx_teach_all_topics` → **Run all** from the top  
# MAGIC 4. Trainer script: `day9/docs/dbx_teach_all_topics_transcript.md`  
# MAGIC
# MAGIC ## Companion notebooks
# MAGIC
# MAGIC | Notebook | Role |
# MAGIC |----------|------|
# MAGIC | `50_data_engineering_problems` | 50 failure modes |
# MAGIC | `finledger_event_processors` | Zach Event runner deep-dive |
# MAGIC | **`dbx_teach_all_topics`** | **This — curated teach path with analogies** |
# MAGIC
# MAGIC ## Topic groups
# MAGIC - **A.** Spark fundamentals (start here)
# MAGIC - **B.** Medallion & cleansing
# MAGIC - **C.** Delta Lake power tools
# MAGIC - **D.** Zach Event model (multi-source)
# MAGIC - **E.** Reliability & quality
# MAGIC - **F.** Performance (hard → easy)
# MAGIC - **G.** SQL, Unity Catalog, secrets
# MAGIC - **H.** Jobs, widgets, ADF hooks
# MAGIC - **I.** Complicated ideas, simple stories
# MAGIC - **J.** Reshape & everyday pitfalls
# MAGIC - **K.** Lakehouse ops & handoff
# MAGIC - **L.** Jobs, governance, teach-ready
# MAGIC
# MAGIC ## Full index
# MAGIC
# MAGIC | # | Topic | Easy approach |
# MAGIC |---|-------|---------------|
# MAGIC | 01 | Lazy vs Action (why nothing happens until .count) | Build a plan first (lazy). Pay only when you ask for a resul |
# MAGIC | 02 | Schema & cast (types before gold) | Cast early in silver. Never trust bronze types. |
# MAGIC | 03 | select / filter / withColumn (everyday verbs) | Stay in DataFrame verbs — Spark parallelizes them. |
# MAGIC | 04 | groupBy + agg (channel totals) | Aggregate in Spark; ship summary rows. |
# MAGIC | 05 | join (txn to returns) without Cartesian bombs | Always join on business keys (`transaction_id`). Know left v |
# MAGIC | 06 | Medallion bronze → silver → gold (layers) | Three layers with different promises: raw / cleansed / busin |
# MAGIC | 07 | Quarantine / dead-letter (bad rows don't kill the batch) | Split good vs bad; write both; never fail the whole night fo |
# MAGIC | 08 | Deduplicate by business key | `dropDuplicates(["transaction_id"])` or window keep latest. |
# MAGIC | 09 | Why Delta (not only Parquet) | Delta = Parquet files + `_delta_log` transaction log (ACID). |
# MAGIC | 10 | Time travel & RESTORE (undo a bad load) | `RESTORE TABLE … TO VERSION AS OF n` or `versionAsOf`. |
# MAGIC | 11 | MERGE upsert (incremental without full reload) | MERGE matched UPDATE / not matched INSERT on business key. |
# MAGIC | 12 | OPTIMIZE + ZORDER (small files) | OPTIMIZE compacts; ZORDER clusters filter columns. |
# MAGIC | 13 | Event schema contract (Zach Wilson style) | Force every source into Event(source, metric_name, timestamp |
# MAGIC | 14 | Processor per source (no spaghetti joins) | Each source has one processor function that only speaks Even |
# MAGIC | 15 | Runner → daily aggregates → pivot (analyst table) | Long Event table → groupBy day → pivot(metric). |
# MAGIC | 16 | Data quality gate (assert before gold) | `assert_quality(df)` — empty or null keys ⇒ fail the job fas |
# MAGIC | 17 | Idempotent re-runs (same run_id safe) | Overwrite/MERGE by `run_id` path — never append blindly. |
# MAGIC | 18 | Watermark / incremental window | Only process rows after last successful watermark date. |
# MAGIC | 19 | Partition pruning (don't scan the world) | Partition by `value_date` or `event_date` you always filter  |
# MAGIC | 20 | Broadcast join (small dim × big fact) | `F.broadcast(small_df)` — send the postage stamp to every wo |
# MAGIC | 21 | Skew / salting (one fat key) | Salt the hot key into N buckets during the heavy stage. |
# MAGIC | 22 | cache / persist (reuse same DataFrame) | `.cache()` after an expensive stage you reuse. |
# MAGIC | 23 | Spark SQL (%sql) same data | Register temp view; use SQL — same engine. |
# MAGIC | 24 | Unity Catalog names (catalog.schema.table) | Three-level namespace + GRANT when UC write is enabled. |
# MAGIC | 25 | COMMENTS + TAGS (docs as metadata) | `COMMENT ON TABLE/COLUMN` + tags for domain/classification. |
# MAGIC | 26 | Secrets — never hard-code keys | `dbutils.secrets.get(scope, key)` only. |
# MAGIC | 27 | Widgets = Job / ADF parameters | `dbutils.widgets.text` then `get` — ADF passes baseParameter |
# MAGIC | 28 | dbutils.notebook.exit (return JSON to ADF) | Exit with JSON string; ADF activity output captures it. |
# MAGIC | 29 | Jobs vs interactive (why schedule) | Databricks Job = schedule + history + alerts; notebook stays |
# MAGIC | 30 | SCD Type 2 (history of a dimension) — hard idea, easy pattern | Keep `valid_from`/`valid_to`/`is_current` rows per key. |
# MAGIC | 31 | Late data (watermark acceptance window) | Accept late events within a watermark window; mark straggler |
# MAGIC | 32 | CDC mindset (changes only) | Process inserts/updates/deletes only — Delta CDF or SQL chan |
# MAGIC | 33 | Cost habits (auto-terminate, right size) | Auto-termination, small skill-cluster for class, stop when d |
# MAGIC | 34 | End-to-end story (wire the pieces) | Narrate bronze → silver → Event → gold → Job/ADF → quality. |
# MAGIC | 35 | Window functions (rank without collapsing rows) | Window `rank()` / `row_number()` over partition — filter aft |
# MAGIC | 36 | Pivot (turn channels into columns) | `groupBy(...).pivot("channel").sum(...)` then fillna(0). |
# MAGIC | 37 | Union vs Join (stack vs stitch) | Same grain + same columns → unionByName. Different entities  |
# MAGIC | 38 | nulls, coalesce, fillna (missing ≠ zero) | `coalesce` for defaults; quarantine null keys — don't invent |
# MAGIC | 39 | Cache / persist (reuse expensive scans) | `cache()` once after expensive clean; `unpersist()` when don |
# MAGIC | 40 | UDF trap (why stay in Spark SQL functions) | Prefer `F.upper`, `F.when`, built-ins; UDF only if no Spark  |
# MAGIC | 41 | VACUUM & retention (don't delete history by accident) | Know retention hours; vacuum only after policy; never vacuum |
# MAGIC | 42 | Partition pruning (only read needed folders) | Partition gold by date; filter that column so Spark skips fo |
# MAGIC | 43 | Structured Streaming mindset (micro-batches) | Same DataFrame API; source is a stream + checkpoint for exac |
# MAGIC | 44 | Auto Loader idea (discover new files) | cloudFiles + schema location; incremental file notification/ |
# MAGIC | 45 | JDBC / Azure SQL bridge (warehouse handoff) | JDBC write/read with secrets; or ADF copy gold → Azure SQL. |
# MAGIC | 46 | Multi-task Jobs (DAG of notebooks) | Job tasks: bronze → silver → gold with depends_on; retry per |
# MAGIC | 47 | Purview / lineage habit (what did we just write?) | Stable paths + table names; scan ADLS; search storage accoun |
# MAGIC | 48 | %run and shared helpers (don't paste auth 50 times) | One bronze_auth builder → generated notebooks; or `%run` a s |
# MAGIC | 49 | Interview story (explain your pipeline in 90 seconds) | Narrate: source → bronze → silver/Event → gold → Job/ADF → q |
# MAGIC | 50 | Capstone checklist (you are ready when…) | Tick every box below on real cluster with FinLedger data. |

# COMMAND ----------

# MAGIC %md
# MAGIC # How to teach with this notebook
# MAGIC
# MAGIC 1. **Read the markdown** (hard problem → easy approach → analogy) **before** clicking Run.
# MAGIC 2. In the code cell, read the `# WHAT / WHY / HOW` banner aloud.
# MAGIC 3. Run the cell; point at the printed output.
# MAGIC 4. Ask: *"Where would this break in production?"* — then point back at the hard problem.
# MAGIC
# MAGIC **Analogy rule:** if you can't explain it with a kitchen / airport / library story, the topic isn't ready.

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

# Teaching widgets — ADF/Jobs can override
dbutils.widgets.text("run_id", "session3-lab", "Pipeline run id")
RUN_ID = dbutils.widgets.get("run_id").strip() or "session3-lab"
print("Teaching RUN_ID =", RUN_ID)

# COMMAND ----------

# =============================================================================
# SETUP — writable Unity Catalog schema OR ADLS / DBFS fallback
# =============================================================================
# WHAT: Find a schema you are allowed to write to (finledger/main.demo_problems).
# WHY:  Shared workspace catalog dbw_shared_* often denies USE SCHEMA to learners.
# HOW:  Probe UC write; if denied, try ADLS Delta; if Serverless blocks key/RBAC → DBFS.
# =============================================================================
from pyspark.sql import functions as F

PREFERRED_CATALOG = "finledger"
FALLBACK_CATALOG = "main"
SCHEMA = "demo_problems"
DEMO_BASE_ADLS = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/demo_50_problems"
DEMO_BASE_DBFS = "dbfs:/FileStore/demo_50_problems"
CATALOG_ROOT = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_unity_catalog/{PREFERRED_CATALOG}"

# Prefer ADLS when RBAC works; otherwise DBFS (Serverless-safe)
DEMO_BASE = DEMO_BASE_ADLS
UC_WRITE_ENABLED = False
WRITTEN_TABLES: list[str] = []


def _try_mkdirs(path: str) -> bool:
    """Create folder; return False if Spark/dbutils cannot talk to that filesystem."""
    try:
        dbutils.fs.mkdirs(path)
        return True
    except Exception as exc:
        print(f"  mkdirs skip ({path[:60]}…):", str(exc)[:120])
        return False


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


def _probe_delta_path(base: str) -> bool:
    """Confirm we can write+read a tiny Delta folder under base."""
    probe = f"{base}/_write_probe"
    try:
        spark.createDataFrame([("ok",)], ["ping"]).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(probe)
        spark.read.format("delta").load(probe).count()
        return True
    except Exception as exc:
        print(f"  Delta probe failed ({base[:50]}…):", str(exc)[:140])
        return False


def _init_storage():
    global CATALOG, UC_TABLE, UC_WRITE_ENABLED, DEMO_BASE
    names = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    print("Catalogs visible:", names)

    candidates = []
    for c in (PREFERRED_CATALOG, FALLBACK_CATALOG):
        if c in names:
            candidates.append(c)
    if PREFERRED_CATALOG not in names:
        # Creating an external catalog needs a managed location — soft if ADLS blocked
        if _try_mkdirs(CATALOG_ROOT):
            try:
                spark.sql(
                    f"CREATE CATALOG IF NOT EXISTS {PREFERRED_CATALOG} MANAGED LOCATION '{CATALOG_ROOT}'"
                )
                candidates.insert(0, PREFERRED_CATALOG)
            except Exception as exc:
                print("CREATE CATALOG finledger skipped:", str(exc)[:160])
        else:
            print("CREATE CATALOG finledger skipped: no ADLS path for managed location")

    for catalog in candidates:
        print(f"Testing write access: {catalog}.{SCHEMA} ...")
        if _try_catalog_write(catalog):
            CATALOG = catalog
            UC_TABLE = f"{catalog}.{SCHEMA}"
            UC_WRITE_ENABLED = True
            print("UC write mode: ENABLED →", UC_TABLE)
            # Still pick a DEMO_BASE for path demos (topics that write Delta folders)
            if _try_mkdirs(DEMO_BASE_ADLS) and _probe_delta_path(DEMO_BASE_ADLS):
                DEMO_BASE = DEMO_BASE_ADLS
            else:
                _try_mkdirs(DEMO_BASE_DBFS)
                DEMO_BASE = DEMO_BASE_DBFS
                print("Path demos use DBFS:", DEMO_BASE)
            return

    # No UC — try ADLS, then DBFS
    CATALOG = "adls"
    UC_TABLE = SCHEMA
    UC_WRITE_ENABLED = False
    if _try_mkdirs(DEMO_BASE_ADLS) and _probe_delta_path(DEMO_BASE_ADLS):
        DEMO_BASE = DEMO_BASE_ADLS
        print("UC write mode: DISABLED — Delta under ADLS:", DEMO_BASE)
    else:
        _try_mkdirs(DEMO_BASE_DBFS)
        DEMO_BASE = DEMO_BASE_DBFS
        print("UC write mode: DISABLED — Delta under DBFS (Serverless-safe):", DEMO_BASE)


_init_storage()


def write_demo_table(df, table_name: str, mode: str = "overwrite") -> str:
    """Save demo output — Unity Catalog if permitted, else DEMO_BASE Delta path."""
    path = f"{DEMO_BASE}/{table_name}"
    writer = df.write.format("delta").mode(mode)
    if mode.lower() == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if UC_WRITE_ENABLED:
        full_name = f"{UC_TABLE}.{table_name}"
        try:
            writer.saveAsTable(full_name)
            print("UC table :", full_name)
            WRITTEN_TABLES.append(full_name)
            return full_name
        except Exception as exc:
            err = str(exc)
            if "PERMISSION_DENIED" not in err and "UNAUTHORIZED" not in err:
                print("UC write error — trying path fallback:", err[:120])
            else:
                print("UC write denied — falling back to path")
    try:
        writer.save(path)
        print("Delta path:", path)
        WRITTEN_TABLES.append(path)
        return path
    except Exception as exc:
        fallback = f"{DEMO_BASE_DBFS}/{table_name}"
        print(f"Path write failed ({str(exc)[:100]}) — DBFS:", fallback)
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(fallback)
        WRITTEN_TABLES.append(fallback)
        return fallback


def read_demo_table(table_name: str):
    """Read demo table from UC or Delta path (ADLS or DBFS)."""
    if UC_WRITE_ENABLED:
        try:
            return spark.table(f"{UC_TABLE}.{table_name}")
        except Exception:
            pass
    for base in (DEMO_BASE, DEMO_BASE_DBFS, DEMO_BASE_ADLS):
        try:
            return spark.read.format("delta").load(f"{base}/{table_name}")
        except Exception:
            continue
    raise FileNotFoundError(f"Demo table not found: {table_name}")


def demo_table_ref(table_name: str) -> str:
    """SQL-safe name for COMMENT / DESCRIBE (UC only)."""
    return f"{UC_TABLE}.{table_name}" if UC_WRITE_ENABLED else f"delta.`{DEMO_BASE}/{table_name}`"


print("=" * 60)
print("SETUP COMPLETE")
print("  Catalog       :", CATALOG)
print("  Schema / mode :", UC_TABLE, "| UC writes:", UC_WRITE_ENABLED)
print("  Demo base     :", DEMO_BASE)
print("  Bronze rows   :", bronze.count())
print("  Auth mode     :", BRONZE_AUTH_MODE)
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Foundations in 60 seconds
# MAGIC
# MAGIC ```
# MAGIC bronze CSV  →  typed silver  →  gold KPI
# MAGIC      ↑              ↑                ↑
# MAGIC   land raw     Event/quality      pivot/report
# MAGIC ```
# MAGIC
# MAGIC Everything below is a **story fragment** you can drop into a 2-hour class.

# COMMAND ----------

# MAGIC %md
# MAGIC # Part A — Spark fundamentals (start here)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 01 — Lazy vs Action (why nothing happens until .count)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Learners think Spark runs every line; they wait for nothing and panic. |
# MAGIC | **Easy approach** | **Build a plan first (lazy). Pay only when you ask for a result (action).** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC Writing a **shopping list** is free. Only when you **check out** do you spend money. `.select` is listing; `.count` / `.show` / `.write` is checkout.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Spark builds a *logical plan* until an action forces computation.
# MAGIC
# MAGIC **WHY:** So Spark can optimize the whole plan (skip columns, prune partitions).
# MAGIC
# MAGIC **HOW:** Transformations = lazy. Actions = `.count()`, `.show()`, `.collect()`, `.write`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 01: Lazy vs Action (why nothing happens until .count)
# HARD: Learners think Spark runs every line; they wait for nothing and panic.
# EASY: Build a plan first (lazy). Pay only when you ask for a result (action)
# ANALOGY: Writing a **shopping list** is free. Only when you **check out** do yo
# ========================================================================

# --- Topic 01: Lazy vs Action ---
# WHAT: Transformations do not touch ADLS yet.
# WHY:  Spark waits to optimize.
# HOW:  Build plan → one ACTION triggers work.

planned = bronze.select("transaction_id", "amount_gbp", "channel")  # lazy — no bill yet
print("Plan ready — no data scanned yet")
n = planned.count()  # ACTION — now Spark reads bronze
print("ACTION done — rows =", n)
planned.show(3, truncate=False)  # another ACTION

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 02 — Schema & cast (types before gold)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | CSV strings make amounts look like text; SUM fails or goes silent-wrong. |
# MAGIC | **Easy approach** | **Cast early in silver. Never trust bronze types.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC Bronze is **raw receipts with pencil marks**. Silver is **typing them into a calculator** so numbers are real numbers.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Enforce types with `.cast("double")` / `to_date`.
# MAGIC
# MAGIC **WHY:** Gold aggregates need real numbers; auditors ask "is amount_gbp a number?"
# MAGIC
# MAGIC **HOW:** Cast → filter bad casts into quarantine.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 02: Schema & cast (types before gold)
# HARD: CSV strings make amounts look like text; SUM fails or goes silent-wron
# EASY: Cast early in silver. Never trust bronze types.
# ANALOGY: Bronze is **raw receipts with pencil marks**. Silver is **typing them 
# ========================================================================

# --- Topic 02: Schema & cast ---
from pyspark.sql import functions as F

typed = bronze.withColumn("amount", F.col("amount_gbp").cast("double")) \
              .withColumn("txn_date", F.to_date("value_date"))
good = typed.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
bad = typed.filter(F.col("amount").isNull() | (F.col("amount") <= 0))
print("good=", good.count(), "quarantine=", bad.count())
write_demo_table(good.select("transaction_id", "amount", "channel", "txn_date"), "teach02_typed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 03 — select / filter / withColumn (everyday verbs)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | People write Python `for row in df.collect()` and OOM the driver. |
# MAGIC | **Easy approach** | **Stay in DataFrame verbs — Spark parallelizes them.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC Don't open every envelope yourself (**collect**). Give the **mail room** a rule: keep only card, strip PII columns.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Core verbs: select, filter/where, withColumn, drop, distinct.
# MAGIC
# MAGIC **WHY:** Driver collect is a trap at bank scale.
# MAGIC
# MAGIC **HOW:** Prefer column expressions with `F.col`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 03: select / filter / withColumn (everyday verbs)
# HARD: People write Python `for row in df.collect()` and OOM the driver.
# EASY: Stay in DataFrame verbs — Spark parallelizes them.
# ANALOGY: Don't open every envelope yourself (**collect**). Give the **mail room
# ========================================================================

# --- Topic 03: Everyday verbs (no collect loops) ---
from pyspark.sql import functions as F

card_only = (
    bronze
    .filter(F.lower(F.col("channel")) == "card")
    .select("transaction_id", "amount_gbp", "channel", "status")
    .withColumn("channel_std", F.upper(F.trim(F.col("channel"))))
)
print("card rows=", card_only.count())
card_only.show(5, truncate=False)
write_demo_table(card_only.limit(100), "teach03_card_verbs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 04 — groupBy + agg (channel totals)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Managers ask "cash by channel" and you dump million-row extracts. |
# MAGIC | **Easy approach** | **Aggregate in Spark; ship summary rows.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC Don't bring every grain of rice to the chef — bring a **bowl per dish** (channel totals).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** `groupBy` + `agg(sum, count, avg)`.
# MAGIC
# MAGIC **WHY:** Gold layer exists to answer business questions cheaply.
# MAGIC
# MAGIC **HOW:** Always name aggregates clearly (`total_gbp`, `txns`).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 04: groupBy + agg (channel totals)
# HARD: Managers ask "cash by channel" and you dump million-row extracts.
# EASY: Aggregate in Spark; ship summary rows.
# ANALOGY: Don't bring every grain of rice to the chef — bring a **bowl per dish*
# ========================================================================

# --- Topic 04: groupBy channel ---
from pyspark.sql import functions as F

by_channel = bronze.withColumn("amount", F.col("amount_gbp").cast("double")) \
    .groupBy("channel") \
    .agg(F.count("*").alias("txns"), F.round(F.sum("amount"), 2).alias("total_gbp")) \
    .orderBy(F.desc("total_gbp"))
by_channel.show(truncate=False)
write_demo_table(by_channel, "teach04_by_channel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 05 — join (txn to returns) without Cartesian bombs
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Joining without a key ≈ Cartesian product → day-long jobs. |
# MAGIC | **Easy approach** | **Always join on business keys (`transaction_id`). Know left vs inner.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC Matching **invoice numbers** between sales and refunds — never "match by customer name" (too fuzzy).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Inner = only matches. Left = keep all left rows.
# MAGIC
# MAGIC **WHY:** Refunds attach to payments via `transaction_id`.
# MAGIC
# MAGIC **HOW:** Check counts before/after join.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 05: join (txn to returns) without Cartesian bombs
# HARD: Joining without a key ≈ Cartesian product → day-long jobs.
# EASY: Always join on business keys (`transaction_id`). Know left vs inner.
# ANALOGY: Matching **invoice numbers** between sales and refunds — never "match 
# ========================================================================

# --- Topic 05: join payments ↔ returns ---
from pyspark.sql import functions as F

# Demo returns inline if lake file missing (still teaches join)
returns = spark.createDataFrame(
    [("RET-1", "TXN-10001", 10.0), ("RET-2", "TXN-99999", 5.0)],
    ["return_id", "transaction_id", "refund_gbp"],
)
pay = bronze.select("transaction_id", F.col("amount_gbp").cast("double").alias("paid"))
inner = pay.join(returns, "transaction_id", "inner")
left = pay.join(returns, "transaction_id", "left")
print("payments=", pay.count(), "inner matches=", inner.count(), "left rows=", left.count())
write_demo_table(inner, "teach05_join_inner")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part B — Medallion & cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 06 — Medallion bronze → silver → gold (layers)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | One giant table mixes raw junk with board KPIs — chaos. |
# MAGIC | **Easy approach** | **Three layers with different promises: raw / cleansed / business.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Farm → kitchen → restaurant plate.** Bronze=farm dirt ok. Silver=washed. Gold=plated dish for the customer.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Bronze immutable land → silver typed/valid → gold aggregates.
# MAGIC
# MAGIC **WHY:** Different consumers, different SLAs.
# MAGIC
# MAGIC **HOW:** Paths under bronze/silver/gold containers; never overwrite bronze history casually.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 06: Medallion bronze → silver → gold (layers)
# HARD: One giant table mixes raw junk with board KPIs — chaos.
# EASY: Three layers with different promises: raw / cleansed / business.
# ANALOGY: **Farm → kitchen → restaurant plate.** Bronze=farm dirt ok. Silver=was
# ========================================================================

# --- Topic 06: mini medallion in-memory (concept) ---
from pyspark.sql import functions as F

# bronze = as landed
b = bronze
# silver = typed + deduped
s = b.withColumn("amount", F.col("amount_gbp").cast("double")) \
     .filter(F.col("amount") > 0) \
     .dropDuplicates(["transaction_id"])
# gold = business question
g = s.groupBy("channel").agg(F.sum("amount").alias("total_gbp"))
print("bronze", b.count(), "→ silver", s.count(), "→ gold channels", g.count())
write_demo_table(g, "teach06_medallion_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 07 — Quarantine / dead-letter (bad rows don't kill the batch)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | One bad CSV cell fails 10 million good rows. |
# MAGIC | **Easy approach** | **Split good vs bad; write both; never fail the whole night for 3 rows.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Airport security:** most passengers board; a few go to a side room. The flight still leaves.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Quarantine = audit path for invalid rows.
# MAGIC
# MAGIC **WHY:** Banks must post good settlements even if 0.01% of rows are dirty.
# MAGIC
# MAGIC **HOW:** `good = filter(ok); bad = filter(not ok)` then write both.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 07: Quarantine / dead-letter (bad rows don't kill the batch)
# HARD: One bad CSV cell fails 10 million good rows.
# EASY: Split good vs bad; write both; never fail the whole night for 3 rows.
# ANALOGY: **Airport security:** most passengers board; a few go to a side room. 
# ========================================================================

# --- Topic 07: quarantine split ---
from pyspark.sql import functions as F

typed = bronze.withColumn("amount", F.col("amount_gbp").cast("double"))
ok = F.col("amount").isNotNull() & (F.col("amount") > 0) & F.col("transaction_id").isNotNull()
good = typed.filter(ok)
bad = typed.filter(~ok)
print("GOOD", good.count(), "BAD→quarantine", bad.count())
write_demo_table(good.limit(50), "teach07_good")
write_demo_table(bad.limit(50), "teach07_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 08 — Deduplicate by business key
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | ADF retry lands the same file twice → double revenue in reports. |
# MAGIC | **Easy approach** | **`dropDuplicates(["transaction_id"])` or window keep latest.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **One passport number = one person.** Two boarding passes with same passport → you don't seat them twice.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Idempotent silver: unique on natural key.
# MAGIC
# MAGIC **WHY:** Retries are normal in cloud pipelines.
# MAGIC
# MAGIC **HOW:** dropDuplicates or ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 08: Deduplicate by business key
# HARD: ADF retry lands the same file twice → double revenue in reports.
# EASY: `dropDuplicates(["transaction_id"])` or window keep latest.
# ANALOGY: **One passport number = one person.** Two boarding passes with same pa
# ========================================================================

# --- Topic 08: dedupe ---
from pyspark.sql import functions as F
from pyspark.sql.window import Window

duped = bronze.unionByName(bronze.limit(5))  # simulate retry double-write
print("with dupes", duped.count())
deduped = duped.dropDuplicates(["transaction_id"])
print("after dropDuplicates", deduped.count())
# Prefer latest status if keys collide (window pattern)
w = Window.partitionBy("transaction_id").orderBy(F.col("value_date").desc())
latest = duped.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
print("window latest", latest.count())
write_demo_table(deduped.limit(50), "teach08_deduped")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part C — Delta Lake power tools

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 09 — Why Delta (not only Parquet)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Parquet folders silently corrupt on half-written jobs; no time travel. |
# MAGIC | **Easy approach** | **Delta = Parquet files + `_delta_log` transaction log (ACID).** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC Parquet is **loose photos**. Delta is a **photo album with a date-stamped contents page** — you can go back to yesterday's album.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Delta Lake adds a transaction log on top of Parquet.
# MAGIC
# MAGIC **WHY:** Atomic commits, MERGE, time travel, OPTIMIZE.
# MAGIC
# MAGIC **HOW:** `.format("delta").save(path)` or `saveAsTable`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 09: Why Delta (not only Parquet)
# HARD: Parquet folders silently corrupt on half-written jobs; no time travel.
# EASY: Delta = Parquet files + `_delta_log` transaction log (ACID).
# ANALOGY: Parquet is **loose photos**. Delta is a **photo album with a date-stam
# ========================================================================

# --- Topic 09: Delta write + history ---
path = f"{DEMO_BASE}/teach09_delta_demo"
bronze.limit(20).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
print("Delta path:", path)
spark.sql(f"DESCRIBE HISTORY delta.`{path}`").show(5, truncate=False)
write_demo_table(bronze.limit(20), "teach09_delta_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 10 — Time travel & RESTORE (undo a bad load)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Analyst ran DELETE … wrong filter; panic. |
# MAGIC | **Easy approach** | **`RESTORE TABLE … TO VERSION AS OF n` or `versionAsOf`.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Git checkout** for tables — undo a bad commit without rebuilding from bronze.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Delta keeps table versions.
# MAGIC
# MAGIC **WHY:** Human error in batch jobs is guaranteed.
# MAGIC
# MAGIC **HOW:** DESCRIBE HISTORY → RESTORE or read with versionAsOf.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 10: Time travel & RESTORE (undo a bad load)
# HARD: Analyst ran DELETE … wrong filter; panic.
# EASY: `RESTORE TABLE … TO VERSION AS OF n` or `versionAsOf`.
# ANALOGY: **Git checkout** for tables — undo a bad commit without rebuilding fro
# ========================================================================

# --- Topic 10: time travel ---
path = f"{DEMO_BASE}/teach10_timetravel"
bronze.limit(10).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
# "bad" overwrite — simulates mistaken load
bronze.limit(2).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
print("current rows", spark.read.format("delta").load(path).count())
print("version 0 rows", spark.read.format("delta").option("versionAsOf", 0).load(path).count())
spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "timestamp", "operation").show(5, truncate=False)
# Uncomment in class to restore: spark.sql(f"RESTORE TABLE delta.`{path}` TO VERSION AS OF 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 11 — MERGE upsert (incremental without full reload)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Nightly full replace of 5 years costs hours. |
# MAGIC | **Easy approach** | **MERGE matched UPDATE / not matched INSERT on business key.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Library card update:** if the card exists, update address; if new member, create the card. Don't reprint the whole phone book.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Delta MERGE is SQL upsert.
# MAGIC
# MAGIC **WHY:** Incremental is cheaper and safer.
# MAGIC
# MAGIC **HOW:** Create target once → MERGE source into target ON key.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 11: MERGE upsert (incremental without full reload)
# HARD: Nightly full replace of 5 years costs hours.
# EASY: MERGE matched UPDATE / not matched INSERT on business key.
# ANALOGY: **Library card update:** if the card exists, update address; if new me
# ========================================================================

# --- Topic 11: MERGE upsert ---
from pyspark.sql import functions as F

target_path = f"{DEMO_BASE}/teach11_merge_target"
src = bronze.withColumn("amount", F.col("amount_gbp").cast("double")).select(
    "transaction_id", "amount", "channel", "status"
)
src.limit(5).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
# Simulate late update on one id
upd = src.limit(1).withColumn("status", F.lit("posted_fixed"))
upd.createOrReplaceTempView("merge_src")
spark.sql(f"CREATE OR REPLACE TEMP VIEW merge_tgt AS SELECT * FROM delta.`{target_path}`")
spark.sql("""
MERGE INTO merge_tgt t
USING merge_src s
ON t.transaction_id = s.transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
# Persist merged view back (lab-simple overwrite of path from temp)
spark.table("merge_tgt").write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
print("MERGE demo complete — rows", spark.read.format("delta").load(target_path).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 12 — OPTIMIZE + ZORDER (small files)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Thousands of tiny files → morning queries crawl. |
# MAGIC | **Easy approach** | **OPTIMIZE compacts; ZORDER clusters filter columns.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Messy desk drawers → one neat binder.** OPTIMIZE = restack papers. ZORDER = tab the binder by channel.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Compact small files; cluster data by filter keys.
# MAGIC
# MAGIC **WHY:** Cloud lists + opens each file — file count kills latency.
# MAGIC
# MAGIC **HOW:** `OPTIMIZE table [ZORDER BY (col)]`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 12: OPTIMIZE + ZORDER (small files)
# HARD: Thousands of tiny files → morning queries crawl.
# EASY: OPTIMIZE compacts; ZORDER clusters filter columns.
# ANALOGY: **Messy desk drawers → one neat binder.** OPTIMIZE = restack papers. Z
# ========================================================================

# --- Topic 12: OPTIMIZE ---
path = f"{DEMO_BASE}/teach12_optimize"
# Many tiny writes → small files (demo)
for i in range(3):
    bronze.limit(5).write.format("delta").mode("append").save(path) if i else \
        bronze.limit(5).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
spark.sql(f"OPTIMIZE delta.`{path}`")
print("OPTIMIZE done on", path)
# If registered as UC table you could: OPTIMIZE catalog.schema.t ZORDER BY (channel)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part D — Zach Event model (multi-source)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 13 — Event schema contract (Zach Wilson style)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Every new source invents a new messy schema → N pipelines. |
# MAGIC | **Easy approach** | **Force every source into Event(source, metric_name, timestamp, metric_value, content).** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Shipping label:** different factories (Fitbit / payments / refunds) → same label format so one warehouse conveyor works.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** One shared Event rowshape across sources.
# MAGIC
# MAGIC **WHY:** Processors stay small; runner unions everything.
# MAGIC
# MAGIC **HOW:** See also `/Shared/day9/finledger_event_processors`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 13: Event schema contract (Zach Wilson style)
# HARD: Every new source invents a new messy schema → N pipelines.
# EASY: Force every source into Event(source, metric_name, timestamp, metric_v
# ANALOGY: **Shipping label:** different factories (Fitbit / payments / refunds) 
# ========================================================================

# --- Topic 13: Event contract ---
from collections import namedtuple
from pyspark.sql import functions as F

Event = namedtuple("Event", "source metric_name timestamp metric_value content")
print("Contract fields:", Event._fields)

events = bronze.select(
    F.lit("transactions").alias("source"),
    F.lit("payment_amount").alias("metric_name"),
    F.to_timestamp("value_date").alias("timestamp"),
    F.col("amount_gbp").cast("double").alias("metric_value"),
    F.col("transaction_id").alias("content"),
)
events.show(5, truncate=False)
write_demo_table(events.limit(50), "teach13_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 14 — Processor per source (no spaghetti joins)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | One notebook knows every system's quirks → unmaintainable. |
# MAGIC | **Easy approach** | **Each source has one processor function that only speaks Event.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Embassies:** UK visa desk doesn't fix French forms — each country (source) has its own window, same passport (Event) out.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** `txn_processor(df)` / `returns_processor(df)`.
# MAGIC
# MAGIC **WHY:** Isolate schema chaos per source.
# MAGIC
# MAGIC **HOW:** Registry dict of processors → runner loops them.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 14: Processor per source (no spaghetti joins)
# HARD: One notebook knows every system's quirks → unmaintainable.
# EASY: Each source has one processor function that only speaks Event.
# ANALOGY: **Embassies:** UK visa desk doesn't fix French forms — each country (s
# ========================================================================

# --- Topic 14: processor pattern ---
from pyspark.sql import functions as F

def payments_processor(raw):
    return raw.select(
        F.lit("transactions").alias("source"),
        F.lit("payment_amount").alias("metric_name"),
        F.to_timestamp("value_date").alias("timestamp"),
        F.col("amount_gbp").cast("double").alias("metric_value"),
        F.col("transaction_id").alias("content"),
    )

def channel_count_processor(raw):
    return raw.select(
        F.lit("transactions").alias("source"),
        F.concat(F.lit("txn_count_"), F.lower(F.col("channel"))).alias("metric_name"),
        F.to_timestamp("value_date").alias("timestamp"),
        F.lit(1.0).alias("metric_value"),
        F.col("transaction_id").alias("content"),
    )

parts = [payments_processor(bronze), channel_count_processor(bronze)]
timeline = parts[0].unionByName(parts[1])
print("timeline metrics:", [r.metric_name for r in timeline.select("metric_name").distinct().collect()])
write_demo_table(timeline.limit(80), "teach14_timeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 15 — Runner → daily aggregates → pivot (analyst table)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Stakeholders want a wide Excel-like day×metric grid. |
# MAGIC | **Easy approach** | **Long Event table → groupBy day → pivot(metric).** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Bank teller journal → end-of-day spreadsheet.** Long list of moves becomes columns for payments / refunds.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Long format for engineering; wide pivot for BI.
# MAGIC
# MAGIC **WHY:** Tableau/Power BI love pivots; MERGE loves long Events.
# MAGIC
# MAGIC **HOW:** `.groupBy("event_date").pivot("metric_name").sum("metric_value")`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 15: Runner → daily aggregates → pivot (analyst table)
# HARD: Stakeholders want a wide Excel-like day×metric grid.
# EASY: Long Event table → groupBy day → pivot(metric).
# ANALOGY: **Bank teller journal → end-of-day spreadsheet.** Long list of moves b
# ========================================================================

# --- Topic 15: pivot for analysts ---
from pyspark.sql import functions as F

long = bronze.select(
    F.to_date("value_date").alias("event_date"),
    F.lit("payment_amount").alias("metric_name"),
    F.col("amount_gbp").cast("double").alias("metric_value"),
)
daily = long.groupBy("event_date", "metric_name").agg(F.sum("metric_value").alias("metric_value"))
wide = daily.groupBy("event_date").pivot("metric_name").sum("metric_value")
wide.show(truncate=False)
write_demo_table(wide, "teach15_pivot")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part E — Reliability & quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 16 — Data quality gate (assert before gold)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Bad gold hits regulatory dashboards before anyone notices. |
# MAGIC | **Easy approach** | **`assert_quality(df)` — empty or null keys ⇒ fail the job fast.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Pre-flight checklist.** Plane doesn't take off if fuel is zero — even if passengers are boarded.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Hard assertions in the notebook / Job.
# MAGIC
# MAGIC **WHY:** Fail early vs silent wrong KPIs.
# MAGIC
# MAGIC **HOW:** count > 0; null business keys == 0.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 16: Data quality gate (assert before gold)
# HARD: Bad gold hits regulatory dashboards before anyone notices.
# EASY: `assert_quality(df)` — empty or null keys ⇒ fail the job fast.
# ANALOGY: **Pre-flight checklist.** Plane doesn't take off if fuel is zero — eve
# ========================================================================

# --- Topic 16: quality gate ---
from pyspark.sql import functions as F

def assert_quality(df, key="transaction_id"):
    n = df.count()
    assert n > 0, "EMPTY — refuse to publish gold"
    nulls = df.filter(F.col(key).isNull()).count()
    assert nulls == 0, f"NULL keys={nulls}"
    print("quality OK:", n, "rows")

silver = bronze.dropDuplicates(["transaction_id"])
assert_quality(silver)
write_demo_table(silver.limit(30), "teach16_quality_ok")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 17 — Idempotent re-runs (same run_id safe)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Re-running doubles gold numbers. |
# MAGIC | **Easy approach** | **Overwrite/MERGE by `run_id` path — never append blindly.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Replay the same game day** on the scoreboard replaces Saturday's score, doesn't add a second Saturday.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Idempotency = run 1 or run 10 → same end state.
# MAGIC
# MAGIC **WHY:** Cloud retries happen.
# MAGIC
# MAGIC **HOW:** Path `.../run={run_id}/` + mode overwrite or MERGE.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 17: Idempotent re-runs (same run_id safe)
# HARD: Re-running doubles gold numbers.
# EASY: Overwrite/MERGE by `run_id` path — never append blindly.
# ANALOGY: **Replay the same game day** on the scoreboard replaces Saturday's sco
# ========================================================================

# --- Topic 17: idempotent path write ---
from pyspark.sql import functions as F

out = f"{DEMO_BASE}/teach17_idempotent/run={RUN_ID}"
payload = bronze.limit(15)
payload.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
n1 = spark.read.format("delta").load(out).count()
payload.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
n2 = spark.read.format("delta").load(out).count()
print("first", n1, "second", n2, "same?", n1 == n2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 18 — Watermark / incremental window
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Full scan every night as history grows. |
# MAGIC | **Easy approach** | **Only process rows after last successful watermark date.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Bookmark in a novel.** Tonight start at the bookmark, not page 1.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Store last successful date/version; filter `>` watermark.
# MAGIC
# MAGIC **WHY:** Cost and time grow with full reloads.
# MAGIC
# MAGIC **HOW:** JSON watermark on ADLS audit or SQL control table.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 18: Watermark / incremental window
# HARD: Full scan every night as history grows.
# EASY: Only process rows after last successful watermark date.
# ANALOGY: **Bookmark in a novel.** Tonight start at the bookmark, not page 1.
# ========================================================================

# --- Topic 18: watermark filter ---
from pyspark.sql import functions as F

# Simulate control-plane watermark (in class this comes from audit/watermark.json)
last_ok = "2026-05-31"
incremental = bronze.filter(F.col("value_date") > F.lit(last_ok))
print("watermark", last_ok, "incremental rows", incremental.count(), "of", bronze.count())
write_demo_table(incremental.limit(40), "teach18_incremental")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part F — Performance (hard → easy)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 19 — Partition pruning (don't scan the world)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Filter on date but data isn't partitioned → full lake scan. |
# MAGIC | **Easy approach** | **Partition by `value_date` or `event_date` you always filter on.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Library shelves by year.** Asking for 2026 shouldn't open 1990 boxes.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Folder/table partitions let Spark skip files.
# MAGIC
# MAGIC **WHY:** Scan less = faster + cheaper.
# MAGIC
# MAGIC **HOW:** `.partitionBy("txn_date")` when writing; filter that column when reading.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 19: Partition pruning (don't scan the world)
# HARD: Filter on date but data isn't partitioned → full lake scan.
# EASY: Partition by `value_date` or `event_date` you always filter on.
# ANALOGY: **Library shelves by year.** Asking for 2026 shouldn't open 1990 boxes
# ========================================================================

# --- Topic 19: partitionBy ---
from pyspark.sql import functions as F

part_path = f"{DEMO_BASE}/teach19_partitioned"
df = bronze.withColumn("txn_date", F.to_date("value_date"))
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("txn_date").save(part_path)
# Predicate on partition column → pruning
one_day = spark.read.format("delta").load(part_path).filter(F.col("txn_date") == "2026-06-01")
print("one-day rows", one_day.count())
print("partition folders under", part_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 20 — Broadcast join (small dim × big fact)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Joining tiny channel dim to huge fact shuffles everything. |
# MAGIC | **Easy approach** | **`F.broadcast(small_df)` — send the postage stamp to every worker.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Cheat sheet** on every student's desk vs shipping the whole encyclopedia around class.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Broadcast replicates small table to executors.
# MAGIC
# MAGIC **WHY:** Avoids expensive shuffle for tiny right-hand tables.
# MAGIC
# MAGIC **HOW:** `fact.join(F.broadcast(dim), "channel")`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 20: Broadcast join (small dim × big fact)
# HARD: Joining tiny channel dim to huge fact shuffles everything.
# EASY: `F.broadcast(small_df)` — send the postage stamp to every worker.
# ANALOGY: **Cheat sheet** on every student's desk vs shipping the whole encyclop
# ========================================================================

# --- Topic 20: broadcast join ---
from pyspark.sql import functions as F

dim = spark.createDataFrame(
    [("card", "Card Rail"), ("wire", "Wire Rail"), ("fps", "Faster Payments")],
    ["channel", "channel_name"],
)
fact = bronze.select("transaction_id", "channel", F.col("amount_gbp").cast("double").alias("amount"))
enriched = fact.join(F.broadcast(dim), "channel", "left")
enriched.show(5, truncate=False)
write_demo_table(enriched.limit(40), "teach20_broadcast")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 21 — Skew / salting (one fat key)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | 90% of rows are `channel=card` → one executor melts. |
# MAGIC | **Easy approach** | **Salt the hot key into N buckets during the heavy stage.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **One overloaded checkout** → open 5 temporary tills labeled card-0…card-4, then merge totals.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Salting spreads hot keys.
# MAGIC
# MAGIC **WHY:** Spark partitions by key; skew = one monster partition.
# MAGIC
# MAGIC **HOW:** Add random salt, aggregate, then strip salt.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 21: Skew / salting (one fat key)
# HARD: 90% of rows are `channel=card` → one executor melts.
# EASY: Salt the hot key into N buckets during the heavy stage.
# ANALOGY: **One overloaded checkout** → open 5 temporary tills labeled card-0…ca
# ========================================================================

# --- Topic 21: salt for skew (concept) ---
from pyspark.sql import functions as F

fact = bronze.withColumn("amount", F.col("amount_gbp").cast("double"))
salted = fact.withColumn("salt", (F.rand() * 8).cast("int")) \
    .withColumn("channel_salt", F.concat_ws("#", F.col("channel"), F.col("salt")))
partial = salted.groupBy("channel_salt", "channel").agg(F.sum("amount").alias("partial"))
final = partial.groupBy("channel").agg(F.sum("partial").alias("total_gbp"))
final.show(truncate=False)
write_demo_table(final, "teach21_salt_agg")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 22 — cache / persist (reuse same DataFrame)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Same silver transform recomputed 5 times → 5× bill. |
# MAGIC | **Easy approach** | **`.cache()` after an expensive stage you reuse.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Print the report once**, put it on the table; don't reprint for every question.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Cache keeps partition results in memory/disk.
# MAGIC
# MAGIC **WHY:** Multiple actions on same lineage recompute otherwise.
# MAGIC
# MAGIC **HOW:** `df.cache(); df.count()` to materialize; `unpersist()` when done.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 22: cache / persist (reuse same DataFrame)
# HARD: Same silver transform recomputed 5 times → 5× bill.
# EASY: `.cache()` after an expensive stage you reuse.
# ANALOGY: **Print the report once**, put it on the table; don't reprint for ever
# ========================================================================

# --- Topic 22: cache ---
from pyspark.sql import functions as F

silver = bronze.withColumn("amount", F.col("amount_gbp").cast("double")).filter(F.col("amount") > 0)
silver.cache()
print("materialize", silver.count())
print("reuse #1 by channel", silver.groupBy("channel").count().count())
print("reuse #2 sum", silver.agg(F.sum("amount")).collect()[0][0])
silver.unpersist()
print("unpersisted")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part G — SQL, Unity Catalog, secrets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 23 — Spark SQL (%sql) same data
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | SQL analysts refuse to learn DataFrame API. |
# MAGIC | **Easy approach** | **Register temp view; use SQL — same engine.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Two cashiers, same till.** PySpark and SQL are dialects talking to one kitchen (Spark).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** `createOrReplaceTempView` + `spark.sql`.
# MAGIC
# MAGIC **WHY:** Meet SQL-minded teammates halfway.
# MAGIC
# MAGIC **HOW:** Prefer views for demos; UC tables for durable objects.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 23: Spark SQL (%sql) same data
# HARD: SQL analysts refuse to learn DataFrame API.
# EASY: Register temp view; use SQL — same engine.
# ANALOGY: **Two cashiers, same till.** PySpark and SQL are dialects talking to o
# ========================================================================

# --- Topic 23: Spark SQL ---
bronze.createOrReplaceTempView("bronze_txn")
spark.sql("""
SELECT channel, count(*) AS txns, round(sum(cast(amount_gbp AS double)), 2) AS total_gbp
FROM bronze_txn
GROUP BY channel
ORDER BY total_gbp DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 24 — Unity Catalog names (catalog.schema.table)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Paths-only assets — nobody knows owners or grants. |
# MAGIC | **Easy approach** | **Three-level namespace + GRANT when UC write is enabled.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Street address:** Country (catalog) → Neighborhood (schema) → House (table).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** `finledger.gold.channel_daily` style names.
# MAGIC
# MAGIC **WHY:** Governance, lineage, grants.
# MAGIC
# MAGIC **HOW:** write_demo_table already prefers UC when permitted.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 24: Unity Catalog names (catalog.schema.table)
# HARD: Paths-only assets — nobody knows owners or grants.
# EASY: Three-level namespace + GRANT when UC write is enabled.
# ANALOGY: **Street address:** Country (catalog) → Neighborhood (schema) → House 
# ========================================================================

# --- Topic 24: UC naming ---
print("UC_WRITE_ENABLED =", UC_WRITE_ENABLED)
print("UC_TABLE        =", UC_TABLE)
ref = write_demo_table(bronze.limit(10), "teach24_uc_named")
print("Registered as:", ref)
print("Use three-level name when UC_WRITE_ENABLED else Delta path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 25 — COMMENTS + TAGS (docs as metadata)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Column meaning lives in Slack lore. |
# MAGIC | **Easy approach** | **`COMMENT ON TABLE/COLUMN` + tags for domain/classification.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Nutrition label on food.** You shouldn't need the chef's phone number to know calories (column meaning).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Table/column comments and governed tags.
# MAGIC
# MAGIC **WHY:** Onboarding + Purview/governance alignment.
# MAGIC
# MAGIC **HOW:** SQL COMMENT / SET TAGS when UC write works.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 25: COMMENTS + TAGS (docs as metadata)
# HARD: Column meaning lives in Slack lore.
# EASY: `COMMENT ON TABLE/COLUMN` + tags for domain/classification.
# ANALOGY: **Nutrition label on food.** You shouldn't need the chef's phone numbe
# ========================================================================

# --- Topic 25: documentation metadata ---
write_demo_table(bronze.limit(5), "teach25_docs")
if UC_WRITE_ENABLED:
    spark.sql(f"COMMENT ON TABLE {UC_TABLE}.teach25_docs IS 'FinLedger teach demo — Topic 25'")
    spark.sql(f"COMMENT ON COLUMN {UC_TABLE}.teach25_docs.transaction_id IS 'Natural payment key'")
    try:
        spark.sql(f"ALTER TABLE {UC_TABLE}.teach25_docs SET TAGS ('domain' = 'payments', 'classification' = 'internal')")
    except Exception as e:
        print("TAGS (runtime dependent):", e)
    spark.sql(f"DESCRIBE TABLE EXTENDED {UC_TABLE}.teach25_docs").show(8, truncate=False)
else:
    print("SKIP COMMENT/TAGS — UC write disabled; still teach the habit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 26 — Secrets — never hard-code keys
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Keys in notebooks get committed to git → incident. |
# MAGIC | **Easy approach** | **`dbutils.secrets.get(scope, key)` only.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Hotel key card** from the front desk — not duct-taped under the welcome mat (git).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Databricks secret scopes (backed by Key Vault in this lab).
# MAGIC
# MAGIC **WHY:** Rotate secrets without editing notebooks.
# MAGIC
# MAGIC **HOW:** Scope `finledger` keys `storage-account`, `storage-key`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 26: Secrets — never hard-code keys
# HARD: Keys in notebooks get committed to git → incident.
# EASY: `dbutils.secrets.get(scope, key)` only.
# ANALOGY: **Hotel key card** from the front desk — not duct-taped under the welc
# ========================================================================

# --- Topic 26: secrets ---
acct = dbutils.secrets.get(scope="finledger", key="storage-account")
# Never print the raw key — only length proves it exists
key_len = len(dbutils.secrets.get(scope="finledger", key="storage-key"))
print("storage-account from scope:", acct)
print("storage-key length (not the key!):", key_len)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part H — Jobs, widgets, ADF hooks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 27 — Widgets = Job / ADF parameters
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Hard-coded `run_id` → can't schedule yesterday vs today. |
# MAGIC | **Easy approach** | **`dbutils.widgets.text` then `get` — ADF passes baseParameters.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Order form fields** on a kitchen ticket — same recipe, different table number (`run_id`).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Widgets capture parameters for Jobs and ADF.
# MAGIC
# MAGIC **WHY:** One notebook, many runs.
# MAGIC
# MAGIC **HOW:** Define widget BEFORE get — Studio Run-all safe.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 27: Widgets = Job / ADF parameters
# HARD: Hard-coded `run_id` → can't schedule yesterday vs today.
# EASY: `dbutils.widgets.text` then `get` — ADF passes baseParameters.
# ANALOGY: **Order form fields** on a kitchen ticket — same recipe, different tab
# ========================================================================

# --- Topic 27: widgets ---
# Already defined in setup: run_id. Demonstrate reading safely.
dbutils.widgets.text("env", "training", "Environment")
print("run_id =", dbutils.widgets.get("run_id"))
print("env    =", dbutils.widgets.get("env"))
print("ADF notebooks get the same keys via baseParameters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 28 — dbutils.notebook.exit (return JSON to ADF)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | ADF doesn't know how many rows silver wrote. |
# MAGIC | **Easy approach** | **Exit with JSON string; ADF activity output captures it.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Receipt at the door** after dinner — "15 tables served" so the manager (ADF Monitor) can log it.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** `dbutils.notebook.exit(json)` ends the notebook with a payload.
# MAGIC
# MAGIC **WHY:** Orchestrators need machine-readable results.
# MAGIC
# MAGIC **HOW:** Put exit in the final cell only (this topic shows build, not force-exit mid-book).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 28: dbutils.notebook.exit (return JSON to ADF)
# HARD: ADF doesn't know how many rows silver wrote.
# EASY: Exit with JSON string; ADF activity output captures it.
# ANALOGY: **Receipt at the door** after dinner — "15 tables served" so the manag
# ========================================================================

# --- Topic 28: exit payload shape (demo print; real exit at notebook end) ---
import json
payload = {"run_id": RUN_ID, "bronze_rows": bronze.count(), "status": "Succeeded"}
print("ADF would receive:")
print(json.dumps(payload, indent=2))
# Mid-notebook we only print. Final cell of dedicated Jobs uses notebook.exit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 29 — Jobs vs interactive (why schedule)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Relying on humans clicking Run at 2am. |
# MAGIC | **Easy approach** | **Databricks Job = schedule + history + alerts; notebook stays the recipe.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Alarm clock vs hoping you wake up.** Job is the alarm; notebook is brushing your teeth.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Jobs run notebooks / multi-task graphs on a schedule or trigger.
# MAGIC
# MAGIC **WHY:** Repeatable, monitored, permissioned.
# MAGIC
# MAGIC **HOW:** Workflows UI or Jobs API; ADF can call Databricks Job Preview.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 29: Jobs vs interactive (why schedule)
# HARD: Relying on humans clicking Run at 2am.
# EASY: Databricks Job = schedule + history + alerts; notebook stays the recip
# ANALOGY: **Alarm clock vs hoping you wake up.** Job is the alarm; notebook is b
# ========================================================================

# --- Topic 29: Job mindset (no API call required for teach) ---
print("Interactive: you click Run — good for learning")
print("Job: schedule notebook /Shared/day9/finledger_event_processors with run_id=")
print("     ", RUN_ID)
print("ADF: pl_db_* Notebook activity OR Job activity — orchestration layer")
print("Rule: Notebook = transform logic. Job/ADF = when & monitor.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part I — Complicated ideas, simple stories

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 30 — SCD Type 2 (history of a dimension) — hard idea, easy pattern
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Channel rename overtime; reports lose history. |
# MAGIC | **Easy approach** | **Keep `valid_from`/`valid_to`/`is_current` rows per key.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Passport history.** Old passport not deleted — stamped expired; new passport is current.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Slowly Changing Dimension Type 2 keeps versions.
# MAGIC
# MAGIC **WHY:** "What was the channel name last June?"
# MAGIC
# MAGIC **HOW:** Close old row (`is_current=false`) + insert new current row.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 30: SCD Type 2 (history of a dimension) — hard idea, easy pattern
# HARD: Channel rename overtime; reports lose history.
# EASY: Keep `valid_from`/`valid_to`/`is_current` rows per key.
# ANALOGY: **Passport history.** Old passport not deleted — stamped expired; new 
# ========================================================================

# --- Topic 30: SCD2 mini ---
from pyspark.sql import functions as F

dim_v1 = spark.createDataFrame([("card", "Card", True)], ["channel_code", "channel_name", "is_current"])
# Business renames display name
incoming = spark.createDataFrame([("card", "Card Payments UK")], ["channel_code", "channel_name"])
closed = dim_v1.withColumn("is_current", F.lit(False))
new_cur = incoming.withColumn("is_current", F.lit(True))
scd2 = closed.unionByName(new_cur)
scd2.show(truncate=False)
write_demo_table(scd2, "teach30_scd2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 31 — Late data (watermark acceptance window)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Mobile payments arrive 6 hours late; daily KPI already closed. |
# MAGIC | **Easy approach** | **Accept late events within a watermark window; mark stragglers.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Box office closes at 9pm but allows tickets sold until 9:15** — after that, they go to tomorrow's report.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Event-time vs processing-time; watermark lag.
# MAGIC
# MAGIC **WHY:** Real systems are late.
# MAGIC
# MAGIC **HOW:** Compare `value_date` to batch `as_of` minus allowed lag.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 31: Late data (watermark acceptance window)
# HARD: Mobile payments arrive 6 hours late; daily KPI already closed.
# EASY: Accept late events within a watermark window; mark stragglers.
# ANALOGY: **Box office closes at 9pm but allows tickets sold until 9:15** — afte
# ========================================================================

# --- Topic 31: late arrivals ---
from pyspark.sql import functions as F

as_of = F.lit("2026-06-02")
lag_days = 1
labeled = bronze.withColumn("txn_date", F.to_date("value_date")) \
    .withColumn("is_late", F.col("txn_date") < F.date_sub(as_of.cast("date"), lag_days))
labeled.groupBy("is_late").count().show()
write_demo_table(labeled.select("transaction_id", "txn_date", "is_late").limit(40), "teach31_late")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 32 — CDC mindset (changes only)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Full extracts crush databases and wallets. |
# MAGIC | **Easy approach** | **Process inserts/updates/deletes only — Delta CDF or SQL change tracking.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Security camera motion clips**, not 24h of empty hallway footage.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Change Data Capture streams row diffs.
# MAGIC
# MAGIC **WHY:** Incremental is the real world.
# MAGIC
# MAGIC **HOW:** ADF `pl_24` teaches SQL change tracking; Delta has Change Data Feed.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 32: CDC mindset (changes only)
# HARD: Full extracts crush databases and wallets.
# EASY: Process inserts/updates/deletes only — Delta CDF or SQL change trackin
# ANALOGY: **Security camera motion clips**, not 24h of empty hallway footage.
# ========================================================================

# --- Topic 32: CDC as labels (concept) ---
from pyspark.sql import functions as F

# Simulate a CDF-like feed
changes = spark.createDataFrame([
    ("TXN-1", "insert", 10.0),
    ("TXN-1", "update", 12.0),
    ("TXN-2", "delete", None),
], ["transaction_id", "_change_type", "amount"])
changes.show(truncate=False)
print("Downstream applies INSERT/UPDATE/DELETE in order — don't reload all history")
write_demo_table(changes, "teach32_cdc_sim")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 33 — Cost habits (auto-terminate, right size)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Idle clusters burn DBUs overnight. |
# MAGIC | **Easy approach** | **Auto-termination, small skill-cluster for class, stop when done.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Turn off meeting-room lights.** Empty room still gets billed if left on.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Cluster policies, auto-terminate, spot where allowed.
# MAGIC
# MAGIC **WHY:** Training estates die from idle cost, not from education.
# MAGIC
# MAGIC **HOW:** Compute UI — Autotermination minutes; orchestrate.cmd --warm-cluster-only when teaching.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 33: Cost habits (auto-terminate, right size)
# HARD: Idle clusters burn DBUs overnight.
# EASY: Auto-termination, small skill-cluster for class, stop when done.
# ANALOGY: **Turn off meeting-room lights.** Empty room still gets billed if left
# ========================================================================

# --- Topic 33: cost checklist (print) ---
tips = [
    ("auto_terminate", "set 30–140 min for class clusters"),
    ("right_size", "Don't use 8 workers for a 5MB CSV"),
    ("serverless_tradeoff", "Serverless easy but may block account-key ADLS — prefer classic with RBAC"),
    ("stop_habit", "Terminate when session ends"),
]
spark.createDataFrame(tips, ["habit", "detail"]).show(truncate=False)
write_demo_table(spark.createDataFrame(tips, ["habit", "detail"]), "teach33_cost_tips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 34 — End-to-end story (wire the pieces)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Learners know buttons but not how the night batch runs. |
# MAGIC | **Easy approach** | **Narrate bronze → silver → Event → gold → Job/ADF → quality.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Restaurant dinner service:** deliveries (bronze) → prep (silver) → plating (gold) → waiter ticket (Job/ADF) → health check (quality).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** One mental plot for FinLedger nightly.
# MAGIC
# MAGIC **WHY:** Interviews and ops need the story, not only APIs.
# MAGIC
# MAGIC **HOW:** Connect this teach notebook to 50-problems + ADF pl_gov_06 / pl_db_*.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 34: End-to-end story (wire the pieces)
# HARD: Learners know buttons but not how the night batch runs.
# EASY: Narrate bronze → silver → Event → gold → Job/ADF → quality.
# ANALOGY: **Restaurant dinner service:** deliveries (bronze) → prep (silver) → p
# ========================================================================

# --- Topic 34: e2e narrative metrics ---
from pyspark.sql import functions as F

story = spark.createDataFrame([
    (RUN_ID, "1_bronze", bronze.count(), "land CSV"),
    (RUN_ID, "2_silver_dedupe", bronze.dropDuplicates(["transaction_id"]).count(), "keys unique"),
    (RUN_ID, "3_gold_channels", bronze.groupBy("channel").count().count(), "KPI rows"),
    (RUN_ID, "4_next", 1, "Job/ADF + Purview lineage"),
], ["run_id", "stage", "metric", "note"])
story.show(truncate=False)
write_demo_table(story, "teach34_e2e_story")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part J — Reshape & everyday pitfalls

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 35 — Window functions (rank without collapsing rows)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Need top txn per channel but keep every row for audit. |
# MAGIC | **Easy approach** | **Window `rank()` / `row_number()` over partition — filter afterward.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **School ranking:** everyone stays in class; you just write a rank sticker on each desk. GroupBy would kick students out into one chair.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Window specs add columns without reducing row count.
# MAGIC
# MAGIC **WHY:** Rankings, running totals, previous-day compare.
# MAGIC
# MAGIC **HOW:** `Window.partitionBy(...).orderBy(...)` + `F.row_number()`.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 35: Window functions (rank without collapsing rows)
# HARD: Need top txn per channel but keep every row for audit.
# EASY: Window `rank()` / `row_number()` over partition — filter afterward.
# ANALOGY: **School ranking:** everyone stays in class; you just write a rank sti
# ========================================================================

# --- Topic 35: windows ---
from pyspark.sql import functions as F, Window

w = Window.partitionBy("channel").orderBy(F.col("amount_gbp").cast("double").desc())
ranked = bronze.withColumn("rn", F.row_number().over(w))
top1 = ranked.filter(F.col("rn") == 1).select("channel", "transaction_id", "amount_gbp", "rn")
top1.show(truncate=False)
write_demo_table(top1, "teach35_window_top")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 36 — Pivot (turn channels into columns)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Excel people want channels as columns, not tall rows. |
# MAGIC | **Easy approach** | **`groupBy(...).pivot("channel").sum(...)` then fillna(0).** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Crossword board.** Tall list of clues becomes a grid; same facts, board-friendly shape.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Pivot reshapes category values into columns.
# MAGIC
# MAGIC **WHY:** Finance dashboards often want wide tables.
# MAGIC
# MAGIC **HOW:** Aggregate first mentally; pivot is an aggregate reshape.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 36: Pivot (turn channels into columns)
# HARD: Excel people want channels as columns, not tall rows.
# EASY: `groupBy(...).pivot("channel").sum(...)` then fillna(0).
# ANALOGY: **Crossword board.** Tall list of clues becomes a grid; same facts, bo
# ========================================================================

# --- Topic 36: pivot ---
from pyspark.sql import functions as F

wide = (
    bronze.withColumn("amount", F.col("amount_gbp").cast("double"))
    .groupBy(F.to_date("value_date").alias("txn_date"))
    .pivot("channel")
    .sum("amount")
)
wide = wide.fillna(0)
wide.show(5, truncate=False)
write_demo_table(wide, "teach36_pivot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 37 — Union vs Join (stack vs stitch)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Mixing twin feeds with a join accidentally multiplies rows. |
# MAGIC | **Easy approach** | **Same grain + same columns → unionByName. Different entities linked by key → join.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Union** = stacking two trays of cookies. **Join** = pairing each cookie with its frosting recipe card by id.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Union appends rows; join stitches columns by key.
# MAGIC
# MAGIC **WHY:** Wrong verb → Cartesian blowups or missing columns.
# MAGIC
# MAGIC **HOW:** `unionByName(allowMissingColumns=True)` for Event stacks.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 37: Union vs Join (stack vs stitch)
# HARD: Mixing twin feeds with a join accidentally multiplies rows.
# EASY: Same grain + same columns → unionByName. Different entities linked by 
# ANALOGY: **Union** = stacking two trays of cookies. **Join** = pairing each coo
# ========================================================================

# --- Topic 37: union vs join ---
from pyspark.sql import functions as F

a = bronze.select("transaction_id", "channel").limit(5).withColumn("src", F.lit("a"))
b = bronze.select("transaction_id", "channel").limit(5).withColumn("src", F.lit("b"))
stacked = a.unionByName(b)
print("union rows", stacked.count())
# Join same keys → can duplicate; demo left key to dim
dim = spark.createDataFrame([("card", "CARD")], ["channel", "code"])
stitched = bronze.limit(20).join(dim, "channel", "left")
print("join cols", stitched.columns)
write_demo_table(stacked, "teach37_union")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 38 — nulls, coalesce, fillna (missing ≠ zero)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Null amounts silently drop out of SUM; null keys break joins. |
# MAGIC | **Easy approach** | **`coalesce` for defaults; quarantine null keys — don't invent truth.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Empty plate vs zero cookies.** Empty plate means "unknown"; zero means "counted none". Don't pretend they are the same.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Null handling with coalesce / fillna / isNull filters.
# MAGIC
# MAGIC **WHY:** Joins and aggs treat null specially.
# MAGIC
# MAGIC **HOW:** Business rule decides default vs reject.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 38: nulls, coalesce, fillna (missing ≠ zero)
# HARD: Null amounts silently drop out of SUM; null keys break joins.
# EASY: `coalesce` for defaults; quarantine null keys — don't invent truth.
# ANALOGY: **Empty plate vs zero cookies.** Empty plate means "unknown"; zero mea
# ========================================================================

# --- Topic 38: nulls ---
from pyspark.sql import functions as F

demo = bronze.withColumn("amount", F.col("amount_gbp").cast("double")) \
    .withColumn("amount_safe", F.coalesce(F.col("amount"), F.lit(0.0)))
print("null amounts", demo.filter(F.col("amount").isNull()).count())
demo.select("transaction_id", "amount", "amount_safe").show(5, truncate=False)
write_demo_table(demo.select("transaction_id", "amount", "amount_safe").limit(50), "teach38_nulls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 39 — Cache / persist (reuse expensive scans)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Same cleaned DF is scanned 5 times in one notebook → 5× ADLS read. |
# MAGIC | **Easy approach** | **`cache()` once after expensive clean; `unpersist()` when done.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Photocopy the map once** for the road trip. Don't drive back to the tourist office for every turn.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Cache keeps a DataFrame in memory/disk across actions.
# MAGIC
# MAGIC **WHY:** Multiple actions on the same lineage recompute otherwise.
# MAGIC
# MAGIC **HOW:** Cache after clean; unpersist to free cluster RAM.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 39: Cache / persist (reuse expensive scans)
# HARD: Same cleaned DF is scanned 5 times in one notebook → 5× ADLS read.
# EASY: `cache()` once after expensive clean; `unpersist()` when done.
# ANALOGY: **Photocopy the map once** for the road trip. Don't drive back to the 
# ========================================================================

# --- Topic 39: cache ---
from pyspark.sql import functions as F

cleaned = bronze.withColumn("amount", F.col("amount_gbp").cast("double")).filter(F.col("amount") > 0)
cleaned.cache()
print("action1", cleaned.count())
print("action2 channels", cleaned.select("channel").distinct().count())
cleaned.unpersist()
print("cache released")
write_demo_table(cleaned.limit(20), "teach39_cache_sample")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 40 — UDF trap (why stay in Spark SQL functions)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Python UDFs serialize row-by-row and kill performance. |
# MAGIC | **Easy approach** | **Prefer `F.upper`, `F.when`, built-ins; UDF only if no Spark equivalent.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Hand-carrying each letter across town** (Python UDF) vs **post office bulk route** (Spark functions).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** UDFs run custom Python/Scala per row.
# MAGIC
# MAGIC **WHY:** They break Catalyst optimization and add Python↔JVM hops.
# MAGIC
# MAGIC **HOW:** Rewrite with `when`/`otherwise` first.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 40: UDF trap (why stay in Spark SQL functions)
# HARD: Python UDFs serialize row-by-row and kill performance.
# EASY: Prefer `F.upper`, `F.when`, built-ins; UDF only if no Spark equivalent
# ANALOGY: **Hand-carrying each letter across town** (Python UDF) vs **post offic
# ========================================================================

# --- Topic 40: prefer built-ins over UDF ---
from pyspark.sql import functions as F

# GOOD — Spark native
flagged = bronze.withColumn(
    "risk",
    F.when(F.col("amount_gbp").cast("double") > 1000, "high").otherwise("normal"),
)
flagged.groupBy("risk").count().show()
print("Avoid: @udf def risk(x): ... — only if no F.when equivalent")
write_demo_table(flagged.select("transaction_id", "amount_gbp", "risk").limit(40), "teach40_no_udf")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part K — Lakehouse ops & handoff

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 41 — VACUUM & retention (don't delete history by accident)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Time travel vanishes after vacuum; auditors angry. |
# MAGIC | **Easy approach** | **Know retention hours; vacuum only after policy; never vacuum mid-demo casually.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Shredding old tax returns.** Legal once retention ends — premature shredding is a lawsuit.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** VACUUM removes unreferenced data files past retention.
# MAGIC
# MAGIC **WHY:** Saves storage; shortens recoverable history.
# MAGIC
# MAGIC **HOW:** Lab: DESCRIBE HISTORY; teach vacuum verbally — avoid aggressive vacuum on shared tables.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 41: VACUUM & retention (don't delete history by accident)
# HARD: Time travel vanishes after vacuum; auditors angry.
# EASY: Know retention hours; vacuum only after policy; never vacuum mid-demo 
# ANALOGY: **Shredding old tax returns.** Legal once retention ends — premature s
# ========================================================================

# --- Topic 41: history vs vacuum awareness ---
path = f"{DEMO_BASE}/teach41_hist"
bronze.limit(30).write.format("delta").mode("overwrite").save(path)
bronze.limit(25).write.format("delta").mode("overwrite").save(path)
spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "timestamp", "operation").show(5, truncate=False)
print("VACUUM would delete old files AFTER retention — demo skips VACUUM on shared lab")
write_demo_table(spark.read.format("delta").load(path), "teach41_hist_current")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 42 — Partition pruning (only read needed folders)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Full lake scan for one value_date burns cost. |
# MAGIC | **Easy approach** | **Partition gold by date; filter that column so Spark skips folders.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Library shelves by year.** Asking 2026 doesn't open the 2019 aisle.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Partition columns become folder hierarchy; filters prune paths.
# MAGIC
# MAGIC **WHY:** Cheap scans on large tables.
# MAGIC
# MAGIC **HOW:** Don't over-partition (too many tiny folders).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 42: Partition pruning (only read needed folders)
# HARD: Full lake scan for one value_date burns cost.
# EASY: Partition gold by date; filter that column so Spark skips folders.
# ANALOGY: **Library shelves by year.** Asking 2026 doesn't open the 2019 aisle.
# ========================================================================

# --- Topic 42: partition prune ---
from pyspark.sql import functions as F

part_path = f"{DEMO_BASE}/teach42_part"
(
    bronze.withColumn("txn_date", F.to_date("value_date"))
    .write.format("delta").mode("overwrite")
    .partitionBy("txn_date")
    .save(part_path)
)
one = spark.read.format("delta").load(part_path).filter(F.col("txn_date") == F.lit("2026-06-01"))
print("pruned read rows", one.count())
write_demo_table(one.limit(20), "teach42_pruned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 43 — Structured Streaming mindset (micro-batches)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | People think streaming = reinvent Spark; class only has batch CSV. |
# MAGIC | **Easy approach** | **Same DataFrame API; source is a stream + checkpoint for exactly-once offsets.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Conveyor belt sushi** vs buffet. Same fish (API); continuous plate arrivals + ticket stubs (checkpoint).
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Structured Streaming processes incrementally with a checkpoint directory.
# MAGIC
# MAGIC **WHY:** Near-real-time KPIs without rewriting all logic.
# MAGIC
# MAGIC **HOW:** Concept cell — rate source or batch-as-stream; production needs checkpoint path on ADLS.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 43: Structured Streaming mindset (micro-batches)
# HARD: People think streaming = reinvent Spark; class only has batch CSV.
# EASY: Same DataFrame API; source is a stream + checkpoint for exactly-once o
# ANALOGY: **Conveyor belt sushi** vs buffet. Same fish (API); continuous plate a
# ========================================================================

# --- Topic 43: streaming concept (batch stand-in) ---
# Full streaming needs a long-running query + checkpoint. In class we show the mental model:
print("stream.readStream.format('cloudFiles'|kafka|rate) → same transforms → writeStream")
print("checkpointLocation = durable offset diary — DO NOT delete casually")
print("FinLedger training stays batch; streaming = same verbs on a moving source")
spark.createDataFrame([
    ("source", "rate / Auto Loader / Event Hubs"),
    ("transforms", "same select/filter/groupBy"),
    ("sink", "Delta with checkpointLocation"),
], ["piece", "meaning"]).show(truncate=False)
write_demo_table(
    spark.createDataFrame([("batch_today", "stream_when_sla_needs_minutes")], ["mode", "when"]),
    "teach43_streaming_map",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 44 — Auto Loader idea (discover new files)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Listing millions of landings by hand; missed files; double loads. |
# MAGIC | **Easy approach** | **cloudFiles + schema location; incremental file notification/listing.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Mailroom barcode scanner** that only processes new parcels since last shift.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Auto Loader (`cloudFiles`) ingests new files incrementally.
# MAGIC
# MAGIC **WHY:** Landed files grow forever; full directory list is expensive.
# MAGIC
# MAGIC **HOW:** Mention in class; ADF copy + batch is enough for FinLedger lab scale.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 44: Auto Loader idea (discover new files)
# HARD: Listing millions of landings by hand; missed files; double loads.
# EASY: cloudFiles + schema location; incremental file notification/listing.
# ANALOGY: **Mailroom barcode scanner** that only processes new parcels since las
# ========================================================================

# --- Topic 44: Auto Loader mental model ---
tips = [
    ("cloudFiles.format", "csv/json/parquet"),
    ("schemaLocation", "evolving schema store"),
    ("checkpoint", "which files already seen"),
    ("lab_today", "ADF lands fixed path; spark.read CSV is fine at demo scale"),
]
spark.createDataFrame(tips, ["knob", "role"]).show(truncate=False)
write_demo_table(spark.createDataFrame(tips, ["knob", "role"]), "teach44_autoloader")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 45 — JDBC / Azure SQL bridge (warehouse handoff)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Gold lives in lake; finance app still speaks SQL Server. |
# MAGIC | **Easy approach** | **JDBC write/read with secrets; or ADF copy gold → Azure SQL.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Customs form.** Lake is the cargo ship; SQL DB is the warehouse with barcode scanners the app already knows.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** `spark.read.jdbc` / `.write.jdbc` or ADF Copy.
# MAGIC
# MAGIC **WHY:** Operational apps rarely query Delta directly.
# MAGIC
# MAGIC **HOW:** Never paste SQL passwords in notebooks — Key Vault / secret scope.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 45: JDBC / Azure SQL bridge (warehouse handoff)
# HARD: Gold lives in lake; finance app still speaks SQL Server.
# EASY: JDBC write/read with secrets; or ADF copy gold → Azure SQL.
# ANALOGY: **Customs form.** Lake is the cargo ship; SQL DB is the warehouse with
# ========================================================================

# --- Topic 45: JDBC pattern (dry-run print) ---
print("Pattern (do NOT run without trainer SQL secrets):")
print('  df.write.format("jdbc").option("url", url).option("dbtable", "dbo.gold_channel").mode("overwrite").save()')
print("FinLedger lab often uses ADF Copy for SQL handoff — same idea, orchestration UI")
write_demo_table(
    spark.createDataFrame([("lake_gold", "azure_sql", "jdbc_or_adf")], ["from_layer", "to_system", "bridge"]),
    "teach45_jdbc_map",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part L — Jobs, governance, teach-ready

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 46 — Multi-task Jobs (DAG of notebooks)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | One mega-notebook; failure mid-way redoes everything. |
# MAGIC | **Easy approach** | **Job tasks: bronze → silver → gold with depends_on; retry per task.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Assembly line stations.** If frosting fails, don't re-bake the cake from flour — restart at frosting.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Databricks Workflows multi-task jobs with dependencies.
# MAGIC
# MAGIC **WHY:** Isolate failures; clearer lineage in Runs UI.
# MAGIC
# MAGIC **HOW:** Separate notebooks or task parameters; shared `run_id` widget.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 46: Multi-task Jobs (DAG of notebooks)
# HARD: One mega-notebook; failure mid-way redoes everything.
# EASY: Job tasks: bronze → silver → gold with depends_on; retry per task.
# ANALOGY: **Assembly line stations.** If frosting fails, don't re-bake the cake 
# ========================================================================

# --- Topic 46: job DAG story ---
tasks = [
    ("t1_bronze", None, "land + validate"),
    ("t2_silver", "t1_bronze", "clean + Event"),
    ("t3_gold", "t2_silver", "KPI + pivot"),
    ("t4_quality", "t3_gold", "assert + exit JSON"),
]
spark.createDataFrame(tasks, ["task", "depends_on", "role"]).show(truncate=False)
write_demo_table(spark.createDataFrame(tasks, ["task", "depends_on", "role"]), "teach46_job_dag")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 47 — Purview / lineage habit (what did we just write?)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Nobody finds assets in Purview; ADF lineage looks empty. |
# MAGIC | **Easy approach** | **Stable paths + table names; scan ADLS; search storage account not factory name.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Library catalog card.** Book must have a spine label (path) before the card catalog helps anyone.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Microsoft Purview catalogs assets and lineage.
# MAGIC
# MAGIC **WHY:** Governance + "who eats this table?"
# MAGIC
# MAGIC **HOW:** Shared lab: classic Purview portal; search `stshared*` paths; Data Curator on MI.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 47: Purview / lineage habit (what did we just write?)
# HARD: Nobody finds assets in Purview; ADF lineage looks empty.
# EASY: Stable paths + table names; scan ADLS; search storage account not fact
# ANALOGY: **Library catalog card.** Book must have a spine label (path) before t
# ========================================================================

# --- Topic 47: lineage checklist ---
rows = [
    ("stable_path", BRONZE_PATH if "BRONZE_PATH" in dir() else f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/..."),
    ("search_hint", STORAGE_ACCOUNT),
    ("not_this", "searching ADF factory name alone often fails"),
    ("next_doc", "shared-adf-lab/docs/purview_portal_search_guide.md"),
]
spark.createDataFrame(rows, ["item", "value"]).show(truncate=False)
write_demo_table(spark.createDataFrame(rows, ["item", "value"]), "teach47_purview_hints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 48 — %run and shared helpers (don't paste auth 50 times)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Auth cell drifts across notebooks; one fix needs 12 edits. |
# MAGIC | **Easy approach** | **One bronze_auth builder → generated notebooks; or `%run` a shared util notebook.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Shared spice rack** in the kitchen — every chef uses the same salt cellar, not personal bags that expire.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** `%run ./utils` pulls another notebook into this one.
# MAGIC
# MAGIC **WHY:** DRY setup — auth, helpers, constants.
# MAGIC
# MAGIC **HOW:** This lab generates cells from `bronze_auth_cell.py` so learners see auth inline too.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 48: %run and shared helpers (don't paste auth 50 times)
# HARD: Auth cell drifts across notebooks; one fix needs 12 edits.
# EASY: One bronze_auth builder → generated notebooks; or `%run` a shared util
# ANALOGY: **Shared spice rack** in the kitchen — every chef uses the same salt c
# ========================================================================

# --- Topic 48: DRY setup ---
print("In Databricks you can: %run /Shared/day9/_utils_auth")
print("In this repo: day9/scripts/bronze_auth_cell.py → injected by builders")
print("STORAGE_ACCOUNT =", STORAGE_ACCOUNT)
print("DEMO_BASE =", DEMO_BASE)
write_demo_table(
    spark.createDataFrame([("bronze_auth_cell", "shared"), ("demo_table_helpers", "shared")], ["module", "scope"]),
    "teach48_dry",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 49 — Interview story (explain your pipeline in 90 seconds)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | Engineer freezes: lists APIs instead of business flow. |
# MAGIC | **Easy approach** | **Narrate: source → bronze → silver/Event → gold → Job/ADF → quality → who consumes.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Explain a recipe to a friend**, not the brand of oven heating element.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Structured verbal story for interviews and stakeholder demos.
# MAGIC
# MAGIC **WHY:** Trust comes from clarity under time pressure.
# MAGIC
# MAGIC **HOW:** Practice aloud using FinLedger nouns (channels, returns, run_id).
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 49: Interview story (explain your pipeline in 90 seconds)
# HARD: Engineer freezes: lists APIs instead of business flow.
# EASY: Narrate: source → bronze → silver/Event → gold → Job/ADF → quality → w
# ANALOGY: **Explain a recipe to a friend**, not the brand of oven heating elemen
# ========================================================================

# --- Topic 49: 90-second script (print + recite) ---
script = [
    "1. Banks land CSVs in bronze via ADF.",
    "2. Databricks silver casts + dedupes; Event model unifies payments + returns.",
    "3. Gold aggregates by channel/day for finance.",
    "4. Job/ADF schedules nightly; quality asserts fail fast.",
    "5. Purview/SQL metadata help ops find owners.",
]
for line in script:
    print(line)
write_demo_table(spark.createDataFrame([(i + 1, s) for i, s in enumerate(script)], ["step", "line"]), "teach49_interview")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Topic 50 — Capstone checklist (you are ready when…)
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | **Hard problem** | "I finished the course" without proving skills. |
# MAGIC | **Easy approach** | **Tick every box below on real cluster with FinLedger data.** |
# MAGIC
# MAGIC ### Analogy
# MAGIC
# MAGIC **Driver's license road test** — not just reading the highway code.
# MAGIC
# MAGIC ### Trainer: WHAT / WHY / HOW
# MAGIC
# MAGIC **WHAT:** Validation checklist for Artem V&V style readiness.
# MAGIC
# MAGIC **WHY:** Verification (runs) ≠ Validation (learner can explain).
# MAGIC
# MAGIC **HOW:** Run teach-all + one 50-problem failure + event processors once.
# MAGIC
# MAGIC ### Run the next cell
# MAGIC
# MAGIC Banner comments explain every block. Read them out loud while you demo.

# COMMAND ----------

# ========================================================================
# TOPIC 50: Capstone checklist (you are ready when…)
# HARD: "I finished the course" without proving skills.
# EASY: Tick every box below on real cluster with FinLedger data.
# ANALOGY: **Driver's license road test** — not just reading the highway code.
# ========================================================================

# --- Topic 50: readiness checklist ---
checks = [
    ("lazy_vs_action", "Can explain without notes"),
    ("medallion", "Draws bronze/silver/gold on whiteboard"),
    ("delta_merge", "Shows MERGE or time travel once"),
    ("event_model", "Maps two sources → one Event grain"),
    ("quality_exit", "Fails job on empty gold"),
    ("job_or_adf", "Points to schedule + run_id param"),
    ("analogy", "Teaches one topic with a kitchen/airport story"),
]
df = spark.createDataFrame(checks, ["skill", "proof"])
df.show(truncate=False)
write_demo_table(df, "teach50_readiness")
print("Teach-all Topics 01–50 complete — practice aloud, then run 50_data_engineering_problems")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final — tables written this run

# COMMAND ----------

print("=" * 70)
print("TEACH-ALL COMPLETE —", len(WRITTEN_TABLES), "outputs")
for p in WRITTEN_TABLES[-15:]:
    print(" ", p)
if len(WRITTEN_TABLES) > 15:
    print(" ...", len(WRITTEN_TABLES) - 15, "earlier outputs")
print("Next: finledger_event_processors OR 50_data_engineering_problems")
print("=" * 70)

# COMMAND ----------
