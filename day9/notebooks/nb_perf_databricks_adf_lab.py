# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Performance & NFR Lab — Databricks + ADF
# MAGIC
# MAGIC **Two readers, one notebook**
# MAGIC - **Novice:** learn what is slow and how to see it
# MAGIC - **Architect:** map to SLA, cost, scale, and ADF orchestration
# MAGIC
# MAGIC | | |
# MAGIC |:---|:---|
# MAGIC | Path | `/Shared/day9/perf_databricks_adf_lab` |
# MAGIC | Pair with | `/Shared/day9/spark_dag_problems_lab` (DAG smell → fix) |
# MAGIC | Code style | Short cells, heavy comments above |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. The big picture (read this first)
# MAGIC
# MAGIC ### For a novice (kitchen story)
# MAGIC
# MAGIC Think of a **bank restaurant**:
# MAGIC
# MAGIC | Role | Tool | Job |
# MAGIC |------|------|-----|
# MAGIC | Manager with a timetable | **ADF** | “Open the kitchen at 2am, copy food into cold storage, then tell chefs to cook” |
# MAGIC | Kitchen + cooks | **Databricks / Spark** | Transform FinLedger data (filter, join, totals) |
# MAGIC | Security cameras | **Spark UI** | See which stove is slow |
# MAGIC | Manager’s logbook | **system tables** | Who ran what, how long, how much bill |
# MAGIC | Gas bill | **Cost / DBUs** | Money burned even if food is fine |
# MAGIC
# MAGIC **Late dinner?**  
# MAGIC 1. Was the **bus/manager** late? → ADF Monitor  
# MAGIC 2. Was a **stove** stuck? → Spark UI  
# MAGIC 3. Is the **gas bill** crazy? → billing  
# MAGIC
# MAGIC ### For an architect (one paragraph)
# MAGIC
# MAGIC ADF owns **orchestration NFRs** (trigger, dependency, timeout, retry, packing Copy vs Notebook).  
# MAGIC Databricks owns **compute NFRs** (throughput, skew, shuffle, cluster sizing, autoscaling).  
# MAGIC You correlate both with a shared **`run_id`**. Optimising Spark while ADF Copy dominates wall-clock is wasted money.
# MAGIC
# MAGIC ```text
# MAGIC ADF pipeline run
# MAGIC    ├─ Copy / wait          ← ADF Monitor duration
# MAGIC    └─ Notebook activity    ← widgets.run_id
# MAGIC           └─ Spark Jobs/Stages/Tasks  ← Spark UI + explain
# MAGIC           └─ system.query.history / billing
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0b. NFR dictionary (novice words + architect words)
# MAGIC
# MAGIC | NFR | Novice question | Architect asks | Where we look |
# MAGIC |-----|-----------------|----------------|---------------|
# MAGIC | **Performance** | How long for one run? | p95 latency vs SLA | wall clock, Spark UI stage time, ADF activity ms |
# MAGIC | **Scalability** | If data ×10? | linear? cliff? | partitions, skew, shuffle bytes, AQE |
# MAGIC | **Cost** | Are we wasting money? | $ per successful pipeline | `system.billing`, cluster idle, SKU |
# MAGIC | **Reliability** | Does it fail randomly? | error budget, retries hide bugs? | Job/ADF fail rate |
# MAGIC | **Observability** | Can a stranger debug at 2am? | run_id, logs, dashboards | secrets-safe logs, Monitor, system tables |
# MAGIC | **Correctness** | Are totals right? | idempotent MERGE? | row counts by run_id, reconciliations |
# MAGIC
# MAGIC **Architect rule:** Correctness → SLA → Cost (never cut cost by skipping reconciliation).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup — load FinLedger bronze
# MAGIC
# MAGIC **Novice:** We need a real table so timings are real.  
# MAGIC **Architect:** Same estate as Sessions 9–15 / 50-problems (`finledger` secrets).

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

# SETUP — helpers everyone reuses
from pyspark.sql import functions as F
from datetime import date, timedelta
import time
import json

dbutils.widgets.text("run_id", "session3-lab", "Same id ADF should pass")
RUN_ID = dbutils.widgets.get("run_id").strip() or "session3-lab"

# Typed FinLedger frame (one place — no copy-paste later)
df = (
    bronze
    .withColumn("amount", F.col("amount_gbp").cast("double"))
    .withColumn("channel", F.upper(F.trim(F.col("channel"))))
    .withColumn("txn_date", F.to_date("value_date"))
    .filter(F.col("amount").isNotNull())
)

SYSTEM_HITS = []  # which system tables worked


def try_sql(label, sql):
    """Soft query: teach even when ACL blocks system.* """
    try:
        out = spark.sql(sql)
        SYSTEM_HITS.append(label)
        print("OK ", label)
        display(out.limit(15))
        return out
    except Exception as e:
        print("SKIP", label, "→", str(e)[:140])
        return None


print("rows =", df.count(), "| auth =", BRONZE_AUTH_MODE, "| run_id =", RUN_ID)
df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Measure one real query (novice stopwatch)
# MAGIC
# MAGIC ### Novice
# MAGIC We cook one dish (channel totals) and measure **wall-clock milliseconds**.  
# MAGIC Then we ask Spark to **print the recipe** (`explain`).
# MAGIC
# MAGIC ### Architect
# MAGIC Establish a **baseline**. Without baseline numbers, “optimisation” is theatre.  
# MAGIC Capture: rows in, wall ms, plan operators (`Exchange`, `BroadcastHashJoin`, `FileScan`).
# MAGIC
# MAGIC ```text
# MAGIC df → groupBy(channel) → sum(amount) → ACTION (collect/show)
# MAGIC                               │
# MAGIC                               └─ creates 1 Spark Job in UI
# MAGIC ```

# COMMAND ----------

# STEP 2 — baseline KPI + explain
t0 = time.time()

kpi = (
    df.groupBy("channel")
    .agg(
        F.count("*").alias("txns"),
        F.round(F.sum("amount"), 2).alias("gbp"),
    )
    .orderBy(F.desc("gbp"))
)

# ACTION — forces Spark to run
kpi_rows = kpi.collect()
ELAPSED_MS = int((time.time() - t0) * 1000)

print("Wall clock ms =", ELAPSED_MS)
print("Channels     =", len(kpi_rows))
display(kpi)

print("\n--- RECIPE (explain) ---")
print("Look for: FileScan / Exchange / HashAggregate")
kpi.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Spark UI tour (your live screenshots)
# MAGIC
# MAGIC **Open:** Compute → your cluster → **Spark UI** → **Jobs** (or **SQL**).
# MAGIC
# MAGIC ### Novice map
# MAGIC
# MAGIC | UI word | Plain English |
# MAGIC |---------|----------------|
# MAGIC | **Job** | One “Go!” from `.count` / `.write` / `.collect` |
# MAGIC | **Stage** | Chunk of work with no shuffle in the middle |
# MAGIC | **Task** | Work on **one partition** (one cook’s tray) |
# MAGIC | **Exchange** | Shuffle truck between kitchens (usually expensive) |
# MAGIC | **Duration** | How long that piece took |
# MAGIC
# MAGIC ### Architect map
# MAGIC
# MAGIC | UI signal | Likely NFR hit | Typical response |
# MAGIC |-----------|----------------|------------------|
# MAGIC | Few fat tasks | Scalability / reliability (OOM) | repartition, salt skew keys |
# MAGIC | Many 20ms tasks | Scheduler overhead | coalesce / fewer files |
# MAGIC | High shuffle R/W | Performance + cost | broadcast small side, filter early |
# MAGIC | Spill | Memory pressure | more memory or more partitions |
# MAGIC | Long queue before job | ADF / cluster start | pools, warm cluster |
# MAGIC
# MAGIC ```text
# MAGIC Action
# MAGIC  └─ Job
# MAGIC      ├─ Stage 0 (narrow: filter/select)
# MAGIC      └─ Stage 1 (after Exchange shuffle)
# MAGIC            └─ Tasks 0..N-1
# MAGIC ```
# MAGIC
# MAGIC **Exercise:** After Step 2, find the Job for `collect` and note stage count.

# COMMAND ----------

# STEP 3 — checklist frame (pin this during demos)
checklist = spark.createDataFrame(
    [
        (1, "Spark UI → Jobs", "Find the Job from Step 2"),
        (2, "Open longest Stage", "See task time spread (skew?)"),
        (3, "SQL tab / explain", "Find Exchange vs Broadcast"),
        (4, "ADF Monitor", "Compare Notebook vs Copy duration"),
        (5, "Billing / cost chart", "Idle overnight burn?"),
    ],
    ["step", "where", "ask"],
)
display(checklist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. system.query.history — the logbook
# MAGIC
# MAGIC ### Novice
# MAGIC A diary of statements: who ran SQL/notebook queries and roughly how long.
# MAGIC
# MAGIC ### Architect
# MAGIC Use for **capacity + accountability**: noisy neighbours, failing statements, regressions after release.  
# MAGIC May be ACL-gated — lab soft-skips and shows a synthetic sample so class continues.
# MAGIC
# MAGIC **Analogy:** Hotel front-desk log: check-in time, guest, room — not the full CCTV.

# COMMAND ----------

# STEP 4 — query history (soft)
qh = try_sql(
    "system.query.history",
    "SELECT * FROM system.query.history ORDER BY 1 DESC LIMIT 25",
)

if qh is None:
    qh = spark.createDataFrame(
        [
            (RUN_ID, "SELECT channel, sum(amount)", 900, "FINISHED"),
            (RUN_ID, "MERGE INTO silver", 5200, "FINISHED"),
            (RUN_ID, "OPTIMIZE gold", 7800, "FINISHED"),
        ],
        ["run_id", "query_text", "duration_ms", "status"],
    )
    print("Synthetic history for teaching")
    display(qh)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cost NFR — system.billing.usage
# MAGIC
# MAGIC ### Novice
# MAGIC The **gas bill**. Fast food that costs a fortune is still a failed restaurant.
# MAGIC
# MAGIC ### Architect
# MAGIC Break down by day/SKU. Watch for: always-on interactive clusters, failed job retries,  
# MAGIC oversized warehouses. Pair with auto-termination and job clusters.
# MAGIC
# MAGIC **Target conversation:** “What is $ per successful FinLedger nightly?”

# COMMAND ----------

# STEP 5 — billing / usage
bill = try_sql(
    "system.billing.usage",
    """
    SELECT usage_date, sku_name, usage_unit, SUM(usage_quantity) AS qty
    FROM system.billing.usage
    WHERE usage_date >= date_add(current_date(), -14)
    GROUP BY usage_date, sku_name, usage_unit
    ORDER BY usage_date DESC
    LIMIT 80
    """,
)

if bill is None:
    bill = spark.createDataFrame(
        [(date.today() - timedelta(days=i), "JOBS", "DBU", float(2 + i % 4)) for i in range(14)],
        ["usage_date", "sku_name", "usage_unit", "qty"],
    )
    print("Synthetic billing for teaching")

display(bill.limit(20))

# Simple chart
try:
    import matplotlib.pyplot as plt

    pdf = bill.toPandas()
    dcol = "usage_date" if "usage_date" in pdf.columns else pdf.columns[0]
    qcol = "qty" if "qty" in pdf.columns else pdf.columns[-1]
    g = pdf.groupby(dcol, as_index=False)[qcol].sum().sort_values(dcol)
    fig, ax = plt.subplots(figsize=(9, 3.5))
    ax.plot(g[dcol], g[qcol], marker="o")
    ax.set_title("Usage over time (cost NFR)")
    ax.set_ylabel(qcol)
    fig.autofmt_xdate()
    fig.tight_layout()
    display(fig)
except Exception as e:
    print("chart skipped:", str(e)[:100])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Compute & Jobs system tables
# MAGIC
# MAGIC ### Novice
# MAGIC “What clusters exist?” and “Did Jobs run?”
# MAGIC
# MAGIC ### Architect
# MAGIC Inventory for governance: abandoned all-purpose clusters, job run timelines,  
# MAGIC rightsizing candidates. Names differ by Databricks version — soft SKIP is OK.

# COMMAND ----------

# STEP 6 — compute / lakeflow (soft)
try_sql("system.compute.clusters", "SELECT * FROM system.compute.clusters LIMIT 12")
try_sql("system.lakeflow.jobs", "SELECT * FROM system.lakeflow.jobs LIMIT 12")
try_sql("system.lakeflow.job_run_timeline", "SELECT * FROM system.lakeflow.job_run_timeline LIMIT 12")
print("Hits:", SYSTEM_HITS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. ADF + Databricks together (architect’s correlation)
# MAGIC
# MAGIC ### Novice
# MAGIC ADF is the **timetable**. Databricks is the **kitchen**.  
# MAGIC If dinner is late, check whether the **bus** arrived late before blaming the cook.
# MAGIC
# MAGIC ### Architect
# MAGIC In Azure portal: **ADF Studio → Monitor → Pipeline runs**.  
# MAGIC Click the run → each activity’s duration:
# MAGIC
# MAGIC | Activity | Typical meaning if slow |
# MAGIC |----------|-------------------------|
# MAGIC | Copy | Data movement / THROTTLING / huge files |
# MAGIC | Notebook | Cluster start queue + Spark work |
# MAGIC | Wait / Until | Explicit delay or polling |
# MAGIC
# MAGIC **Contract:** pass `run_id` (or use ADF `pipeline().RunId`) into notebook `baseParameters`.
# MAGIC
# MAGIC ```text
# MAGIC ADF pipelineRunId  ──baseParameters──►  widget run_id
# MAGIC                                          │
# MAGIC                                          ▼
# MAGIC                               Delta / logs tagged with same id
# MAGIC ```
# MAGIC
# MAGIC This lab shows a **template table** (synthetic) next to your live Spark wall clock.

# COMMAND ----------

# STEP 7 — correlate ADF (template) with Spark baseline
adf = spark.createDataFrame(
    [
        (RUN_ID, "Copy_to_bronze", 45_000, "Succeeded"),
        (RUN_ID, "Notebook_transform", 180_000, "Succeeded"),
        (RUN_ID, "pipeline_total", 240_000, "Succeeded"),
    ],
    ["run_id", "activity", "duration_ms", "status"],
)
print("Replace these numbers with ADF Monitor screenshots in class")
display(adf)

compare = spark.createDataFrame(
    [
        ("ADF Copy (example)", 45000),
        ("ADF Notebook activity (example)", 180000),
        ("Spark KPI wall clock (this notebook)", ELAPSED_MS),
    ],
    ["segment", "duration_ms"],
)
display(compare.orderBy(F.desc("duration_ms")))

print("If Notebook activity >> Spark UI time → cluster start / queue")
print("If Copy >> Notebook → fix movement, not joins")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Debug playbook — symptom → reason → fix
# MAGIC
# MAGIC ### Novice: use as a recipe card
# MAGIC
# MAGIC | If you see… | It often means… | Try this |
# MAGIC |-------------|-----------------|----------|
# MAGIC | One task much longer | **Skew** | salt key / filter / AQE |
# MAGIC | Exchange + large shuffle | Heavy **groupBy/join** | filter early, broadcast small |
# MAGIC | Driver OOM | **collect**/big pandas | aggregate in Spark |
# MAGIC | Thousands of tiny tasks | Too many partitions/files | coalesce / OPTIMIZE |
# MAGIC | Spill in stage metrics | Not enough RAM per task | more partitions or memory |
# MAGIC | Random timeouts | Cluster cold / throttle | pools, retries, smaller parallel Copies |
# MAGIC
# MAGIC ### Architect: decision tree
# MAGIC
# MAGIC ```text
# MAGIC Wall clock bad?
# MAGIC   ├─ ADF Copy dominates?  → storage, format, parallelism, integration runtime
# MAGIC   ├─ Cluster pending long? → pools / warm / job cluster sizing
# MAGIC   └─ Spark stage slow?
# MAGIC         ├─ Skew task? → salt / skew join
# MAGIC         ├─ Shuffle huge? → broadcast / denoise columns
# MAGIC         └─ Scan huge? → partition prune / file layout
# MAGIC ```
# MAGIC
# MAGIC Next cell runs **safe mini-demos** (bounded sizes).

# COMMAND ----------

# STEP 8 — four safe probes (simple code)

# A) Broadcast small dim (good pattern)
dim = spark.createDataFrame([("CARD", "Card"), ("WIRE", "Wire")], ["channel", "label"])
print("Broadcast join rows:", df.join(F.broadcast(dim), "channel", "left").count())

# B) Skew toy (see counts — then remember salt from DAG lab)
skew = df.withColumn("k", F.when(F.rand() < 0.8, F.lit("HOT")).otherwise(F.col("channel")))
skew.groupBy("k").count().orderBy(F.desc("count")).show()

# C) Don't collect facts — KPI only
display(df.groupBy("channel").sum("amount"))

# D) Tag snapshot with run_id (observability)
snap = kpi.withColumn("run_id", F.lit(RUN_ID)).withColumn("elapsed_ms", F.lit(ELAPSED_MS))
display(snap)
try:
    spark.sql("CREATE DATABASE IF NOT EXISTS perf_lab")
    snap.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "perf_lab.last_kpi_snapshot"
    )
    print("Wrote perf_lab.last_kpi_snapshot")
except Exception as e:
    print("save skipped:", str(e)[:120])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Scalability scenarios (whiteboard for architects)
# MAGIC
# MAGIC | Scenario | Bad smell | Healthy smell |
# MAGIC |----------|-----------|---------------|
# MAGIC | Data ×10 overnight | Runtime ×10 forever | More workers / better files, SLA held |
# MAGIC | 5 pipelines at once | Interactive cluster dies | Job clusters / SQL warehouse isolation |
# MAGIC | Peak hour ADF | Copy stampedes | Throttle + IR sizing |
# MAGIC | Failure mid-run | Full reload | MERGE / idempotent `run_id` paths |
# MAGIC | Weekend | Flat cost high | Auto-terminate / stop play clusters |
# MAGIC
# MAGIC **Novice takeaway:** Bigger data needs a **plan**, not only a bigger button.
# MAGIC
# MAGIC **Architect takeaway:** Write NFRs as numbers: *“Nightly FinLedger < 30 min p95 at 10× volume; cost < X DBUs.”*

# COMMAND ----------

# STEP 9 — scorecard (fill value column during workshop)
score = spark.createDataFrame(
    [
        ("Performance", "KPI wall ms", str(ELAPSED_MS), "Agree SLA with business"),
        ("Scalability", "Skew / partitions", "check Spark UI", "No single task >> others"),
        ("Cost", "14d usage trend", "Step 5 chart", "No idle overnight"),
        ("Reliability", "ADF+Job fails 7d", "Monitor portal", "Near-zero fail rate"),
        ("Observability", "run_id linked", RUN_ID, "Same id ADF ↔ Delta"),
        ("Correctness", "recon totals", "manual/SQL", "Idempotent re-run"),
    ],
    ["nfr", "signal", "value_today", "healthy_looks_like"],
)
display(score)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Wrap — what to say in a design review
# MAGIC
# MAGIC **Novice script (30 seconds):**  
# MAGIC “ADF starts the pipeline. Spark does the heavy transform. I use Spark UI for stages,  
# MAGIC system tables for history and cost, and the same run_id so I can line up Monitor with the lake.”
# MAGIC
# MAGIC **Architect script (30 seconds):**  
# MAGIC “We split NFRs: orchestration vs compute. Baselines from explain + UI.  
# MAGIC Cost and skew are first-class. Copy vs Notebook time decides where we invest.  
# MAGIC DAG smells are remediated with broadcast, salting, pruning — see spark_dag_problems_lab.”
# MAGIC
# MAGIC **Companion notebook:** `/Shared/day9/spark_dag_problems_lab`

# COMMAND ----------

# EXIT
summary = {
    "lab": "perf_databricks_adf_lab",
    "run_id": RUN_ID,
    "kpi_wall_ms": ELAPSED_MS,
    "system_hits": SYSTEM_HITS,
    "status": "Succeeded",
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))

# COMMAND ----------
