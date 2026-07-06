# Databricks notebook source

# MAGIC %md
# MAGIC # Master PySpark Lab — Theory + Practice (Shared Class)
# MAGIC
# MAGIC **Estate:** `rg-shared-class1` · storage `stsharedqgr7mj` · scope `finledger`
# MAGIC
# MAGIC **Run:** Attach a **Single-user** cluster → **Run all** top to bottom.
# MAGIC
# MAGIC **How this notebook is structured:**
# MAGIC - **Small cells** — one idea per cell (WHAT → WHY → diagram → HOW → code)
# MAGIC - **Mermaid diagrams** — visual flow for each major concept
# MAGIC - **ASCII flows** — step-by-step when diagrams help memory
# MAGIC
# MAGIC **You will learn:**
# MAGIC 1. Data engineering framework — Ingest → Transform → Orchestrate
# MAGIC 2. Medallion lake — bronze → silver → gold on ADLS (`abfss://`)
# MAGIC 3. PySpark DataFrame API — lazy transforms, eager actions
# MAGIC 4. FinLedger pipeline — read bronze, cast, quarantine, aggregate gold
# MAGIC
# MAGIC > **Note:** On Databricks, `spark` already exists. Do **not** create `SparkSession`.

# COMMAND ----------

# Cell 0 — Load FinLedger bronze (Serverless + classic cluster safe)
SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()
RUN_ID = "session3-lab"


def _read_bronze_transactions(spark, account: str, key: str, run_id: str):
    """Read bronze CSV; works when spark.conf account keys are blocked (Spark Connect)."""
    path = (
        f"abfss://bronze@{account}.dfs.core.windows.net"
        f"/loaded/run={run_id}/sample_transactions.csv"
    )

    # 1) Classic attached cluster — storage account key in Spark config
    try:
        spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "storage_key_spark_conf"
    except Exception:
        pass

    # 2) Azure RBAC / credential passthrough — no key in config
    try:
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "azure_rbac_passthrough"
    except Exception:
        pass

    # 3) Driver SDK fallback (training CSV is small) — Serverless / Spark Connect
    from io import StringIO
    import pandas as pd

    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        import subprocess
        import sys
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "azure-storage-file-datalake", "-q"]
        )
        from azure.storage.filedatalake import DataLakeServiceClient

    rel = f"loaded/run={run_id}/sample_transactions.csv"
    client = DataLakeServiceClient(
        account_url=f"https://{account}.dfs.core.windows.net",
        credential=key,
    )
    raw = (
        client.get_file_system_client("bronze")
        .get_file_client(rel)
        .download_file()
        .readall()
        .decode("utf-8")
    )
    pdf = pd.read_csv(StringIO(raw))
    return spark.createDataFrame(pdf), path, "driver_sdk"


bronze, BRONZE_PATH, BRONZE_AUTH_MODE = _read_bronze_transactions(
    spark, STORAGE_ACCOUNT, _storage_key, RUN_ID
)
df = bronze

print("Storage     :", STORAGE_ACCOUNT)
print("Bronze path :", BRONZE_PATH)
print("Auth mode   :", BRONZE_AUTH_MODE)
print("Bronze rows :", bronze.count())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 0 — How Spark thinks (read this first)
# MAGIC
# MAGIC Every concept below uses four lenses:
# MAGIC
# MAGIC | Lens | Question |
# MAGIC |------|----------|
# MAGIC | **WHAT** | What is this thing? |
# MAGIC | **WHY** | Why does Spark work this way? |
# MAGIC | **HOW** | What happens on the cluster? |
# MAGIC | **ANALOGY** | Real-world picture to remember it |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.1 — What is Apache Spark?

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC Spark is a **distributed compute engine** for processing large tables (DataFrames) across many machines. You write declarative steps (`filter`, `groupBy`); Spark figures out how to run them in parallel.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC A single laptop cannot hold or process terabytes of bank transactions. Banks need **scale** and **fault tolerance** — if one machine fails, the job continues.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC You (notebook)              Driver                 Executors
# MAGIC ┌──────────────┐         ┌──────────────┐       ┌──────────────┐
# MAGIC │ df.filter()  │────────►│ build plan   │──────►│ partition A  │
# MAGIC │ df.groupBy() │         │ schedule     │       │ partition B  │
# MAGIC └──────────────┘         └──────┬───────┘       └──────┬───────┘
# MAGIC                                 └──────────────────────┘
# MAGIC                                          │
# MAGIC                                          ▼
# MAGIC                                   result to driver
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC Spark splits your table into **partitions**, ships **tasks** to **executors**, merges results on the **driver**, and uses the **Catalyst** optimizer to rewrite your plan before execution.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Restaurant chain:** You (driver) write one menu recipe. Head office (Spark) sends the same recipe to many kitchens (executors). Each kitchen cooks its batch (partition). You plate the final dish (action result).

# COMMAND ----------

print("Spark version:", spark.version)
print("You are the chef writing recipes; Spark runs the kitchens.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.2 — Driver vs Executor (WHO does the work?)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC **Driver** = the coordinator JVM (your notebook cell's brain). **Executor** = worker JVM on cluster nodes that process data partitions.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC One machine cannot scan a 500 GB CSV. Work must be **parallelised** across workers while something coordinates the plan.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐
# MAGIC  │   DRIVER    │  sends tasks
# MAGIC  │ coordinator │
# MAGIC  └──────┬──────┘
# MAGIC         ├──────────────────┐
# MAGIC         ▼                  ▼
# MAGIC  ┌─────────────┐    ┌─────────────┐
# MAGIC  │ EXECUTOR 1  │    │ EXECUTOR 2  │
# MAGIC  │ partition 1 │    │ partition 2 │
# MAGIC  └─────────────┘    └─────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC Driver builds the **logical plan** from your DataFrame API calls. On an **action**, the driver asks executors to run **tasks** (read, filter, aggregate). Executors return **metrics** or **small results** to the driver.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Film director vs actors:** The **director** (driver) does not act in every scene — they shout instructions. **Actors** (executors) perform scenes (partitions) in parallel. The director reviews the final cut (`show`, `count`).

# COMMAND ----------

print("Default parallelism (hint of partition count):", spark.sparkContext.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.3 — Partitions (HOW data is chopped up)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC A **partition** is a horizontal slice of a DataFrame — a subset of rows that one task can process. Files, shuffles, and `repartition()` all affect partition layout.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC Parallelism only works if data is split. One giant chunk = one executor does everything = no scale.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC DataFrame (1000 rows)
# MAGIC  ┌────┬────┬────┬────┐
# MAGIC  │ P1 │ P2 │ P3 │ P4 │  ◄── each partition = one task
# MAGIC  └────┴────┴────┴────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC When you `read.csv()`, Spark creates partitions (often ~one per file block). Each partition is processed by a **task** on an executor. Narrow ops (`filter`, `select`) often stay **partition-local** (no shuffle).

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Pizza delivery zones:** A city is split into zones (partitions). Each driver (executor) delivers only their zone. You don't send one driver across the whole country.

# COMMAND ----------

print('Partitions after read:', bronze.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.4 — DataFrame vs RDD (WHAT you code against)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC **DataFrame** = named columns + schema + Catalyst optimiser. **RDD** = lower-level resilient distributed dataset (rows without rich schema).

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC DataFrames let Spark **optimise** your query (predicate pushdown, column pruning). RDDs require manual tuning.

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC Your `df.filter(...).select(...)` compiles to a **logical plan**. Catalyst rewrites it, then runs a **physical plan** on executors. You almost always use DataFrame API in production.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Spreadsheet vs SQL database:** RDD is like raw cell ranges. DataFrame is like a **typed table** where the system knows column names and can optimise queries.

# COMMAND ----------

print('We use DataFrame API:', type(bronze))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.5 — Lazy transformations (WHY Spark waits)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC **Transformations** (`select`, `filter`, `withColumn`, `join`, `groupBy`) are **lazy** — they record intent in a plan but do **not** run immediately.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC If Spark ran every line as you typed, it would **re-scan** the lake many times (slow, expensive). Lazy lets Spark **batch** work into one efficient job.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC filter  ──► select ──► withColumn     (lazy — nothing runs)
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                       count() / show()  (ACTION — cluster runs)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-by-step flow
# MAGIC
# MAGIC ```
# MAGIC filter()   →  (nothing yet)
# MAGIC  select()   →  (nothing yet)
# MAGIC  count()    →  RUN full plan
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC Each transform returns a **new DataFrame** linked to a growing **DAG** (directed acyclic graph) of steps. Nothing touches disk/network until an **action**.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Shopping list vs shopping trip:** Writing `filter` and `select` is adding items to a list (lazy). Calling `count()` is **going to the shop once** with the full list (action).

# COMMAND ----------

step_a = bronze.filter("status = 'posted'")
step_b = step_a.select('transaction_id', 'channel')
print('List written — no shopping trip yet (lazy)')

# COMMAND ----------

print('Now we shop once:', step_b.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.6 — Actions (WHAT triggers real work)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC **Actions** (`show`, `count`, `collect`, `write`, `take`) tell Spark: **execute the full plan now** and give me a result.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC You need *some* trigger to see output or save data. Actions are that trigger — but each action can re-run the chain unless you `cache()`.

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC Driver schedules **stages** (groups of tasks separated by **shuffles**). Executors run tasks, report back. `show()` pulls a **small sample** to driver; `write()` streams rows to storage.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Microwave start button:** Transformations set temperature and time (lazy). **Start** (action) actually cooks. Pressing Start twice cooks twice — same for two `count()` calls without cache.

# COMMAND ----------

step_b.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.7 — Catalyst optimizer (HOW Spark rewrites your recipe)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC **Catalyst** is Spark's query planner. It turns your DataFrame/SQL into an optimised **physical plan** before executors run.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC Humans write steps in a convenient order; machines can **merge filters**, **drop unused columns**, and **push predicates** to file readers.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC DataFrame API ──┐
# MAGIC                  ├──► Logical plan ──► Catalyst ──► Physical plan ──► Executors
# MAGIC  SQL ────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC `df.filter(...).select(...).filter(...)` may become **one** file scan reading only needed columns. Use `.explain()` to see logical vs physical plans.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **GPS recalculating route:** You ask for A→B→C (lazy steps). GPS (Catalyst) finds a **shorter single route** before you drive (action).

# COMMAND ----------

bronze.filter(F.col('channel') == 'wire').select('transaction_id', 'amount_gbp').explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.8 — Narrow vs wide transformations

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC **Narrow** ops (`filter`, `select`, `withColumn`) — each output row depends only on **one input row** in the same partition. **Wide** ops (`groupBy`, `join`) may need **shuffle**.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC Narrow = cheap (no cross-machine data movement). Wide = expensive (network shuffle). Knowing the difference helps you design fast pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-by-step flow
# MAGIC
# MAGIC ```
# MAGIC NARROW (cheap)              WIDE (shuffle)
# MAGIC  filter, select              groupBy, join
# MAGIC  same partition              data moves across executors
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC Narrow: executor processes partition locally. Wide: Spark **exchanges** data so all rows with the same key land on the same partition (shuffle), then aggregates.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Narrow = sorting papers on your desk.** **Wide = merging piles from every desk in the office** into one room sorted by customer name (shuffle).

# COMMAND ----------

print('Narrow example: filter')
bronze.filter(F.col('status')=='posted').explain()

# COMMAND ----------

print('Wide example: groupBy')
bronze.groupBy('channel').count().explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.9 — Shuffle (WHY groupBy feels slower)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC A **shuffle** is Spark **redistributing** data across executors so rows with the same key are co-located (needed for `groupBy`, `join`).

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC `groupBy('channel')` must gather all `wire` rows together — they may start on different partitions/machines.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC Before                    Shuffle                 After
# MAGIC  Exec1: wire,card    ──►   network move    ──►   Exec1: all wire
# MAGIC  Exec2: wire,fps                              Exec2: card+fps
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC Spark writes **map output** to local disk, ships blocks over the network, merges **reduce** side per key. Shuffle = **most expensive** part of many jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Group project:** Everyone (executors) has random pages of a book. To count words, you must **pass piles** so all pages of chapter 3 sit with one person (shuffle) before counting.

# COMMAND ----------

bronze.groupBy('channel').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.10 — Column API `F.col` (HOW to express logic)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC `from pyspark.sql import functions as F` gives **column expressions** — logic that runs on executors, not Python loops on the driver.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC Python `for row in df.collect()` pulls **all data** to one machine (breaks scale). Column expressions stay distributed.

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC `F.col('amount_gbp').cast('double')` builds an expression tree. Catalyst implements it in optimised JVM code across partitions.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Assembly line stamp:** Instead of one worker painting each car by hand (Python loop), a **machine arm** (`F.col`) stamps every car on the line the same way — parallel, repeatable.

# COMMAND ----------

bronze.select(F.col('transaction_id'), F.upper(F.col('channel')).alias('CH')).show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.11 — FinLedger in one Spark story

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT (definition)**
# MAGIC
# MAGIC **WHAT:** Read bronze CSV → cast amounts → split good/quarantine → dedupe → join lookup → aggregate gold by channel.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY (reason it exists)**
# MAGIC
# MAGIC **WHY:** Raw bank files are messy strings; reports need typed, trusted totals per payment channel.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC bronze ──► cast ──► quarantine ──► dedupe ──► join ──► gold
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-by-step flow
# MAGIC
# MAGIC ```
# MAGIC Sources
# MAGIC     │
# MAGIC     ▼
# MAGIC  BRONZE ──► SILVER ──► GOLD
# MAGIC               │
# MAGIC               ▼
# MAGIC          QUARANTINE
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW (what Spark does on the cluster)**
# MAGIC
# MAGIC **HOW:** One lazy chain ending in `show()`/`write()`; Catalyst merges filters; shuffle only on `groupBy`/`join`.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY (picture it)**
# MAGIC
# MAGIC **Factory line:** **Bronze** = raw goods arrival. **Silver** = QC + labelling. **Gold** = daily production report. Spark is the factory automation system.

# COMMAND ----------

df = bronze
print('FinLedger story starts — Part 11 runs the full line')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.12 — Cheat sheet: lazy vs action (pin this mentally)

# COMMAND ----------

# MAGIC %md
# MAGIC | Type | Examples | Analogy | Runs cluster? |
# MAGIC |------|----------|---------|---------------|
# MAGIC | **Lazy** | `filter`, `select`, `withColumn`, `join`, `groupBy` | Add to shopping list | No |
# MAGIC | **Action** | `show`, `count`, `collect`, `write` | Go shopping / save meal | **Yes** |

# COMMAND ----------

# MAGIC %md
# MAGIC **Golden rule:** Chain many lazy steps → **one** action at the end.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lazy chain → single action
# MAGIC
# MAGIC ```
# MAGIC filter  ──► select ──► withColumn     (lazy — nothing runs)
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                       count() / show()  (ACTION — cluster runs)
# MAGIC ```

# COMMAND ----------

print('Part 0 complete — you understand HOW Spark thinks. Now apply it to FinLedger.')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — What is data engineering?

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 The three pillars

# COMMAND ----------

# MAGIC %md
# MAGIC Data engineering prepares raw data so downstream teams (analytics, BI, ML) can extract value.

# COMMAND ----------

# MAGIC %md
# MAGIC | Pillar | Meaning | Our lab |
# MAGIC |--------|---------|---------|
# MAGIC | **Ingest** | Bring data into the platform | Read CSV from `abfss://bronze/...` |
# MAGIC | **Transform** | Clean, type, filter, aggregate | PySpark `withColumn`, `filter`, `groupBy` |
# MAGIC | **Orchestrate** | Schedule, monitor, retry pipelines | ADF + Databricks Jobs (Session 4+) |

# COMMAND ----------

# MAGIC %md
# MAGIC **Analogy:** Ingest = delivery truck. Transform = kitchen prep. Orchestrate = restaurant schedule.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌────────┐     ┌────────────┐     ┌─────────────┐
# MAGIC  │ INGEST │────►│ TRANSFORM  │────►│ ORCHESTRATE │
# MAGIC  │ abfss  │     │ PySpark    │     │ ADF / Jobs  │
# MAGIC  └────────┘     └────────────┘     └─────────────┘
# MAGIC ```

# COMMAND ----------

print("Today: Ingest + Transform in PySpark. Orchestrate comes in Session 4.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Medallion architecture

# COMMAND ----------

# MAGIC %md
# MAGIC The **medallion** pattern organises the lake in three quality layers:

# COMMAND ----------

# MAGIC %md
# MAGIC | Layer | Quality | Storage | FinLedger example |
# MAGIC |-------|---------|---------|-------------------|
# MAGIC | **Bronze** | Raw, as landed | `bronze/` container | CSV with string `amount_gbp` |
# MAGIC | **Silver** | Cleansed, typed, deduped | `silver/` container | `amount` as `double`, bad rows quarantined |
# MAGIC | **Gold** | Business aggregates | `gold/` container | Total GBP per `channel` |

# COMMAND ----------

# MAGIC %md
# MAGIC Paths use **ADLS Gen2** with hierarchical namespace (`abfss://`).

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC Sources ──► BRONZE ──► SILVER ──► GOLD
# MAGIC                   └──► QUARANTINE
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-by-step flow
# MAGIC
# MAGIC ```
# MAGIC Sources
# MAGIC     │
# MAGIC     ▼
# MAGIC  BRONZE ──► SILVER ──► GOLD
# MAGIC               │
# MAGIC               ▼
# MAGIC          QUARANTINE
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC abfss://bronze@<account>.dfs.core.windows.net/loaded/run=<id>/file.csv
# MAGIC abfss://silver@<account>.dfs.core.windows.net/transactions
# MAGIC abfss://gold@<account>.dfs.core.windows.net/daily_channel_summary
# MAGIC ```

# COMMAND ----------

for layer in ("bronze", "silver", "gold"):
    print(f"abfss://{layer}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Why PySpark (not Python loops)?

# COMMAND ----------

# MAGIC %md
# MAGIC | Approach | Runs on | Scale | FinLedger |
# MAGIC |----------|---------|-------|-----------|
# MAGIC | Python `for` loop over rows | Driver laptop | Thousands of rows | ❌ Too slow |
# MAGIC | **PySpark DataFrame** | Cluster executors | Billions of rows | ✅ Production |

# COMMAND ----------

# MAGIC %md
# MAGIC **Rule:** Express logic as **column expressions** (`F.col`, `filter`, `withColumn`) — Spark runs them in parallel across partitions.

# COMMAND ----------

# MAGIC %md
# MAGIC **Wrong:** `for row in df.collect(): ...`  (pulls all data to one machine)

# COMMAND ----------

# MAGIC %md
# MAGIC **Right:** `df.filter(F.col("status") == "posted")`  (distributed)

# COMMAND ----------

print("PySpark = distributed SQL-like transforms on the cluster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — How Spark thinks (architecture)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Driver, executors, partitions

# COMMAND ----------

# MAGIC %md
# MAGIC When you run a notebook cell on Databricks, Spark coordinates work across the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC - **Driver** — coordinates work, holds small results (`show`, `count`)
# MAGIC - **Executor** — worker JVM that processes **partitions** of data in parallel
# MAGIC - **Partition** — chunk of rows (e.g. one CSV split or one parquet file)

# COMMAND ----------

# MAGIC %md
# MAGIC You write one expression; Spark splits work across partitions automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐
# MAGIC  │   DRIVER    │  sends tasks
# MAGIC  │ coordinator │
# MAGIC  └──────┬──────┘
# MAGIC         ├──────────────────┐
# MAGIC         ▼                  ▼
# MAGIC  ┌─────────────┐    ┌─────────────┐
# MAGIC  │ EXECUTOR 1  │    │ EXECUTOR 2  │
# MAGIC  │ partition 1 │    │ partition 2 │
# MAGIC  └─────────────┘    └─────────────┘
# MAGIC ```

# COMMAND ----------

print("Executors:", spark.sparkContext.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Lazy transformations vs eager actions

# COMMAND ----------

# MAGIC %md
# MAGIC This is the **most important** PySpark concept.

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations (LAZY)** — build a recipe, no work yet:
# MAGIC `select`, `filter`, `withColumn`, `join`, `groupBy`, `orderBy`, `dropDuplicates`

# COMMAND ----------

# MAGIC %md
# MAGIC **Actions (EAGER)** — run the full recipe now:
# MAGIC `show`, `count`, `collect`, `take`, `write`, `foreach`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC filter  ──► select ──► withColumn     (lazy — nothing runs)
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                       count() / show()  (ACTION — cluster runs)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-by-step flow
# MAGIC
# MAGIC ```
# MAGIC filter()   →  (nothing yet)
# MAGIC  select()   →  (nothing yet)
# MAGIC  count()    →  RUN full plan
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **Example mental model:**
# MAGIC ```python
# MAGIC step1 = df.filter(...)      # lazy — instant return
# MAGIC step2 = step1.select(...)   # lazy — still no execution
# MAGIC step2.count()               # ACTION — cluster runs step1 + step2 + count
# MAGIC ```
# MAGIC
# MAGIC **Why lazy?** Catalyst optimizer can merge filters, push predicates to files, pick efficient joins — before any data moves.

# COMMAND ----------

# bronze already loaded in cell 0
step = bronze.filter("status = 'posted'")
print('Lazy steps defined — cluster has NOT read all rows yet')

# COMMAND ----------

print('Posted count (ACTION):', step.count())

# COMMAND ----------

step.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Catalyst optimizer and `explain()`

# COMMAND ----------

# MAGIC %md
# MAGIC Spark's **Catalyst** engine optimises your logical plan before execution.

# COMMAND ----------

# MAGIC %md
# MAGIC Use `.explain()` to see the plan (great for debugging):

# COMMAND ----------

# MAGIC %md
# MAGIC - **Logical plan** — what you asked for
# MAGIC - **Physical plan** — how cluster will execute (shuffles, scans)

# COMMAND ----------

# MAGIC %md
# MAGIC Narrow transforms (`filter`, `select`) stay on one partition when possible.
# MAGIC Wide transforms (`groupBy`, `join`) may **shuffle** data between executors (slower).

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC DataFrame API ──┐
# MAGIC                  ├──► Logical plan ──► Catalyst ──► Physical plan ──► Executors
# MAGIC  SQL ────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-by-step flow
# MAGIC
# MAGIC ```
# MAGIC NARROW (cheap)              WIDE (shuffle)
# MAGIC  filter, select              groupBy, join
# MAGIC  same partition              data moves across executors
# MAGIC ```

# COMMAND ----------

bronze.filter(F.col('channel') == 'wire').explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 — Ingest: read bronze from the lake

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 `abfss://` paths

# COMMAND ----------

# MAGIC %md
# MAGIC **ADLS Gen2** exposes a DFS endpoint. Spark reads via:

# COMMAND ----------

# MAGIC %md
# MAGIC `abfss://<container>@<storage-account>.dfs.core.windows.net/<path>`

# COMMAND ----------

# MAGIC %md
# MAGIC | Part | Our value |
# MAGIC |------|-----------|
# MAGIC | Container | `bronze` |
# MAGIC | Account | from secret `storage-account` |
# MAGIC | Path | `loaded/run=session3-lab/sample_transactions.csv` |

# COMMAND ----------

# MAGIC %md
# MAGIC The `run=` folder is a **partition** convention — multiple pipeline runs land in separate folders.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC Notebook ──► secret scope ──► spark.read.csv ──► ADLS bronze ──► DataFrame
# MAGIC ```

# COMMAND ----------

print(BRONZE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 `spark.read` options

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC df = spark.read \
# MAGIC     .option("header", True)      # first row = column names \
# MAGIC     .option("inferSchema", True) # guess types (bronze CSV still has string amounts) \
# MAGIC     .csv(path)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC | Option | Why |
# MAGIC |--------|-----|
# MAGIC | `header=True` | CSV has column names in row 1 |
# MAGIC | `inferSchema=True` | Quick exploration; production often uses explicit schema |

# COMMAND ----------

# MAGIC %md
# MAGIC After read, always run `printSchema()` and `show()` before transforms.

# COMMAND ----------

df = bronze

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print('Row count:', df.count())

# COMMAND ----------

df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Bronze schema reality

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze CSV stores **everything as strings** (or inferred types that may be wrong).

# COMMAND ----------

# MAGIC %md
# MAGIC FinLedger columns:
# MAGIC - `transaction_id` — business key
# MAGIC - `amount_gbp` — **string** in bronze (`"1250.50"`)
# MAGIC - `channel` — wire | card | fps
# MAGIC - `status` — posted | pending

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver rule:** cast `amount_gbp` → `double` as column `amount`, then filter invalid values.

# COMMAND ----------

df.select('transaction_id', 'channel', 'amount_gbp', 'status').show()

# COMMAND ----------

df.groupBy('status').count().show()

# COMMAND ----------

df.groupBy('channel').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 — Core PySpark objects

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC `spark` is the entry point. On Databricks it exists automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC Local laptops use `SparkSession.builder...getOrCreate()` — **not needed here**.

# COMMAND ----------

print('Spark version:', spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC A **distributed table** with named columns — like SQL table or pandas DataFrame, but lazy and cluster-scale.

# COMMAND ----------

print('Columns:', df.columns)
print('Type:', type(df))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Column expressions (`F`)

# COMMAND ----------

# MAGIC %md
# MAGIC Import once: `from pyspark.sql import functions as F`

# COMMAND ----------

# MAGIC %md
# MAGIC Use `F.col('name')` in `select`, `filter`, `withColumn`, `agg` — never compare columns with bare Python `==` on rows.

# COMMAND ----------

df.select(F.col('transaction_id'), F.upper(F.col('channel')).alias('channel_upper')).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Schema

# COMMAND ----------

# MAGIC %md
# MAGIC `printSchema()` shows `struct` tree with types. After cast, verify `amount: double`.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Row vs DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC One **Row** = one record. Avoid `.collect()` on large data — it pulls all rows to the driver.

# COMMAND ----------

sample = df.limit(3).collect()
print('Rows on driver:', len(sample))
print(sample[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5 — `select` and `filter` (narrow transforms)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Narrow transforms stay on the same partition
# MAGIC
# MAGIC ```
# MAGIC NARROW (cheap)              WIDE (shuffle)
# MAGIC  filter, select              groupBy, join
# MAGIC  same partition              data moves across executors
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 `select` — projection

# COMMAND ----------

# MAGIC %md
# MAGIC **Purpose:** keep only columns you need (reduces IO and memory).

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC df.select('transaction_id', 'channel', 'amount_gbp', 'status')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Alias with `F.col(...).alias('new_name')`.

# COMMAND ----------

slim = df.select('transaction_id', 'channel', 'amount_gbp', 'status')
slim.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 `filter` / `where` — SQL string

# COMMAND ----------

# MAGIC %md
# MAGIC String SQL inside `filter()` — readable for simple rules:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC df.filter("status = 'posted'")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC `where` is an alias for `filter`.

# COMMAND ----------

posted = slim.filter("status = 'posted'")
print('Posted rows:', posted.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 `filter` — Column API (production style)

# COMMAND ----------

# MAGIC %md
# MAGIC Prefer `F.col` for composable conditions:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC df.filter(F.col('status') == 'posted')
# MAGIC ```

# COMMAND ----------

posted_f = slim.filter(F.col('status') == 'posted')
posted_f.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Combine conditions — `&` and `|`

# COMMAND ----------

# MAGIC %md
# MAGIC Use `&` (and), `|` (or). **Wrap each condition in parentheses.**

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC df.filter((F.col('channel')=='wire') & (F.col('status')=='pending'))
# MAGIC ```

# COMMAND ----------

pending_wire = slim.filter((F.col('channel') == 'wire') & (F.col('status') == 'pending'))
pending_wire.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 `isin`, `like`, `isNull`

# COMMAND ----------

# MAGIC %md
# MAGIC | Method | Example |
# MAGIC |--------|--------|
# MAGIC | `isin` | `F.col('channel').isin(['wire','card'])` |
# MAGIC | `like` | `F.col('transaction_id').like('TXN-100%')` |
# MAGIC | `isNull` | `F.col('amount_gbp').isNull()` |

# COMMAND ----------

slim.filter(F.col('channel').isin(['wire', 'card'])).show()

# COMMAND ----------

slim.filter(F.col('transaction_id').like('TXN-100%')).show()

# COMMAND ----------

slim.filter(F.col('status') != 'posted').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.6 Chain transforms — one action at end

# COMMAND ----------

# MAGIC %md
# MAGIC Best practice: chain lazy steps, call **one** action to execute.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC result = (df
# MAGIC   .filter(...)
# MAGIC   .select(...)
# MAGIC   .withColumn(...))
# MAGIC result.show()  # single action
# MAGIC ```

# COMMAND ----------

chain = (slim
    .filter(F.col('status') == 'posted')
    .select('transaction_id', 'channel', 'amount_gbp'))
chain.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6 — `withColumn`, `cast`, data quality (bronze → silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Why cast on bronze?

# COMMAND ----------

# MAGIC %md
# MAGIC CSV stores numbers as text. Maths (`>`, `sum`) needs numeric types.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC typed = df.withColumn('amount', F.col('amount_gbp').cast('double'))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Invalid text (e.g. `'INVALID'`) becomes **`null`** after cast.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC bronze STRING ──► cast double ──► good / quarantine
# MAGIC ```

# COMMAND ----------

typed = df.withColumn('amount', F.col('amount_gbp').cast('double'))
typed.printSchema()

# COMMAND ----------

typed.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Silver good vs quarantine

# COMMAND ----------

# MAGIC %md
# MAGIC **FinLedger pattern:** never fail the pipeline on bad rows — **split** them.

# COMMAND ----------

# MAGIC %md
# MAGIC | Dataset | Rule |
# MAGIC |---------|------|
# MAGIC | `silver_good` | `amount` not null AND `amount > 0` |
# MAGIC | `silver_bad` | null OR `<= 0` → quarantine path (Session 9) |

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC bronze STRING ──► cast double ──► good / quarantine
# MAGIC ```

# COMMAND ----------

silver_good = typed.filter(F.col('amount').isNotNull() & (F.col('amount') > 0))
silver_bad = typed.filter(F.col('amount').isNull() | (F.col('amount') <= 0))
print('good:', silver_good.count(), '| quarantine:', silver_bad.count())

# COMMAND ----------

silver_bad.show()

# COMMAND ----------

silver_good.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 `when` / `otherwise` — conditional column

# COMMAND ----------

# MAGIC %md
# MAGIC SQL `CASE WHEN` as column expression:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC F.when(F.col('amount') >= 10000, 'WATCH').otherwise('OK')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Use for fraud flags, bucketing, status labels.

# COMMAND ----------

flagged = silver_good.withColumn(
    'fraud_flag',
    F.when(F.col('amount') >= 10000, 'WATCH').otherwise('OK'),
)
flagged.select('transaction_id', 'amount', 'fraud_flag').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 `lit` and `coalesce`

# COMMAND ----------

# MAGIC %md
# MAGIC **`F.lit(value)`** — inject constant column (e.g. `source_system='finledger'`).

# COMMAND ----------

# MAGIC %md
# MAGIC **`F.coalesce(a, b)`** — first non-null value (defaults for missing data).

# COMMAND ----------

tagged = df.withColumn('source_system', F.lit('finledger'))
tagged.select('transaction_id', 'source_system').show(3)

# COMMAND ----------

df.withColumn('amt', F.coalesce(F.col('amount_gbp'), F.lit('0'))).show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 `dropDuplicates`

# COMMAND ----------

# MAGIC %md
# MAGIC Keep one row per business key before silver write:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC df.dropDuplicates(['transaction_id'])
# MAGIC ```

# COMMAND ----------

print('before:', df.count())
deduped = df.dropDuplicates(['transaction_id'])
print('after:', deduped.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7 — `groupBy` and aggregates (gold layer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Wide transform — shuffle

# COMMAND ----------

# MAGIC %md
# MAGIC `groupBy` **collapses** rows into buckets — often requires **shuffle** (data moves between executors). Slower than `filter` but essential for reports.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC rows ──► shuffle by channel ──► sum/count per channel
# MAGIC ```

# COMMAND ----------

clean = typed.filter(F.col('amount') > 0)
by_ch = clean.groupBy('channel').agg(F.count('*').alias('txns'), F.sum('amount').alias('total_gbp'))
by_ch.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Common aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC | Function | Purpose |
# MAGIC |----------|--------|
# MAGIC | `F.count('*')` | Row count per group |
# MAGIC | `F.sum('amount')` | Total GBP |
# MAGIC | `F.avg('amount')` | Average |
# MAGIC | `F.min` / `F.max` | Range |

# COMMAND ----------

stats = clean.groupBy('channel').agg(
    F.avg('amount').alias('avg_gbp'),
    F.min('amount').alias('min_gbp'),
    F.max('amount').alias('max_gbp'),
)
stats.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 `orderBy` after aggregate

# COMMAND ----------

# MAGIC %md
# MAGIC Gold reports are usually sorted for humans:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC by_ch.orderBy(F.desc('total_gbp'))
# MAGIC ```

# COMMAND ----------

by_ch.orderBy(F.desc('total_gbp')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Gold preview

# COMMAND ----------

# MAGIC %md
# MAGIC This shape is what lands in `abfss://gold@.../daily_channel_summary` in later sessions.

# COMMAND ----------

display(by_ch)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8 — `join` (enrich silver)

# COMMAND ----------

lookup = spark.createDataFrame(
    [('wire', 'high'), ('card', 'medium'), ('fps', 'low')],
    ['channel', 'risk'],
)
lookup.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Join keys

# COMMAND ----------

# MAGIC %md
# MAGIC Join on matching column(s): `df.join(other, on='channel', how='left')`

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare **silver** first (typed, deduped), then join small **reference** tables.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC silver + lookup ──► join on channel ──► enriched rows
# MAGIC ```

# COMMAND ----------

silver = silver_good.dropDuplicates(['transaction_id'])
enriched = silver.join(lookup, on='channel', how='left')
enriched.select('transaction_id', 'channel', 'amount', 'risk').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Join types

# COMMAND ----------

# MAGIC %md
# MAGIC | how= | Keeps |
# MAGIC |------|-------|
# MAGIC | `inner` | Only rows with match on **both** sides |
# MAGIC | `left` | All left rows + matching right (null if no match) |
# MAGIC | `right` | All right + matching left |
# MAGIC | `full` | Everything from both |

# COMMAND ----------

silver.join(lookup, 'channel', 'inner').count()

# COMMAND ----------

silver.join(lookup, 'channel', 'left').show()

# COMMAND ----------

silver.join(lookup, 'channel', 'full').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9 — Window functions (rank without collapsing)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 `groupBy` vs Window

# COMMAND ----------

# MAGIC %md
# MAGIC **groupBy** collapses to one row per key.

# COMMAND ----------

# MAGIC %md
# MAGIC **Window** keeps **all rows** and adds calculated columns per partition (e.g. rank, running total).

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC groupBy = 1 row per key  |  Window = keep all rows + rank column
# MAGIC ```

# COMMAND ----------

print('groupBy collapses; Window keeps every row')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 Window specification

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC w = Window.partitionBy('channel').orderBy(F.desc('amount'))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC - `partitionBy` — groups for the window (like GROUP BY keys)
# MAGIC - `orderBy` — sort within each partition

# COMMAND ----------

w = Window.partitionBy('channel').orderBy(F.desc('amount'))
ranked = clean.withColumn('rank_in_channel', F.row_number().over(w))
ranked.select('channel', 'transaction_id', 'amount', 'rank_in_channel').show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 `row_number` vs `rank`

# COMMAND ----------

# MAGIC %md
# MAGIC | Function | Ties |
# MAGIC |----------|------|
# MAGIC | `row_number()` | Unique 1,2,3… |
# MAGIC | `rank()` | Same rank for ties (1,1,3) |
# MAGIC | `dense_rank()` | No gaps (1,1,2) |

# COMMAND ----------

top1 = ranked.filter(F.col('rank_in_channel') == 1)
top1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.4 Running sum

# COMMAND ----------

# MAGIC %md
# MAGIC Unbounded window — cumulative total ordered by amount within channel.

# COMMAND ----------

w2 = Window.partitionBy('channel').orderBy('amount').rowsBetween(Window.unboundedPreceding, Window.currentRow)
running = clean.withColumn('running_total', F.sum('amount').over(w2))
running.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10 — SQL API (same Catalyst engine)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 Temp views

# COMMAND ----------

# MAGIC %md
# MAGIC Register DataFrame as SQL table for this session:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC df.createOrReplaceTempView('bronze_txn')
# MAGIC ```

# COMMAND ----------

df.createOrReplaceTempView('bronze_txn')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 SQL queries

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame API and SQL compile to the **same** optimised plan — use whichever your team prefers.

# COMMAND ----------

spark.sql('SELECT channel, COUNT(*) AS n FROM bronze_txn GROUP BY channel').show()

# COMMAND ----------

spark.sql("SELECT * FROM bronze_txn WHERE status = 'posted'").show()

# COMMAND ----------

spark.sql('SELECT channel, SUM(CAST(amount_gbp AS DOUBLE)) AS total FROM bronze_txn GROUP BY channel').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 11 — End-to-end bronze → silver → gold

# COMMAND ----------

# MAGIC %md
# MAGIC #### Full FinLedger pipeline flow
# MAGIC
# MAGIC ```
# MAGIC bronze ──► cast ──► quarantine ──► dedupe ──► join ──► gold
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 Step 1 — Read bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest raw CSV from lake.

# COMMAND ----------

b = bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 Step 2 — Cast amount

# COMMAND ----------

# MAGIC %md
# MAGIC Silver typing.

# COMMAND ----------

s = b.withColumn('amount', F.col('amount_gbp').cast('double'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.3 Step 3 — Split quality

# COMMAND ----------

# MAGIC %md
# MAGIC Good vs quarantine.

# COMMAND ----------

g = s.filter(F.col('amount').isNotNull() & (F.col('amount') > 0))
q = s.filter(F.col('amount').isNull() | (F.col('amount') <= 0))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.4 Step 4 — Counts

# COMMAND ----------

# MAGIC %md
# MAGIC Validate split.

# COMMAND ----------

print('good:', g.count(), 'quarantine:', q.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.5 Step 5 — Dedupe

# COMMAND ----------

# MAGIC %md
# MAGIC One row per txn.

# COMMAND ----------

g2 = g.dropDuplicates(['transaction_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.6 Step 6 — Enrich

# COMMAND ----------

# MAGIC %md
# MAGIC Left join risk lookup.

# COMMAND ----------

g3 = g2.join(lookup, 'channel', 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.7 Step 7 — Gold aggregate

# COMMAND ----------

# MAGIC %md
# MAGIC Business metric.

# COMMAND ----------

gold = g3.groupBy('channel').agg(F.sum('amount').alias('total_gbp'), F.count('*').alias('txns'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.8 Step 8 — Present gold

# COMMAND ----------

# MAGIC %md
# MAGIC Sorted report.

# COMMAND ----------

gold.orderBy(F.desc('total_gbp')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.9 Write preview (Session 9+)
# MAGIC
# MAGIC Production pipelines **write** silver and gold to the lake (actions):
# MAGIC
# MAGIC ```python
# MAGIC # silver_good.write.mode('overwrite').parquet(silver_path)
# MAGIC # gold.write.mode('overwrite').format('delta').save(gold_path)
# MAGIC ```
# MAGIC
# MAGIC `mode`: `append` | `overwrite` | `error` | `ignore`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 12 — Performance tips and common mistakes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.1 `cache` / `persist`

# COMMAND ----------

# MAGIC %md
# MAGIC If you reuse a DataFrame in **many actions**, cache after first quality filter:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC good = ...filter...
# MAGIC good.cache()
# MAGIC good.count()  # materialises cache
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Call `unpersist()` when done.

# COMMAND ----------

cached = clean.cache()
print(cached.count())
print(cached.count())
cached.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.2 Avoid `collect()` on big data

# COMMAND ----------

# MAGIC %md
# MAGIC `.collect()` returns **all** rows to the driver — OK for `limit(10)` only.

# COMMAND ----------

small = clean.limit(5).collect()
print(len(small), 'rows on driver')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.3 Mistakes cheat sheet

# COMMAND ----------

# MAGIC %md
# MAGIC | Mistake | Fix |
# MAGIC |---------|-----|
# MAGIC | `collect()` on millions of rows | `.show()` or write to lake |
# MAGIC | Compare columns with bare `==` in Python | `F.col('a') == F.col('b')` |
# MAGIC | Skip cast on bronze | `.cast('double')` then filter nulls |
# MAGIC | Many actions in a loop | One pipeline, one write |
# MAGIC | Hardcode storage keys | Secret scope `finledger` |

# COMMAND ----------

print('Review checklist above before Session 9')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 13 — Concept drills (theory + one-line example each)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC **SparkSession:** Entry point — exists as `spark` on Databricks.

# COMMAND ----------

print(spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC **DataFrame:** Distributed table with named columns.

# COMMAND ----------

print(type(df))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformation:** Lazy: select, filter, withColumn.

# COMMAND ----------

x = df.select('transaction_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — Action

# COMMAND ----------

# MAGIC %md
# MAGIC **Action:** Eager: show, count, write.

# COMMAND ----------

print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — F.col

# COMMAND ----------

# MAGIC %md
# MAGIC **F.col:** Reference a column in expressions.

# COMMAND ----------

df.select(F.col('channel')).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — cast

# COMMAND ----------

# MAGIC %md
# MAGIC **cast:** Change column type.

# COMMAND ----------

df.select(F.col('amount_gbp').cast('double')).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — filter

# COMMAND ----------

# MAGIC %md
# MAGIC **filter:** Keep matching rows.

# COMMAND ----------

df.filter(F.col('status')=='posted').count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — select

# COMMAND ----------

# MAGIC %md
# MAGIC **select:** Choose columns.

# COMMAND ----------

df.select('transaction_id','channel').show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC **withColumn:** Add or replace column.

# COMMAND ----------

df.withColumn('x', F.lit(1)).select('x').show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — groupBy

# COMMAND ----------

# MAGIC %md
# MAGIC **groupBy:** Bucket rows by key.

# COMMAND ----------

df.groupBy('channel').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — agg

# COMMAND ----------

# MAGIC %md
# MAGIC **agg:** Summarise buckets.

# COMMAND ----------

df.groupBy('channel').agg(F.count('*')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — orderBy

# COMMAND ----------

# MAGIC %md
# MAGIC **orderBy:** Sort output.

# COMMAND ----------

df.orderBy('channel').show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — join

# COMMAND ----------

# MAGIC %md
# MAGIC **join:** Combine two DataFrames.

# COMMAND ----------

df.join(lookup,'channel','left').show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — dropDuplicates

# COMMAND ----------

# MAGIC %md
# MAGIC **dropDuplicates:** Dedupe by key.

# COMMAND ----------

df.dropDuplicates(['transaction_id']).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — Window

# COMMAND ----------

# MAGIC %md
# MAGIC **Window:** Per-partition calc without collapse.

# COMMAND ----------

w = Window.partitionBy('channel')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — row_number

# COMMAND ----------

# MAGIC %md
# MAGIC **row_number:** Rank within window.

# COMMAND ----------

df.withColumn('r', F.row_number().over(Window.partitionBy('channel').orderBy('amount_gbp'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — explain

# COMMAND ----------

# MAGIC %md
# MAGIC **explain:** Show logical plan.

# COMMAND ----------

df.filter(F.col('status')=='posted').explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — createOrReplaceTempView

# COMMAND ----------

# MAGIC %md
# MAGIC **createOrReplaceTempView:** Register for SQL.

# COMMAND ----------

df.createOrReplaceTempView('t')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — spark.sql

# COMMAND ----------

# MAGIC %md
# MAGIC **spark.sql:** Run SQL on temp views.

# COMMAND ----------

spark.sql('select count(*) from t').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — lit

# COMMAND ----------

# MAGIC %md
# MAGIC **lit:** Constant column.

# COMMAND ----------

df.select(F.lit('finledger').alias('src')).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — when/otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC **when/otherwise:** Conditional column.

# COMMAND ----------

df.withColumn('f', F.when(F.col('status')=='posted','Y').otherwise('N')).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — coalesce

# COMMAND ----------

# MAGIC %md
# MAGIC **coalesce:** First non-null.

# COMMAND ----------

df.select(F.coalesce(F.col('amount_gbp'),F.lit('0'))).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — isNull

# COMMAND ----------

# MAGIC %md
# MAGIC **isNull:** Find nulls.

# COMMAND ----------

df.filter(F.col('amount_gbp').isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — isin

# COMMAND ----------

# MAGIC %md
# MAGIC **isin:** Value in list.

# COMMAND ----------

df.filter(F.col('channel').isin(['wire','card'])).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — like

# COMMAND ----------

# MAGIC %md
# MAGIC **like:** Pattern match.

# COMMAND ----------

df.filter(F.col('transaction_id').like('TXN%')).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — abfss

# COMMAND ----------

# MAGIC %md
# MAGIC **abfss:** ADLS path scheme.

# COMMAND ----------

print(BRONZE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — bronze

# COMMAND ----------

# MAGIC %md
# MAGIC **bronze:** Raw layer.

# COMMAND ----------

print('bronze = raw CSV')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — silver

# COMMAND ----------

# MAGIC %md
# MAGIC **silver:** Cleansed layer.

# COMMAND ----------

print('silver = typed + deduped')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — gold

# COMMAND ----------

# MAGIC %md
# MAGIC **gold:** Aggregate layer.

# COMMAND ----------

print('gold = channel totals')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — quarantine

# COMMAND ----------

# MAGIC %md
# MAGIC **quarantine:** Bad rows isolated.

# COMMAND ----------

print('quarantine = invalid amounts')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — filter channel `wire`

# COMMAND ----------

# MAGIC %md
# MAGIC Practice filtering a single payment channel. Count how many bronze rows have `channel = 'wire'`.

# COMMAND ----------

print('wire rows:', df.filter(F.col('channel') == 'wire').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — filter channel `card`

# COMMAND ----------

# MAGIC %md
# MAGIC Practice filtering a single payment channel. Count how many bronze rows have `channel = 'card'`.

# COMMAND ----------

print('card rows:', df.filter(F.col('channel') == 'card').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — filter channel `fps`

# COMMAND ----------

# MAGIC %md
# MAGIC Practice filtering a single payment channel. Count how many bronze rows have `channel = 'fps'`.

# COMMAND ----------

print('fps rows:', df.filter(F.col('channel') == 'fps').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — filter channel `chaps`

# COMMAND ----------

# MAGIC %md
# MAGIC Practice filtering a single payment channel. Count how many bronze rows have `channel = 'chaps'`.

# COMMAND ----------

print('chaps rows:', df.filter(F.col('channel') == 'chaps').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — filter status `posted`

# COMMAND ----------

# MAGIC %md
# MAGIC Status drives settlement logic. Filter `status = 'posted'` and show sample rows.

# COMMAND ----------

df.filter(F.col('status') == 'posted').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill — filter status `pending`

# COMMAND ----------

# MAGIC %md
# MAGIC Status drives settlement logic. Filter `status = 'pending'` and show sample rows.

# COMMAND ----------

df.filter(F.col('status') == 'pending').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 14 — Data engineering theory (Big Book alignment)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.1 Ingest — batch from object storage

# COMMAND ----------

# MAGIC %md
# MAGIC Batch ingestion lands files in **bronze** unchanged. Cloud object storage (ADLS) scales cheaply.

# COMMAND ----------

# MAGIC %md
# MAGIC Our ingest path: ADF or manual upload → `bronze/loaded/run=<id>/` → Spark `read.csv`.

# COMMAND ----------

print('Ingest complete when df.count() > 0')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.2 Transform — data quality

# COMMAND ----------

# MAGIC %md
# MAGIC High quality data is essential ('garbage in, garbage out'). Engineers validate types, ranges, and keys.

# COMMAND ----------

# MAGIC %md
# MAGIC FinLedger: cast amounts, quarantine invalid, dedupe by `transaction_id`.

# COMMAND ----------

print('Quality: good', silver_good.count() if 'silver_good' in dir() else 'run Part 6 first')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.3 Transform — medallion flow

# COMMAND ----------

# MAGIC %md
# MAGIC Each hop adds quality and structure. Never skip bronze — keep raw for audit.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC Sources ──► BRONZE ──► SILVER ──► GOLD
# MAGIC                   └──► QUARANTINE
# MAGIC ```

# COMMAND ----------

print('bronze:', BRONZE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.4 Orchestrate — what comes next

# COMMAND ----------

# MAGIC %md
# MAGIC Orchestration schedules pipelines, handles retries, and monitors failures.

# COMMAND ----------

# MAGIC %md
# MAGIC Session 4: **Azure Data Factory** triggers Databricks notebooks on a schedule.

# COMMAND ----------

print('Today: interactive notebook. Later: ADF pipeline activity.')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.5 Lakehouse and Delta Lake (preview)

# COMMAND ----------

# MAGIC %md
# MAGIC **Delta Lake** adds ACID transactions, time travel, and schema enforcement on top of parquet files.

# COMMAND ----------

# MAGIC %md
# MAGIC Session 9+: `df.write.format('delta').save(silver_path)`

# COMMAND ----------

print('Delta format = silver/gold default in production')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.6 Unity Catalog (preview)

# COMMAND ----------

# MAGIC %md
# MAGIC **Unity Catalog** centralises permissions, auditing, and lineage.

# COMMAND ----------

# MAGIC %md
# MAGIC Tables appear as `catalog.schema.table` — we use paths first, then UC in Session 3+.

# COMMAND ----------

print('UC: finledger.bronze.sample_transactions (future)')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.7 Structured Streaming (preview)

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming ingests real-time events. Same DataFrame API with `readStream` / `writeStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC Batch CSV (today) → Kafka/Event Hubs (advanced).

# COMMAND ----------

print('Batch = read(). Streaming = readStream()')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.8 Workflows (preview)

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks **Workflows** chains notebook tasks with dependencies and alerting.

# COMMAND ----------

# MAGIC %md
# MAGIC Replaces ad-hoc 'Run all' in production.

# COMMAND ----------

print('Workflow = multi-step job with retries')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 15 — Extended examples

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Aggregate `count`

# COMMAND ----------

# MAGIC %md
# MAGIC Use `F.count('*').alias('txns')` inside `groupBy('channel').agg(...)` to summarise per channel.

# COMMAND ----------

clean.groupBy('channel').agg(F.count('*').alias('txns')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Aggregate `sum`

# COMMAND ----------

# MAGIC %md
# MAGIC Use `F.sum('amount').alias('total_gbp')` inside `groupBy('channel').agg(...)` to summarise per channel.

# COMMAND ----------

clean.groupBy('channel').agg(F.sum('amount').alias('total_gbp')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Aggregate `avg`

# COMMAND ----------

# MAGIC %md
# MAGIC Use `F.avg('amount').alias('avg_gbp')` inside `groupBy('channel').agg(...)` to summarise per channel.

# COMMAND ----------

clean.groupBy('channel').agg(F.avg('amount').alias('avg_gbp')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Aggregate `min`

# COMMAND ----------

# MAGIC %md
# MAGIC Use `F.min('amount').alias('min_gbp')` inside `groupBy('channel').agg(...)` to summarise per channel.

# COMMAND ----------

clean.groupBy('channel').agg(F.min('amount').alias('min_gbp')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Aggregate `max`

# COMMAND ----------

# MAGIC %md
# MAGIC Use `F.max('amount').alias('max_gbp')` inside `groupBy('channel').agg(...)` to summarise per channel.

# COMMAND ----------

clean.groupBy('channel').agg(F.max('amount').alias('max_gbp')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Join `inner`

# COMMAND ----------

# MAGIC %md
# MAGIC Demonstrate `inner` join between silver and channel risk lookup.

# COMMAND ----------

silver.join(lookup, 'channel', 'inner').select('transaction_id', 'channel', 'risk').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Join `left`

# COMMAND ----------

# MAGIC %md
# MAGIC Demonstrate `left` join between silver and channel risk lookup.

# COMMAND ----------

silver.join(lookup, 'channel', 'left').select('transaction_id', 'channel', 'risk').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Join `right`

# COMMAND ----------

# MAGIC %md
# MAGIC Demonstrate `right` join between silver and channel risk lookup.

# COMMAND ----------

silver.join(lookup, 'channel', 'right').select('transaction_id', 'channel', 'risk').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Join `full`

# COMMAND ----------

# MAGIC %md
# MAGIC Demonstrate `full` join between silver and channel risk lookup.

# COMMAND ----------

silver.join(lookup, 'channel', 'full').select('transaction_id', 'channel', 'risk').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Pipeline replay step 1

# COMMAND ----------

# MAGIC %md
# MAGIC Re-run gold aggregate (step 1/3) to reinforce bronze→gold muscle memory.

# COMMAND ----------

print('pipeline step', 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Pipeline replay step 2

# COMMAND ----------

# MAGIC %md
# MAGIC Re-run gold aggregate (step 2/3) to reinforce bronze→gold muscle memory.

# COMMAND ----------

print('pipeline step', 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.x Pipeline replay step 3

# COMMAND ----------

# MAGIC %md
# MAGIC Re-run gold aggregate (step 3/3) to reinforce bronze→gold muscle memory.

# COMMAND ----------

gold.orderBy(F.desc('total_gbp')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 16 — FinLedger row-by-row exploration

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16.x Transaction `TXN-10001`

# COMMAND ----------

# MAGIC %md
# MAGIC Filter to one business key. Inspect amount, channel, and status for `TXN-10001` — practice `filter` + `show`.

# COMMAND ----------

df.filter(F.col('transaction_id') == 'TXN-10001').show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16.x Transaction `TXN-10002`

# COMMAND ----------

# MAGIC %md
# MAGIC Filter to one business key. Inspect amount, channel, and status for `TXN-10002` — practice `filter` + `show`.

# COMMAND ----------

df.filter(F.col('transaction_id') == 'TXN-10002').show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16.x Transaction `TXN-10003`

# COMMAND ----------

# MAGIC %md
# MAGIC Filter to one business key. Inspect amount, channel, and status for `TXN-10003` — practice `filter` + `show`.

# COMMAND ----------

df.filter(F.col('transaction_id') == 'TXN-10003').show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16.x Transaction `TXN-10004`

# COMMAND ----------

# MAGIC %md
# MAGIC Filter to one business key. Inspect amount, channel, and status for `TXN-10004` — practice `filter` + `show`.

# COMMAND ----------

df.filter(F.col('transaction_id') == 'TXN-10004').show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why lazy?
# MAGIC
# MAGIC Spark builds an optimised plan before moving data — multiple filters merge into one scan.

# COMMAND ----------

print("FAQ: Why lazy?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why not pandas?
# MAGIC
# MAGIC pandas runs on one machine RAM. PySpark scales to TB on a cluster.

# COMMAND ----------

print("FAQ: Why not pandas?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why cast?
# MAGIC
# MAGIC Bronze CSV stores numbers as text — maths needs `double` or `decimal`.

# COMMAND ----------

print("FAQ: Why cast?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why quarantine?
# MAGIC
# MAGIC Bad rows must not block the pipeline — isolate and alert separately.

# COMMAND ----------

print("FAQ: Why quarantine?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why dedupe?
# MAGIC
# MAGIC Sources replay files; silver keeps one row per `transaction_id`.

# COMMAND ----------

print("FAQ: Why dedupe?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why left join?
# MAGIC
# MAGIC Keep all transactions even if lookup has no matching channel.

# COMMAND ----------

print("FAQ: Why left join?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why window?
# MAGIC
# MAGIC Rank or running totals without collapsing to one row per group.

# COMMAND ----------

print("FAQ: Why window?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why secrets?
# MAGIC
# MAGIC Keys in code leak via git and logs — scopes encrypt at rest.

# COMMAND ----------

print("FAQ: Why secrets?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why Single-user cluster?
# MAGIC
# MAGIC Storage key auth works per user; Shared clusters may block it.

# COMMAND ----------

print("FAQ: Why Single-user cluster?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FAQ — Why abfss?
# MAGIC
# MAGIC ADLS Gen2 hierarchical paths for medallion containers.

# COMMAND ----------

print("FAQ: Why abfss?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 17 — Quick recap cards

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `select`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`select`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: select')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `filter`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`filter`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: filter')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `withColumn`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`withColumn`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: withColumn')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `cast`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`cast`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: cast')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `groupBy`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`groupBy`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: groupBy')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `agg`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`agg`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: agg')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `orderBy`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`orderBy`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: orderBy')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `join`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`join`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: join')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `dropDuplicates`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`dropDuplicates`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: dropDuplicates')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `window`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`window`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: window')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `explain`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`explain`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: explain')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recap — `display`

# COMMAND ----------

# MAGIC %md
# MAGIC Before Session 9, confirm you can explain **`display`** in one sentence and write one example from memory.

# COMMAND ----------

print('I can explain: display')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion checklist
# MAGIC
# MAGIC - [ ] Secret scope `finledger` loaded storage auth (cell 0)
# MAGIC - [ ] I read bronze from `abfss://` and inspected schema
# MAGIC - [ ] I can explain **lazy** vs **action** with an example
# MAGIC - [ ] I cast `amount_gbp` → `amount` and split good vs quarantine
# MAGIC - [ ] I built a **gold** aggregate by `channel`
# MAGIC - [ ] I used `join` and `row_number` window
# MAGIC - [ ] Next: **Session 9** — write silver Delta + quarantine to the lake

# COMMAND ----------
