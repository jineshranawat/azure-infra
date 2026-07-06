#!/usr/bin/env python3
"""Generate nb_master_pyspark_complete.py — PySpark theory + practice (300+ cells).

No Day 7 Python/pandas/Polars — PySpark + medallion lake only.
Deploy: python scripts/deploy_shared_lab.py
"""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bronze_auth_cell import BRONZE_AUTH_CELL
from box_diagrams import (
    ASCII_LAZY,
    ASCII_MEDALLION,
    ASCII_NARROW_WIDE,
    BRONZE_READ_FLOW,
    CAST_FLOW,
    CATALYST,
    DRIVER_EXECUTOR,
    GROUPBY_FLOW,
    INGEST_TRANSFORM_ORCHESTRATE,
    JOIN_FLOW,
    LAZY_ACTION,
    MEDALLION,
    PARTITIONS,
    PIPELINE_E2E,
    SHUFFLE,
    SPARK_OVERVIEW,
    WINDOW_FLOW,
)

OUT = Path(__file__).resolve().parent.parent / "notebooks" / "nb_master_pyspark_complete.py"


def _emit(parts: list[str], cell_type: str, content: str) -> None:
    if cell_type == "md":
        parts.append("# MAGIC %md")
        for line in content.strip().splitlines():
            parts.append(f"# MAGIC {line}" if line else "# MAGIC")
    else:
        parts.extend(content.strip().splitlines())
    parts.append("")
    parts.append("# COMMAND ----------")
    parts.append("")


def md(parts: list[str], text: str) -> None:
    _emit(parts, "md", text)


def code(parts: list[str], text: str) -> None:
    _emit(parts, "code", text)


def box_diagram(parts: list[str], diagram: str, caption: str = "Diagram") -> None:
    """ASCII box diagram — no Mermaid."""
    md(parts, f"#### {caption}\n\n```\n{diagram.strip()}\n```")


def ascii_flow(parts: list[str], diagram: str, caption: str = "Step-by-step flow") -> None:
    box_diagram(parts, diagram, caption)


def theory_then_example(
    parts: list[str],
    title: str,
    theory: str,
    example_code: str,
    *,
    mermaid_diagram: str | None = None,
    ascii_diagram: str | None = None,
    extra_md: list[str] | None = None,
    extra_code: list[str] | None = None,
) -> None:
    """Small cells: title → theory paragraphs → diagram → code."""
    md(parts, f"### {title}")
    for para in theory.strip().split("\n\n"):
        block = para.strip()
        if block:
            md(parts, block)
    if mermaid_diagram:
        box_diagram(parts, mermaid_diagram)
    if ascii_diagram:
        ascii_flow(parts, ascii_diagram)
    if extra_md:
        for block in extra_md:
            md(parts, block)
    code(parts, example_code)
    if extra_code:
        for c in extra_code:
            code(parts, c)


def concept_lesson(
    parts: list[str],
    title: str,
    what: str,
    why: str,
    how: str,
    analogy: str,
    example_code: str,
    *,
    mermaid_diagram: str | None = None,
    ascii_diagram: str | None = None,
    extra_code: list[str] | None = None,
) -> None:
    """One concept = many **small** cells: WHAT → WHY → diagram → HOW → analogy → code."""
    md(parts, f"### {title}")
    md(parts, f"**WHAT (definition)**\n\n{what}")
    md(parts, f"**WHY (reason it exists)**\n\n{why}")
    if mermaid_diagram:
        box_diagram(parts, mermaid_diagram)
    if ascii_diagram:
        ascii_flow(parts, ascii_diagram)
    md(parts, f"**HOW (what Spark does on the cluster)**\n\n{how}")
    md(parts, f"**ANALOGY (picture it)**\n\n{analogy}")
    code(parts, example_code)
    if extra_code:
        for c in extra_code:
            code(parts, c)


def what_why_how(
    parts: list[str],
    title: str,
    what: str,
    why: str,
    how: str,
    analogy: str,
    example_code: str,
    *,
    mermaid_diagram: str | None = None,
    ascii_diagram: str | None = None,
    extra_code: list[str] | None = None,
) -> None:
    """Alias for concept_lesson — small cells + optional diagrams."""
    concept_lesson(
        parts,
        title,
        what,
        why,
        how,
        analogy,
        example_code,
        mermaid_diagram=mermaid_diagram,
        ascii_diagram=ascii_diagram,
        extra_code=extra_code,
    )


def _build_spark_thinking_content(parts: list[str]) -> None:
    what_why_how(
        parts,
        "0.1 — What is Apache Spark?",
        "Spark is a **distributed compute engine** for processing large tables (DataFrames) across many machines. You write declarative steps (`filter`, `groupBy`); Spark figures out how to run them in parallel.",
        "A single laptop cannot hold or process terabytes of bank transactions. Banks need **scale** and **fault tolerance** — if one machine fails, the job continues.",
        "Spark splits your table into **partitions**, ships **tasks** to **executors**, merges results on the **driver**, and uses the **Catalyst** optimizer to rewrite your plan before execution.",
        "**Restaurant chain:** You (driver) write one menu recipe. Head office (Spark) sends the same recipe to many kitchens (executors). Each kitchen cooks its batch (partition). You plate the final dish (action result).",
        'print("Spark version:", spark.version)\nprint("You are the chef writing recipes; Spark runs the kitchens.")',
        mermaid_diagram=SPARK_OVERVIEW,
    )

    what_why_how(
        parts,
        "0.2 — Driver vs Executor (WHO does the work?)",
        "**Driver** = the coordinator JVM (your notebook cell's brain). **Executor** = worker JVM on cluster nodes that process data partitions.",
        "One machine cannot scan a 500 GB CSV. Work must be **parallelised** across workers while something coordinates the plan.",
        "Driver builds the **logical plan** from your DataFrame API calls. On an **action**, the driver asks executors to run **tasks** (read, filter, aggregate). Executors return **metrics** or **small results** to the driver.",
        "**Film director vs actors:** The **director** (driver) does not act in every scene — they shout instructions. **Actors** (executors) perform scenes (partitions) in parallel. The director reviews the final cut (`show`, `count`).",
        'print("Default parallelism (hint of partition count):", spark.sparkContext.defaultParallelism)',
        mermaid_diagram=DRIVER_EXECUTOR,
    )

    what_why_how(
        parts,
        "0.3 — Partitions (HOW data is chopped up)",
        "A **partition** is a horizontal slice of a DataFrame — a subset of rows that one task can process. Files, shuffles, and `repartition()` all affect partition layout.",
        "Parallelism only works if data is split. One giant chunk = one executor does everything = no scale.",
        "When you `read.csv()`, Spark creates partitions (often ~one per file block). Each partition is processed by a **task** on an executor. Narrow ops (`filter`, `select`) often stay **partition-local** (no shuffle).",
        "**Pizza delivery zones:** A city is split into zones (partitions). Each driver (executor) delivers only their zone. You don't send one driver across the whole country.",
        "print('Partitions after read:', bronze.rdd.getNumPartitions())",
        mermaid_diagram=PARTITIONS,
    )

    what_why_how(
        parts,
        "0.4 — DataFrame vs RDD (WHAT you code against)",
        "**DataFrame** = named columns + schema + Catalyst optimiser. **RDD** = lower-level resilient distributed dataset (rows without rich schema).",
        "DataFrames let Spark **optimise** your query (predicate pushdown, column pruning). RDDs require manual tuning.",
        "Your `df.filter(...).select(...)` compiles to a **logical plan**. Catalyst rewrites it, then runs a **physical plan** on executors. You almost always use DataFrame API in production.",
        "**Spreadsheet vs SQL database:** RDD is like raw cell ranges. DataFrame is like a **typed table** where the system knows column names and can optimise queries.",
        "print('We use DataFrame API:', type(bronze))",
    )

    what_why_how(
        parts,
        "0.5 — Lazy transformations (WHY Spark waits)",
        "**Transformations** (`select`, `filter`, `withColumn`, `join`, `groupBy`) are **lazy** — they record intent in a plan but do **not** run immediately.",
        "If Spark ran every line as you typed, it would **re-scan** the lake many times (slow, expensive). Lazy lets Spark **batch** work into one efficient job.",
        "Each transform returns a **new DataFrame** linked to a growing **DAG** (directed acyclic graph) of steps. Nothing touches disk/network until an **action**.",
        "**Shopping list vs shopping trip:** Writing `filter` and `select` is adding items to a list (lazy). Calling `count()` is **going to the shop once** with the full list (action).",
        "step_a = bronze.filter(\"status = 'posted'\")\nstep_b = step_a.select('transaction_id', 'channel')\nprint('List written — no shopping trip yet (lazy)')",
        mermaid_diagram=LAZY_ACTION,
        ascii_diagram=ASCII_LAZY,
        extra_code=["print('Now we shop once:', step_b.count())"],
    )

    what_why_how(
        parts,
        "0.6 — Actions (WHAT triggers real work)",
        "**Actions** (`show`, `count`, `collect`, `write`, `take`) tell Spark: **execute the full plan now** and give me a result.",
        "You need *some* trigger to see output or save data. Actions are that trigger — but each action can re-run the chain unless you `cache()`.",
        "Driver schedules **stages** (groups of tasks separated by **shuffles**). Executors run tasks, report back. `show()` pulls a **small sample** to driver; `write()` streams rows to storage.",
        "**Microwave start button:** Transformations set temperature and time (lazy). **Start** (action) actually cooks. Pressing Start twice cooks twice — same for two `count()` calls without cache.",
        "step_b.show(5)",
    )

    what_why_how(
        parts,
        "0.7 — Catalyst optimizer (HOW Spark rewrites your recipe)",
        "**Catalyst** is Spark's query planner. It turns your DataFrame/SQL into an optimised **physical plan** before executors run.",
        "Humans write steps in a convenient order; machines can **merge filters**, **drop unused columns**, and **push predicates** to file readers.",
        "`df.filter(...).select(...).filter(...)` may become **one** file scan reading only needed columns. Use `.explain()` to see logical vs physical plans.",
        "**GPS recalculating route:** You ask for A→B→C (lazy steps). GPS (Catalyst) finds a **shorter single route** before you drive (action).",
        "bronze.filter(F.col('channel') == 'wire').select('transaction_id', 'amount_gbp').explain()",
        mermaid_diagram=CATALYST,
    )

    what_why_how(
        parts,
        "0.8 — Narrow vs wide transformations",
        "**Narrow** ops (`filter`, `select`, `withColumn`) — each output row depends only on **one input row** in the same partition. **Wide** ops (`groupBy`, `join`) may need **shuffle**.",
        "Narrow = cheap (no cross-machine data movement). Wide = expensive (network shuffle). Knowing the difference helps you design fast pipelines.",
        "Narrow: executor processes partition locally. Wide: Spark **exchanges** data so all rows with the same key land on the same partition (shuffle), then aggregates.",
        "**Narrow = sorting papers on your desk.** **Wide = merging piles from every desk in the office** into one room sorted by customer name (shuffle).",
        "print('Narrow example: filter')\nbronze.filter(F.col('status')=='posted').explain()",
        ascii_diagram=ASCII_NARROW_WIDE,
        extra_code=["print('Wide example: groupBy')\nbronze.groupBy('channel').count().explain()"],
    )

    what_why_how(
        parts,
        "0.9 — Shuffle (WHY groupBy feels slower)",
        "A **shuffle** is Spark **redistributing** data across executors so rows with the same key are co-located (needed for `groupBy`, `join`).",
        "`groupBy('channel')` must gather all `wire` rows together — they may start on different partitions/machines.",
        "Spark writes **map output** to local disk, ships blocks over the network, merges **reduce** side per key. Shuffle = **most expensive** part of many jobs.",
        "**Group project:** Everyone (executors) has random pages of a book. To count words, you must **pass piles** so all pages of chapter 3 sit with one person (shuffle) before counting.",
        "bronze.groupBy('channel').count().show()",
        mermaid_diagram=SHUFFLE,
    )

    what_why_how(
        parts,
        "0.10 — Column API `F.col` (HOW to express logic)",
        "`from pyspark.sql import functions as F` gives **column expressions** — logic that runs on executors, not Python loops on the driver.",
        "Python `for row in df.collect()` pulls **all data** to one machine (breaks scale). Column expressions stay distributed.",
        "`F.col('amount_gbp').cast('double')` builds an expression tree. Catalyst implements it in optimised JVM code across partitions.",
        "**Assembly line stamp:** Instead of one worker painting each car by hand (Python loop), a **machine arm** (`F.col`) stamps every car on the line the same way — parallel, repeatable.",
        "bronze.select(F.col('transaction_id'), F.upper(F.col('channel')).alias('CH')).show(3)",
    )

    what_why_how(
        parts,
        "0.11 — FinLedger in one Spark story",
        "**WHAT:** Read bronze CSV → cast amounts → split good/quarantine → dedupe → join lookup → aggregate gold by channel.",
        "**WHY:** Raw bank files are messy strings; reports need typed, trusted totals per payment channel.",
        "**HOW:** One lazy chain ending in `show()`/`write()`; Catalyst merges filters; shuffle only on `groupBy`/`join`.",
        "**Factory line:** **Bronze** = raw goods arrival. **Silver** = QC + labelling. **Gold** = daily production report. Spark is the factory automation system.",
        "df = bronze\nprint('FinLedger story starts — Part 11 runs the full line')",
        mermaid_diagram=PIPELINE_E2E,
        ascii_diagram=ASCII_MEDALLION,
    )

    md(
        parts,
        "### 0.12 — Cheat sheet: lazy vs action (pin this mentally)",
    )
    md(
        parts,
        """| Type | Examples | Analogy | Runs cluster? |
|------|----------|---------|---------------|
| **Lazy** | `filter`, `select`, `withColumn`, `join`, `groupBy` | Add to shopping list | No |
| **Action** | `show`, `count`, `collect`, `write` | Go shopping / save meal | **Yes** |""",
    )
    md(parts, "**Golden rule:** Chain many lazy steps → **one** action at the end.")
    box_diagram(parts, LAZY_ACTION, caption="Lazy chain → single action")

    code(parts, "print('Part 0 complete — you understand HOW Spark thinks. Now apply it to FinLedger.')")


def build() -> list[str]:
    p: list[str] = ["# Databricks notebook source", ""]

    md(
        p,
        """# Master PySpark Lab — Theory + Practice (Shared Class)

**Estate:** `rg-shared-class1` · storage `stsharedqgr7mj` · scope `finledger`

**Run:** Attach a **Single-user** cluster → **Run all** top to bottom.

**How this notebook is structured:**
- **Small cells** — one idea per cell (WHAT → WHY → diagram → HOW → code)
- **Mermaid diagrams** — visual flow for each major concept
- **ASCII flows** — step-by-step when diagrams help memory

**You will learn:**
1. Data engineering framework — Ingest → Transform → Orchestrate
2. Medallion lake — bronze → silver → gold on ADLS (`abfss://`)
3. PySpark DataFrame API — lazy transforms, eager actions
4. FinLedger pipeline — read bronze, cast, quarantine, aggregate gold

> **Note:** On Databricks, `spark` already exists. Do **not** create `SparkSession`.""",
    )

    code(p, BRONZE_AUTH_CELL)

    code(p, "from pyspark.sql import functions as F\nfrom pyspark.sql.window import Window")

    # ── PART 0: How Spark thinks (WHAT / WHY / HOW + analogies) ──────────────
    md(
        p,
        """# Part 0 — How Spark thinks (read this first)

Every concept below uses four lenses:

| Lens | Question |
|------|----------|
| **WHAT** | What is this thing? |
| **WHY** | Why does Spark work this way? |
| **HOW** | What happens on the cluster? |
| **ANALOGY** | Real-world picture to remember it |""",
    )

    _build_spark_thinking_content(p)

    md(p, "## Part 1 — What is data engineering?")

    theory_then_example(
        p,
        "1.1 The three pillars",
        """Data engineering prepares raw data so downstream teams (analytics, BI, ML) can extract value.

| Pillar | Meaning | Our lab |
|--------|---------|---------|
| **Ingest** | Bring data into the platform | Read CSV from `abfss://bronze/...` |
| **Transform** | Clean, type, filter, aggregate | PySpark `withColumn`, `filter`, `groupBy` |
| **Orchestrate** | Schedule, monitor, retry pipelines | ADF + Databricks Jobs (Session 4+) |

**Analogy:** Ingest = delivery truck. Transform = kitchen prep. Orchestrate = restaurant schedule.""",
        'print("Today: Ingest + Transform in PySpark. Orchestrate comes in Session 4.")',
        mermaid_diagram=INGEST_TRANSFORM_ORCHESTRATE,
    )

    theory_then_example(
        p,
        "1.2 Medallion architecture",
        """The **medallion** pattern organises the lake in three quality layers:

| Layer | Quality | Storage | FinLedger example |
|-------|---------|---------|-------------------|
| **Bronze** | Raw, as landed | `bronze/` container | CSV with string `amount_gbp` |
| **Silver** | Cleansed, typed, deduped | `silver/` container | `amount` as `double`, bad rows quarantined |
| **Gold** | Business aggregates | `gold/` container | Total GBP per `channel` |

Paths use **ADLS Gen2** with hierarchical namespace (`abfss://`).""",
        f'for layer in ("bronze", "silver", "gold"):\n    print(f"abfss://{{layer}}@{{STORAGE_ACCOUNT}}.dfs.core.windows.net/")',
        mermaid_diagram=MEDALLION,
        ascii_diagram=ASCII_MEDALLION,
        extra_md=[
            """```
abfss://bronze@<account>.dfs.core.windows.net/loaded/run=<id>/file.csv
abfss://silver@<account>.dfs.core.windows.net/transactions
abfss://gold@<account>.dfs.core.windows.net/daily_channel_summary
```"""
        ],
    )

    theory_then_example(
        p,
        "1.3 Why PySpark (not Python loops)?",
        """| Approach | Runs on | Scale | FinLedger |
|----------|---------|-------|-----------|
| Python `for` loop over rows | Driver laptop | Thousands of rows | ❌ Too slow |
| **PySpark DataFrame** | Cluster executors | Billions of rows | ✅ Production |

**Rule:** Express logic as **column expressions** (`F.col`, `filter`, `withColumn`) — Spark runs them in parallel across partitions.

**Wrong:** `for row in df.collect(): ...`  (pulls all data to one machine)

**Right:** `df.filter(F.col("status") == "posted")`  (distributed)""",
        'print("PySpark = distributed SQL-like transforms on the cluster")',
    )

    # ── PART 2: Spark architecture ───────────────────────────────────────────
    md(p, "## Part 2 — How Spark thinks (architecture)")

    theory_then_example(
        p,
        "2.1 Driver, executors, partitions",
        """When you run a notebook cell on Databricks, Spark coordinates work across the cluster.

- **Driver** — coordinates work, holds small results (`show`, `count`)
- **Executor** — worker JVM that processes **partitions** of data in parallel
- **Partition** — chunk of rows (e.g. one CSV split or one parquet file)

You write one expression; Spark splits work across partitions automatically.""",
        'print("Executors:", spark.sparkContext.defaultParallelism)',
        mermaid_diagram=DRIVER_EXECUTOR,
    )

    theory_then_example(
        p,
        "2.2 Lazy transformations vs eager actions",
        """This is the **most important** PySpark concept.

**Transformations (LAZY)** — build a recipe, no work yet:
`select`, `filter`, `withColumn`, `join`, `groupBy`, `orderBy`, `dropDuplicates`

**Actions (EAGER)** — run the full recipe now:
`show`, `count`, `collect`, `take`, `write`, `foreach`""",
        "# bronze already loaded in cell 0\nstep = bronze.filter(\"status = 'posted'\")\nprint('Lazy steps defined — cluster has NOT read all rows yet')",
        mermaid_diagram=LAZY_ACTION,
        ascii_diagram=ASCII_LAZY,
        extra_md=[
            """**Example mental model:**
```python
step1 = df.filter(...)      # lazy — instant return
step2 = step1.select(...)   # lazy — still no execution
step2.count()               # ACTION — cluster runs step1 + step2 + count
```

**Why lazy?** Catalyst optimizer can merge filters, push predicates to files, pick efficient joins — before any data moves."""
        ],
        extra_code=[
            "print('Posted count (ACTION):', step.count())",
            "step.show(5)",
        ],
    )

    theory_then_example(
        p,
        "2.3 Catalyst optimizer and `explain()`",
        """Spark's **Catalyst** engine optimises your logical plan before execution.

Use `.explain()` to see the plan (great for debugging):

- **Logical plan** — what you asked for
- **Physical plan** — how cluster will execute (shuffles, scans)

Narrow transforms (`filter`, `select`) stay on one partition when possible.
Wide transforms (`groupBy`, `join`) may **shuffle** data between executors (slower).""",
        "bronze.filter(F.col('channel') == 'wire').explain()",
        mermaid_diagram=CATALYST,
        ascii_diagram=ASCII_NARROW_WIDE,
    )

    # ── PART 3: Read bronze (ingest) ─────────────────────────────────────────
    md(p, "## Part 3 — Ingest: read bronze from the lake")

    theory_then_example(
        p,
        "3.1 `abfss://` paths",
        """**ADLS Gen2** exposes a DFS endpoint. Spark reads via:

`abfss://<container>@<storage-account>.dfs.core.windows.net/<path>`

| Part | Our value |
|------|-----------|
| Container | `bronze` |
| Account | from secret `storage-account` |
| Path | `loaded/run=session3-lab/sample_transactions.csv` |

The `run=` folder is a **partition** convention — multiple pipeline runs land in separate folders.""",
        "print(BRONZE_PATH)",
        mermaid_diagram=BRONZE_READ_FLOW,
    )

    theory_then_example(
        p,
        "3.2 `spark.read` options",
        """```python
df = spark.read \\
    .option("header", True)      # first row = column names \\
    .option("inferSchema", True) # guess types (bronze CSV still has string amounts) \\
    .csv(path)
```

| Option | Why |
|--------|-----|
| `header=True` | CSV has column names in row 1 |
| `inferSchema=True` | Quick exploration; production often uses explicit schema |

After read, always run `printSchema()` and `show()` before transforms.""",
        "df = bronze",
        extra_code=[
            "df.printSchema()",
            "print('Row count:', df.count())",
            "df.show(10, truncate=False)",
        ],
    )

    theory_then_example(
        p,
        "3.3 Bronze schema reality",
        """Bronze CSV stores **everything as strings** (or inferred types that may be wrong).

FinLedger columns:
- `transaction_id` — business key
- `amount_gbp` — **string** in bronze (`"1250.50"`)
- `channel` — wire | card | fps
- `status` — posted | pending

**Silver rule:** cast `amount_gbp` → `double` as column `amount`, then filter invalid values.""",
        "df.select('transaction_id', 'channel', 'amount_gbp', 'status').show()",
        extra_code=["df.groupBy('status').count().show()", "df.groupBy('channel').count().show()"],
    )

    # ── PART 4: Core objects ───────────────────────────────────────────────────
    md(p, "## Part 4 — Core PySpark objects")

    objects = [
        (
            "4.1 SparkSession",
            "`spark` is the entry point. On Databricks it exists automatically.\n\nLocal laptops use `SparkSession.builder...getOrCreate()` — **not needed here**.",
            "print('Spark version:', spark.version)",
        ),
        (
            "4.2 DataFrame",
            "A **distributed table** with named columns — like SQL table or pandas DataFrame, but lazy and cluster-scale.",
            "print('Columns:', df.columns)\nprint('Type:', type(df))",
        ),
        (
            "4.3 Column expressions (`F`)",
            "Import once: `from pyspark.sql import functions as F`\n\nUse `F.col('name')` in `select`, `filter`, `withColumn`, `agg` — never compare columns with bare Python `==` on rows.",
            "df.select(F.col('transaction_id'), F.upper(F.col('channel')).alias('channel_upper')).show(5)",
        ),
        (
            "4.4 Schema",
            "`printSchema()` shows `struct` tree with types. After cast, verify `amount: double`.",
            "df.printSchema()",
        ),
        (
            "4.5 Row vs DataFrame",
            "One **Row** = one record. Avoid `.collect()` on large data — it pulls all rows to the driver.",
            "sample = df.limit(3).collect()\nprint('Rows on driver:', len(sample))\nprint(sample[0])",
        ),
    ]
    for title, theory, ex in objects:
        theory_then_example(p, title, theory, ex)

    # ── PART 5: select & filter ────────────────────────────────────────────────
    md(p, "## Part 5 — `select` and `filter` (narrow transforms)")
    ascii_flow(p, ASCII_NARROW_WIDE, caption="Narrow transforms stay on the same partition")

    select_filter_topics = [
        (
            "5.1 `select` — projection",
            "**Purpose:** keep only columns you need (reduces IO and memory).\n\n```python\ndf.select('transaction_id', 'channel', 'amount_gbp', 'status')\n```\n\nAlias with `F.col(...).alias('new_name')`.",
            "slim = df.select('transaction_id', 'channel', 'amount_gbp', 'status')\nslim.show(5)",
        ),
        (
            "5.2 `filter` / `where` — SQL string",
            "String SQL inside `filter()` — readable for simple rules:\n\n```python\ndf.filter(\"status = 'posted'\")\n```\n\n`where` is an alias for `filter`.",
            "posted = slim.filter(\"status = 'posted'\")\nprint('Posted rows:', posted.count())",
        ),
        (
            "5.3 `filter` — Column API (production style)",
            "Prefer `F.col` for composable conditions:\n\n```python\ndf.filter(F.col('status') == 'posted')\n```",
            "posted_f = slim.filter(F.col('status') == 'posted')\nposted_f.show(3)",
        ),
        (
            "5.4 Combine conditions — `&` and `|`",
            "Use `&` (and), `|` (or). **Wrap each condition in parentheses.**\n\n```python\ndf.filter((F.col('channel')=='wire') & (F.col('status')=='pending'))\n```",
            "pending_wire = slim.filter((F.col('channel') == 'wire') & (F.col('status') == 'pending'))\npending_wire.show()",
        ),
        (
            "5.5 `isin`, `like`, `isNull`",
            "| Method | Example |\n|--------|--------|\n| `isin` | `F.col('channel').isin(['wire','card'])` |\n| `like` | `F.col('transaction_id').like('TXN-100%')` |\n| `isNull` | `F.col('amount_gbp').isNull()` |",
            "slim.filter(F.col('channel').isin(['wire', 'card'])).show()",
            [
                "slim.filter(F.col('transaction_id').like('TXN-100%')).show()",
                "slim.filter(F.col('status') != 'posted').show()",
            ],
        ),
        (
            "5.6 Chain transforms — one action at end",
            "Best practice: chain lazy steps, call **one** action to execute.\n\n```python\nresult = (df\n  .filter(...)\n  .select(...)\n  .withColumn(...))\nresult.show()  # single action\n```",
            "chain = (slim\n    .filter(F.col('status') == 'posted')\n    .select('transaction_id', 'channel', 'amount_gbp'))\nchain.show()",
        ),
    ]
    for item in select_filter_topics:
        if len(item) == 3:
            theory_then_example(p, item[0], item[1], item[2])
        else:
            theory_then_example(p, item[0], item[1], item[2], extra_code=item[3])

    # ── PART 6: withColumn & cast ──────────────────────────────────────────────
    md(p, "## Part 6 — `withColumn`, `cast`, data quality (bronze → silver)")

    cast_topics = [
        (
            "6.1 Why cast on bronze?",
            "CSV stores numbers as text. Maths (`>`, `sum`) needs numeric types.\n\n```python\ntyped = df.withColumn('amount', F.col('amount_gbp').cast('double'))\n```\n\nInvalid text (e.g. `'INVALID'`) becomes **`null`** after cast.",
            "typed = df.withColumn('amount', F.col('amount_gbp').cast('double'))\ntyped.printSchema()",
            ["typed.show(10)"],
        ),
        (
            "6.2 Silver good vs quarantine",
            "**FinLedger pattern:** never fail the pipeline on bad rows — **split** them.\n\n| Dataset | Rule |\n|---------|------|\n| `silver_good` | `amount` not null AND `amount > 0` |\n| `silver_bad` | null OR `<= 0` → quarantine path (Session 9) |",
            "silver_good = typed.filter(F.col('amount').isNotNull() & (F.col('amount') > 0))\nsilver_bad = typed.filter(F.col('amount').isNull() | (F.col('amount') <= 0))\nprint('good:', silver_good.count(), '| quarantine:', silver_bad.count())",
            ["silver_bad.show()", "silver_good.show()"],
        ),
        (
            "6.3 `when` / `otherwise` — conditional column",
            "SQL `CASE WHEN` as column expression:\n\n```python\nF.when(F.col('amount') >= 10000, 'WATCH').otherwise('OK')\n```\n\nUse for fraud flags, bucketing, status labels.",
            "flagged = silver_good.withColumn(\n    'fraud_flag',\n    F.when(F.col('amount') >= 10000, 'WATCH').otherwise('OK'),\n)\nflagged.select('transaction_id', 'amount', 'fraud_flag').show()",
        ),
        (
            "6.4 `lit` and `coalesce`",
            "**`F.lit(value)`** — inject constant column (e.g. `source_system='finledger'`).\n\n**`F.coalesce(a, b)`** — first non-null value (defaults for missing data).",
            "tagged = df.withColumn('source_system', F.lit('finledger'))\ntagged.select('transaction_id', 'source_system').show(3)",
            ["df.withColumn('amt', F.coalesce(F.col('amount_gbp'), F.lit('0'))).show(3)"],
        ),
        (
            "6.5 `dropDuplicates`",
            "Keep one row per business key before silver write:\n\n```python\ndf.dropDuplicates(['transaction_id'])\n```",
            "print('before:', df.count())\ndeduped = df.dropDuplicates(['transaction_id'])\nprint('after:', deduped.count())",
        ),
    ]
    cast_diagram_titles = {"6.1 Why cast on bronze?", "6.2 Silver good vs quarantine"}
    for item in cast_topics:
        kwargs: dict = {}
        if len(item) > 3:
            kwargs["extra_code"] = item[3]
        if item[0] in cast_diagram_titles:
            kwargs["mermaid_diagram"] = CAST_FLOW
        theory_then_example(p, item[0], item[1], item[2], **kwargs)

    # ── PART 7: groupBy & agg ─────────────────────────────────────────────────
    md(p, "## Part 7 — `groupBy` and aggregates (gold layer)")

    agg_topics = [
        (
            "7.1 Wide transform — shuffle",
            "`groupBy` **collapses** rows into buckets — often requires **shuffle** (data moves between executors). Slower than `filter` but essential for reports.",
            "clean = typed.filter(F.col('amount') > 0)\nby_ch = clean.groupBy('channel').agg(F.count('*').alias('txns'), F.sum('amount').alias('total_gbp'))\nby_ch.show()",
            GROUPBY_FLOW,
        ),
        (
            "7.2 Common aggregates",
            "| Function | Purpose |\n|----------|--------|\n| `F.count('*')` | Row count per group |\n| `F.sum('amount')` | Total GBP |\n| `F.avg('amount')` | Average |\n| `F.min` / `F.max` | Range |",
            "stats = clean.groupBy('channel').agg(\n    F.avg('amount').alias('avg_gbp'),\n    F.min('amount').alias('min_gbp'),\n    F.max('amount').alias('max_gbp'),\n)\nstats.show()",
        ),
        (
            "7.3 `orderBy` after aggregate",
            "Gold reports are usually sorted for humans:\n\n```python\nby_ch.orderBy(F.desc('total_gbp'))\n```",
            "by_ch.orderBy(F.desc('total_gbp')).show()",
        ),
        (
            "7.4 Gold preview",
            "This shape is what lands in `abfss://gold@.../daily_channel_summary` in later sessions.",
            "display(by_ch)",
        ),
    ]
    for item in agg_topics:
        if len(item) == 4:
            theory_then_example(p, item[0], item[1], item[2], mermaid_diagram=item[3])
        else:
            theory_then_example(p, item[0], item[1], item[2])

    # ── PART 8: joins ────────────────────────────────────────────────────────
    md(p, "## Part 8 — `join` (enrich silver)")

    code(p, "lookup = spark.createDataFrame(\n    [('wire', 'high'), ('card', 'medium'), ('fps', 'low')],\n    ['channel', 'risk'],\n)\nlookup.show()")

    join_topics = [
        (
            "8.1 Join keys",
            "Join on matching column(s): `df.join(other, on='channel', how='left')`\n\nPrepare **silver** first (typed, deduped), then join small **reference** tables.",
            "silver = silver_good.dropDuplicates(['transaction_id'])\nenriched = silver.join(lookup, on='channel', how='left')\nenriched.select('transaction_id', 'channel', 'amount', 'risk').show()",
            JOIN_FLOW,
        ),
        (
            "8.2 Join types",
            "| how= | Keeps |\n|------|-------|\n| `inner` | Only rows with match on **both** sides |\n| `left` | All left rows + matching right (null if no match) |\n| `right` | All right + matching left |\n| `full` | Everything from both |",
            "silver.join(lookup, 'channel', 'inner').count()",
            ["silver.join(lookup, 'channel', 'left').show()", "silver.join(lookup, 'channel', 'full').show()"],
        ),
    ]
    for item in join_topics:
        if len(item) == 4 and isinstance(item[3], list):
            theory_then_example(p, item[0], item[1], item[2], extra_code=item[3])
        elif len(item) == 4:
            theory_then_example(p, item[0], item[1], item[2], mermaid_diagram=item[3])
        else:
            theory_then_example(p, item[0], item[1], item[2])

    # ── PART 9: window functions ─────────────────────────────────────────────
    md(p, "## Part 9 — Window functions (rank without collapsing)")

    window_topics = [
        (
            "9.1 `groupBy` vs Window",
            "**groupBy** collapses to one row per key.\n\n**Window** keeps **all rows** and adds calculated columns per partition (e.g. rank, running total).",
            "print('groupBy collapses; Window keeps every row')",
            WINDOW_FLOW,
        ),
        (
            "9.2 Window specification",
            "```python\nw = Window.partitionBy('channel').orderBy(F.desc('amount'))\n```\n\n- `partitionBy` — groups for the window (like GROUP BY keys)\n- `orderBy` — sort within each partition",
            "w = Window.partitionBy('channel').orderBy(F.desc('amount'))\nranked = clean.withColumn('rank_in_channel', F.row_number().over(w))\nranked.select('channel', 'transaction_id', 'amount', 'rank_in_channel').show(10)",
        ),
        (
            "9.3 `row_number` vs `rank`",
            "| Function | Ties |\n|----------|------|\n| `row_number()` | Unique 1,2,3… |\n| `rank()` | Same rank for ties (1,1,3) |\n| `dense_rank()` | No gaps (1,1,2) |",
            "top1 = ranked.filter(F.col('rank_in_channel') == 1)\ntop1.show()",
        ),
        (
            "9.4 Running sum",
            "Unbounded window — cumulative total ordered by amount within channel.",
            "w2 = Window.partitionBy('channel').orderBy('amount').rowsBetween(Window.unboundedPreceding, Window.currentRow)\nrunning = clean.withColumn('running_total', F.sum('amount').over(w2))\nrunning.show(10)",
        ),
    ]
    for item in window_topics:
        if len(item) == 4:
            theory_then_example(p, item[0], item[1], item[2], mermaid_diagram=item[3])
        else:
            theory_then_example(p, item[0], item[1], item[2])

    # ── PART 10: SQL API ─────────────────────────────────────────────────────
    md(p, "## Part 10 — SQL API (same Catalyst engine)")

    sql_topics = [
        (
            "10.1 Temp views",
            "Register DataFrame as SQL table for this session:\n\n```python\ndf.createOrReplaceTempView('bronze_txn')\n```",
            "df.createOrReplaceTempView('bronze_txn')",
        ),
        (
            "10.2 SQL queries",
            "DataFrame API and SQL compile to the **same** optimised plan — use whichever your team prefers.",
            "spark.sql('SELECT channel, COUNT(*) AS n FROM bronze_txn GROUP BY channel').show()",
            [
                "spark.sql(\"SELECT * FROM bronze_txn WHERE status = 'posted'\").show()",
                "spark.sql('SELECT channel, SUM(CAST(amount_gbp AS DOUBLE)) AS total FROM bronze_txn GROUP BY channel').show()",
            ],
        ),
    ]
    for item in sql_topics:
        if len(item) == 3:
            theory_then_example(p, item[0], item[1], item[2])
        else:
            theory_then_example(p, item[0], item[1], item[2], extra_code=item[3])

    # ── PART 11: Full pipeline ───────────────────────────────────────────────
    md(p, "## Part 11 — End-to-end bronze → silver → gold")

    box_diagram(p, PIPELINE_E2E, caption="Full FinLedger pipeline flow")

    pipeline_steps = [
        ("11.1 Step 1 — Read bronze", "Ingest raw CSV from lake.", "b = bronze"),
        ("11.2 Step 2 — Cast amount", "Silver typing.", "s = b.withColumn('amount', F.col('amount_gbp').cast('double'))"),
        ("11.3 Step 3 — Split quality", "Good vs quarantine.", "g = s.filter(F.col('amount').isNotNull() & (F.col('amount') > 0))\nq = s.filter(F.col('amount').isNull() | (F.col('amount') <= 0))"),
        ("11.4 Step 4 — Counts", "Validate split.", "print('good:', g.count(), 'quarantine:', q.count())"),
        ("11.5 Step 5 — Dedupe", "One row per txn.", "g2 = g.dropDuplicates(['transaction_id'])"),
        ("11.6 Step 6 — Enrich", "Left join risk lookup.", "g3 = g2.join(lookup, 'channel', 'left')"),
        ("11.7 Step 7 — Gold aggregate", "Business metric.", "gold = g3.groupBy('channel').agg(F.sum('amount').alias('total_gbp'), F.count('*').alias('txns'))"),
        ("11.8 Step 8 — Present gold", "Sorted report.", "gold.orderBy(F.desc('total_gbp')).show()"),
    ]
    for title, theory, ex in pipeline_steps:
        theory_then_example(p, title, theory, ex)

    md(
        p,
        """### 11.9 Write preview (Session 9+)

Production pipelines **write** silver and gold to the lake (actions):

```python
# silver_good.write.mode('overwrite').parquet(silver_path)
# gold.write.mode('overwrite').format('delta').save(gold_path)
```

`mode`: `append` | `overwrite` | `error` | `ignore`""",
    )

  # ── PART 12: Performance & mistakes ────────────────────────────────────────
    md(p, "## Part 12 — Performance tips and common mistakes")

    perf_topics = [
        (
            "12.1 `cache` / `persist`",
            "If you reuse a DataFrame in **many actions**, cache after first quality filter:\n\n```python\ngood = ...filter...\ngood.cache()\ngood.count()  # materialises cache\n```\n\nCall `unpersist()` when done.",
            "cached = clean.cache()\nprint(cached.count())\nprint(cached.count())\ncached.unpersist()",
        ),
        (
            "12.2 Avoid `collect()` on big data",
            "`.collect()` returns **all** rows to the driver — OK for `limit(10)` only.",
            "small = clean.limit(5).collect()\nprint(len(small), 'rows on driver')",
        ),
        (
            "12.3 Mistakes cheat sheet",
            "| Mistake | Fix |\n|---------|-----|\n| `collect()` on millions of rows | `.show()` or write to lake |\n| Compare columns with bare `==` in Python | `F.col('a') == F.col('b')` |\n| Skip cast on bronze | `.cast('double')` then filter nulls |\n| Many actions in a loop | One pipeline, one write |\n| Hardcode storage keys | Secret scope `finledger` |",
            "print('Review checklist above before Session 9')",
        ),
    ]
    for title, theory, ex in perf_topics:
        theory_then_example(p, title, theory, ex)

    # ── PART 13: Deep-dive drills (bulk to 300+ cells) ───────────────────────
    md(p, "## Part 13 — Concept drills (theory + one-line example each)")

    drills = [
        ("SparkSession", "Entry point — exists as `spark` on Databricks.", "print(spark.version)"),
        ("DataFrame", "Distributed table with named columns.", "print(type(df))"),
        ("Transformation", "Lazy: select, filter, withColumn.", "x = df.select('transaction_id')"),
        ("Action", "Eager: show, count, write.", "print(df.count())"),
        ("F.col", "Reference a column in expressions.", "df.select(F.col('channel')).show(2)"),
        ("cast", "Change column type.", "df.select(F.col('amount_gbp').cast('double')).show(2)"),
        ("filter", "Keep matching rows.", "df.filter(F.col('status')=='posted').count()"),
        ("select", "Choose columns.", "df.select('transaction_id','channel').show(2)"),
        ("withColumn", "Add or replace column.", "df.withColumn('x', F.lit(1)).select('x').show(2)"),
        ("groupBy", "Bucket rows by key.", "df.groupBy('channel').count().show()"),
        ("agg", "Summarise buckets.", "df.groupBy('channel').agg(F.count('*')).show()"),
        ("orderBy", "Sort output.", "df.orderBy('channel').show(2)"),
        ("join", "Combine two DataFrames.", "df.join(lookup,'channel','left').show(2)"),
        ("dropDuplicates", "Dedupe by key.", "df.dropDuplicates(['transaction_id']).count()"),
        ("Window", "Per-partition calc without collapse.", "w = Window.partitionBy('channel')"),
        ("row_number", "Rank within window.", "df.withColumn('r', F.row_number().over(Window.partitionBy('channel').orderBy('amount_gbp'))).show()"),
        ("explain", "Show logical plan.", "df.filter(F.col('status')=='posted').explain()"),
        ("createOrReplaceTempView", "Register for SQL.", "df.createOrReplaceTempView('t')"),
        ("spark.sql", "Run SQL on temp views.", "spark.sql('select count(*) from t').show()"),
        ("lit", "Constant column.", "df.select(F.lit('finledger').alias('src')).show(2)"),
        ("when/otherwise", "Conditional column.", "df.withColumn('f', F.when(F.col('status')=='posted','Y').otherwise('N')).show(2)"),
        ("coalesce", "First non-null.", "df.select(F.coalesce(F.col('amount_gbp'),F.lit('0'))).show(2)"),
        ("isNull", "Find nulls.", "df.filter(F.col('amount_gbp').isNull()).count()"),
        ("isin", "Value in list.", "df.filter(F.col('channel').isin(['wire','card'])).count()"),
        ("like", "Pattern match.", "df.filter(F.col('transaction_id').like('TXN%')).count()"),
        ("abfss", "ADLS path scheme.", "print(BRONZE_PATH)"),
        ("bronze", "Raw layer.", "print('bronze = raw CSV')"),
        ("silver", "Cleansed layer.", "print('silver = typed + deduped')"),
        ("gold", "Aggregate layer.", "print('gold = channel totals')"),
        ("quarantine", "Bad rows isolated.", "print('quarantine = invalid amounts')"),
    ]

    for name, theory, ex in drills:
        md(p, f"### Drill — {name}")
        md(p, f"**{name}:** {theory}")
        code(p, ex)

    # Extended variant drills for more cells
    channels = ["wire", "card", "fps", "chaps"]
    for ch in channels:
        theory_then_example(
            p,
            f"Drill — filter channel `{ch}`",
            f"Practice filtering a single payment channel. Count how many bronze rows have `channel = '{ch}'`.",
            f"print('{ch} rows:', df.filter(F.col('channel') == '{ch}').count())",
        )

    statuses = ["posted", "pending"]
    for st in statuses:
        theory_then_example(
            p,
            f"Drill — filter status `{st}`",
            f"Status drives settlement logic. Filter `status = '{st}'` and show sample rows.",
            f"df.filter(F.col('status') == '{st}').show(5)",
        )

    # ── PART 14: Data engineering deep theory (PDF-aligned) ────────────────────
    md(p, "## Part 14 — Data engineering theory (Big Book alignment)")

    pdf_topics = [
        (
            "14.1 Ingest — batch from object storage",
            "Batch ingestion lands files in **bronze** unchanged. Cloud object storage (ADLS) scales cheaply.\n\nOur ingest path: ADF or manual upload → `bronze/loaded/run=<id>/` → Spark `read.csv`.",
            "print('Ingest complete when df.count() > 0')",
        ),
        (
            "14.2 Transform — data quality",
            "High quality data is essential ('garbage in, garbage out'). Engineers validate types, ranges, and keys.\n\nFinLedger: cast amounts, quarantine invalid, dedupe by `transaction_id`.",
            "print('Quality: good', silver_good.count() if 'silver_good' in dir() else 'run Part 6 first')",
        ),
        (
            "14.3 Transform — medallion flow",
            "Each hop adds quality and structure. Never skip bronze — keep raw for audit.",
            "print('bronze:', BRONZE_PATH)",
            MEDALLION,
        ),
        (
            "14.4 Orchestrate — what comes next",
            "Orchestration schedules pipelines, handles retries, and monitors failures.\n\nSession 4: **Azure Data Factory** triggers Databricks notebooks on a schedule.",
            "print('Today: interactive notebook. Later: ADF pipeline activity.')",
        ),
        (
            "14.5 Lakehouse and Delta Lake (preview)",
            "**Delta Lake** adds ACID transactions, time travel, and schema enforcement on top of parquet files.\n\nSession 9+: `df.write.format('delta').save(silver_path)`",
            "print('Delta format = silver/gold default in production')",
        ),
        (
            "14.6 Unity Catalog (preview)",
            "**Unity Catalog** centralises permissions, auditing, and lineage.\n\nTables appear as `catalog.schema.table` — we use paths first, then UC in Session 3+.",
            "print('UC: finledger.bronze.sample_transactions (future)')",
        ),
        (
            "14.7 Structured Streaming (preview)",
            "Streaming ingests real-time events. Same DataFrame API with `readStream` / `writeStream`.\n\nBatch CSV (today) → Kafka/Event Hubs (advanced).",
            "print('Batch = read(). Streaming = readStream()')",
        ),
        (
            "14.8 Workflows (preview)",
            "Databricks **Workflows** chains notebook tasks with dependencies and alerting.\n\nReplaces ad-hoc 'Run all' in production.",
            "print('Workflow = multi-step job with retries')",
        ),
    ]
    for item in pdf_topics:
        if len(item) == 4:
            theory_then_example(p, item[0], item[1], item[2], mermaid_diagram=item[3])
        else:
            theory_then_example(p, item[0], item[1], item[2])

    # ── PART 15: Extended examples (one concept × many variants) ─────────────
    md(p, "## Part 15 — Extended examples")

    agg_fns = [
        ("count", "F.count('*')", "txns"),
        ("sum", "F.sum('amount')", "total_gbp"),
        ("avg", "F.avg('amount')", "avg_gbp"),
        ("min", "F.min('amount')", "min_gbp"),
        ("max", "F.max('amount')", "max_gbp"),
    ]
    for fn, expr, alias in agg_fns:
        theory_then_example(
            p,
            f"15.x Aggregate `{fn}`",
            f"Use `{expr}.alias('{alias}')` inside `groupBy('channel').agg(...)` to summarise per channel.",
            f"clean.groupBy('channel').agg({expr}.alias('{alias}')).show()",
        )

    join_types = ["inner", "left", "right", "full"]
    for jt in join_types:
        theory_then_example(
            p,
            f"15.x Join `{jt}`",
            f"Demonstrate `{jt}` join between silver and channel risk lookup.",
            f"silver.join(lookup, 'channel', '{jt}').select('transaction_id', 'channel', 'risk').show(5)",
        )

    for n in range(1, 4):
        theory_then_example(
            p,
            f"15.x Pipeline replay step {n}",
            f"Re-run gold aggregate (step {n}/3) to reinforce bronze→gold muscle memory.",
            "gold.orderBy(F.desc('total_gbp')).show()" if n == 3 else f"print('pipeline step', {n})",
        )

    md(p, "## Part 16 — FinLedger row-by-row exploration")

    for txn in ["TXN-10001", "TXN-10002", "TXN-10003", "TXN-10004"]:
        theory_then_example(
            p,
            f"16.x Transaction `{txn}`",
            f"Filter to one business key. Inspect amount, channel, and status for `{txn}` — practice `filter` + `show`.",
            f"df.filter(F.col('transaction_id') == '{txn}').show(truncate=False)",
        )

    faq = [
        ("Why lazy?", "Spark builds an optimised plan before moving data — multiple filters merge into one scan."),
        ("Why not pandas?", "pandas runs on one machine RAM. PySpark scales to TB on a cluster."),
        ("Why cast?", "Bronze CSV stores numbers as text — maths needs `double` or `decimal`."),
        ("Why quarantine?", "Bad rows must not block the pipeline — isolate and alert separately."),
        ("Why dedupe?", "Sources replay files; silver keeps one row per `transaction_id`."),
        ("Why left join?", "Keep all transactions even if lookup has no matching channel."),
        ("Why window?", "Rank or running totals without collapsing to one row per group."),
        ("Why secrets?", "Keys in code leak via git and logs — scopes encrypt at rest."),
        ("Why Single-user cluster?", "Storage key auth works per user; Shared clusters may block it."),
        ("Why abfss?", "ADLS Gen2 hierarchical paths for medallion containers."),
    ]
    for q, a in faq:
        md(p, f"### FAQ — {q}\n\n{a}")
        code(p, f'print("FAQ: {q}")')

    md(p, "## Part 17 — Quick recap cards")
    recap_ops = [
        "select", "filter", "withColumn", "cast", "groupBy", "agg",
        "orderBy", "join", "dropDuplicates", "window", "explain", "display",
    ]
    for op in recap_ops:
        theory_then_example(
            p,
            f"Recap — `{op}`",
            f"Before Session 9, confirm you can explain **`{op}`** in one sentence and write one example from memory.",
            f"print('I can explain: {op}')",
        )

    md(
        p,
        """## Completion checklist

- [ ] Secret scope `finledger` loaded storage auth (cell 0)
- [ ] I read bronze from `abfss://` and inspected schema
- [ ] I can explain **lazy** vs **action** with an example
- [ ] I cast `amount_gbp` → `amount` and split good vs quarantine
- [ ] I built a **gold** aggregate by `channel`
- [ ] I used `join` and `row_number` window
- [ ] Next: **Session 9** — write silver Delta + quarantine to the lake""",
    )

    while p and p[-1] == "# COMMAND ----------":
        p.pop()
    if p and p[-1] == "":
        p.pop()
    return p


def main() -> None:
    lines = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    cell_count = sum(1 for line in lines if line == "# COMMAND ----------") + 1
    print(f"Wrote {OUT}")
    print(f"Cells: {cell_count}")


if __name__ == "__main__":
    main()
