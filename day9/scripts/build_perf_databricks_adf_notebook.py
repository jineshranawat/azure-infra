#!/usr/bin/env python3
"""Generate nb_perf_databricks_adf_lab.py — performance/NFR lab for novice + architect.

Detailed teaching markdown (two voices: beginner + architect).
Simple, short code cells — no clever tricks.

Run:    python day9/scripts/build_perf_databricks_adf_notebook.py
Deploy: python scripts/deploy_perf_databricks_adf_lab.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from bronze_auth_cell import BRONZE_AUTH_CELL
from notebook_helpers import code, finish, md

OUT = ROOT.parent / "notebooks" / "nb_perf_databricks_adf_lab.py"


def build() -> Path:
    parts: list[str] = [
        "# Databricks notebook source",
        "# MAGIC %md",
        "# MAGIC # Spark Performance & NFR Lab — Databricks + ADF",
        "# MAGIC",
        "# MAGIC **Two readers, one notebook**",
        "# MAGIC - **Novice:** learn what is slow and how to see it",
        "# MAGIC - **Architect:** map to SLA, cost, scale, and ADF orchestration",
        "# MAGIC",
        "# MAGIC | | |",
        "# MAGIC |:---|:---|",
        "# MAGIC | Path | `/Shared/day9/perf_databricks_adf_lab` |",
        "# MAGIC | Pair with | `/Shared/day9/spark_dag_problems_lab` (DAG smell → fix) |",
        "# MAGIC | Code style | Short cells, heavy comments above |",
        "",
        "# COMMAND ----------",
        "",
    ]

    # ========== INTRO ==========
    md(
        parts,
        """## 0. The big picture (read this first)

### For a novice (kitchen story)

Think of a **bank restaurant**:

| Role | Tool | Job |
|------|------|-----|
| Manager with a timetable | **ADF** | “Open the kitchen at 2am, copy food into cold storage, then tell chefs to cook” |
| Kitchen + cooks | **Databricks / Spark** | Transform FinLedger data (filter, join, totals) |
| Security cameras | **Spark UI** | See which stove is slow |
| Manager’s logbook | **system tables** | Who ran what, how long, how much bill |
| Gas bill | **Cost / DBUs** | Money burned even if food is fine |

**Late dinner?**  
1. Was the **bus/manager** late? → ADF Monitor  
2. Was a **stove** stuck? → Spark UI  
3. Is the **gas bill** crazy? → billing  

### For an architect (one paragraph)

ADF owns **orchestration NFRs** (trigger, dependency, timeout, retry, packing Copy vs Notebook).  
Databricks owns **compute NFRs** (throughput, skew, shuffle, cluster sizing, autoscaling).  
You correlate both with a shared **`run_id`**. Optimising Spark while ADF Copy dominates wall-clock is wasted money.

```text
ADF pipeline run
   ├─ Copy / wait          ← ADF Monitor duration
   └─ Notebook activity    ← widgets.run_id
          └─ Spark Jobs/Stages/Tasks  ← Spark UI + explain
          └─ system.query.history / billing
```""",
    )

    md(
        parts,
        """## 0b. NFR dictionary (novice words + architect words)

| NFR | Novice question | Architect asks | Where we look |
|-----|-----------------|----------------|---------------|
| **Performance** | How long for one run? | p95 latency vs SLA | wall clock, Spark UI stage time, ADF activity ms |
| **Scalability** | If data ×10? | linear? cliff? | partitions, skew, shuffle bytes, AQE |
| **Cost** | Are we wasting money? | $ per successful pipeline | `system.billing`, cluster idle, SKU |
| **Reliability** | Does it fail randomly? | error budget, retries hide bugs? | Job/ADF fail rate |
| **Observability** | Can a stranger debug at 2am? | run_id, logs, dashboards | secrets-safe logs, Monitor, system tables |
| **Correctness** | Are totals right? | idempotent MERGE? | row counts by run_id, reconciliations |

**Architect rule:** Correctness → SLA → Cost (never cut cost by skipping reconciliation).""",
    )

    # ========== SETUP ==========
    md(
        parts,
        """## 1. Setup — load FinLedger bronze

**Novice:** We need a real table so timings are real.  
**Architect:** Same estate as Sessions 9–15 / 50-problems (`finledger` secrets).""",
    )
    parts.extend(BRONZE_AUTH_CELL.strip().splitlines())
    parts.extend(["", "# COMMAND ----------", ""])

    code(
        parts,
        '''# SETUP — helpers everyone reuses
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
df.show(3)''',
    )

    # ========== LIVE PERF ==========
    md(
        parts,
        """## 2. Measure one real query (novice stopwatch)

### Novice
We cook one dish (channel totals) and measure **wall-clock milliseconds**.  
Then we ask Spark to **print the recipe** (`explain`).

### Architect
Establish a **baseline**. Without baseline numbers, “optimisation” is theatre.  
Capture: rows in, wall ms, plan operators (`Exchange`, `BroadcastHashJoin`, `FileScan`).

```text
df → groupBy(channel) → sum(amount) → ACTION (collect/show)
                              │
                              └─ creates 1 Spark Job in UI
```""",
    )
    code(
        parts,
        '''# STEP 2 — baseline KPI + explain
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

print("\\n--- RECIPE (explain) ---")
print("Look for: FileScan / Exchange / HashAggregate")
kpi.explain()''',
    )

    md(
        parts,
        """## 3. Spark UI tour (your live screenshots)

**Open:** Compute → your cluster → **Spark UI** → **Jobs** (or **SQL**).

### Novice map

| UI word | Plain English |
|---------|----------------|
| **Job** | One “Go!” from `.count` / `.write` / `.collect` |
| **Stage** | Chunk of work with no shuffle in the middle |
| **Task** | Work on **one partition** (one cook’s tray) |
| **Exchange** | Shuffle truck between kitchens (usually expensive) |
| **Duration** | How long that piece took |

### Architect map

| UI signal | Likely NFR hit | Typical response |
|-----------|----------------|------------------|
| Few fat tasks | Scalability / reliability (OOM) | repartition, salt skew keys |
| Many 20ms tasks | Scheduler overhead | coalesce / fewer files |
| High shuffle R/W | Performance + cost | broadcast small side, filter early |
| Spill | Memory pressure | more memory or more partitions |
| Long queue before job | ADF / cluster start | pools, warm cluster |

```text
Action
 └─ Job
     ├─ Stage 0 (narrow: filter/select)
     └─ Stage 1 (after Exchange shuffle)
           └─ Tasks 0..N-1
```

**Exercise:** After Step 2, find the Job for `collect` and note stage count.""",
    )
    code(
        parts,
        '''# STEP 3 — checklist frame (pin this during demos)
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
display(checklist)''',
    )

    # ========== SYSTEM TABLES ==========
    md(
        parts,
        """## 4. system.query.history — the logbook

### Novice
A diary of statements: who ran SQL/notebook queries and roughly how long.

### Architect
Use for **capacity + accountability**: noisy neighbours, failing statements, regressions after release.  
May be ACL-gated — lab soft-skips and shows a synthetic sample so class continues.

**Analogy:** Hotel front-desk log: check-in time, guest, room — not the full CCTV.""",
    )
    code(
        parts,
        '''# STEP 4 — query history (soft)
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
    display(qh)''',
    )

    md(
        parts,
        """## 5. Cost NFR — system.billing.usage

### Novice
The **gas bill**. Fast food that costs a fortune is still a failed restaurant.

### Architect
Break down by day/SKU. Watch for: always-on interactive clusters, failed job retries,  
oversized warehouses. Pair with auto-termination and job clusters.

**Target conversation:** “What is $ per successful FinLedger nightly?”""",
    )
    code(
        parts,
        '''# STEP 5 — billing / usage
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
    print("chart skipped:", str(e)[:100])''',
    )

    md(
        parts,
        """## 6. Compute & Jobs system tables

### Novice
“What clusters exist?” and “Did Jobs run?”

### Architect
Inventory for governance: abandoned all-purpose clusters, job run timelines,  
rightsizing candidates. Names differ by Databricks version — soft SKIP is OK.""",
    )
    code(
        parts,
        '''# STEP 6 — compute / lakeflow (soft)
try_sql("system.compute.clusters", "SELECT * FROM system.compute.clusters LIMIT 12")
try_sql("system.lakeflow.jobs", "SELECT * FROM system.lakeflow.jobs LIMIT 12")
try_sql("system.lakeflow.job_run_timeline", "SELECT * FROM system.lakeflow.job_run_timeline LIMIT 12")
print("Hits:", SYSTEM_HITS)''',
    )

    # ========== ADF ==========
    md(
        parts,
        """## 7. ADF + Databricks together (architect’s correlation)

### Novice
ADF is the **timetable**. Databricks is the **kitchen**.  
If dinner is late, check whether the **bus** arrived late before blaming the cook.

### Architect
In Azure portal: **ADF Studio → Monitor → Pipeline runs**.  
Click the run → each activity’s duration:

| Activity | Typical meaning if slow |
|----------|-------------------------|
| Copy | Data movement / THROTTLING / huge files |
| Notebook | Cluster start queue + Spark work |
| Wait / Until | Explicit delay or polling |

**Contract:** pass `run_id` (or use ADF `pipeline().RunId`) into notebook `baseParameters`.

```text
ADF pipelineRunId  ──baseParameters──►  widget run_id
                                         │
                                         ▼
                              Delta / logs tagged with same id
```

This lab shows a **template table** (synthetic) next to your live Spark wall clock.""",
    )
    code(
        parts,
        '''# STEP 7 — correlate ADF (template) with Spark baseline
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
print("If Copy >> Notebook → fix movement, not joins")''',
    )

    # ========== DEBUG PLAYBOOK ==========
    md(
        parts,
        """## 8. Debug playbook — symptom → reason → fix

### Novice: use as a recipe card

| If you see… | It often means… | Try this |
|-------------|-----------------|----------|
| One task much longer | **Skew** | salt key / filter / AQE |
| Exchange + large shuffle | Heavy **groupBy/join** | filter early, broadcast small |
| Driver OOM | **collect**/big pandas | aggregate in Spark |
| Thousands of tiny tasks | Too many partitions/files | coalesce / OPTIMIZE |
| Spill in stage metrics | Not enough RAM per task | more partitions or memory |
| Random timeouts | Cluster cold / throttle | pools, retries, smaller parallel Copies |

### Architect: decision tree

```text
Wall clock bad?
  ├─ ADF Copy dominates?  → storage, format, parallelism, integration runtime
  ├─ Cluster pending long? → pools / warm / job cluster sizing
  └─ Spark stage slow?
        ├─ Skew task? → salt / skew join
        ├─ Shuffle huge? → broadcast / denoise columns
        └─ Scan huge? → partition prune / file layout
```

Next cell runs **safe mini-demos** (bounded sizes).""",
    )
    code(
        parts,
        '''# STEP 8 — four safe probes (simple code)

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
    print("save skipped:", str(e)[:120])''',
    )

    # ========== SCALE + SCORECARD ==========
    md(
        parts,
        """## 9. Scalability scenarios (whiteboard for architects)

| Scenario | Bad smell | Healthy smell |
|----------|-----------|---------------|
| Data ×10 overnight | Runtime ×10 forever | More workers / better files, SLA held |
| 5 pipelines at once | Interactive cluster dies | Job clusters / SQL warehouse isolation |
| Peak hour ADF | Copy stampedes | Throttle + IR sizing |
| Failure mid-run | Full reload | MERGE / idempotent `run_id` paths |
| Weekend | Flat cost high | Auto-terminate / stop play clusters |

**Novice takeaway:** Bigger data needs a **plan**, not only a bigger button.

**Architect takeaway:** Write NFRs as numbers: *“Nightly FinLedger < 30 min p95 at 10× volume; cost < X DBUs.”*""",
    )
    code(
        parts,
        '''# STEP 9 — scorecard (fill value column during workshop)
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
display(score)''',
    )

    md(
        parts,
        """## 10. Wrap — what to say in a design review

**Novice script (30 seconds):**  
“ADF starts the pipeline. Spark does the heavy transform. I use Spark UI for stages,  
system tables for history and cost, and the same run_id so I can line up Monitor with the lake.”

**Architect script (30 seconds):**  
“We split NFRs: orchestration vs compute. Baselines from explain + UI.  
Cost and skew are first-class. Copy vs Notebook time decides where we invest.  
DAG smells are remediated with broadcast, salting, pruning — see spark_dag_problems_lab.”

**Companion notebook:** `/Shared/day9/spark_dag_problems_lab`""",
    )
    code(
        parts,
        '''# EXIT
summary = {
    "lab": "perf_databricks_adf_lab",
    "run_id": RUN_ID,
    "kpi_wall_ms": ELAPSED_MS,
    "system_hits": SYSTEM_HITS,
    "status": "Succeeded",
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))''',
    )

    OUT.write_text("\n".join(finish(parts)) + "\n", encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
