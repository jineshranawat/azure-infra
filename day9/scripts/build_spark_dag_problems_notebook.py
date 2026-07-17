#!/usr/bin/env python3
"""Generate nb_spark_dag_problems_lab.py — SIMPLE Spark DAG issues → fix.

Easy cells: short code, little repetition, ASCII diagrams in markdown (not complex plotting).

Run:    python day9/scripts/build_spark_dag_problems_notebook.py
Deploy: python scripts/deploy_spark_dag_problems_lab.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from bronze_auth_cell import BRONZE_AUTH_CELL
from notebook_helpers import code, finish, md

OUT = ROOT.parent / "notebooks" / "nb_spark_dag_problems_lab.py"


def build() -> Path:
    parts: list[str] = [
        "# Databricks notebook source",
        "# MAGIC %md",
        "# MAGIC # Spark DAG Lab (simple) — problem → fix",
        "# MAGIC",
        "# MAGIC Short cells. Read the text, run the code, open **Spark UI** in another tab.",
        "# MAGIC",
        "# MAGIC | | |",
        "# MAGIC |:---|:---|",
        "# MAGIC | Path | `/Shared/day9/spark_dag_problems_lab` |",
        "# MAGIC | Data | FinLedger `bronze` |",
        "# MAGIC | Rule | One idea per cell |",
        "",
        "# COMMAND ----------",
        "",
    ]

    md(
        parts,
        """## How to learn

1. Read 5 lines of text  
2. Run **problem** cell  
3. Open **Compute → Spark UI → Jobs**  
4. Run **fix** cell  

```text
filter/select  = narrow (easy)
groupBy/join   = wide (shuffle truck)
.count/.write  = action (starts the Job)
```""",
    )

    md(parts, "## Setup — load FinLedger")
    parts.extend(BRONZE_AUTH_CELL.strip().splitlines())
    parts.extend(["", "# COMMAND ----------", ""])

    code(
        parts,
        '''# SETUP — one typed table we reuse (no copies, no loops)
from pyspark.sql import functions as F
import time

df = (
    bronze
    .withColumn("amount", F.col("amount_gbp").cast("double"))
    .withColumn("channel", F.upper(F.trim(F.col("channel"))))
    .filter(F.col("amount").isNotNull())
)

print("rows =", df.count())
df.show(3)''',
    )

    # --- 01 ---
    md(
        parts,
        """## 1) Lazy plan — nothing runs until an action

**Idea:** `.select` / `.filter` only build a plan. `.count()` starts a Job.

```text
select → filter → groupBy → count   ← Job starts here
```""",
    )
    code(
        parts,
        '''# PROBLEM — plan only (no Job yet)
plan = df.select("channel", "amount").filter(F.col("amount") > 0)
print("Plan ready")

# ACTION — now Spark works
print("rows =", plan.count())''',
    )
    md(parts, """### Fix 1 — explain first, then one action""")
    code(
        parts,
        '''# FIX — look at plan, then aggregate once
plan.explain()
plan.groupBy("channel").count().show()''',
    )

    # --- 02 ---
    md(
        parts,
        """## 2) Wide vs narrow — shuffle

**Narrow:** `filter`, `select` (no truck).  
**Wide:** `groupBy`, `join` (shuffle truck = `Exchange` in explain).

```text
filter ──► groupBy ──► Exchange (shuffle) ──► result
```""",
    )
    code(
        parts,
        '''# PROBLEM — groupBy causes shuffle
wide = df.groupBy("channel").sum("amount")
wide.explain()
wide.show()''',
    )
    md(parts, """### Fix 2 — filter before groupBy (less data to shuffle)""")
    code(
        parts,
        '''# FIX
df.filter(F.col("amount") > 10).groupBy("channel").sum("amount").show()''',
    )

    # --- 03 ---
    md(
        parts,
        """## 3) Too many partitions — tiny tasks

**Smell:** `repartition(200)` on small data → many tiny tasks.""",
    )
    code(
        parts,
        '''# PROBLEM
t0 = time.time()
print(df.repartition(200).count(), "ms", int((time.time() - t0) * 1000))''',
    )
    md(parts, """### Fix 3 — coalesce to fewer partitions""")
    code(
        parts,
        '''# FIX
t0 = time.time()
print(df.coalesce(4).count(), "ms", int((time.time() - t0) * 1000))''',
    )

    # --- 04 ---
    md(
        parts,
        """## 4) Too few partitions — fat tasks

**Smell:** `coalesce(1)` → one cook does everything.""",
    )
    code(
        parts,
        '''# PROBLEM
t0 = time.time()
print(df.coalesce(1).count(), "ms", int((time.time() - t0) * 1000))''',
    )
    md(parts, """### Fix 4 — repartition for balance""")
    code(
        parts,
        '''# FIX
t0 = time.time()
print(df.repartition(8, "channel").groupBy("channel").count().count(), "ms", int((time.time() - t0) * 1000))''',
    )

    # --- 05 ---
    md(
        parts,
        """## 5) Skew — one fat key

**Smell:** most rows share one key → one slow task.

```text
HOT HOT HOT HOT  other other
      ▲
  one fat task
```""",
    )
    code(
        parts,
        '''# PROBLEM — make an artificial HOT key
skewed = df.withColumn(
    "k",
    F.when(F.rand() < 0.8, F.lit("HOT")).otherwise(F.col("channel")),
)
skewed.groupBy("k").count().orderBy(F.desc("count")).show()''',
    )
    md(parts, """### Fix 5 — salt the HOT key, then re-aggregate""")
    code(
        parts,
        '''# FIX — split HOT into HOT_0 .. HOT_7
salted = skewed.withColumn(
    "k2",
    F.when(F.col("k") == "HOT", F.concat(F.lit("HOT_"), (F.rand() * 8).cast("int")))
    .otherwise(F.col("k")),
)
part = salted.groupBy("k2").count()
part.withColumn(
    "k",
    F.when(F.col("k2").startswith("HOT_"), F.lit("HOT")).otherwise(F.col("k2")),
).groupBy("k").sum("count").show()''',
    )

    # --- 06 ---
    md(
        parts,
        """## 6) Broadcast join (small table)

**Smell:** shuffle-joining a tiny lookup table.  
**Fix:** `broadcast(small)`.

```text
tiny dim ──mail──► every worker
big fact ─────────► join local (no fact shuffle)
```""",
    )
    code(
        parts,
        '''# PROBLEM — join without broadcast
dim = spark.createDataFrame([("CARD", "Card"), ("WIRE", "Wire")], ["channel", "label"])
df.join(dim, "channel", "left").explain()''',
    )
    md(parts, """### Fix 6 — broadcast the small side""")
    code(
        parts,
        '''# FIX
df.join(F.broadcast(dim), "channel", "left").explain()
df.join(F.broadcast(dim), "channel", "left").groupBy("label").count().show()''',
    )

    # --- 07 ---
    md(
        parts,
        """## 7) Never `collect` big data

**Smell:** `collect()` / big `toPandas()` → driver OOM.  
**Fix:** aggregate in Spark; `display()` KPIs only.""",
    )
    code(
        parts,
        '''# PROBLEM — only OK because we limit
rows = df.limit(10).collect()
print("collected", len(rows), "rows (never do this on full lake)")''',
    )
    md(parts, """### Fix 7 — aggregate first""")
    code(
        parts,
        '''# FIX
kpi = df.groupBy("channel").agg(F.round(F.sum("amount"), 2).alias("gbp"))
display(kpi)''',
    )

    # --- 08 ---
    md(
        parts,
        """## 8) Cache when you reuse a DataFrame

**Smell:** two `.count()` rebuild the same heavy plan twice.  
**Fix:** `.cache()` then `unpersist()`.""",
    )
    code(
        parts,
        '''# PROBLEM — work twice
x = df.filter(F.col("amount") > 0)
print(x.count())
print(x.select("channel").distinct().count())''',
    )
    md(parts, """### Fix 8 — cache once""")
    code(
        parts,
        '''# FIX
x = df.filter(F.col("amount") > 0).cache()
print(x.count())
print(x.select("channel").distinct().count())
x.unpersist()''',
    )

    # --- 09 ---
    md(
        parts,
        """## 9) Partition pruning

**Smell:** partitioned table but no filter on partition column → full scan.  
**Fix:** filter `txn_date`.""",
    )
    code(
        parts,
        '''# PROBLEM — write partitioned, read all
path = "dbfs:/tmp/dag_simple_part"
(
    df.select("transaction_id", "amount", "channel", F.to_date("value_date").alias("txn_date"))
    .write.format("delta").mode("overwrite").partitionBy("txn_date").save(path)
)
print("full scan", spark.read.format("delta").load(path).count())''',
    )
    md(parts, """### Fix 9 — filter the partition column""")
    code(
        parts,
        '''# FIX
one = spark.read.format("delta").load(path).filter(F.col("txn_date") == "2026-06-01")
one.explain()
print("pruned", one.count())''',
    )

    # --- 10 ---
    md(
        parts,
        """## 10) Accidental cross join

**Smell:** join with no key → rows explode.  
**Fix:** always join ON a key; check counts.""",
    )
    code(
        parts,
        '''# PROBLEM — 5 x 5 = 25
a = df.select("transaction_id").limit(5)
b = df.select(F.col("transaction_id").alias("id2")).limit(5)
print("cross rows", a.crossJoin(b).count())''',
    )
    md(parts, """### Fix 10 — join on key""")
    code(
        parts,
        '''# FIX
b2 = df.select("transaction_id", "amount").limit(5)
print("inner rows", a.join(b2, "transaction_id").count())''',
    )

    # --- 11 ---
    md(
        parts,
        """## 11) Python UDF is slow

**Smell:** `@udf` row-by-row.  
**Fix:** use `F.when`.""",
    )
    code(
        parts,
        '''# PROBLEM
from pyspark.sql.functions import udf

@udf("string")
def risk(x):
    return "high" if x and float(x) > 1000 else "ok"

df.withColumn("risk", risk(F.col("amount"))).groupBy("risk").count().show()''',
    )
    md(parts, """### Fix 11 — native when""")
    code(
        parts,
        '''# FIX
(
    df.withColumn("risk", F.when(F.col("amount") > 1000, "high").otherwise("ok"))
    .groupBy("risk").count().show()
)''',
    )

    # --- 12 ---
    md(
        parts,
        """## 12) Spill risk — heavy agg on few partitions

**Smell:** `collect_list` + `coalesce(2)` → memory pressure / spill in UI.  
**Fix:** only sum/count; more partitions.""",
    )
    code(
        parts,
        '''# PROBLEM
(
    df.coalesce(2)
    .groupBy("channel")
    .agg(F.collect_list("transaction_id").alias("ids"))
    .show(truncate=False)
)''',
    )
    md(parts, """### Fix 12 — light metrics + repartition""")
    code(
        parts,
        '''# FIX
(
    df.repartition(8, "channel")
    .groupBy("channel")
    .agg(F.count("*").alias("n"), F.round(F.sum("amount"), 2).alias("gbp"))
    .show()
)''',
    )

    # cheat sheet + exit
    md(
        parts,
        """## Cheat sheet

| UI word | Meaning |
|---------|---------|
| Job | one action |
| Stage | work between shuffles |
| Task | one partition |
| Exchange | shuffle |
| BroadcastExchange | small table mailed out |
| Spill | used disk (bad) |

**Also see:** `/Shared/day9/perf_databricks_adf_lab` for system tables + ADF.""",
    )
    code(
        parts,
        '''print("DONE — open Spark UI and re-run any problem/fix pair")
dbutils.notebook.exit('{"lab":"spark_dag_problems_lab","status":"Succeeded"}')''',
    )

    OUT.write_text("\n".join(finish(parts)) + "\n", encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
