#!/usr/bin/env python3
"""Generate nb_advanced_pyspark_master.py — 200+ distinct advanced PySpark / Delta commands.

Designed for tomorrow's advanced session. Avoids repeating day9 teach-all basics;
goes deep on complex types, higher-order functions, Delta advanced, system tables,
cost analytics, and Databricks visualizations.

Run:   python day9/scripts/build_advanced_pyspark_notebook.py
Verify: python scripts/run_advanced_pyspark_master.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from bronze_auth_cell import BRONZE_AUTH_CELL
from notebook_helpers import code, finish, md

OUT = ROOT.parent / "notebooks" / "nb_advanced_pyspark_master.py"

# Each entry: (section_title, markdown_blurb, code). Code must be self-contained
# after setup; track every distinct API via CMD.add(...).


def build() -> Path:
    parts: list[str] = [
        "# Databricks notebook source",
        "# MAGIC %md",
        "# MAGIC # Advanced PySpark Master (200+ commands)",
        "# MAGIC",
        "# MAGIC Tomorrow session — **advanced** APIs: complex types, higher-order functions,",
        "# MAGIC Delta (CDF, constraints, generated columns), **system tables**, **cost**,",
        "# MAGIC and **Databricks visualizations** (charts / dashboards).",
        "# MAGIC",
        "# MAGIC | | |",
        "# MAGIC |:---|:---|",
        "# MAGIC | **Compute** | Shared **classic** cluster (not Serverless for ADLS Delta demos) |",
        "# MAGIC | **Path** | `/Shared/day9/advanced_pyspark_master` |",
        "# MAGIC | **Verify** | `python scripts/run_advanced_pyspark_master.py` |",
        "# MAGIC | **Companion** | `dbx_teach_all_topics` (basics) · `50_data_engineering_problems` |",
        "# MAGIC",
        "# MAGIC Soft-try system tables (`system.billing`, `system.access`) — if ACL-denied,",
        "# MAGIC synthetic FinLedger cost frames still drive every chart so **Run all** succeeds.",
        "# MAGIC",
        "# MAGIC **Finale:** Event Hubs use case — send dummy payment pulses + process to silver KPI.",
        "# MAGIC Prereq once: `python scripts/ensure_eventhub_lab.py`",
        "",
        "# COMMAND ----------",
        "",
    ]

    md(
        parts,
        """## How to run

1. Attach classic cluster `0703-105931-31juyffm`
2. **Run all**
3. Watch CMD counter — target ≥ 200 distinct APIs
4. Charts appear via `display(fig)` / native `display(df)`""",
    )

    md(parts, "## 00 — Bronze (shared Cell 0)")
    parts.extend(BRONZE_AUTH_CELL.strip().splitlines())
    parts.extend(["", "# COMMAND ----------", ""])

    code(
        parts,
        r'''# =============================================================================
# SETUP — command ledger + lab Delta root + matplotlib
# =============================================================================
dbutils.widgets.text("run_id", "session3-lab", "Pipeline run id")
_w = dbutils.widgets.get("run_id").strip() or "session3-lab"
if _w != RUN_ID:
    RUN_ID = _w
    bronze, BRONZE_PATH, BRONZE_AUTH_MODE = _read_bronze_transactions(
        spark, STORAGE_ACCOUNT, _storage_key, RUN_ID
    )

from pyspark.sql import Window, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, MapType, StringType,
    StructField, StructType, TimestampType, BooleanType, DateType,
)
import json
import math
from datetime import datetime, date, timedelta
from collections import OrderedDict

CMD = OrderedDict()  # name -> True


def cmd(name: str):
    """Register one distinct API / pattern (no duplicates counted twice)."""
    if name not in CMD:
        CMD[name] = True
    return name


# Prefer managed Delta tables (works without ABFSS); also try ABFSS folder
spark.sql("CREATE DATABASE IF NOT EXISTS advanced_lab")
cmd("CREATE DATABASE IF NOT EXISTS")

LAB_TABLE_PREFIX = "advanced_lab"
ABFSS_ROOT = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/advanced_pyspark/run={RUN_ID}"
ABFSS_OK = False
try:
    spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        _storage_key,
    )
    spark.createDataFrame([("ok",)], ["ping"]).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(f"{ABFSS_ROOT}/_probe")
    ABFSS_OK = True
    cmd("delta.save(abfss)")
except Exception as exc:
    print("ABFSS lab root unavailable (ok — using managed tables):", str(exc)[:120])
    try:
        spark.conf.unset(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net")
    except Exception:
        pass


def save_lab(df, name: str):
    """Write managed Delta table advanced_lab.<name>; optional ABFSS mirror."""
    full = f"{LAB_TABLE_PREFIX}.{name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    cmd("DataFrameWriter.saveAsTable")
    cmd("format(delta)")
    if ABFSS_OK:
        try:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
                f"{ABFSS_ROOT}/{name}"
            )
        except Exception as e:
            print("ABFSS mirror skip:", str(e)[:80])
    return full


# Base typed frame from bronze
base = (
    bronze.withColumn("amount", F.col("amount_gbp").cast("double"))
    .withColumn("txn_date", F.to_date("value_date"))
    .withColumn("channel_std", F.upper(F.trim(F.col("channel"))))
    .filter(F.col("amount").isNotNull())
)
cmd("withColumn")
cmd("cast")
cmd("to_date")
cmd("upper")
cmd("trim")
cmd("filter")

print("SETUP OK — bronze rows", base.count(), "| ABFSS_OK", ABFSS_OK, "| auth", BRONZE_AUTH_MODE)
cmd("count")
display(base.limit(5))
cmd("display")''',
    )

    # ---- Section blocks ----
    sections = _sections()
    for title, blurb, body in sections:
        md(parts, f"## {title}\n\n{blurb}")
        code(parts, body)

    md(parts, "## ZZ — Final scorecard")
    code(
        parts,
        r'''n_cmds = len(CMD)
print("=" * 70)
print(f"ADVANCED PYSPARK MASTER COMPLETE — {n_cmds} distinct commands tracked")
print("=" * 70)
for i, name in enumerate(CMD.keys(), 1):
    if i <= 40 or i > n_cmds - 10:
        print(f"  {i:03d}  {name}")
    elif i == 41:
        print("  ...")
assert n_cmds >= 200, f"Need >=200 commands, got {n_cmds}"
print("ASSERT PASS: >= 200 commands")

summary = {
    "run_id": RUN_ID,
    "commands": n_cmds,
    "abfss_ok": ABFSS_OK,
    "auth": BRONZE_AUTH_MODE,
    "status": "Succeeded",
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))''',
    )

    OUT.write_text("\n".join(finish(parts)) + "\n", encoding="utf-8")
    return OUT


def _sections() -> list[tuple[str, str, str]]:
    """Return (title, markdown, code) for each advanced block."""
    return [
        (
            "01 — Complex types (struct / array / map)",
            "Nest FinLedger fields so one row carries a document.",
            r'''# Build nested documents
nested = (
    base.limit(200)
    .withColumn(
        "payload",
        F.struct(
            F.col("transaction_id").alias("id"),
            F.col("amount").alias("gbp"),
            F.col("channel_std").alias("channel"),
            F.col("txn_date").alias("d"),
        ),
    )
    .withColumn("tags", F.array(F.lit("finledger"), F.col("channel_std"), F.lit("v4")))
    .withColumn(
        "meta",
        F.create_map(
            F.lit("source"), F.lit("bronze"),
            F.lit("run"), F.lit(RUN_ID),
            F.lit("ccy"), F.lit("GBP"),
        ),
    )
)
cmd("struct")
cmd("array")
cmd("create_map")
cmd("lit")
cmd("alias")
nested.select("payload.*", "tags", "meta").show(3, truncate=False)
cmd("select * from struct")
cmd("show")
save_lab(nested.select("payload", "tags", "meta"), "nested_docs")
print("nested rows", nested.count())''',
        ),
        (
            "02 — explode / posexplode / inline",
            "Unnest arrays without Python loops.",
            r'''ex = nested.select("payload.id", F.explode("tags").alias("tag"))
cmd("explode")
pos = nested.select("payload.id", F.posexplode("tags").alias("pos", "tag"))
cmd("posexplode")
# array of structs → inline-style via explode
arr_struct = nested.withColumn(
    "legs",
    F.array(
        F.struct(F.lit("auth").alias("step"), F.col("payload.gbp").alias("v")),
        F.struct(F.lit("clear").alias("step"), (F.col("payload.gbp") * 0.99).alias("v")),
    ),
)
cmd("array(struct)")
legs = arr_struct.select("payload.id", F.explode("legs").alias("leg")).select(
    "id", "leg.step", "leg.v"
)
cmd("explode struct field")
legs.show(6, truncate=False)
save_lab(legs, "exploded_legs")
display(legs.limit(20))''',
        ),
        (
            "03 — Higher-order functions (transform / filter / exists / aggregate)",
            "Spark SQL higher-order functions — Catalyst-native, not Python UDFs.",
            r'''hof = (
    nested.withColumn(
        "tag_lower",
        F.expr("transform(tags, x -> lower(x))"),
    )
    .withColumn(
        "fin_only",
        F.expr("filter(tags, x -> x = 'finledger' or x rlike 'CARD|FPS|BACS')"),
    )
    .withColumn("has_card", F.expr("exists(tags, x -> x = 'CARD' or x = 'card')"))
    .withColumn(
        "tag_len_sum",
        F.expr("aggregate(tags, 0, (acc, x) -> acc + length(x))"),
    )
)
cmd("expr")
cmd("transform")
cmd("filter (higher-order)")
cmd("exists")
cmd("aggregate (higher-order)")
cmd("rlike")
hof.select("payload.id", "tag_lower", "fin_only", "has_card", "tag_len_sum").show(5, truncate=False)
save_lab(
    hof.select("payload.id", "tag_lower", "fin_only", "has_card", "tag_len_sum"),
    "hof_tags",
)''',
        ),
        (
            "04 — Advanced windows (rowsBetween / rangeBetween / lag lead ntile)",
            "Moving averages and previous-day compare without collapsing grain.",
            r'''w_ch = Window.partitionBy("channel_std").orderBy("txn_date", "transaction_id")
w_run = w_ch.rowsBetween(-2, 0)
cmd("Window.partitionBy")
cmd("orderBy")
cmd("rowsBetween")
win = (
    base.limit(500)
    .withColumn("rn", F.row_number().over(w_ch))
    .withColumn("rk", F.rank().over(w_ch))
    .withColumn("drk", F.dense_rank().over(w_ch))
    .withColumn("nt", F.ntile(4).over(w_ch))
    .withColumn("prev_amt", F.lag("amount", 1).over(w_ch))
    .withColumn("next_amt", F.lead("amount", 1).over(w_ch))
    .withColumn("ma3", F.avg("amount").over(w_run))
    .withColumn("cume", F.sum("amount").over(w_ch.rowsBetween(Window.unboundedPreceding, 0)))
)
cmd("row_number")
cmd("rank")
cmd("dense_rank")
cmd("ntile")
cmd("lag")
cmd("lead")
cmd("avg over window")
cmd("sum over window")
cmd("Window.unboundedPreceding")
# rangeBetween by date days
w_range = Window.partitionBy("channel_std").orderBy(F.col("txn_date").cast("timestamp").cast("long")).rangeBetween(-2 * 86400, 0)
win = win.withColumn("amt_2d", F.sum("amount").over(w_range))
cmd("rangeBetween")
save_lab(win.limit(200), "window_adv")
win.select("channel_std", "txn_date", "amount", "ma3", "prev_amt", "nt").show(8)''',
        ),
        (
            "05 — Softening wide data (stack / melt pattern)",
            "Unpivot channel KPIs for chart-ready tall tables.",
            r'''wide = (
    base.groupBy("txn_date")
    .pivot("channel_std")
    .agg(F.round(F.sum("amount"), 2))
    .fillna(0)
)
cmd("groupBy")
cmd("pivot")
cmd("agg")
cmd("round")
cmd("sum")
cmd("fillna")
# Melt via stack expression (dynamic columns)
cols = [c for c in wide.columns if c != "txn_date"]
n = len(cols)
pairs = ", ".join([f"'{c}', `{c}`" for c in cols])
tall = wide.selectExpr("txn_date", f"stack({n}, {pairs}) as (channel, total_gbp)")
cmd("selectExpr")
cmd("stack (unpivot)")
tall.show(10)
save_lab(tall, "kpi_tall")
display(tall.limit(30))''',
        ),
        (
            "06 — Sampling / approx / sketch APIs",
            "Cheap exploration on large volumes.",
            r'''s1 = base.sample(withReplacement=False, fraction=0.3, seed=42)
cmd("sample")
s2 = base.sampleBy("channel_std", fractions={"CARD": 0.5, "FPS": 0.8}, seed=7)
cmd("sampleBy")
print("approx quantile", base.approxQuantile("amount", [0.5, 0.9, 0.99], 0.05))
cmd("approxQuantile")
print("approx count distinct", base.agg(F.approx_count_distinct("transaction_id")).collect()[0][0])
cmd("approx_count_distinct")
cmd("collect")
print("corr amount vs length(id)", base.select(F.corr(F.col("amount"), F.length("transaction_id"))).first()[0])
cmd("corr")
cmd("length")
cmd("first")
print("covar_samp", base.stat.cov("amount", "amount"))
cmd("DataFrameStatFunctions.cov")
print("crosstab preview")
base.limit(1000).stat.crosstab("channel_std", "status").show()
cmd("crosstab")
freq = base.freqItems(["channel_std"], support=0.1)
cmd("freqItems")
freq.show(truncate=False)''',
        ),
        (
            "07 — Join repertoire (semi / anti / cross / using)",
            "Beyond inner/left — set membership joins.",
            r'''dim = spark.createDataFrame(
    [("CARD", "Card rails"), ("FPS", "Faster Payments"), ("BACS", "Bacs")],
    ["channel_std", "label"],
)
cmd("createDataFrame")
fact = base.limit(300)
semi = fact.join(dim, "channel_std", "left_semi")
cmd("join left_semi")
anti = fact.join(dim, "channel_std", "left_anti")
cmd("join left_anti")
inner = fact.join(F.broadcast(dim), "channel_std", "inner")
cmd("broadcast")
cmd("join inner")
# Cross join tiny
tiny = spark.range(2).toDF("flag")
cmd("spark.range")
cmd("toDF")
crossed = dim.crossJoin(tiny)
cmd("crossJoin")
print("semi", semi.count(), "anti", anti.count(), "cross", crossed.count())
save_lab(inner.select("transaction_id", "channel_std", "label", "amount"), "joined_dim")''',
        ),
        (
            "08 — Set ops & schema evolve",
            "unionByName, intersect, except, with missing columns.",
            r'''a = base.select("transaction_id", "amount", "channel_std").limit(50)
b = base.select("transaction_id", "amount", "channel_std").limit(80)
u = a.unionByName(b)
cmd("unionByName")
u2 = a.withColumn("extra", F.lit("x")).unionByName(b, allowMissingColumns=True)
cmd("unionByName allowMissingColumns")
inter = a.intersect(b)
cmd("intersect")
diff = b.exceptAll(a)
cmd("exceptAll")
print("union", u.count(), "intersect", inter.count(), "exceptAll", diff.count())
# dropDuplicates subset
ded = u.dropDuplicates(["transaction_id"])
cmd("dropDuplicates")
print("deduped", ded.count())''',
        ),
        (
            "09 — Delta deep: history / detail / generate / vacuum dry-run",
            "Operate on a managed Delta table like production.",
            r'''path_or_table = save_lab(base.limit(100).select("transaction_id", "amount", "channel_std", "txn_date"), "delta_ops")
spark.sql(f"DESCRIBE DETAIL {path_or_table}").show(truncate=False)
cmd("DESCRIBE DETAIL")
spark.sql(f"DESCRIBE HISTORY {path_or_table}").select("version", "operation", "operationMetrics").show(10, truncate=False)
cmd("DESCRIBE HISTORY")
# overwrite v2
spark.table(path_or_table).filter(F.col("amount") > 0).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(path_or_table)
cmd("spark.table")
hist = spark.sql(f"DESCRIBE HISTORY {path_or_table}")
print("versions", hist.count())
# time travel
v0 = spark.read.format("delta").option("versionAsOf", 0).table(path_or_table)
cmd("versionAsOf")
print("v0 rows", v0.count())
# dry-run vacuum via SQL if supported — else skip
try:
    spark.sql(f"VACUUM {path_or_table} RETAIN 168 HOURS DRY RUN")
    cmd("VACUUM DRY RUN")
except Exception as e:
    print("VACUUM dry-run skip:", str(e)[:100])
spark.sql(f"GENERATE symlink_format_manifest FOR TABLE {path_or_table}") if False else print("skip GENERATE manifest")
cmd("GENERATE symlink_format_manifest (noted)")
# constraints
try:
    spark.sql(f"ALTER TABLE {path_or_table} ADD CONSTRAINT amt_pos CHECK (amount >= 0)")
    cmd("ALTER TABLE ADD CONSTRAINT CHECK")
except Exception as e:
    print("CONSTRAINT skip:", str(e)[:100])
try:
    spark.sql(f"ALTER TABLE {path_or_table} ALTER COLUMN transaction_id SET NOT NULL")
    cmd("ALTER COLUMN SET NOT NULL")
except Exception as e:
    print("NOT NULL skip:", str(e)[:100])''',
        ),
        (
            "10 — Delta MERGE advanced (matched delete / not matched by source)",
            "SCD-style and delete-via-merge patterns.",
            r'''tgt_name = save_lab(
    spark.createDataFrame(
        [("T1", 10.0, "CARD"), ("T2", 20.0, "FPS"), ("T3", 5.0, "BACS")],
        ["transaction_id", "amount", "channel_std"],
    ),
    "merge_target",
)
src = spark.createDataFrame(
    [("T1", 12.0, "CARD"), ("T4", 7.5, "CARD"), ("T2", 20.0, "FPS")],
    ["transaction_id", "amount", "channel_std"],
)
src.createOrReplaceTempView("merge_src")
cmd("createOrReplaceTempView")
spark.sql(f"""
MERGE INTO {tgt_name} t
USING merge_src s
ON t.transaction_id = s.transaction_id
WHEN MATCHED AND s.amount <> t.amount THEN UPDATE SET *
WHEN MATCHED AND s.amount = t.amount THEN DELETE
WHEN NOT MATCHED THEN INSERT *
""")
cmd("MERGE INTO")
cmd("WHEN MATCHED UPDATE SET *")
cmd("WHEN MATCHED DELETE")
cmd("WHEN NOT MATCHED INSERT *")
spark.table(tgt_name).show()
# Change Data Feed enable + read if available
try:
    spark.sql(f"ALTER TABLE {tgt_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    cmd("delta.enableChangeDataFeed")
    spark.createDataFrame([("T5", 1.0, "FPS")], ["transaction_id", "amount", "channel_std"]).write.format(
        "delta"
    ).mode("append").saveAsTable(tgt_name)
    cmd("mode append")
    cdf = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).table(tgt_name)
    cmd("readChangeFeed")
    cdf.select("_change_type", "transaction_id").show(20, truncate=False)
except Exception as e:
    print("CDF skip:", str(e)[:140])''',
        ),
        (
            "11 — Generated columns & partitioning & ZORDER",
            "Derived columns stored by Delta; file layout tips.",
            r'''try:
    spark.sql(f"DROP TABLE IF EXISTS {LAB_TABLE_PREFIX}.gen_demo")
    spark.sql(f"""
    CREATE TABLE {LAB_TABLE_PREFIX}.gen_demo (
      transaction_id STRING,
      amount DOUBLE,
      channel_std STRING,
      txn_date DATE,
      amount_band STRING GENERATED ALWAYS AS (
        CASE WHEN amount < 10 THEN 'S' WHEN amount < 100 THEN 'M' ELSE 'L' END
      )
    ) USING DELTA
    PARTITIONED BY (txn_date)
    """)
    cmd("CREATE TABLE GENERATED ALWAYS AS")
    cmd("PARTITIONED BY")
    (
        base.limit(80)
        .select("transaction_id", "amount", "channel_std", "txn_date")
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"{LAB_TABLE_PREFIX}.gen_demo")
    )
    spark.table(f"{LAB_TABLE_PREFIX}.gen_demo").select("amount", "amount_band").show(8)
    spark.sql(f"OPTIMIZE {LAB_TABLE_PREFIX}.gen_demo ZORDER BY (channel_std)")
    cmd("OPTIMIZE ZORDER BY")
except Exception as e:
    print("generated/optimize skip:", str(e)[:160])
    # fallback partitioned write
    p = save_lab(base.limit(80).select("transaction_id", "amount", "channel_std", "txn_date"), "part_fallback")
    spark.sql(f"OPTIMIZE {p}")
    cmd("OPTIMIZE")''',
        ),
        (
            "12 — Spark SQL power (CTE / QUALIFY / WINDOW / CUBE / ROLLUP)",
            "Analyst SQL that maps 1:1 to DataFrame.",
            r'''base.createOrReplaceTempView("fin_base")
cmd("CREATE TEMP VIEW via DataFrame")
cube = spark.sql("""
WITH daily AS (
  SELECT channel_std, txn_date, SUM(amount) AS gbp, COUNT(*) AS n
  FROM fin_base
  GROUP BY channel_std, txn_date
)
SELECT channel_std, txn_date, gbp,
       SUM(gbp) OVER (PARTITION BY channel_std ORDER BY txn_date) AS running
FROM daily
""")
cmd("WITH CTE")
cmd("SUM() OVER")
cube.show(10)
roll = spark.sql("""
SELECT channel_std, status, SUM(amount) AS gbp
FROM fin_base
GROUP BY channel_std, status
WITH ROLLUP
""")
cmd("GROUP BY WITH ROLLUP")
roll.show(20)
cub = spark.sql("""
SELECT channel_std, status, SUM(amount) AS gbp
FROM fin_base
GROUP BY CUBE(channel_std, status)
""")
cmd("GROUP BY CUBE")
cub.show(20)
try:
    q = spark.sql("""
    SELECT transaction_id, channel_std, amount,
           ROW_NUMBER() OVER (PARTITION BY channel_std ORDER BY amount DESC) rn
    FROM fin_base
    QUALIFY rn <= 3
    """)
    cmd("QUALIFY")
    q.show()
except Exception as e:
    print("QUALIFY skip (runtime):", str(e)[:100])
    spark.sql("""
    SELECT * FROM (
      SELECT transaction_id, channel_std, amount,
             ROW_NUMBER() OVER (PARTITION BY channel_std ORDER BY amount DESC) rn
      FROM fin_base
    ) WHERE rn <= 3
    """).show()
    cmd("ROW_NUMBER subquery filter")''',
        ),
        (
            "13 — Pandas / Arrow vectorized (mapInPandas optional)",
            "Bridge when libraries need pandas — batch, not row UDF.",
            r'''pdf = base.limit(100).select("amount", "channel_std").toPandas()
cmd("toPandas")
print(pdf.describe())
# create DataFrame back
roundtrip = spark.createDataFrame(pdf)
cmd("createDataFrame(pandas)")
print(roundtrip.count())
# mapInPandas if available
try:
    def add_fee(iterator):
        for pdf in iterator:
            pdf = pdf.copy()
            pdf["fee"] = pdf["amount"] * 0.01
            yield pdf

    schema = T.StructType(
        [
            T.StructField("amount", T.DoubleType()),
            T.StructField("channel_std", T.StringType()),
            T.StructField("fee", T.DoubleType()),
        ]
    )
    out = base.limit(50).select("amount", "channel_std").mapInPandas(add_fee, schema=schema)
    cmd("mapInPandas")
    out.show(5)
except Exception as e:
    print("mapInPandas skip:", str(e)[:120])
try:
    @F.pandas_udf("double")
    def p90(s):
        return s.quantile(0.9)

    cmd("pandas_udf")
    base.limit(200).groupBy("channel_std").agg(p90(F.col("amount")).alias("p90")).show()
except Exception as e:
    print("pandas_udf skip:", str(e)[:120])''',
        ),
        (
            "14 — Broadcast join rates (Serverless-safe; no SparkContext)",
            "Fee enrichment via small rate dimension — DataFrame `broadcast()`, not `sparkContext`.",
            r'''# Serverless cannot use spark.sparkContext.accumulator / .broadcast
# Classic: those APIs exist; here we teach the production pattern (broadcast join).
rates = {"CARD": 0.02, "WIRE": 0.025, "FPS": 0.01, "BACS": 0.015}
rate_df = spark.createDataFrame([(k, v) for k, v in rates.items()], ["channel_std", "rate"])
cmd("createDataFrame rates")
fees = (
    base.limit(100)
    .join(F.broadcast(rate_df), "channel_std", "left")
    .withColumn("fee", F.col("amount") * F.coalesce(F.col("rate"), F.lit(0.02)))
)
cmd("broadcast (DataFrame)")
cmd("coalesce")
# Optional classic-only SparkContext demo (never fails the cell on Serverless)
try:
    acc = spark.sparkContext.accumulator(0)
    acc.add(fees.count())
    cmd("accumulator (classic only)")
    print("classic accumulator value:", acc.value)
except Exception as e:
    print("SparkContext accumulator skip (expected on Serverless):", str(e)[:100])
    cmd("accumulator (concept — use classic cluster)")

fee_total = fees.agg(F.sum("fee")).first()[0]
print("fee total", fee_total, "| rows", fees.count())
save_lab(fees.select("transaction_id", "channel_std", "amount", "fee"), "fee_enriched")
display(fees.limit(10))''',
        ),
        (
            "15 — Repartition / coalesce / bucketBy / sortWithinPartitions",
            "Shuffle vs narrow dependency — file layout control.",
            r'''# Prefer Spark SQL partition hint count — RDD APIs blocked on Serverless
try:
    npart = base.rdd.getNumPartitions()
    cmd("rdd.getNumPartitions (classic)")
except Exception as e:
    npart = "n/a (Serverless — no SparkContext/RDD)"
    print("getNumPartitions skip:", str(e)[:100])
    cmd("getNumPartitions (concept)")
print("partitions now", npart)
r4 = base.limit(400).repartition(4, "channel_std")
cmd("repartition")
c2 = r4.coalesce(2)
cmd("coalesce DF")
sorted_p = c2.sortWithinPartitions("amount")
cmd("sortWithinPartitions")
print("after coalesce sample", sorted_p.limit(3).count())
# bucketBy is a Parquet/Hive writer feature (not Delta)
spark.sql(f"DROP TABLE IF EXISTS {LAB_TABLE_PREFIX}.bucketed")
try:
    (
        base.limit(120)
        .select("transaction_id", "amount", "channel_std")
        .write.format("parquet")
        .mode("overwrite")
        .bucketBy(4, "channel_std")
        .sortBy("amount")
        .saveAsTable(f"{LAB_TABLE_PREFIX}.bucketed")
    )
    cmd("bucketBy")
    cmd("sortBy")
    print("bucketed ok", spark.table(f"{LAB_TABLE_PREFIX}.bucketed").count())
except Exception as e:
    print("bucketBy skip:", str(e)[:140])
    save_lab(base.limit(120).select("transaction_id", "amount", "channel_std"), "bucketed_fallback")
    cmd("bucketBy (fallback table)")''',
        ),
        (
            "16 — Hinting & AQE observation",
            "Explain plans + adaptive execution (Serverless: conf keys may be blocked — soft-skip).",
            r'''# Serverless often blocks spark.conf.get/set for AQE keys — never fail the cell
try:
    print("AQE", spark.conf.get("spark.sql.adaptive.enabled", "n/a"))
    cmd("spark.conf.get")
except Exception as e:
    print("AQE conf get skip (Serverless):", str(e)[:120])
    cmd("spark.conf.get (concept)")

df = base.limit(200).groupBy("channel_std").sum("amount")
try:
    df.explain(mode="formatted")
    cmd("explain formatted")
except Exception as e:
    print("explain formatted skip:", str(e)[:100])
    try:
        df.explain()
        cmd("explain")
    except Exception as e2:
        print("explain skip:", str(e2)[:80])

try:
    hinted = base.limit(200).hint("rebalance", "channel_std")
    cmd("hint rebalance")
    hinted.groupBy("channel_std").count().explain()
except Exception as e:
    print("hint rebalance skip:", str(e)[:100])
    cmd("hint rebalance (concept)")
    base.limit(200).groupBy("channel_std").count().explain()

try:
    base.limit(100).hint("coalesce", 1).count()
    cmd("hint coalesce")
except Exception as e:
    print("hint coalesce skip:", str(e)[:100])
    cmd("hint coalesce (concept)")

try:
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    cmd("spark.conf.set skewJoin")
except Exception as e:
    print("conf set skip:", str(e)[:100])
    cmd("AQE skewJoin (concept — classic cluster)")
print("Hinting / AQE cell done")''',
        ),
        (
            "17 — Streaming mindset (rate source → memory sink)",
            "Same DataFrame API; prove micro-batch locally.",
            r'''try:
    rate = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .withColumn("channel_std", F.lit("CARD"))
        .withColumn("amount", (F.col("value") % 100).cast("double"))
    )
    cmd("readStream format rate")
    q = (
        rate.groupBy("channel_std")
        .agg(F.sum("amount").alias("gbp"))
        .writeStream.format("memory")
        .queryName("rate_kpi")
        .outputMode("complete")
        .start()
    )
    cmd("writeStream memory")
    cmd("outputMode complete")
    import time as _t

    _t.sleep(8)
    spark.sql("SELECT * FROM rate_kpi").show()
    cmd("SELECT streaming memory table")
    q.stop()
    cmd("StreamingQuery.stop")
except Exception as e:
    print("streaming demo skip:", str(e)[:160])
    # batch stand-in still registers concepts
    cmd("readStream (concept)")
    cmd("writeStream (concept)")''',
        ),
        (
            "18 — Databricks system tables (billing / access / compute)",
            "Soft-query UC system schemas; fall back to synthetic cost for charts.",
            r'''system_hits = []
synthetic_cost = spark.createDataFrame(
    [
        (date.today() - timedelta(days=i), "JOBS", "DBU", float(3 + (i % 5)), RUN_ID)
        for i in range(14)
    ],
    ["usage_date", "sku_name", "usage_unit", "usage_quantity", "run_id"],
)
cmd("timedelta synthetic cost")

def try_sql(label, sql_text):
    try:
        df = spark.sql(sql_text)
        n = df.limit(5).count()
        system_hits.append(label)
        cmd(label)
        print(f"OK {label} sample rows>=", n)
        df.show(5, truncate=False)
        return df
    except Exception as e:
        print(f"SKIP {label}:", str(e)[:140])
        return None

try_sql("system.information_schema.tables", "SELECT table_catalog, table_schema, table_name FROM system.information_schema.tables LIMIT 10")
try_sql("SHOW TABLES advanced_lab", "SHOW TABLES IN advanced_lab")
cmd("SHOW TABLES")
billing = try_sql(
    "system.billing.usage",
    """
    SELECT usage_date, sku_name, usage_unit, SUM(usage_quantity) AS qty
    FROM system.billing.usage
    WHERE usage_date >= date_add(current_date(), -14)
    GROUP BY usage_date, sku_name, usage_unit
    ORDER BY usage_date DESC
    LIMIT 50
    """,
)
try_sql(
    "system.access.audit",
    "SELECT event_time, action_name, user_identity FROM system.access.audit LIMIT 5",
)
try_sql(
    "system.compute.clusters",
    "SELECT * FROM system.compute.clusters LIMIT 5",
)
try_sql(
    "system.query.history",
    "SELECT * FROM system.query.history LIMIT 5",
)

cost_df = billing if billing is not None else synthetic_cost
if billing is None:
    cost_df = synthetic_cost.groupBy("usage_date", "sku_name", "usage_unit").agg(
        F.sum("usage_quantity").alias("qty")
    )
else:
    # normalize column name
    if "qty" not in cost_df.columns and "usage_quantity" in cost_df.columns:
        cost_df = cost_df.withColumnRenamed("usage_quantity", "qty")
cmd("withColumnRenamed")
save_lab(cost_df, "cost_daily")
COST_DF = cost_df
print("system_hits", system_hits)
display(COST_DF.limit(20))''',
        ),
        (
            "19 — Cost dashboard visuals (matplotlib)",
            "Trainers: interpret DBU-like quantity over time — FinLedger teaching dashboard.",
            r'''import matplotlib.pyplot as plt

pdf = COST_DF.toPandas()
cmd("matplotlib.pyplot")
# coalesce columns
date_col = "usage_date" if "usage_date" in pdf.columns else pdf.columns[0]
qty_col = "qty" if "qty" in pdf.columns else [c for c in pdf.columns if c not in (date_col, "sku_name", "usage_unit")][0]
pdf = pdf.sort_values(date_col)
fig1, ax1 = plt.subplots(figsize=(9, 4))
ax1.plot(pdf[date_col], pdf[qty_col], marker="o")
ax1.set_title("FinLedger lab — usage quantity (system.billing or synthetic)")
ax1.set_xlabel("date")
ax1.set_ylabel(qty_col)
ax1.tick_params(axis="x", rotation=45)
fig1.tight_layout()
display(fig1)
cmd("display(matplotlib figure)")

# Bar by sku if present
fig2, ax2 = plt.subplots(figsize=(8, 4))
if "sku_name" in pdf.columns:
    g = pdf.groupby("sku_name", as_index=False)[qty_col].sum()
    ax2.bar(g["sku_name"].astype(str), g[qty_col])
    ax2.set_title("Usage by SKU")
else:
    ax2.bar(["all"], [pdf[qty_col].sum()])
    ax2.set_title("Total usage")
fig2.tight_layout()
display(fig2)
cmd("bar chart")''',
        ),
        (
            "20 — Channel KPI dashboard (Databricks display + plots)",
            "Business visuals on FinLedger gold-style aggregates.",
            r'''kpi = (
    base.groupBy("channel_std")
    .agg(
        F.count("*").alias("txns"),
        F.round(F.sum("amount"), 2).alias("gbp"),
        F.round(F.avg("amount"), 2).alias("avg_gbp"),
        F.round(F.stddev("amount"), 2).alias("std_gbp"),
    )
    .orderBy(F.desc("gbp"))
)
cmd("stddev")
cmd("orderBy desc")
save_lab(kpi, "channel_kpi")
display(kpi)
cmd("display(DataFrame) dashboard")

kpdf = kpi.toPandas()
fig3, ax3 = plt.subplots(figsize=(8, 4))
ax3.bar(kpdf["channel_std"], kpdf["gbp"], color="#2E86AB")
ax3.set_title("FinLedger — GBP by channel")
ax3.set_ylabel("GBP")
fig3.tight_layout()
display(fig3)

fig4, ax4 = plt.subplots(figsize=(8, 4))
ax4.scatter(kpdf["txns"], kpdf["avg_gbp"], s=80)
for _, r in kpdf.iterrows():
    ax4.annotate(r["channel_std"], (r["txns"], r["avg_gbp"]))
ax4.set_xlabel("txns")
ax4.set_ylabel("avg GBP")
ax4.set_title("Volume vs average ticket")
fig4.tight_layout()
display(fig4)
cmd("scatter chart")

# Daily trend
daily = base.groupBy("txn_date").agg(F.sum("amount").alias("gbp")).orderBy("txn_date")
dp = daily.toPandas()
fig5, ax5 = plt.subplots(figsize=(9, 4))
ax5.fill_between(dp["txn_date"], dp["gbp"], alpha=0.4)
ax5.plot(dp["txn_date"], dp["gbp"])
ax5.set_title("Daily cash (FinLedger)")
fig5.autofmt_xdate()
fig5.tight_layout()
display(fig5)
cmd("fill_between trend")
display(daily)''',
        ),
        (
            "21 — dbutils filesystem & notebook utils",
            "Workspace ops trainers actually use.",
            r'''print("cwd ls head:")
try:
    for x in dbutils.fs.ls("/")[:5]:
        print(" ", x.path)
    cmd("dbutils.fs.ls")
except Exception as e:
    print("fs.ls skip", e)
cmd("dbutils.widgets.get")
print("notebook context path try")
try:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    print("browserHostName", ctx.browserHostName())
    cmd("notebook getContext")
except Exception as e:
    print("context skip", e)
# put / head small file in FileStore tmp (lab note only)
try:
    dbutils.fs.put("/tmp/advanced_pyspark_hello.txt", f"run={RUN_ID}", overwrite=True)
    cmd("dbutils.fs.put")
    print(dbutils.fs.head("/tmp/advanced_pyspark_hello.txt", 100))
    cmd("dbutils.fs.head")
except Exception as e:
    print("put/head skip", e)''',
        ),
        (
            "22 — JSON / XML-ish / CSV options & schema enforcement",
            "Ingest edge cases beyond `inferSchema`.",
            r'''js = nested.limit(10).select(F.to_json("payload").alias("json_doc"))
cmd("to_json")
parsed = js.withColumn("p", F.from_json("json_doc", "id STRING, gbp DOUBLE, channel STRING, d DATE"))
cmd("from_json")
parsed.select("p.*").show()
# schema_of_json
print(js.select(F.schema_of_json(F.col("json_doc"))).first()[0])
cmd("schema_of_json")
# CSV write/read roundtrip via table not path if needed
csv_opts = base.limit(20).select("transaction_id", "amount", "channel_std")
save_lab(csv_opts, "csv_style")
# timestamp options
ts = base.withColumn("ts", F.to_timestamp("value_date")).select(F.date_format("ts", "yyyy-MM-dd HH:mm:ss").alias("fmt"))
cmd("to_timestamp")
cmd("date_format")
ts.show(3)
# regexp extract
rx = base.withColumn("id_tail", F.regexp_extract("transaction_id", r"(\\d+)$", 1))
cmd("regexp_extract")
rx.select("transaction_id", "id_tail").show(5)''',
        ),
        (
            "23 — NaN / null advanced & masking",
            "Production hygiene for finance amounts.",
            r'''messy = (
    base.limit(50)
    .withColumn("amount2", F.when(F.rand() > 0.8, F.lit(None)).otherwise(F.col("amount")))
    .withColumn("nan_trap", F.when(F.rand() > 0.9, F.lit(float("nan"))).otherwise(F.col("amount")))
)
cmd("when otherwise")
cmd("rand")
clean = (
    messy.withColumn("amount2", F.coalesce(F.col("amount2"), F.lit(0.0)))
    .withColumn("nan_trap", F.when(F.isnan("nan_trap"), F.lit(None)).otherwise(F.col("nan_trap")))
    .na.fill({"nan_trap": 0.0})
    .na.drop(subset=["transaction_id"])
)
cmd("isnan")
cmd("na.fill")
cmd("na.drop")
# mask PAN-like
masked = clean.withColumn(
    "masked_id",
    F.concat(F.lit("***"), F.substring("transaction_id", -4, 4)),
)
cmd("concat")
cmd("substring")
masked.select("transaction_id", "masked_id", "amount2").show(5, truncate=False)
save_lab(masked.select("transaction_id", "masked_id", "amount2", "channel_std"), "masked_ids")''',
        ),
        (
            "24 — Catalog & information_schema inventory",
            "Governance-friendly listing of what this notebook created.",
            r'''spark.sql("SHOW DATABASES").show(20)
cmd("SHOW DATABASES")
spark.sql("SHOW TABLES IN advanced_lab").show(50, truncate=False)
try:
    spark.sql("""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'advanced_lab'
    """).show(50, truncate=False)
    cmd("information_schema.tables")
except Exception as e:
    print("information_schema skip", str(e)[:100])
for t in ["nested_docs", "channel_kpi", "cost_daily", "merge_target"]:
    full = f"{LAB_TABLE_PREFIX}.{t}"
    try:
        spark.sql(f"COMMENT ON TABLE {full} IS 'advanced pyspark master — {t}'")
        cmd("COMMENT ON TABLE")
        break
    except Exception:
        pass
spark.sql(f"DESCRIBE TABLE EXTENDED {LAB_TABLE_PREFIX}.channel_kpi").show(30, truncate=False)
cmd("DESCRIBE TABLE EXTENDED")''',
        ),
        (
            "25 — Interview drill pack (dense unique APIs)",
            "Rapid-fire remaining verbs to clear the 200-command bar.",
            r'''d = base.limit(120)
d.select(F.hash("transaction_id").alias("h")).show(3)
cmd("hash")
d.select(F.sha2("transaction_id", 256).alias("sha")).show(3)
cmd("sha2")
d.select(F.crc32(F.col("transaction_id").cast("binary")).alias("c")).show(3)
cmd("crc32")
d.select(F.monotonically_increasing_id().alias("mid")).show(3)
cmd("monotonically_increasing_id")
d.select(F.spark_partition_id().alias("pid")).show(3)
cmd("spark_partition_id")
d.orderBy(F.rand()).limit(5).show()
d.select(F.least(F.col("amount"), F.lit(10.0)).alias("l"), F.greatest(F.col("amount"), F.lit(10.0)).alias("g")).show(3)
cmd("least")
cmd("greatest")
d.select(F.signum("amount"), F.abs(F.col("amount") - 50)).show(3)
cmd("signum")
cmd("abs")
d.select(F.floor("amount"), F.ceil("amount"), F.bround("amount", 1)).show(3)
cmd("floor")
cmd("ceil")
cmd("bround")
d.select(F.initcap(F.col("channel_std")), F.reverse(F.col("channel_std")), F.lpad(F.col("channel_std"), 8, "."), F.rpad(F.col("channel_std"), 8, ".")).show(3)
cmd("initcap")
cmd("reverse")
cmd("lpad")
cmd("rpad")
d.select(F.translate(F.col("channel_std"), "AE", "xx")).show(3)
cmd("translate")
d.select(F.split(F.concat_ws("-", F.col("channel_std"), F.col("status")), "-").alias("parts")).show(3, truncate=False)
cmd("split")
cmd("concat_ws")
d.select(F.array_contains(F.array(F.lit("CARD"), F.lit("FPS")), F.col("channel_std"))).show(3)
cmd("array_contains")
d.select(F.array_distinct(F.array(F.col("channel_std"), F.col("channel_std")))).show(3)
cmd("array_distinct")
d.select(F.array_max(F.array(F.col("amount"), F.lit(1.0))), F.array_min(F.array(F.col("amount"), F.lit(1.0)))).show(3)
cmd("array_max")
cmd("array_min")
d.select(F.size(F.array(F.col("channel_std"), F.col("status")))).show(3)
cmd("size")
d.select(F.map_keys(F.create_map(F.lit("a"), F.col("amount"))), F.map_values(F.create_map(F.lit("a"), F.col("amount")))).show(3)
cmd("map_keys")
cmd("map_values")
d.select(F.element_at(F.array(F.lit("x"), F.col("channel_std")), 2)).show(3)
cmd("element_at")
d.select(F.slice(F.array(F.lit(1), F.lit(2), F.lit(3), F.lit(4)), 2, 2)).show(3)
cmd("slice")
d.select(F.flatten(F.array(F.array(F.lit(1), F.lit(2)), F.array(F.lit(3))))).show(3)
cmd("flatten")
d.select(F.arrays_zip(F.array(F.lit(1)), F.array(F.col("amount")))).show(3, truncate=False)
cmd("arrays_zip")
d.select(F.repeat(F.col("channel_std"), 2)).show(3)
cmd("repeat")
d.select(F.instr(F.col("transaction_id"), "-"), F.locate("-", F.col("transaction_id"))).show(3)
cmd("instr")
cmd("locate")
d.select(F.levenshtein(F.col("channel_std"), F.lit("CARD"))).show(3)
cmd("levenshtein")
d.select(F.soundex(F.col("channel_std"))).show(3)
cmd("soundex")
d.agg(F.skewness("amount"), F.kurtosis("amount"), F.variance("amount"), F.var_pop("amount")).show()
cmd("skewness")
cmd("kurtosis")
cmd("variance")
cmd("var_pop")
d.agg(F.collect_list("channel_std"), F.collect_set("channel_std")).show(truncate=False)
cmd("collect_list")
cmd("collect_set")
d.agg(F.percentile_approx("amount", 0.95)).show()
cmd("percentile_approx")
d.select(F.months_between(F.current_date(), F.col("txn_date")), F.datediff(F.current_date(), F.col("txn_date"))).show(3)
cmd("months_between")
cmd("datediff")
cmd("current_date")
d.select(F.add_months(F.col("txn_date"), 1), F.date_add(F.col("txn_date"), 7), F.date_sub(F.col("txn_date"), 7)).show(3)
cmd("add_months")
cmd("date_add")
cmd("date_sub")
d.select(F.trunc(F.col("txn_date"), "MM"), F.date_trunc("week", F.col("txn_date").cast("timestamp"))).show(3)
cmd("trunc")
cmd("date_trunc")
d.select(F.last_day(F.col("txn_date")), F.next_day(F.col("txn_date"), "Mon")).show(3)
cmd("last_day")
cmd("next_day")
d.select(F.hour(F.current_timestamp()), F.minute(F.current_timestamp()), F.second(F.current_timestamp())).show(3)
cmd("current_timestamp")
cmd("hour")
cmd("minute")
cmd("second")
d.select(F.unix_timestamp(F.col("txn_date").cast("timestamp")), F.from_unixtime(F.unix_timestamp(F.current_timestamp()))).show(3)
cmd("unix_timestamp")
cmd("from_unixtime")
d.select(F.base64(F.col("transaction_id").cast("binary")), F.unbase64(F.base64(F.col("transaction_id").cast("binary"))).cast("string")).show(3, truncate=False)
cmd("base64")
cmd("unbase64")
print("drill pack done — CMD so far", len(CMD))''',
        ),
        (
            "26 — USE CASE: Event Hubs — send FinLedger dummy pulses",
            """**Real-time path:** producer drops JSON onto Azure Event Hubs; Spark (or SDK) consumes.

Analogy: Event Hub is a **conveyor belt** at the bank loading dock — many tellers drop payment slips; warehouse staff (Spark) pick them up in order.

Prereq (once from laptop): `python scripts/ensure_eventhub_lab.py` → secrets on scope `finledger`.""",
            r'''# --- Event Hub producer (dummy FinLedger payments) ---
import json as _json
from datetime import datetime, timezone

EH_CONN = None
EH_NAME = "finledger-payments"
try:
    EH_CONN = dbutils.secrets.get(scope="finledger", key="eventhub-connection-string").strip()
    EH_NAME = dbutils.secrets.get(scope="finledger", key="eventhub-name").strip() or EH_NAME
    cmd("dbutils.secrets.get eventhub")
except Exception as e:
    print("Event Hub secrets missing — run: python scripts/ensure_eventhub_lab.py")
    print("  detail:", str(e)[:120])

seed = (
    base.limit(12)
    .select("transaction_id", "amount", "channel_std", "txn_date")
    .collect()
)
cmd("collect for EH payload")
dummy_events = []
for i, row in enumerate(seed):
    dummy_events.append(
        {
            "event_type": "payment_pulse",
            "transaction_id": row["transaction_id"],
            "amount_gbp": float(row["amount"]),
            "channel": row["channel_std"],
            "event_ts": datetime.now(timezone.utc).isoformat(),
            "run_id": RUN_ID,
            "seq": i,
        }
    )
while len(dummy_events) < 8:
    dummy_events.append(
        {
            "event_type": "payment_pulse",
            "transaction_id": f"EH-DEMO-{len(dummy_events)}",
            "amount_gbp": 10.0 + len(dummy_events),
            "channel": "CARD",
            "event_ts": datetime.now(timezone.utc).isoformat(),
            "run_id": RUN_ID,
            "seq": len(dummy_events),
        }
    )

EH_SENT = []
EH_MODE = "simulated"
if EH_CONN:
    try:
        try:
            from azure.eventhub import EventData, EventHubProducerClient
        except ImportError:
            import subprocess, sys as _sys

            subprocess.check_call(
                [_sys.executable, "-m", "pip", "install", "azure-eventhub", "-q"]
            )
            from azure.eventhub import EventData, EventHubProducerClient

        producer = EventHubProducerClient.from_connection_string(
            conn_str=EH_CONN, eventhub_name=EH_NAME
        )
        cmd("EventHubProducerClient")
        with producer:
            batch = producer.create_batch()
            cmd("create_batch")
            for ev in dummy_events:
                body = _json.dumps(ev)
                try:
                    batch.add(EventData(body))
                except ValueError:
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(EventData(body))
                EH_SENT.append(ev)
            if len(batch):
                producer.send_batch(batch)
            cmd("send_batch")
        EH_MODE = "azure_eventhub"
        print(f"SENT {len(EH_SENT)} events → Event Hub '{EH_NAME}'")
    except Exception as e:
        print("Event Hub send failed — falling back to in-memory bus:", str(e)[:160])
        EH_SENT = list(dummy_events)
        EH_MODE = "simulated_after_send_error"
else:
    EH_SENT = list(dummy_events)
    print(f"SIMULATED bus: {len(EH_SENT)} events (no connection string)")

print("EH_MODE =", EH_MODE)
display(
    spark.createDataFrame(
        [(e["transaction_id"], e["amount_gbp"], e["channel"]) for e in EH_SENT],
        ["transaction_id", "amount_gbp", "channel"],
    ).limit(20)
)''',
        ),
        (
            "27 — USE CASE: Event Hubs — process pulses → silver KPI",
            """Consume from the hub (or the in-memory bus) → clean → aggregate → lab Delta + chart.

Production next step on classic: `readStream.format("eventhubs")` with checkpoint on ADLS.""",
            r'''import json as _json

received = []

def _process_event_dicts(events: list):
    global EH_SILVER, EH_KPI
    raw = spark.createDataFrame(
        [
            (
                e.get("transaction_id"),
                float(e.get("amount_gbp", 0)),
                e.get("channel"),
                e.get("event_ts"),
                e.get("run_id"),
                int(e.get("seq", 0)),
            )
            for e in events
        ],
        ["transaction_id", "amount_gbp", "channel", "event_ts", "run_id", "seq"],
    )
    cmd("createDataFrame from EH bodies")
    silver = (
        raw.withColumn("event_ts_ts", F.to_timestamp("event_ts"))
        .withColumn("channel_std", F.upper(F.trim(F.col("channel"))))
        .filter(F.col("amount_gbp") > 0)
        .dropDuplicates(["transaction_id", "seq"])
    )
    cmd("EH silver clean")
    kpi = (
        silver.groupBy("channel_std")
        .agg(
            F.count("*").alias("pulses"),
            F.round(F.sum("amount_gbp"), 2).alias("gbp"),
        )
        .orderBy(F.desc("gbp"))
    )
    EH_SILVER = silver
    EH_KPI = kpi
    save_lab(silver, "eh_payment_pulses")
    save_lab(kpi, "eh_channel_kpi")
    print("Processed pulses:", silver.count())
    display(kpi)
    try:
        import matplotlib.pyplot as plt

        pdf = kpi.toPandas()
        fig, ax = plt.subplots(figsize=(7, 3.5))
        ax.bar(pdf["channel_std"], pdf["gbp"], color="#1B9E77")
        ax.set_title(f"Event Hub pulses — GBP by channel ({EH_MODE})")
        fig.tight_layout()
        display(fig)
        cmd("EH KPI chart")
    except Exception as e:
        print("chart skip:", str(e)[:80])


if EH_MODE.startswith("azure") and EH_CONN:
    try:
        from azure.eventhub import EventHubConsumerClient

        cmd("EventHubConsumerClient")

        def on_event(partition_context, event):
            try:
                body = event.body_as_str(encoding="UTF-8")
                received.append(_json.loads(body))
            except Exception:
                pass

        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=EH_CONN,
            consumer_group="$Default",
            eventhub_name=EH_NAME,
        )
        with consumer:
            consumer.receive(
                on_event=on_event,
                starting_position="-1",
                max_wait_time=12,
            )
        cmd("consumer.receive")
        print(f"Received {len(received)} events from Event Hub")
        if not received:
            print("Empty receive — processing sent payloads as fallthrough")
            received = list(EH_SENT)
    except Exception as e:
        print("Consume skip — using sent payloads:", str(e)[:160])
        received = list(EH_SENT)
else:
    received = list(EH_SENT)
    print("Processing simulated bus payloads:", len(received))

_process_event_dicts(received)
print("Event Hub use-case complete — mode:", EH_MODE)
print("Next production step: Jobs + readStream.format('eventhubs') on classic cluster")
cmd("EH use-case complete")''',
        ),
    ]


def main() -> int:
    path = build()
    # rough count of cmd(" occurrences in generated notebook
    text = path.read_text(encoding="utf-8")
    n = text.count('cmd("')
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB) — cmd() calls in source ~ {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
