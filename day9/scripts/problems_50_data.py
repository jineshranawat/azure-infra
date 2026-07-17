"""All 50 data-engineering problems → runnable Databricks demo code (batch-first, UC tables)."""

from __future__ import annotations

# Each entry: problem, solution, theory (markdown), code (python), uc_table (optional)
PROBLEMS_50: list[dict[str, str]] = [
    {
        "num": "01",
        "problem": "Duplicate Records",
        "solution": "Deduplication",
        "theory": (
            "**WHAT:** Same business key appears more than once in a batch.\n\n"
            "**WHY:** Re-runs, upstream bugs, or append-only loads create duplicates.\n\n"
            "**HOW:** `dropDuplicates(['transaction_id'])` or `DISTINCT` on natural key before silver write."
        ),
        "code": '''# --- Problem 01: Duplicate Records → Deduplication ---
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
deduped.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob01_deduped")
print("UC table created:", f"{UC_TABLE}.prob01_deduped")
spark.table(f"{UC_TABLE}.prob01_deduped").show(3)''',
        "uc_table": "prob01_deduped",
    },
    {
        "num": "02",
        "problem": "Late Arriving Data",
        "solution": "Watermarking",
        "theory": (
            "**WHAT:** Events arrive after the window you already aggregated.\n\n"
            "**WHY:** Mobile/offline sources sync late; streaming windows need bounds.\n\n"
            "**HOW:** In Structured Streaming: `.withWatermark('event_time', '10 minutes')` — "
            "batch lab: filter rows where `event_time` within acceptable lateness window."
        ),
        "code": '''# --- Problem 02: Late Arriving Data → Watermarking (batch simulation) ---
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

on_time.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob02_watermarked")
print("UC table:", f"{UC_TABLE}.prob02_watermarked")''',
        "uc_table": "prob02_watermarked",
    },
    {
        "num": "03",
        "problem": "CDC Processing",
        "solution": "Change Data Feed",
        "theory": (
            "**WHAT:** Track inserts, updates, deletes on a Delta table.\n\n"
            "**WHY:** Downstream needs incremental CDC not full table scans.\n\n"
            "**HOW:** Enable `delta.enableChangeDataFeed=true` then `readChangeFeed`."
        ),
        "code": '''# --- Problem 03: CDC → Delta Change Data Feed ---
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

cdf.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob03_cdc_feed")
print("UC table:", f"{UC_TABLE}.prob03_cdc_feed")''',
        "uc_table": "prob03_cdc_feed",
    },
    {
        "num": "04",
        "problem": "Data Skew",
        "solution": "Salting",
        "theory": (
            "**WHAT:** One key has vastly more rows than others → one executor does all work.\n\n"
            "**WHY:** Hot keys (e.g. `channel='ONLINE'`) cause stragglers.\n\n"
            "**HOW:** Add random salt column, aggregate per salt, then roll up."
        ),
        "code": '''# --- Problem 04: Data Skew → Salting ---
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

skew_safe.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob04_salted_agg")
print("UC table:", f"{UC_TABLE}.prob04_salted_agg")''',
        "uc_table": "prob04_salted_agg",
    },
    {
        "num": "05",
        "problem": "Small Files",
        "solution": "OPTIMIZE",
        "theory": (
            "**WHAT:** Many tiny Parquet/Delta files under one folder.\n\n"
            "**WHY:** Per-file overhead slows reads and increases metadata cost.\n\n"
            "**HOW:** `OPTIMIZE table` + optional `ZORDER BY` on filter columns."
        ),
        "code": '''# --- Problem 05: Small Files → OPTIMIZE ---
from pyspark.sql import functions as F

small_path = f"{DEMO_BASE}/prob05_small_files"
for i in range(5):
    bronze.limit(10).write.format("delta").mode("append").save(small_path)

spark.sql(f"OPTIMIZE delta.`{small_path}`")
files_before = len(dbutils.fs.ls(small_path))  # rough folder listing
print("OPTIMIZE completed on", small_path)
print("Top-level entries after OPTIMIZE:", files_before)

write_demo_table(spark.read.format("delta").load(small_path), "prob05_optimized")
print("Saved optimized table as prob05_optimized")''',
        "uc_table": "prob05_optimized",
    },
    {
        "num": "06",
        "problem": "Schema Changes",
        "solution": "Schema Evolution",
        "theory": (
            "**WHAT:** Upstream adds a new column mid-pipeline.\n\n"
            "**WHY:** Rigid schemas break nightly loads.\n\n"
            "**HOW:** Delta `mergeSchema=true` on write; new cols appear as nullable."
        ),
        "code": '''# --- Problem 06: Schema Changes → Schema Evolution ---
from pyspark.sql import functions as F

evo_path = f"{DEMO_BASE}/prob06_evo"
bronze.write.format("delta").mode("overwrite").save(evo_path)

# Batch 2 adds a new column
bronze.withColumn("source_system", F.lit("finledger")).write.format("delta").mode("append").option(
    "mergeSchema", "true"
).save(evo_path)

evolved = spark.read.format("delta").load(evo_path)
print("Columns after evolution:", evolved.columns)
evolved.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob06_evolved")
print("UC table:", f"{UC_TABLE}.prob06_evolved")''',
        "uc_table": "prob06_evolved",
    },
    {
        "num": "07",
        "problem": "Pipeline Failures",
        "solution": "Dead Letter Queue (DLQ)",
        "theory": (
            "**WHAT:** Bad rows should not crash the whole pipeline.\n\n"
            "**WHY:** One corrupt line should not block millions of good rows.\n\n"
            "**HOW:** Split valid vs invalid; write invalid to `audit/quarantine` DLQ path."
        ),
        "code": '''# --- Problem 07: Pipeline Failures → DLQ ---
from pyspark.sql import functions as F

typed = bronze.withColumn("amount_num", F.col("amount_gbp").cast("double"))
valid = typed.filter(F.col("amount_num").isNotNull() & (F.col("amount_num") > 0))
invalid = typed.filter(F.col("amount_num").isNull() | (F.col("amount_num") <= 0))

dlq_path = f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net/dlq/run={RUN_ID}/prob07"
valid.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob07_valid")
invalid.write.format("delta").mode("overwrite").save(dlq_path)

print("Valid rows  :", valid.count())
print("DLQ rows    :", invalid.count())
print("DLQ path    :", dlq_path)
print("UC table    :", f"{UC_TABLE}.prob07_valid")''',
        "uc_table": "prob07_valid",
    },
    {
        "num": "08",
        "problem": "Data Quality Issues",
        "solution": "Data Validation Framework",
        "theory": (
            "**WHAT:** Systematic checks on nulls, ranges, and referential rules.\n\n"
            "**WHY:** Catch issues before gold / regulatory reporting.\n\n"
            "**HOW:** Filter + aggregate DQ metrics; optional Great Expectations / DQX in production."
        ),
        "code": '''# --- Problem 08: Data Quality → Validation framework ---
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
passed.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob08_dq_passed")
checks.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob08_dq_metrics")
print("UC tables: prob08_dq_passed, prob08_dq_metrics")''',
        "uc_table": "prob08_dq_passed",
    },
    {
        "num": "09",
        "problem": "Missing Data / Nulls",
        "solution": "Imputation / Default Values",
        "theory": (
            "**WHAT:** Nullable columns break aggregations or ML features.\n\n"
            "**WHY:** Source systems omit optional fields.\n\n"
            "**HOW:** `coalesce`, `fillna`, or domain defaults (e.g. channel='UNKNOWN')."
        ),
        "code": '''# --- Problem 09: Missing Data → Imputation ---
from pyspark.sql import functions as F

imputed = bronze.withColumn(
    "channel_clean",
    F.coalesce(F.col("channel"), F.lit("UNKNOWN")),
).withColumn(
    "amount_clean",
    F.coalesce(F.col("amount_gbp").cast("double"), F.lit(0.0)),
)
imputed.select("channel", "channel_clean", "amount_gbp", "amount_clean").show(5)
imputed.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob09_imputed")
print("UC table:", f"{UC_TABLE}.prob09_imputed")''',
        "uc_table": "prob09_imputed",
    },
    {
        "num": "10",
        "problem": "Inconsistent Data Formats",
        "solution": "Standardization",
        "theory": (
            "**WHAT:** Same concept stored different ways (`uk`, `UK`, ` United Kingdom `).\n\n"
            "**WHY:** Breaks joins and group-bys.\n\n"
            "**HOW:** `trim`, `upper`, `regexp_replace` to canonical forms."
        ),
        "code": '''# --- Problem 10: Inconsistent Formats → Standardization ---
from pyspark.sql import functions as F

std = bronze.withColumn(
    "channel_std",
    F.upper(F.trim(F.col("channel"))),
).withColumn(
    "txn_date_std",
    F.to_date(F.col("value_date")),
)
std.select("channel", "channel_std", "value_date", "txn_date_std").show(5)
std.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob10_standardized")
print("UC table:", f"{UC_TABLE}.prob10_standardized")''',
        "uc_table": "prob10_standardized",
    },
]

# Problems 11-50 continue in part 2
PROBLEMS_50_PART2: list[dict[str, str]] = [
    {
        "num": "11",
        "problem": "Large Data Volume",
        "solution": "Partitioning",
        "theory": "**WHAT:** Billions of rows in one folder.\n\n**WHY:** Full scans are slow and expensive.\n\n**HOW:** `partitionBy('value_date')` on write.",
        "code": '''# --- Problem 11: Large Volume → Partitioning ---
from pyspark.sql import functions as F

part_path = f"{DEMO_BASE}/prob11_partitioned"
bronze.withColumn("txn_date", F.to_date(F.col("value_date"))).write.format("delta").mode(
    "overwrite"
).partitionBy("txn_date").save(part_path)

parts = [r.name for r in dbutils.fs.ls(part_path) if r.name.startswith("txn_date=")]
print("Partition folders:", parts[:5], "...")
spark.read.format("delta").load(part_path).write.format("delta").mode("overwrite").saveAsTable(
    f"{UC_TABLE}.prob11_partitioned"
)
print("UC table:", f"{UC_TABLE}.prob11_partitioned")''',
        "uc_table": "prob11_partitioned",
    },
    {
        "num": "12",
        "problem": "Slow Queries",
        "solution": "Partition Pruning & Indexing",
        "theory": "**WHAT:** Queries scan all data when only one day is needed.\n\n**WHY:** No partition filter in WHERE clause.\n\n**HOW:** Filter on partition column; Z-ORDER on high-cardinality filters.",
        "code": '''# --- Problem 12: Slow Queries → Partition pruning ---
from pyspark.sql import functions as F

tbl = spark.table(f"{UC_TABLE}.prob11_partitioned")
sample_date = tbl.select("txn_date").first()[0]
pruned = tbl.filter(F.col("txn_date") == sample_date)
print("Filter date:", sample_date)
print("Pruned rows:", pruned.count())
print("Physical plan (look for PartitionFilters):")
pruned.explain()''',
        "uc_table": None,
    },
    {
        "num": "13",
        "problem": "High Latency",
        "solution": "Caching",
        "theory": "**WHAT:** Same DataFrame recomputed many times in one notebook/job.\n\n**WHY:** Lazy evaluation re-reads from storage each action.\n\n**HOW:** `.cache()` or `.persist()` after expensive transform.",
        "code": '''# --- Problem 13: High Latency → Caching ---
from pyspark.sql import functions as F

agg = bronze.groupBy("channel").agg(F.sum("amount_gbp").alias("total"))
agg.cache()
print("First action (populates cache):", agg.count())
print("Second action (from cache)     :", agg.count())
agg.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob13_cached_agg")
agg.unpersist()
print("UC table:", f"{UC_TABLE}.prob13_cached_agg")''',
        "uc_table": "prob13_cached_agg",
    },
    {
        "num": "14",
        "problem": "Frequent Reprocessing",
        "solution": "Incremental Processing",
        "theory": "**WHAT:** Re-running entire history every night.\n\n**WHY:** Wastes compute and extends SLAs.\n\n**HOW:** Track `last_processed_date` watermark; filter `WHERE date > watermark`.",
        "code": '''# --- Problem 14: Frequent Reprocessing → Incremental ---
from pyspark.sql import functions as F

watermark = "2024-01-01"  # in production: read from control table
incremental = bronze.filter(F.col("value_date") >= watermark)
print("Incremental rows since", watermark, ":", incremental.count())
incremental.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob14_incremental")
print("UC table:", f"{UC_TABLE}.prob14_incremental")''',
        "uc_table": "prob14_incremental",
    },
    {
        "num": "15",
        "problem": "Unnecessary Full Loads",
        "solution": "Incremental Loads",
        "theory": "**WHAT:** Full table extract when only deltas changed.\n\n**WHY:** Source DB and network cost scale with full exports.\n\n**HOW:** `MERGE` on key with CDC or `modified_at` column.",
        "code": '''# --- Problem 15: Unnecessary Full Loads → Incremental MERGE ---
from pyspark.sql import functions as F
from delta.tables import DeltaTable

target_path = f"{DEMO_BASE}/prob15_merge"
bronze.limit(50).write.format("delta").mode("overwrite").save(target_path)
updates = bronze.limit(5).withColumn("amount_gbp", F.col("amount_gbp").cast("double") * 1.05)

DeltaTable.forPath(spark, target_path).alias("t").merge(
    updates.alias("s"), "t.transaction_id = s.transaction_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

merged = spark.read.format("delta").load(target_path)
merged.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob15_merged")
print("MERGE complete — UC table:", f"{UC_TABLE}.prob15_merged")''',
        "uc_table": "prob15_merged",
    },
    {
        "num": "16",
        "problem": "Manual Interventions",
        "solution": "Automation",
        "theory": "**WHAT:** Ops manually re-run failed steps.\n\n**WHY:** Not repeatable; error-prone at 2am.\n\n**HOW:** Databricks Jobs + ADF triggers + parameterized widgets.",
        "code": '''# --- Problem 16: Manual Interventions → Automation ---
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
auto_log.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob16_automation_log")
print("UC table:", f"{UC_TABLE}.prob16_automation_log")''',
        "uc_table": "prob16_automation_log",
    },
    {
        "num": "17",
        "problem": "Monitoring Gaps",
        "solution": "Logging & Alerting",
        "theory": "**WHAT:** Silent failures until users report wrong dashboards.\n\n**WHY:** No structured logs or row-count alerts.\n\n**HOW:** Log metrics to Delta control table; alert if `dq_fail_pct > threshold`.",
        "code": '''# --- Problem 17: Monitoring Gaps → Logging & alerting ---
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
metrics_df.write.format("delta").mode("append").saveAsTable(f"{UC_TABLE}.prob17_pipeline_metrics")
print("UC table:", f"{UC_TABLE}.prob17_pipeline_metrics")''',
        "uc_table": "prob17_pipeline_metrics",
    },
    {
        "num": "18",
        "problem": "Backfill Challenges",
        "solution": "Batch Backfill Strategy",
        "theory": "**WHAT:** Re-load historical dates after a logic fix.\n\n**WHY:** One big bang risks timeout.\n\n**HOW:** Loop over date partitions; idempotent write per day.",
        "code": '''# --- Problem 18: Backfill → Batch by date ---
from pyspark.sql import functions as F

dates = [r.value_date for r in bronze.select("value_date").distinct().limit(3).collect()]
backfill_rows = 0
for d in dates:
    day_df = bronze.filter(F.col("value_date") == d)
    n = day_df.count()
    backfill_rows += n
    print(f"Backfill {d}: {n} rows")
print("Total backfilled:", backfill_rows)

bronze.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob18_backfill_result")
print("UC table:", f"{UC_TABLE}.prob18_backfill_result")''',
        "uc_table": "prob18_backfill_result",
    },
    {
        "num": "19",
        "problem": "Data Loss",
        "solution": "Checkpointing",
        "theory": "**WHAT:** Streaming job restarts from scratch and duplicates or loses data.\n\n**WHY:** No durable offset store.\n\n**HOW:** Structured Streaming checkpoint location on ADLS; batch: idempotent MERGE keys.",
        "code": '''# --- Problem 19: Data Loss → Checkpointing ---
checkpoint = f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/run={RUN_ID}/prob19"
dbutils.fs.mkdirs(checkpoint)
print("Checkpoint path (streaming):", checkpoint)
print("Batch idempotency: MERGE on transaction_id (see Problem 15)")

bronze.select("transaction_id").dropDuplicates().write.format("delta").mode("overwrite").saveAsTable(
    f"{UC_TABLE}.prob19_idempotent_keys"
)
print("UC table:", f"{UC_TABLE}.prob19_idempotent_keys")''',
        "uc_table": "prob19_idempotent_keys",
    },
    {
        "num": "20",
        "problem": "Out-of-Order Events",
        "solution": "Event Time Processing",
        "theory": "**WHAT:** Events arrive non-chronologically.\n\n**WHY:** Network delay across regions.\n\n**HOW:** Window on `event_time` not `current_timestamp()` (processing time).",
        "code": '''# --- Problem 20: Out-of-Order → Event time ---
from pyspark.sql import functions as F

events = bronze.withColumn("event_ts", F.to_timestamp(F.col("value_date")))
by_event = events.groupBy(F.window(F.col("event_ts"), "7 days"), "channel").agg(
    F.count("*").alias("txn_count")
)
by_event.show(5, truncate=False)
by_event.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob20_event_time_windows")
print("UC table:", f"{UC_TABLE}.prob20_event_time_windows")''',
        "uc_table": "prob20_event_time_windows",
    },
]

PROBLEMS_50_PART3: list[dict[str, str]] = [
    {
        "num": "21",
        "problem": "Streaming Backlog",
        "solution": "Trigger Optimization",
        "theory": "**WHAT:** Micro-batches pile up faster than processing.\n\n**WHY:** `processingTime` trigger too aggressive for volume.\n\n**HOW:** Tune trigger interval; use `AvailableNow` for catch-up batches.",
        "code": '''# --- Problem 21: Streaming Backlog → Trigger tuning ---
print("Structured Streaming trigger options:")
print("  processingTime='30 seconds'  — steady micro-batches")
print("  availableNow=True            — drain backlog once (Databricks)")
print("  once=True                    — single batch for testing")
print("Lab uses batch; in production set trigger on streaming query.")

triggers = spark.createDataFrame([
    ("processingTime", "30 seconds", "steady load"),
    ("availableNow", "true", "catch-up backlog"),
], ["trigger_type", "value", "use_case"])
triggers.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob21_trigger_guide")
print("UC table:", f"{UC_TABLE}.prob21_trigger_guide")''',
        "uc_table": "prob21_trigger_guide",
    },
    {
        "num": "22",
        "problem": "Memory Pressure (OOM)",
        "solution": "Right-sizing & Caching",
        "theory": "**WHAT:** Driver or executor runs out of heap.\n\n**WHY:** `collect()` on huge data, too many wide transformations.\n\n**HOW:** Never collect large datasets; increase workers; spill-friendly joins.",
        "code": '''# --- Problem 22: OOM → Right-sizing ---
from pyspark.sql import functions as F

# BAD:  bronze.collect()  # never on production volume
# GOOD: aggregate then take small sample
summary = bronze.agg(F.count("*").alias("n"), F.avg("amount_gbp").alias("avg_gbp"))
summary.show()
print("Executor memory: set in cluster UI (e.g. 8g standard)")
print("Driver memory  : avoid collect(); use show(20) or write")

summary.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob22_memory_safe_summary")
print("UC table:", f"{UC_TABLE}.prob22_memory_safe_summary")''',
        "uc_table": "prob22_memory_safe_summary",
    },
    {
        "num": "23",
        "problem": "Shuffle Bottlenecks",
        "solution": "Reduce Shuffles / AQE",
        "theory": "**WHAT:** Expensive shuffle before every wide join/groupBy.\n\n**WHY:** Default partition count wrong for skewed data.\n\n**HOW:** Enable Adaptive Query Execution (AQE); pre-filter; salting.",
        "code": '''# --- Problem 23: Shuffle → AQE ---
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))

from pyspark.sql import functions as F
shuffled = bronze.repartition(4, "channel").groupBy("channel").count()
shuffled.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob23_aqe_agg")
print("UC table:", f"{UC_TABLE}.prob23_aqe_agg")''',
        "uc_table": "prob23_aqe_agg",
    },
    {
        "num": "24",
        "problem": "Slow Joins",
        "solution": "Broadcast Join",
        "theory": "**WHAT:** Joining fact to small dimension scans both sides.\n\n**WHY:** Default sort-merge join shuffles both tables.\n\n**HOW:** `broadcast(dim_df)` when dimension < ~10MB.",
        "code": '''# --- Problem 24: Slow Joins → Broadcast ---
from pyspark.sql import functions as F

dim = bronze.select("channel").distinct().withColumn("tier", F.lit("standard"))
joined = bronze.join(F.broadcast(dim), "channel", "left")
joined.select("transaction_id", "channel", "tier").show(5)
joined.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob24_broadcast_join")
print("UC table:", f"{UC_TABLE}.prob24_broadcast_join")''',
        "uc_table": "prob24_broadcast_join",
    },
    {
        "num": "25",
        "problem": "Large Joins",
        "solution": "Bucketing / Partitioning",
        "theory": "**WHAT:** Two huge tables joined on same key.\n\n**WHY:** Shuffle join does not scale.\n\n**HOW:** Bucket both tables on join key; co-locate partitions.",
        "code": '''# --- Problem 25: Large Joins → Bucketing / co-partitioning ---
from pyspark.sql import functions as F

bucket_path = f"{DEMO_BASE}/prob25_copartitioned"
# Co-partition on channel — same layout on both sides of join reduces shuffle
bronze.write.format("delta").mode("overwrite").partitionBy("channel").save(bucket_path)
print("Partitioned by channel at:", bucket_path)
print("Classic cluster + Hive: bucketBy(8, 'channel').sortBy('channel').saveAsTable(...)")
bronze.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob25_bucket_guide")
print("UC table:", f"{UC_TABLE}.prob25_bucket_guide")''',
        "uc_table": "prob25_bucket_guide",
    },
    {
        "num": "26",
        "problem": "High Cost",
        "solution": "Cluster Optimization",
        "theory": "**WHAT:** DBU spend higher than needed.\n\n**WHY:** Oversized nodes, always-on clusters.\n\n**HOW:** Job clusters, right node type, photon where licensed.",
        "code": '''# --- Problem 26: High Cost → Cluster optimization ---
cost_tips = [
    ("Use Jobs cluster", "Terminate after job — not interactive 24/7"),
    ("Smallest node", "Standard_DS3_v2 for labs; scale up only if OOM"),
    ("Auto-terminate", "30 min idle on interactive clusters"),
    ("Serverless SQL", "For ad-hoc queries only when licensed"),
]
for tip, detail in cost_tips:
    print(f"  {tip}: {detail}")

spark.createDataFrame(cost_tips, ["tip", "detail"]).write.format("delta").mode("overwrite").saveAsTable(
    f"{UC_TABLE}.prob26_cost_tips"
)
print("UC table:", f"{UC_TABLE}.prob26_cost_tips")''',
        "uc_table": "prob26_cost_tips",
    },
    {
        "num": "27",
        "problem": "Underutilized Clusters",
        "solution": "Auto Scaling & Auto Termination",
        "theory": "**WHAT:** Cluster runs at 1 worker while paying for 8.\n\n**WHY:** Fixed size + no auto-terminate.\n\n**HOW:** `autoscale min_workers=1 max_workers=4`; terminate after 30 min idle.",
        "code": '''# --- Problem 27: Underutilized → Autoscale ---
cluster_policy = {
    "autoscale": {"min_workers": 1, "max_workers": 4},
    "autotermination_minutes": 30,
    "spark_version": "latest LTS",
}
import json
print("Recommended cluster JSON (set in Compute UI):")
print(json.dumps(cluster_policy, indent=2))

spark.createDataFrame([(json.dumps(cluster_policy),)], ["cluster_config_json"]).write.format(
    "delta"
).mode("overwrite").saveAsTable(f"{UC_TABLE}.prob27_autoscale_config")
print("UC table:", f"{UC_TABLE}.prob27_autoscale_config")''',
        "uc_table": "prob27_autoscale_config",
    },
    {
        "num": "28",
        "problem": "Cluster Startup Delays",
        "solution": "Warm Pools / Cluster Pools",
        "theory": "**WHAT:** 5–10 min wait for new cluster each job.\n\n**WHY:** Cold VM provisioning.\n\n**HOW:** Instance pool keeps VMs warm; policy attaches jobs to pool.",
        "code": '''# --- Problem 28: Startup delays → Instance pools ---
print("Instance pool (Azure Databricks admin):")
print("  1. Compute → Instance pools → Create")
print("  2. Min idle instances = 1 (warm VM ready)")
print("  3. Job cluster policy → use pool id")
print("Trade-off: warm VMs cost $ even when idle — use for SLA-critical jobs only.")

spark.createDataFrame([("instance_pool", "reduces cold start", RUN_ID)], ["component", "benefit", "run_id"]).write.format(
    "delta"
).mode("overwrite").saveAsTable(f"{UC_TABLE}.prob28_pool_guide")
print("UC table:", f"{UC_TABLE}.prob28_pool_guide")''',
        "uc_table": "prob28_pool_guide",
    },
    {
        "num": "29",
        "problem": "Security Issues",
        "solution": "RBAC & Unity Catalog",
        "theory": "**WHAT:** Everyone can read PII gold tables.\n\n**WHY:** No central governance.\n\n**HOW:** Unity Catalog GRANT/REVOKE on catalog.schema.table.",
        "code": '''# --- Problem 29: Security → Unity Catalog RBAC ---
print("Catalog :", CATALOG)
print("Schema  :", UC_TABLE)
print("Example GRANT (trainer runs as metastore admin):")
print(f"  GRANT USE SCHEMA ON SCHEMA {UC_TABLE} TO `account users`")
print(f"  GRANT SELECT ON TABLE {UC_TABLE}.prob01_deduped TO `account users`")

spark.sql(f"SHOW TABLES IN {UC_TABLE}").show(10, truncate=False)''',
        "uc_table": None,
    },
    {
        "num": "30",
        "problem": "Access Control Problems",
        "solution": "Fine-Grained Permissions",
        "theory": "**WHAT:** Analysts need channel totals but not transaction_id.\n\n**WHY:** Column-level PII restrictions.\n\n**HOW:** UC column masks, row filters, dynamic views.",
        "code": '''# --- Problem 30: Fine-grained access → Column masks / views ---
from pyspark.sql import functions as F

# Masked view: hide transaction_id — show only aggregates
masked = bronze.groupBy("channel").agg(
    F.count("*").alias("txn_count"),
    F.round(F.sum("amount_gbp"), 2).alias("total_gbp"),
)
masked.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob30_masked_gold")
print("UC table (safe for analysts):", f"{UC_TABLE}.prob30_masked_gold")
print("Production: CREATE VIEW ... + GRANT SELECT ON VIEW")''',
        "uc_table": "prob30_masked_gold",
    },
]

PROBLEMS_50_PART4: list[dict[str, str]] = [
    {
        "num": "31",
        "problem": "Secrets Management",
        "solution": "Secret Scopes / Key Vault",
        "theory": "**WHAT:** Storage keys pasted in notebook cells.\n\n**WHY:** Git leak, shoulder surfing.\n\n**HOW:** `dbutils.secrets.get('finledger', 'storage-key')` — scope backed by Databricks (lab) or Key Vault (prod).",
        "code": '''# --- Problem 31: Secrets → Secret scope ---
acct = dbutils.secrets.get(scope="finledger", key="storage-account")
key_len = len(dbutils.secrets.get(scope="finledger", key="storage-key"))
print("Scope     : finledger")
print("Account   :", acct)
print("Key loaded: yes (" + str(key_len) + " chars — not printed)")
print("Production: --scope-backend-type AZURE_KEYVAULT on create-scope")

spark.createDataFrame([("finledger", acct, key_len)], ["scope", "storage_account", "key_chars"]).write.format(
    "delta"
).mode("overwrite").saveAsTable(f"{UC_TABLE}.prob31_secrets_audit")
print("UC table:", f"{UC_TABLE}.prob31_secrets_audit")''',
        "uc_table": "prob31_secrets_audit",
    },
    {
        "num": "32",
        "problem": "Data Lineage Gaps",
        "solution": "Data Lineage Tools",
        "theory": "**WHAT:** Lineage is a dependency map — which source tables feed which downstream tables and reports.\n\n**WHY:** Without it, nobody knows impact of a schema change; compliance cannot trace data to source.\n\n**HOW:** Create downstream from upstream (prob32 from prob01) → open Catalog **Lineage** tab → Purview for enterprise (Session 24+).",
        "code": '''# --- Problem 32: Lineage → UC + Purview manifest edge ---
src_table = "prob01_deduped"
dst_table = "prob32_lineage_demo"
src = demo_table_ref(src_table)
write_demo_table(read_demo_table(src_table).limit(10), dst_table)
dst = demo_table_ref(dst_table)
register_lineage_edge(src_table, dst_table, "pl_prob32_lineage_demo")
print("LINEAGE DEMO")
print("  upstream (source) :", src)
print("  downstream (new)  :", dst)
print("  rows written      :", read_demo_table(dst_table).count())
print("View lineage: Catalog -> Lineage tab (UC) | Purview manifest at", MANIFEST_PATH)
try:
    spark.sql(f"DESCRIBE EXTENDED {dst}").filter("col_name = 'Provider'").show(truncate=False)
except Exception as e:
    print("Extended metadata (UC only):", e)''',
        "uc_table": "prob32_lineage_demo",
    },
    {
        "num": "33",
        "problem": "Poor Documentation",
        "solution": "Centralized Documentation",
        "theory": "**WHAT:** Column meanings live in someone's head.\n\n**WHY:** Onboarding slow; wrong joins.\n\n**HOW:** UC table/column COMMENTs; markdown in repo README.",
        "code": '''# --- Problem 33: Documentation → UC comments + SQL metadata row ---
apply_table_documentation("prob01_deduped", "demo", "01")
if UC_WRITE_ENABLED:
    print("Comments applied. View: DESCRIBE TABLE EXTENDED", f"{UC_TABLE}.prob01_deduped")
    spark.sql(f"DESCRIBE TABLE EXTENDED {UC_TABLE}.prob01_deduped").show(5, truncate=False)
else:
    print("SKIP: UC comments need write permission on schema")
print("SQL metadata model export (capstone):", SQL_EXPORT_PATH)''',
        "uc_table": None,
    },
    {
        "num": "34",
        "problem": "Inconsistent Metrics",
        "solution": "Metric Standardization",
        "theory": "**WHAT:** `revenue`, `amount`, `gbp_amt` mean the same thing.\n\n**WHY:** Different teams name columns differently.\n\n**HOW:** Gold layer canonical names + data dictionary.",
        "code": '''# --- Problem 34: Metric standardization ---
from pyspark.sql import functions as F

canonical = bronze.select(
    F.col("transaction_id"),
    F.col("amount_gbp").cast("double").alias("metric_amount_gbp"),  # canonical name
    F.col("channel").alias("dim_channel"),
)
canonical.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob34_canonical_metrics")
print("UC table:", f"{UC_TABLE}.prob34_canonical_metrics")
canonical.printSchema()''',
        "uc_table": "prob34_canonical_metrics",
    },
    {
        "num": "35",
        "problem": "Data Drift",
        "solution": "Data Drift Detection",
        "theory": "**WHAT:** Today's distribution differs from last week.\n\n**WHY:** Upstream bug or market shift.\n\n**HOW:** Compare null %, min/max, row counts run-over-run.",
        "code": '''# --- Problem 35: Data drift detection ---
from pyspark.sql import functions as F

profile = bronze.agg(
    F.count("*").alias("row_count"),
    F.avg("amount_gbp").alias("avg_amount"),
    F.sum(F.when(F.col("channel").isNull(), 1).otherwise(0)).alias("null_channels"),
)
profile = profile.withColumn("run_id", F.lit(RUN_ID))
profile.write.format("delta").mode("append").saveAsTable(f"{UC_TABLE}.prob35_drift_profile")
print("Append profile each run; alert if avg_amount shifts > 20%")
profile.show()''',
        "uc_table": "prob35_drift_profile",
    },
    {
        "num": "36",
        "problem": "Dimension Changes (SCD)",
        "solution": "SCD Type 2",
        "theory": "**WHAT:** Customer address changes; need history.\n\n**WHY:** Point-in-time reporting.\n\n**HOW:** `effective_from`, `effective_to`, `is_current` + MERGE.",
        "code": '''# --- Problem 36: SCD Type 2 ---
from pyspark.sql import functions as F

dim = bronze.select(
    F.col("channel").alias("channel_key"),
    F.col("channel").alias("channel_name"),
    F.current_date().alias("effective_from"),
    F.lit(None).cast("date").alias("effective_to"),
    F.lit(True).alias("is_current"),
).distinct()
dim.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob36_scd2_channel")
print("UC table:", f"{UC_TABLE}.prob36_scd2_channel")
dim.show()''',
        "uc_table": "prob36_scd2_channel",
    },
    {
        "num": "37",
        "problem": "Reference Data Updates",
        "solution": "Dimension Refresh Strategy",
        "theory": "**WHAT:** Lookup table (FX rates, product codes) changes daily.\n\n**WHY:** Stale joins produce wrong amounts.\n\n**HOW:** Overwrite small dimension or MERGE on key.",
        "code": '''# --- Problem 37: Reference data refresh ---
from pyspark.sql import functions as F

ref = bronze.select("channel").distinct().withColumn("refreshed_at", F.current_timestamp())
ref.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob37_ref_channel")
print("Full overwrite OK for small reference dims (<100k rows)")
ref.show()''',
        "uc_table": "prob37_ref_channel",
    },
    {
        "num": "38",
        "problem": "External API Failures",
        "solution": "Retry Logic / Backoff",
        "theory": "**WHAT:** REST call fails transiently.\n\n**WHY:** Network blips, 429 throttling.\n\n**HOW:** Exponential backoff retries (3–5 attempts).",
        "code": '''# --- Problem 38: API failures → Retry with backoff ---
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

spark.createDataFrame([(result, "ok")], ["rows", "status"]).write.format("delta").mode("overwrite").saveAsTable(
    f"{UC_TABLE}.prob38_retry_result"
)
print("UC table:", f"{UC_TABLE}.prob38_retry_result")''',
        "uc_table": "prob38_retry_result",
    },
    {
        "num": "39",
        "problem": "Rate Limiting",
        "solution": "Throttling / Queuing",
        "theory": "**WHAT:** API returns HTTP 429 Too Many Requests.\n\n**WHY:** Exceeding provider quota.\n\n**HOW:** Sleep between calls; queue work; batch requests.",
        "code": '''# --- Problem 39: Rate limiting → Throttle (small lab batch) ---
import time

BATCH_SIZE = 50
total = bronze.count()
batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
for b in range(batches):
    print(f"Batch {b + 1}/{batches} — throttle sleep 0.3s (simulates API rate limit)")
    time.sleep(0.3)
print("Throttling prevents HTTP 429 from partner APIs")

spark.createDataFrame([(total, BATCH_SIZE, batches)], ["total_rows", "batch_size", "batch_count"]).write.format(
    "delta"
).mode("overwrite").saveAsTable(f"{UC_TABLE}.prob39_throttle_log")
print("UC table:", f"{UC_TABLE}.prob39_throttle_log")''',
        "uc_table": "prob39_throttle_log",
    },
    {
        "num": "40",
        "problem": "File Corruption",
        "solution": "File Validation & Checksums",
        "theory": "**WHAT:** Partial upload or bit rot in landing zone.\n\n**WHY:** Silent bad parses.\n\n**HOW:** MD5/SHA256 on landing; reject if mismatch.",
        "code": '''# --- Problem 40: File corruption → Checksum / row-count integrity ---
import hashlib

row_count = bronze.count()
checksum = hashlib.sha256(f"{BRONZE_PATH}:{row_count}".encode()).hexdigest()[:16]
print("Bronze path  :", BRONZE_PATH)
print("Row count    :", row_count)
print("Integrity hash:", checksum)
print("Production: compare SHA256 from landing manifest before ingest")

spark.createDataFrame([(BRONZE_PATH, row_count, checksum)], ["path", "rows", "checksum"]).write.format(
    "delta"
).mode("overwrite").saveAsTable(f"{UC_TABLE}.prob40_file_integrity")
print("UC table:", f"{UC_TABLE}.prob40_file_integrity")''',
        "uc_table": "prob40_file_integrity",
    },
]

PROBLEMS_50_PART5: list[dict[str, str]] = [
    {
        "num": "41",
        "problem": "Inconsistent Timestamps",
        "solution": "Time Standardization (UTC)",
        "theory": "**WHAT:** Mix of local times without timezone.\n\n**WHY:** DST and locale bugs in reports.\n\n**HOW:** Store UTC; convert at presentation layer.",
        "code": '''# --- Problem 41: Timestamps → UTC ---
from pyspark.sql import functions as F

utc = bronze.withColumn(
    "txn_utc",
    F.to_utc_timestamp(F.to_timestamp(F.col("value_date")), "Europe/London"),
)
utc.select("value_date", "txn_utc").show(3)
utc.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob41_utc_timestamps")
print("UC table:", f"{UC_TABLE}.prob41_utc_timestamps")''',
        "uc_table": "prob41_utc_timestamps",
    },
    {
        "num": "42",
        "problem": "Time Zone Issues",
        "solution": "Time Zone Normalization",
        "theory": "**WHAT:** London vs Mumbai vs New York same instant.\n\n**WHY:** Global bank operates across zones.\n\n**HOW:** `from_utc_timestamp` for display region.",
        "code": '''# --- Problem 42: Time zone normalization ---
from pyspark.sql import functions as F

base = spark.table(f"{UC_TABLE}.prob41_utc_timestamps")
local = base.withColumn("txn_london", F.from_utc_timestamp(F.col("txn_utc"), "Europe/London"))
local.select("txn_utc", "txn_london").show(3)
local.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob42_tz_normalized")
print("UC table:", f"{UC_TABLE}.prob42_tz_normalized")''',
        "uc_table": "prob42_tz_normalized",
    },
    {
        "num": "43",
        "problem": "Orphan Records",
        "solution": "Referential Integrity Checks",
        "theory": "**WHAT:** Fact rows reference missing dimension keys.\n\n**WHY:** Late dimension load or bad FK.\n\n**HOW:** Left anti-join facts vs dims → quarantine orphans.",
        "code": '''# --- Problem 43: Orphan records ---
from pyspark.sql import functions as F

facts = bronze
dims = bronze.select("channel").distinct()
orphans = facts.join(dims, "channel", "left_anti")
print("Orphan rows (channel not in dim):", orphans.count())
valid = facts.join(dims, "channel", "inner")
valid.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob43_referential_valid")
orphans.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob43_orphans")
print("UC tables: prob43_referential_valid, prob43_orphans")''',
        "uc_table": "prob43_referential_valid",
    },
    {
        "num": "44",
        "problem": "Data Duplication Across Systems",
        "solution": "Idempotent Processing",
        "theory": "**WHAT:** Same batch loaded twice into gold.\n\n**WHY:** ADF retry without idempotency key.\n\n**HOW:** MERGE on natural key; deterministic run_id.",
        "code": '''# --- Problem 44: Idempotent processing ---
from pyspark.sql import functions as F

run_df = bronze.withColumn("pipeline_run_id", F.lit(RUN_ID))
run_df.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob44_idempotent")
# Re-run same RUN_ID → overwrite same table (idempotent for this demo)
print("Same RUN_ID re-run overwrites — production: MERGE on business key")
print("UC table:", f"{UC_TABLE}.prob44_idempotent")''',
        "uc_table": "prob44_idempotent",
    },
    {
        "num": "45",
        "problem": "Slow ETL Jobs",
        "solution": "Optimize Code & Resources",
        "theory": "**WHAT:** Job takes 3 hours instead of 30 minutes.\n\n**WHY:** UDFs, collect, wide selects.\n\n**HOW:** Native Spark functions; column pruning; broadcast small dims.",
        "code": '''# --- Problem 45: Slow ETL → Optimize ---
from pyspark.sql import functions as F

optimized = (
    bronze.select("transaction_id", "channel", "amount_gbp")  # prune columns early
    .filter(F.col("amount_gbp").isNotNull())
    .groupBy("channel")
    .agg(F.sum("amount_gbp").alias("total"))
)
optimized.write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob45_optimized_etl")
print("UC table:", f"{UC_TABLE}.prob45_optimized_etl")''',
        "uc_table": "prob45_optimized_etl",
    },
    {
        "num": "46",
        "problem": "High I/O Costs",
        "solution": "Data Compression",
        "theory": "**WHAT:** Storage egress and scan costs dominate.\n\n**WHY:** Uncompressed CSV landing.\n\n**HOW:** Delta/Parquet with snappy or zstd compression.",
        "code": '''# --- Problem 46: I/O cost → Compression ---
comp_path = f"{DEMO_BASE}/prob46_compressed"
bronze.write.format("delta").option("compression", "snappy").mode("overwrite").save(comp_path)
print("Delta default: Parquet + snappy compression at", comp_path)
spark.read.format("delta").load(comp_path).write.format("delta").mode("overwrite").saveAsTable(
    f"{UC_TABLE}.prob46_compressed"
)
print("UC table:", f"{UC_TABLE}.prob46_compressed")''',
        "uc_table": "prob46_compressed",
    },
    {
        "num": "47",
        "problem": "Data Retention Issues",
        "solution": "Lifecycle Policies",
        "theory": "**WHAT:** Bronze kept forever → cost and GDPR risk.\n\n**WHY:** No TTL on raw layers.\n\n**HOW:** ADLS lifecycle move to cool/archive; Delta `VACUUM` after retention.",
        "code": '''# --- Problem 47: Retention → Lifecycle ---
print("ADLS lifecycle (Azure Portal → Storage → Lifecycle management):")
print("  bronze/loaded/* → cool after 90 days")
print("  audit/quarantine/* → delete after 365 days")
print("Delta: VACUUM table RETAIN 168 HOURS (7 days) — after legal hold check")

spark.createDataFrame([
    ("bronze", 90, "cool"),
    ("audit", 365, "delete"),
], ["layer", "days", "action"]).write.format("delta").mode("overwrite").saveAsTable(
    f"{UC_TABLE}.prob47_retention_policy"
)
print("UC table:", f"{UC_TABLE}.prob47_retention_policy")''',
        "uc_table": "prob47_retention_policy",
    },
    {
        "num": "48",
        "problem": "Compliance & Governance",
        "solution": "Data Governance Framework",
        "theory": "**WHAT:** No owner, classification, or audit trail.\n\n**WHY:** Regulatory (GDPR, PCI) requires traceability.\n\n**HOW:** UC tags + Purview classification (Session 24).",
        "code": '''# --- Problem 48: Governance → UC tags + Purview classifications ---
apply_table_documentation("prob01_deduped", "demo", "48")
if UC_WRITE_ENABLED:
    spark.sql(f"SHOW TBLPROPERTIES {UC_TABLE}.prob01_deduped").show(10, truncate=False)
else:
    print("SKIP: UC tags need write permission")
asset = problem_asset_record("48", "Compliance & Governance", "Data Governance Framework", demo_table_ref("prob01_deduped"))
print("Purview classification:", asset["classification"], "| glossary:", asset["glossary_term"])
print("Full manifest written at capstone:", MANIFEST_PATH)''',
        "uc_table": None,
    },
    {
        "num": "49",
        "problem": "Disaster Recovery",
        "solution": "Backup & Restore Strategy",
        "theory": "**WHAT:** Accidental DROP TABLE or bad MERGE.\n\n**WHY:** Human error happens.\n\n**HOW:** Delta time travel RESTORE; geo-redundant ADLS.",
        "code": '''# --- Problem 49: Disaster recovery → Delta RESTORE ---
from pyspark.sql import functions as F

dr_path = f"{DEMO_BASE}/prob49_dr"
bronze.limit(30).write.format("delta").mode("overwrite").save(dr_path)
history = spark.sql(f"DESCRIBE HISTORY delta.`{dr_path}`")
history.show(3, truncate=False)
print("RESTORE TABLE ... TO VERSION AS OF <n>  — or restore path via clone")
bronze.limit(30).write.format("delta").mode("overwrite").saveAsTable(f"{UC_TABLE}.prob49_dr_backup")
print("UC table:", f"{UC_TABLE}.prob49_dr_backup")''',
        "uc_table": "prob49_dr_backup",
    },
    {
        "num": "50",
        "problem": "End-to-End Visibility",
        "solution": "End-to-End Monitoring",
        "theory": "**WHAT:** Cannot trace bronze → gold → dashboard latency.\n\n**WHY:** Siloed logs per tool.\n\n**HOW:** Unified metrics table + ADF Monitor + Databricks job runs.",
        "code": '''# --- Problem 50: End-to-end monitoring + governance inventory ---
tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {UC_TABLE}").collect()] if UC_WRITE_ENABLED else []
inventory = build_problem_inventory()
e2e = spark.createDataFrame([
    (RUN_ID, "bronze_ingest", bronze.count(), "OK"),
    (RUN_ID, "prob_tables", len(inventory), "OK"),
    (RUN_ID, "lineage_edges", len(LINEAGE_EDGES), "OK"),
    (RUN_ID, "notebook", "50_problems", "READY_FOR_CAPSTONE"),
], ["run_id", "stage", "metric_value", "status"])

write_demo_table(e2e, "prob50_e2e_monitoring")
print("E2E summary (feeds capstone + SQL metadata model):")
e2e.show()
print("Problem assets registered for Purview:", len(inventory))
for asset in sorted(inventory, key=lambda a: a.get("problem_num", ""))[:10]:
    print(" ", asset["problem_num"], asset["qualified_name"][:70])
if len(inventory) > 10:
    print(" ... and", len(inventory) - 10, "more — full list in Part K capstone")''',
        "uc_table": "prob50_e2e_monitoring",
    },
]

ALL_PROBLEMS_50 = PROBLEMS_50 + PROBLEMS_50_PART2 + PROBLEMS_50_PART3 + PROBLEMS_50_PART4 + PROBLEMS_50_PART5
