"""FinLedger Databricks teaching topics — theory + analogy + commented code.

Used by build_dbx_teach_all_notebook.py to generate a large trainer notebook
similar in style to the 50-problems lab.
"""

from __future__ import annotations

# Each topic: num, title, hard_problem, easy_approach, analogy, theory, code
# Groups listed at bottom for navigation.

TOPICS: list[dict[str, str]] = [
    {
        "num": "01",
        "group": "A",
        "title": "Lazy vs Action (why nothing happens until .count)",
        "hard_problem": "Learners think Spark runs every line; they wait for nothing and panic.",
        "easy_approach": "Build a plan first (lazy). Pay only when you ask for a result (action).",
        "analogy": "Writing a **shopping list** is free. Only when you **check out** do you spend money. `.select` is listing; `.count` / `.show` / `.write` is checkout.",
        "theory": (
            "**WHAT:** Spark builds a *logical plan* until an action forces computation.\n\n"
            "**WHY:** So Spark can optimize the whole plan (skip columns, prune partitions).\n\n"
            "**HOW:** Transformations = lazy. Actions = `.count()`, `.show()`, `.collect()`, `.write`."
        ),
        "code": '''# --- Topic 01: Lazy vs Action ---
# WHAT: Transformations do not touch ADLS yet.
# WHY:  Spark waits to optimize.
# HOW:  Build plan → one ACTION triggers work.

planned = bronze.select("transaction_id", "amount_gbp", "channel")  # lazy — no bill yet
print("Plan ready — no data scanned yet")
n = planned.count()  # ACTION — now Spark reads bronze
print("ACTION done — rows =", n)
planned.show(3, truncate=False)  # another ACTION''',
    },
    {
        "num": "02",
        "group": "A",
        "title": "Schema & cast (types before gold)",
        "hard_problem": "CSV strings make amounts look like text; SUM fails or goes silent-wrong.",
        "easy_approach": "Cast early in silver. Never trust bronze types.",
        "analogy": "Bronze is **raw receipts with pencil marks**. Silver is **typing them into a calculator** so numbers are real numbers.",
        "theory": (
            "**WHAT:** Enforce types with `.cast(\"double\")` / `to_date`.\n\n"
            "**WHY:** Gold aggregates need real numbers; auditors ask \"is amount_gbp a number?\"\n\n"
            "**HOW:** Cast → filter bad casts into quarantine."
        ),
        "code": '''# --- Topic 02: Schema & cast ---
from pyspark.sql import functions as F

typed = bronze.withColumn("amount", F.col("amount_gbp").cast("double")) \\
              .withColumn("txn_date", F.to_date("value_date"))
good = typed.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
bad = typed.filter(F.col("amount").isNull() | (F.col("amount") <= 0))
print("good=", good.count(), "quarantine=", bad.count())
write_demo_table(good.select("transaction_id", "amount", "channel", "txn_date"), "teach02_typed")''',
    },
    {
        "num": "03",
        "group": "A",
        "title": "select / filter / withColumn (everyday verbs)",
        "hard_problem": "People write Python `for row in df.collect()` and OOM the driver.",
        "easy_approach": "Stay in DataFrame verbs — Spark parallelizes them.",
        "analogy": "Don't open every envelope yourself (**collect**). Give the **mail room** a rule: keep only card, strip PII columns.",
        "theory": (
            "**WHAT:** Core verbs: select, filter/where, withColumn, drop, distinct.\n\n"
            "**WHY:** Driver collect is a trap at bank scale.\n\n"
            "**HOW:** Prefer column expressions with `F.col`."
        ),
        "code": '''# --- Topic 03: Everyday verbs (no collect loops) ---
from pyspark.sql import functions as F

card_only = (
    bronze
    .filter(F.lower(F.col("channel")) == "card")
    .select("transaction_id", "amount_gbp", "channel", "status")
    .withColumn("channel_std", F.upper(F.trim(F.col("channel"))))
)
print("card rows=", card_only.count())
card_only.show(5, truncate=False)
write_demo_table(card_only.limit(100), "teach03_card_verbs")''',
    },
    {
        "num": "04",
        "group": "A",
        "title": "groupBy + agg (channel totals)",
        "hard_problem": "Managers ask \"cash by channel\" and you dump million-row extracts.",
        "easy_approach": "Aggregate in Spark; ship summary rows.",
        "analogy": "Don't bring every grain of rice to the chef — bring a **bowl per dish** (channel totals).",
        "theory": (
            "**WHAT:** `groupBy` + `agg(sum, count, avg)`.\n\n"
            "**WHY:** Gold layer exists to answer business questions cheaply.\n\n"
            "**HOW:** Always name aggregates clearly (`total_gbp`, `txns`)."
        ),
        "code": '''# --- Topic 04: groupBy channel ---
from pyspark.sql import functions as F

by_channel = bronze.withColumn("amount", F.col("amount_gbp").cast("double")) \\
    .groupBy("channel") \\
    .agg(F.count("*").alias("txns"), F.round(F.sum("amount"), 2).alias("total_gbp")) \\
    .orderBy(F.desc("total_gbp"))
by_channel.show(truncate=False)
write_demo_table(by_channel, "teach04_by_channel")''',
    },
    {
        "num": "05",
        "group": "A",
        "title": "join (txn to returns) without Cartesian bombs",
        "hard_problem": "Joining without a key ≈ Cartesian product → day-long jobs.",
        "easy_approach": "Always join on business keys (`transaction_id`). Know left vs inner.",
        "analogy": "Matching **invoice numbers** between sales and refunds — never \"match by customer name\" (too fuzzy).",
        "theory": (
            "**WHAT:** Inner = only matches. Left = keep all left rows.\n\n"
            "**WHY:** Refunds attach to payments via `transaction_id`.\n\n"
            "**HOW:** Check counts before/after join."
        ),
        "code": '''# --- Topic 05: join payments ↔ returns ---
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
write_demo_table(inner, "teach05_join_inner")''',
    },
    {
        "num": "06",
        "group": "B",
        "title": "Medallion bronze → silver → gold (layers)",
        "hard_problem": "One giant table mixes raw junk with board KPIs — chaos.",
        "easy_approach": "Three layers with different promises: raw / cleansed / business.",
        "analogy": "**Farm → kitchen → restaurant plate.** Bronze=farm dirt ok. Silver=washed. Gold=plated dish for the customer.",
        "theory": (
            "**WHAT:** Bronze immutable land → silver typed/valid → gold aggregates.\n\n"
            "**WHY:** Different consumers, different SLAs.\n\n"
            "**HOW:** Paths under bronze/silver/gold containers; never overwrite bronze history casually."
        ),
        "code": '''# --- Topic 06: mini medallion in-memory (concept) ---
from pyspark.sql import functions as F

# bronze = as landed
b = bronze
# silver = typed + deduped
s = b.withColumn("amount", F.col("amount_gbp").cast("double")) \\
     .filter(F.col("amount") > 0) \\
     .dropDuplicates(["transaction_id"])
# gold = business question
g = s.groupBy("channel").agg(F.sum("amount").alias("total_gbp"))
print("bronze", b.count(), "→ silver", s.count(), "→ gold channels", g.count())
write_demo_table(g, "teach06_medallion_gold")''',
    },
    {
        "num": "07",
        "group": "B",
        "title": "Quarantine / dead-letter (bad rows don't kill the batch)",
        "hard_problem": "One bad CSV cell fails 10 million good rows.",
        "easy_approach": "Split good vs bad; write both; never fail the whole night for 3 rows.",
        "analogy": "**Airport security:** most passengers board; a few go to a side room. The flight still leaves.",
        "theory": (
            "**WHAT:** Quarantine = audit path for invalid rows.\n\n"
            "**WHY:** Banks must post good settlements even if 0.01% of rows are dirty.\n\n"
            "**HOW:** `good = filter(ok); bad = filter(not ok)` then write both."
        ),
        "code": '''# --- Topic 07: quarantine split ---
from pyspark.sql import functions as F

typed = bronze.withColumn("amount", F.col("amount_gbp").cast("double"))
ok = F.col("amount").isNotNull() & (F.col("amount") > 0) & F.col("transaction_id").isNotNull()
good = typed.filter(ok)
bad = typed.filter(~ok)
print("GOOD", good.count(), "BAD→quarantine", bad.count())
write_demo_table(good.limit(50), "teach07_good")
write_demo_table(bad.limit(50), "teach07_quarantine")''',
    },
    {
        "num": "08",
        "group": "B",
        "title": "Deduplicate by business key",
        "hard_problem": "ADF retry lands the same file twice → double revenue in reports.",
        "easy_approach": "`dropDuplicates([\"transaction_id\"])` or window keep latest.",
        "analogy": "**One passport number = one person.** Two boarding passes with same passport → you don't seat them twice.",
        "theory": (
            "**WHAT:** Idempotent silver: unique on natural key.\n\n"
            "**WHY:** Retries are normal in cloud pipelines.\n\n"
            "**HOW:** dropDuplicates or ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC)."
        ),
        "code": '''# --- Topic 08: dedupe ---
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
write_demo_table(deduped.limit(50), "teach08_deduped")''',
    },
    {
        "num": "09",
        "group": "C",
        "title": "Why Delta (not only Parquet)",
        "hard_problem": "Parquet folders silently corrupt on half-written jobs; no time travel.",
        "easy_approach": "Delta = Parquet files + `_delta_log` transaction log (ACID).",
        "analogy": "Parquet is **loose photos**. Delta is a **photo album with a date-stamped contents page** — you can go back to yesterday's album.",
        "theory": (
            "**WHAT:** Delta Lake adds a transaction log on top of Parquet.\n\n"
            "**WHY:** Atomic commits, MERGE, time travel, OPTIMIZE.\n\n"
            "**HOW:** `.format(\"delta\").save(path)` or `saveAsTable`."
        ),
        "code": '''# --- Topic 09: Delta write + history ---
path = f"{DEMO_BASE}/teach09_delta_demo"
bronze.limit(20).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
print("Delta path:", path)
spark.sql(f"DESCRIBE HISTORY delta.`{path}`").show(5, truncate=False)
write_demo_table(bronze.limit(20), "teach09_delta_table")''',
    },
    {
        "num": "10",
        "group": "C",
        "title": "Time travel & RESTORE (undo a bad load)",
        "hard_problem": "Analyst ran DELETE … wrong filter; panic.",
        "easy_approach": "`RESTORE TABLE … TO VERSION AS OF n` or `versionAsOf`.",
        "analogy": "**Git checkout** for tables — undo a bad commit without rebuilding from bronze.",
        "theory": (
            "**WHAT:** Delta keeps table versions.\n\n"
            "**WHY:** Human error in batch jobs is guaranteed.\n\n"
            "**HOW:** DESCRIBE HISTORY → RESTORE or read with versionAsOf."
        ),
        "code": '''# --- Topic 10: time travel ---
path = f"{DEMO_BASE}/teach10_timetravel"
bronze.limit(10).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
# "bad" overwrite — simulates mistaken load
bronze.limit(2).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
print("current rows", spark.read.format("delta").load(path).count())
print("version 0 rows", spark.read.format("delta").option("versionAsOf", 0).load(path).count())
spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "timestamp", "operation").show(5, truncate=False)
# Uncomment in class to restore: spark.sql(f"RESTORE TABLE delta.`{path}` TO VERSION AS OF 0")''',
    },
    {
        "num": "11",
        "group": "C",
        "title": "MERGE upsert (incremental without full reload)",
        "hard_problem": "Nightly full replace of 5 years costs hours.",
        "easy_approach": "MERGE matched UPDATE / not matched INSERT on business key.",
        "analogy": "**Library card update:** if the card exists, update address; if new member, create the card. Don't reprint the whole phone book.",
        "theory": (
            "**WHAT:** Delta MERGE is SQL upsert.\n\n"
            "**WHY:** Incremental is cheaper and safer.\n\n"
            "**HOW:** Create target once → MERGE source into target ON key."
        ),
        "code": '''# --- Topic 11: MERGE upsert ---
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
print("MERGE demo complete — rows", spark.read.format("delta").load(target_path).count())''',
    },
    {
        "num": "12",
        "group": "C",
        "title": "OPTIMIZE + ZORDER (small files)",
        "hard_problem": "Thousands of tiny files → morning queries crawl.",
        "easy_approach": "OPTIMIZE compacts; ZORDER clusters filter columns.",
        "analogy": "**Messy desk drawers → one neat binder.** OPTIMIZE = restack papers. ZORDER = tab the binder by channel.",
        "theory": (
            "**WHAT:** Compact small files; cluster data by filter keys.\n\n"
            "**WHY:** Cloud lists + opens each file — file count kills latency.\n\n"
            "**HOW:** `OPTIMIZE table [ZORDER BY (col)]`."
        ),
        "code": '''# --- Topic 12: OPTIMIZE ---
path = f"{DEMO_BASE}/teach12_optimize"
# Many tiny writes → small files (demo)
for i in range(3):
    bronze.limit(5).write.format("delta").mode("append").save(path) if i else \\
        bronze.limit(5).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
spark.sql(f"OPTIMIZE delta.`{path}`")
print("OPTIMIZE done on", path)
# If registered as UC table you could: OPTIMIZE catalog.schema.t ZORDER BY (channel)''',
    },
    {
        "num": "13",
        "group": "D",
        "title": "Event schema contract (Zach Wilson style)",
        "hard_problem": "Every new source invents a new messy schema → N pipelines.",
        "easy_approach": "Force every source into Event(source, metric_name, timestamp, metric_value, content).",
        "analogy": "**Shipping label:** different factories (Fitbit / payments / refunds) → same label format so one warehouse conveyor works.",
        "theory": (
            "**WHAT:** One shared Event rowshape across sources.\n\n"
            "**WHY:** Processors stay small; runner unions everything.\n\n"
            "**HOW:** See also `/Shared/day9/finledger_event_processors`."
        ),
        "code": '''# --- Topic 13: Event contract ---
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
write_demo_table(events.limit(50), "teach13_events")''',
    },
    {
        "num": "14",
        "group": "D",
        "title": "Processor per source (no spaghetti joins)",
        "hard_problem": "One notebook knows every system's quirks → unmaintainable.",
        "easy_approach": "Each source has one processor function that only speaks Event.",
        "analogy": "**Embassies:** UK visa desk doesn't fix French forms — each country (source) has its own window, same passport (Event) out.",
        "theory": (
            "**WHAT:** `txn_processor(df)` / `returns_processor(df)`.\n\n"
            "**WHY:** Isolate schema chaos per source.\n\n"
            "**HOW:** Registry dict of processors → runner loops them."
        ),
        "code": '''# --- Topic 14: processor pattern ---
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
write_demo_table(timeline.limit(80), "teach14_timeline")''',
    },
    {
        "num": "15",
        "group": "D",
        "title": "Runner → daily aggregates → pivot (analyst table)",
        "hard_problem": "Stakeholders want a wide Excel-like day×metric grid.",
        "easy_approach": "Long Event table → groupBy day → pivot(metric).",
        "analogy": "**Bank teller journal → end-of-day spreadsheet.** Long list of moves becomes columns for payments / refunds.",
        "theory": (
            "**WHAT:** Long format for engineering; wide pivot for BI.\n\n"
            "**WHY:** Tableau/Power BI love pivots; MERGE loves long Events.\n\n"
            "**HOW:** `.groupBy(\"event_date\").pivot(\"metric_name\").sum(\"metric_value\")`."
        ),
        "code": '''# --- Topic 15: pivot for analysts ---
from pyspark.sql import functions as F

long = bronze.select(
    F.to_date("value_date").alias("event_date"),
    F.lit("payment_amount").alias("metric_name"),
    F.col("amount_gbp").cast("double").alias("metric_value"),
)
daily = long.groupBy("event_date", "metric_name").agg(F.sum("metric_value").alias("metric_value"))
wide = daily.groupBy("event_date").pivot("metric_name").sum("metric_value")
wide.show(truncate=False)
write_demo_table(wide, "teach15_pivot")''',
    },
    {
        "num": "16",
        "group": "E",
        "title": "Data quality gate (assert before gold)",
        "hard_problem": "Bad gold hits regulatory dashboards before anyone notices.",
        "easy_approach": "`assert_quality(df)` — empty or null keys ⇒ fail the job fast.",
        "analogy": "**Pre-flight checklist.** Plane doesn't take off if fuel is zero — even if passengers are boarded.",
        "theory": (
            "**WHAT:** Hard assertions in the notebook / Job.\n\n"
            "**WHY:** Fail early vs silent wrong KPIs.\n\n"
            "**HOW:** count > 0; null business keys == 0."
        ),
        "code": '''# --- Topic 16: quality gate ---
from pyspark.sql import functions as F

def assert_quality(df, key="transaction_id"):
    n = df.count()
    assert n > 0, "EMPTY — refuse to publish gold"
    nulls = df.filter(F.col(key).isNull()).count()
    assert nulls == 0, f"NULL keys={nulls}"
    print("quality OK:", n, "rows")

silver = bronze.dropDuplicates(["transaction_id"])
assert_quality(silver)
write_demo_table(silver.limit(30), "teach16_quality_ok")''',
    },
    {
        "num": "17",
        "group": "E",
        "title": "Idempotent re-runs (same run_id safe)",
        "hard_problem": "Re-running doubles gold numbers.",
        "easy_approach": "Overwrite/MERGE by `run_id` path — never append blindly.",
        "analogy": "**Replay the same game day** on the scoreboard replaces Saturday's score, doesn't add a second Saturday.",
        "theory": (
            "**WHAT:** Idempotency = run 1 or run 10 → same end state.\n\n"
            "**WHY:** Cloud retries happen.\n\n"
            "**HOW:** Path `.../run={run_id}/` + mode overwrite or MERGE."
        ),
        "code": '''# --- Topic 17: idempotent path write ---
from pyspark.sql import functions as F

out = f"{DEMO_BASE}/teach17_idempotent/run={RUN_ID}"
payload = bronze.limit(15)
payload.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
n1 = spark.read.format("delta").load(out).count()
payload.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
n2 = spark.read.format("delta").load(out).count()
print("first", n1, "second", n2, "same?", n1 == n2)''',
    },
    {
        "num": "18",
        "group": "E",
        "title": "Watermark / incremental window",
        "hard_problem": "Full scan every night as history grows.",
        "easy_approach": "Only process rows after last successful watermark date.",
        "analogy": "**Bookmark in a novel.** Tonight start at the bookmark, not page 1.",
        "theory": (
            "**WHAT:** Store last successful date/version; filter `>` watermark.\n\n"
            "**WHY:** Cost and time grow with full reloads.\n\n"
            "**HOW:** JSON watermark on ADLS audit or SQL control table."
        ),
        "code": '''# --- Topic 18: watermark filter ---
from pyspark.sql import functions as F

# Simulate control-plane watermark (in class this comes from audit/watermark.json)
last_ok = "2026-05-31"
incremental = bronze.filter(F.col("value_date") > F.lit(last_ok))
print("watermark", last_ok, "incremental rows", incremental.count(), "of", bronze.count())
write_demo_table(incremental.limit(40), "teach18_incremental")''',
    },
    {
        "num": "19",
        "group": "F",
        "title": "Partition pruning (don't scan the world)",
        "hard_problem": "Filter on date but data isn't partitioned → full lake scan.",
        "easy_approach": "Partition by `value_date` or `event_date` you always filter on.",
        "analogy": "**Library shelves by year.** Asking for 2026 shouldn't open 1990 boxes.",
        "theory": (
            "**WHAT:** Folder/table partitions let Spark skip files.\n\n"
            "**WHY:** Scan less = faster + cheaper.\n\n"
            "**HOW:** `.partitionBy(\"txn_date\")` when writing; filter that column when reading."
        ),
        "code": '''# --- Topic 19: partitionBy ---
from pyspark.sql import functions as F

part_path = f"{DEMO_BASE}/teach19_partitioned"
df = bronze.withColumn("txn_date", F.to_date("value_date"))
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("txn_date").save(part_path)
# Predicate on partition column → pruning
one_day = spark.read.format("delta").load(part_path).filter(F.col("txn_date") == "2026-06-01")
print("one-day rows", one_day.count())
print("partition folders under", part_path)''',
    },
    {
        "num": "20",
        "group": "F",
        "title": "Broadcast join (small dim × big fact)",
        "hard_problem": "Joining tiny channel dim to huge fact shuffles everything.",
        "easy_approach": "`F.broadcast(small_df)` — send the postage stamp to every worker.",
        "analogy": "**Cheat sheet** on every student's desk vs shipping the whole encyclopedia around class.",
        "theory": (
            "**WHAT:** Broadcast replicates small table to executors.\n\n"
            "**WHY:** Avoids expensive shuffle for tiny right-hand tables.\n\n"
            "**HOW:** `fact.join(F.broadcast(dim), \"channel\")`."
        ),
        "code": '''# --- Topic 20: broadcast join ---
from pyspark.sql import functions as F

dim = spark.createDataFrame(
    [("card", "Card Rail"), ("wire", "Wire Rail"), ("fps", "Faster Payments")],
    ["channel", "channel_name"],
)
fact = bronze.select("transaction_id", "channel", F.col("amount_gbp").cast("double").alias("amount"))
enriched = fact.join(F.broadcast(dim), "channel", "left")
enriched.show(5, truncate=False)
write_demo_table(enriched.limit(40), "teach20_broadcast")''',
    },
    {
        "num": "21",
        "group": "F",
        "title": "Skew / salting (one fat key)",
        "hard_problem": "90% of rows are `channel=card` → one executor melts.",
        "easy_approach": "Salt the hot key into N buckets during the heavy stage.",
        "analogy": "**One overloaded checkout** → open 5 temporary tills labeled card-0…card-4, then merge totals.",
        "theory": (
            "**WHAT:** Salting spreads hot keys.\n\n"
            "**WHY:** Spark partitions by key; skew = one monster partition.\n\n"
            "**HOW:** Add random salt, aggregate, then strip salt."
        ),
        "code": '''# --- Topic 21: salt for skew (concept) ---
from pyspark.sql import functions as F

fact = bronze.withColumn("amount", F.col("amount_gbp").cast("double"))
salted = fact.withColumn("salt", (F.rand() * 8).cast("int")) \\
    .withColumn("channel_salt", F.concat_ws("#", F.col("channel"), F.col("salt")))
partial = salted.groupBy("channel_salt", "channel").agg(F.sum("amount").alias("partial"))
final = partial.groupBy("channel").agg(F.sum("partial").alias("total_gbp"))
final.show(truncate=False)
write_demo_table(final, "teach21_salt_agg")''',
    },
    {
        "num": "22",
        "group": "F",
        "title": "cache / persist (reuse same DataFrame)",
        "hard_problem": "Same silver transform recomputed 5 times → 5× bill.",
        "easy_approach": "`.cache()` after an expensive stage you reuse.",
        "analogy": "**Print the report once**, put it on the table; don't reprint for every question.",
        "theory": (
            "**WHAT:** Cache keeps partition results in memory/disk.\n\n"
            "**WHY:** Multiple actions on same lineage recompute otherwise.\n\n"
            "**HOW:** `df.cache(); df.count()` to materialize; `unpersist()` when done."
        ),
        "code": '''# --- Topic 22: cache ---
from pyspark.sql import functions as F

silver = bronze.withColumn("amount", F.col("amount_gbp").cast("double")).filter(F.col("amount") > 0)
silver.cache()
print("materialize", silver.count())
print("reuse #1 by channel", silver.groupBy("channel").count().count())
print("reuse #2 sum", silver.agg(F.sum("amount")).collect()[0][0])
silver.unpersist()
print("unpersisted")''',
    },
    {
        "num": "23",
        "group": "G",
        "title": "Spark SQL (%sql) same data",
        "hard_problem": "SQL analysts refuse to learn DataFrame API.",
        "easy_approach": "Register temp view; use SQL — same engine.",
        "analogy": "**Two cashiers, same till.** PySpark and SQL are dialects talking to one kitchen (Spark).",
        "theory": (
            "**WHAT:** `createOrReplaceTempView` + `spark.sql`.\n\n"
            "**WHY:** Meet SQL-minded teammates halfway.\n\n"
            "**HOW:** Prefer views for demos; UC tables for durable objects."
        ),
        "code": '''# --- Topic 23: Spark SQL ---
bronze.createOrReplaceTempView("bronze_txn")
spark.sql("""
SELECT channel, count(*) AS txns, round(sum(cast(amount_gbp AS double)), 2) AS total_gbp
FROM bronze_txn
GROUP BY channel
ORDER BY total_gbp DESC
""").show(truncate=False)''',
    },
    {
        "num": "24",
        "group": "G",
        "title": "Unity Catalog names (catalog.schema.table)",
        "hard_problem": "Paths-only assets — nobody knows owners or grants.",
        "easy_approach": "Three-level namespace + GRANT when UC write is enabled.",
        "analogy": "**Street address:** Country (catalog) → Neighborhood (schema) → House (table).",
        "theory": (
            "**WHAT:** `finledger.gold.channel_daily` style names.\n\n"
            "**WHY:** Governance, lineage, grants.\n\n"
            "**HOW:** write_demo_table already prefers UC when permitted."
        ),
        "code": '''# --- Topic 24: UC naming ---
print("UC_WRITE_ENABLED =", UC_WRITE_ENABLED)
print("UC_TABLE        =", UC_TABLE)
ref = write_demo_table(bronze.limit(10), "teach24_uc_named")
print("Registered as:", ref)
print("Use three-level name when UC_WRITE_ENABLED else Delta path")''',
    },
    {
        "num": "25",
        "group": "G",
        "title": "COMMENTS + TAGS (docs as metadata)",
        "hard_problem": "Column meaning lives in Slack lore.",
        "easy_approach": "`COMMENT ON TABLE/COLUMN` + tags for domain/classification.",
        "analogy": "**Nutrition label on food.** You shouldn't need the chef's phone number to know calories (column meaning).",
        "theory": (
            "**WHAT:** Table/column comments and governed tags.\n\n"
            "**WHY:** Onboarding + Purview/governance alignment.\n\n"
            "**HOW:** SQL COMMENT / SET TAGS when UC write works."
        ),
        "code": '''# --- Topic 25: documentation metadata ---
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
    print("SKIP COMMENT/TAGS — UC write disabled; still teach the habit")''',
    },
    {
        "num": "26",
        "group": "G",
        "title": "Secrets — never hard-code keys",
        "hard_problem": "Keys in notebooks get committed to git → incident.",
        "easy_approach": "`dbutils.secrets.get(scope, key)` only.",
        "analogy": "**Hotel key card** from the front desk — not duct-taped under the welcome mat (git).",
        "theory": (
            "**WHAT:** Databricks secret scopes (backed by Key Vault in this lab).\n\n"
            "**WHY:** Rotate secrets without editing notebooks.\n\n"
            "**HOW:** Scope `finledger` keys `storage-account`, `storage-key`."
        ),
        "code": '''# --- Topic 26: secrets ---
acct = dbutils.secrets.get(scope="finledger", key="storage-account")
# Never print the raw key — only length proves it exists
key_len = len(dbutils.secrets.get(scope="finledger", key="storage-key"))
print("storage-account from scope:", acct)
print("storage-key length (not the key!):", key_len)''',
    },
    {
        "num": "27",
        "group": "H",
        "title": "Widgets = Job / ADF parameters",
        "hard_problem": "Hard-coded `run_id` → can't schedule yesterday vs today.",
        "easy_approach": "`dbutils.widgets.text` then `get` — ADF passes baseParameters.",
        "analogy": "**Order form fields** on a kitchen ticket — same recipe, different table number (`run_id`).",
        "theory": (
            "**WHAT:** Widgets capture parameters for Jobs and ADF.\n\n"
            "**WHY:** One notebook, many runs.\n\n"
            "**HOW:** Define widget BEFORE get — Studio Run-all safe."
        ),
        "code": '''# --- Topic 27: widgets ---
# Already defined in setup: run_id. Demonstrate reading safely.
dbutils.widgets.text("env", "training", "Environment")
print("run_id =", dbutils.widgets.get("run_id"))
print("env    =", dbutils.widgets.get("env"))
print("ADF notebooks get the same keys via baseParameters")''',
    },
    {
        "num": "28",
        "group": "H",
        "title": "dbutils.notebook.exit (return JSON to ADF)",
        "hard_problem": "ADF doesn't know how many rows silver wrote.",
        "easy_approach": "Exit with JSON string; ADF activity output captures it.",
        "analogy": "**Receipt at the door** after dinner — \"15 tables served\" so the manager (ADF Monitor) can log it.",
        "theory": (
            "**WHAT:** `dbutils.notebook.exit(json)` ends the notebook with a payload.\n\n"
            "**WHY:** Orchestrators need machine-readable results.\n\n"
            "**HOW:** Put exit in the final cell only (this topic shows build, not force-exit mid-book)."
        ),
        "code": '''# --- Topic 28: exit payload shape (demo print; real exit at notebook end) ---
import json
payload = {"run_id": RUN_ID, "bronze_rows": bronze.count(), "status": "Succeeded"}
print("ADF would receive:")
print(json.dumps(payload, indent=2))
# Mid-notebook we only print. Final cell of dedicated Jobs uses notebook.exit.'''
    },
    {
        "num": "29",
        "group": "H",
        "title": "Jobs vs interactive (why schedule)",
        "hard_problem": "Relying on humans clicking Run at 2am.",
        "easy_approach": "Databricks Job = schedule + history + alerts; notebook stays the recipe.",
        "analogy": "**Alarm clock vs hoping you wake up.** Job is the alarm; notebook is brushing your teeth.",
        "theory": (
            "**WHAT:** Jobs run notebooks / multi-task graphs on a schedule or trigger.\n\n"
            "**WHY:** Repeatable, monitored, permissioned.\n\n"
            "**HOW:** Workflows UI or Jobs API; ADF can call Databricks Job Preview."
        ),
        "code": '''# --- Topic 29: Job mindset (no API call required for teach) ---
print("Interactive: you click Run — good for learning")
print("Job: schedule notebook /Shared/day9/finledger_event_processors with run_id=")
print("     ", RUN_ID)
print("ADF: pl_db_* Notebook activity OR Job activity — orchestration layer")
print("Rule: Notebook = transform logic. Job/ADF = when & monitor.")''',
    },
    {
        "num": "30",
        "group": "I",
        "title": "SCD Type 2 (history of a dimension) — hard idea, easy pattern",
        "hard_problem": "Channel rename overtime; reports lose history.",
        "easy_approach": "Keep `valid_from`/`valid_to`/`is_current` rows per key.",
        "analogy": "**Passport history.** Old passport not deleted — stamped expired; new passport is current.",
        "theory": (
            "**WHAT:** Slowly Changing Dimension Type 2 keeps versions.\n\n"
            "**WHY:** \"What was the channel name last June?\"\n\n"
            "**HOW:** Close old row (`is_current=false`) + insert new current row."
        ),
        "code": '''# --- Topic 30: SCD2 mini ---
from pyspark.sql import functions as F

dim_v1 = spark.createDataFrame([("card", "Card", True)], ["channel_code", "channel_name", "is_current"])
# Business renames display name
incoming = spark.createDataFrame([("card", "Card Payments UK")], ["channel_code", "channel_name"])
closed = dim_v1.withColumn("is_current", F.lit(False))
new_cur = incoming.withColumn("is_current", F.lit(True))
scd2 = closed.unionByName(new_cur)
scd2.show(truncate=False)
write_demo_table(scd2, "teach30_scd2")''',
    },
    {
        "num": "31",
        "group": "I",
        "title": "Late data (watermark acceptance window)",
        "hard_problem": "Mobile payments arrive 6 hours late; daily KPI already closed.",
        "easy_approach": "Accept late events within a watermark window; mark stragglers.",
        "analogy": "**Box office closes at 9pm but allows tickets sold until 9:15** — after that, they go to tomorrow's report.",
        "theory": (
            "**WHAT:** Event-time vs processing-time; watermark lag.\n\n"
            "**WHY:** Real systems are late.\n\n"
            "**HOW:** Compare `value_date` to batch `as_of` minus allowed lag."
        ),
        "code": '''# --- Topic 31: late arrivals ---
from pyspark.sql import functions as F

as_of = F.lit("2026-06-02")
lag_days = 1
labeled = bronze.withColumn("txn_date", F.to_date("value_date")) \\
    .withColumn("is_late", F.col("txn_date") < F.date_sub(as_of.cast("date"), lag_days))
labeled.groupBy("is_late").count().show()
write_demo_table(labeled.select("transaction_id", "txn_date", "is_late").limit(40), "teach31_late")''',
    },
    {
        "num": "32",
        "group": "I",
        "title": "CDC mindset (changes only)",
        "hard_problem": "Full extracts crush databases and wallets.",
        "easy_approach": "Process inserts/updates/deletes only — Delta CDF or SQL change tracking.",
        "analogy": "**Security camera motion clips**, not 24h of empty hallway footage.",
        "theory": (
            "**WHAT:** Change Data Capture streams row diffs.\n\n"
            "**WHY:** Incremental is the real world.\n\n"
            "**HOW:** ADF `pl_24` teaches SQL change tracking; Delta has Change Data Feed."
        ),
        "code": '''# --- Topic 32: CDC as labels (concept) ---
from pyspark.sql import functions as F

# Simulate a CDF-like feed
changes = spark.createDataFrame([
    ("TXN-1", "insert", 10.0),
    ("TXN-1", "update", 12.0),
    ("TXN-2", "delete", None),
], ["transaction_id", "_change_type", "amount"])
changes.show(truncate=False)
print("Downstream applies INSERT/UPDATE/DELETE in order — don't reload all history")
write_demo_table(changes, "teach32_cdc_sim")''',
    },
    {
        "num": "33",
        "group": "I",
        "title": "Cost habits (auto-terminate, right size)",
        "hard_problem": "Idle clusters burn DBUs overnight.",
        "easy_approach": "Auto-termination, small skill-cluster for class, stop when done.",
        "analogy": "**Turn off meeting-room lights.** Empty room still gets billed if left on.",
        "theory": (
            "**WHAT:** Cluster policies, auto-terminate, spot where allowed.\n\n"
            "**WHY:** Training estates die from idle cost, not from education.\n\n"
            "**HOW:** Compute UI — Autotermination minutes; orchestrate.cmd --warm-cluster-only when teaching."
        ),
        "code": '''# --- Topic 33: cost checklist (print) ---
tips = [
    ("auto_terminate", "set 30–140 min for class clusters"),
    ("right_size", "Don't use 8 workers for a 5MB CSV"),
    ("serverless_tradeoff", "Serverless easy but may block account-key ADLS — prefer classic with RBAC"),
    ("stop_habit", "Terminate when session ends"),
]
spark.createDataFrame(tips, ["habit", "detail"]).show(truncate=False)
write_demo_table(spark.createDataFrame(tips, ["habit", "detail"]), "teach33_cost_tips")''',
    },
    {
        "num": "34",
        "group": "I",
        "title": "End-to-end story (wire the pieces)",
        "hard_problem": "Learners know buttons but not how the night batch runs.",
        "easy_approach": "Narrate bronze → silver → Event → gold → Job/ADF → quality.",
        "analogy": "**Restaurant dinner service:** deliveries (bronze) → prep (silver) → plating (gold) → waiter ticket (Job/ADF) → health check (quality).",
        "theory": (
            "**WHAT:** One mental plot for FinLedger nightly.\n\n"
            "**WHY:** Interviews and ops need the story, not only APIs.\n\n"
            "**HOW:** Connect this teach notebook to 50-problems + ADF pl_gov_06 / pl_db_*."
        ),
        "code": '''# --- Topic 34: e2e narrative metrics ---
from pyspark.sql import functions as F

story = spark.createDataFrame([
    (RUN_ID, "1_bronze", bronze.count(), "land CSV"),
    (RUN_ID, "2_silver_dedupe", bronze.dropDuplicates(["transaction_id"]).count(), "keys unique"),
    (RUN_ID, "3_gold_channels", bronze.groupBy("channel").count().count(), "KPI rows"),
    (RUN_ID, "4_next", 1, "Job/ADF + Purview lineage"),
], ["run_id", "stage", "metric", "note"])
story.show(truncate=False)
write_demo_table(story, "teach34_e2e_story")''',
    },
    {
        "num": "35",
        "group": "J",
        "title": "Window functions (rank without collapsing rows)",
        "hard_problem": "Need top txn per channel but keep every row for audit.",
        "easy_approach": "Window `rank()` / `row_number()` over partition — filter afterward.",
        "analogy": "**School ranking:** everyone stays in class; you just write a rank sticker on each desk. GroupBy would kick students out into one chair.",
        "theory": (
            "**WHAT:** Window specs add columns without reducing row count.\n\n"
            "**WHY:** Rankings, running totals, previous-day compare.\n\n"
            "**HOW:** `Window.partitionBy(...).orderBy(...)` + `F.row_number()`."
        ),
        "code": '''# --- Topic 35: windows ---
from pyspark.sql import functions as F, Window

w = Window.partitionBy("channel").orderBy(F.col("amount_gbp").cast("double").desc())
ranked = bronze.withColumn("rn", F.row_number().over(w))
top1 = ranked.filter(F.col("rn") == 1).select("channel", "transaction_id", "amount_gbp", "rn")
top1.show(truncate=False)
write_demo_table(top1, "teach35_window_top")''',
    },
    {
        "num": "36",
        "group": "J",
        "title": "Pivot (turn channels into columns)",
        "hard_problem": "Excel people want channels as columns, not tall rows.",
        "easy_approach": "`groupBy(...).pivot(\"channel\").sum(...)` then fillna(0).",
        "analogy": "**Crossword board.** Tall list of clues becomes a grid; same facts, board-friendly shape.",
        "theory": (
            "**WHAT:** Pivot reshapes category values into columns.\n\n"
            "**WHY:** Finance dashboards often want wide tables.\n\n"
            "**HOW:** Aggregate first mentally; pivot is an aggregate reshape."
        ),
        "code": '''# --- Topic 36: pivot ---
from pyspark.sql import functions as F

wide = (
    bronze.withColumn("amount", F.col("amount_gbp").cast("double"))
    .groupBy(F.to_date("value_date").alias("txn_date"))
    .pivot("channel")
    .sum("amount")
)
wide = wide.fillna(0)
wide.show(5, truncate=False)
write_demo_table(wide, "teach36_pivot")''',
    },
    {
        "num": "37",
        "group": "J",
        "title": "Union vs Join (stack vs stitch)",
        "hard_problem": "Mixing twin feeds with a join accidentally multiplies rows.",
        "easy_approach": "Same grain + same columns → unionByName. Different entities linked by key → join.",
        "analogy": "**Union** = stacking two trays of cookies. **Join** = pairing each cookie with its frosting recipe card by id.",
        "theory": (
            "**WHAT:** Union appends rows; join stitches columns by key.\n\n"
            "**WHY:** Wrong verb → Cartesian blowups or missing columns.\n\n"
            "**HOW:** `unionByName(allowMissingColumns=True)` for Event stacks."
        ),
        "code": '''# --- Topic 37: union vs join ---
from pyspark.sql import functions as F

a = bronze.select("transaction_id", "channel").limit(5).withColumn("src", F.lit("a"))
b = bronze.select("transaction_id", "channel").limit(5).withColumn("src", F.lit("b"))
stacked = a.unionByName(b)
print("union rows", stacked.count())
# Join same keys → can duplicate; demo left key to dim
dim = spark.createDataFrame([("card", "CARD")], ["channel", "code"])
stitched = bronze.limit(20).join(dim, "channel", "left")
print("join cols", stitched.columns)
write_demo_table(stacked, "teach37_union")''',
    },
    {
        "num": "38",
        "group": "J",
        "title": "nulls, coalesce, fillna (missing ≠ zero)",
        "hard_problem": "Null amounts silently drop out of SUM; null keys break joins.",
        "easy_approach": "`coalesce` for defaults; quarantine null keys — don't invent truth.",
        "analogy": "**Empty plate vs zero cookies.** Empty plate means \"unknown\"; zero means \"counted none\". Don't pretend they are the same.",
        "theory": (
            "**WHAT:** Null handling with coalesce / fillna / isNull filters.\n\n"
            "**WHY:** Joins and aggs treat null specially.\n\n"
            "**HOW:** Business rule decides default vs reject."
        ),
        "code": '''# --- Topic 38: nulls ---
from pyspark.sql import functions as F

demo = bronze.withColumn("amount", F.col("amount_gbp").cast("double")) \\
    .withColumn("amount_safe", F.coalesce(F.col("amount"), F.lit(0.0)))
print("null amounts", demo.filter(F.col("amount").isNull()).count())
demo.select("transaction_id", "amount", "amount_safe").show(5, truncate=False)
write_demo_table(demo.select("transaction_id", "amount", "amount_safe").limit(50), "teach38_nulls")''',
    },
    {
        "num": "39",
        "group": "J",
        "title": "Cache / persist (reuse expensive scans)",
        "hard_problem": "Same cleaned DF is scanned 5 times in one notebook → 5× ADLS read.",
        "easy_approach": "`cache()` once after expensive clean; `unpersist()` when done.",
        "analogy": "**Photocopy the map once** for the road trip. Don't drive back to the tourist office for every turn.",
        "theory": (
            "**WHAT:** Cache keeps a DataFrame in memory/disk across actions.\n\n"
            "**WHY:** Multiple actions on the same lineage recompute otherwise.\n\n"
            "**HOW:** Cache after clean; unpersist to free cluster RAM."
        ),
        "code": '''# --- Topic 39: cache ---
from pyspark.sql import functions as F

cleaned = bronze.withColumn("amount", F.col("amount_gbp").cast("double")).filter(F.col("amount") > 0)
cleaned.cache()
print("action1", cleaned.count())
print("action2 channels", cleaned.select("channel").distinct().count())
cleaned.unpersist()
print("cache released")
write_demo_table(cleaned.limit(20), "teach39_cache_sample")''',
    },
    {
        "num": "40",
        "group": "J",
        "title": "UDF trap (why stay in Spark SQL functions)",
        "hard_problem": "Python UDFs serialize row-by-row and kill performance.",
        "easy_approach": "Prefer `F.upper`, `F.when`, built-ins; UDF only if no Spark equivalent.",
        "analogy": "**Hand-carrying each letter across town** (Python UDF) vs **post office bulk route** (Spark functions).",
        "theory": (
            "**WHAT:** UDFs run custom Python/Scala per row.\n\n"
            "**WHY:** They break Catalyst optimization and add Python↔JVM hops.\n\n"
            "**HOW:** Rewrite with `when`/`otherwise` first."
        ),
        "code": '''# --- Topic 40: prefer built-ins over UDF ---
from pyspark.sql import functions as F

# GOOD — Spark native
flagged = bronze.withColumn(
    "risk",
    F.when(F.col("amount_gbp").cast("double") > 1000, "high").otherwise("normal"),
)
flagged.groupBy("risk").count().show()
print("Avoid: @udf def risk(x): ... — only if no F.when equivalent")
write_demo_table(flagged.select("transaction_id", "amount_gbp", "risk").limit(40), "teach40_no_udf")''',
    },
    {
        "num": "41",
        "group": "K",
        "title": "VACUUM & retention (don't delete history by accident)",
        "hard_problem": "Time travel vanishes after vacuum; auditors angry.",
        "easy_approach": "Know retention hours; vacuum only after policy; never vacuum mid-demo casually.",
        "analogy": "**Shredding old tax returns.** Legal once retention ends — premature shredding is a lawsuit.",
        "theory": (
            "**WHAT:** VACUUM removes unreferenced data files past retention.\n\n"
            "**WHY:** Saves storage; shortens recoverable history.\n\n"
            "**HOW:** Lab: DESCRIBE HISTORY; teach vacuum verbally — avoid aggressive vacuum on shared tables."
        ),
        "code": '''# --- Topic 41: history vs vacuum awareness ---
path = f"{DEMO_BASE}/teach41_hist"
bronze.limit(30).write.format("delta").mode("overwrite").save(path)
bronze.limit(25).write.format("delta").mode("overwrite").save(path)
spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "timestamp", "operation").show(5, truncate=False)
print("VACUUM would delete old files AFTER retention — demo skips VACUUM on shared lab")
write_demo_table(spark.read.format("delta").load(path), "teach41_hist_current")''',
    },
    {
        "num": "42",
        "group": "K",
        "title": "Partition pruning (only read needed folders)",
        "hard_problem": "Full lake scan for one value_date burns cost.",
        "easy_approach": "Partition gold by date; filter that column so Spark skips folders.",
        "analogy": "**Library shelves by year.** Asking 2026 doesn't open the 2019 aisle.",
        "theory": (
            "**WHAT:** Partition columns become folder hierarchy; filters prune paths.\n\n"
            "**WHY:** Cheap scans on large tables.\n\n"
            "**HOW:** Don't over-partition (too many tiny folders)."
        ),
        "code": '''# --- Topic 42: partition prune ---
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
write_demo_table(one.limit(20), "teach42_pruned")''',
    },
    {
        "num": "43",
        "group": "K",
        "title": "Structured Streaming mindset (micro-batches)",
        "hard_problem": "People think streaming = reinvent Spark; class only has batch CSV.",
        "easy_approach": "Same DataFrame API; source is a stream + checkpoint for exactly-once offsets.",
        "analogy": "**Conveyor belt sushi** vs buffet. Same fish (API); continuous plate arrivals + ticket stubs (checkpoint).",
        "theory": (
            "**WHAT:** Structured Streaming processes incrementally with a checkpoint directory.\n\n"
            "**WHY:** Near-real-time KPIs without rewriting all logic.\n\n"
            "**HOW:** Concept cell — rate source or batch-as-stream; production needs checkpoint path on ADLS."
        ),
        "code": '''# --- Topic 43: streaming concept (batch stand-in) ---
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
)''',
    },
    {
        "num": "44",
        "group": "K",
        "title": "Auto Loader idea (discover new files)",
        "hard_problem": "Listing millions of landings by hand; missed files; double loads.",
        "easy_approach": "cloudFiles + schema location; incremental file notification/listing.",
        "analogy": "**Mailroom barcode scanner** that only processes new parcels since last shift.",
        "theory": (
            "**WHAT:** Auto Loader (`cloudFiles`) ingests new files incrementally.\n\n"
            "**WHY:** Landed files grow forever; full directory list is expensive.\n\n"
            "**HOW:** Mention in class; ADF copy + batch is enough for FinLedger lab scale."
        ),
        "code": '''# --- Topic 44: Auto Loader mental model ---
tips = [
    ("cloudFiles.format", "csv/json/parquet"),
    ("schemaLocation", "evolving schema store"),
    ("checkpoint", "which files already seen"),
    ("lab_today", "ADF lands fixed path; spark.read CSV is fine at demo scale"),
]
spark.createDataFrame(tips, ["knob", "role"]).show(truncate=False)
write_demo_table(spark.createDataFrame(tips, ["knob", "role"]), "teach44_autoloader")''',
    },
    {
        "num": "45",
        "group": "K",
        "title": "JDBC / Azure SQL bridge (warehouse handoff)",
        "hard_problem": "Gold lives in lake; finance app still speaks SQL Server.",
        "easy_approach": "JDBC write/read with secrets; or ADF copy gold → Azure SQL.",
        "analogy": "**Customs form.** Lake is the cargo ship; SQL DB is the warehouse with barcode scanners the app already knows.",
        "theory": (
            "**WHAT:** `spark.read.jdbc` / `.write.jdbc` or ADF Copy.\n\n"
            "**WHY:** Operational apps rarely query Delta directly.\n\n"
            "**HOW:** Never paste SQL passwords in notebooks — Key Vault / secret scope."
        ),
        "code": '''# --- Topic 45: JDBC pattern (dry-run print) ---
print("Pattern (do NOT run without trainer SQL secrets):")
print('  df.write.format("jdbc").option("url", url).option("dbtable", "dbo.gold_channel").mode("overwrite").save()')
print("FinLedger lab often uses ADF Copy for SQL handoff — same idea, orchestration UI")
write_demo_table(
    spark.createDataFrame([("lake_gold", "azure_sql", "jdbc_or_adf")], ["from_layer", "to_system", "bridge"]),
    "teach45_jdbc_map",
)''',
    },
    {
        "num": "46",
        "group": "L",
        "title": "Multi-task Jobs (DAG of notebooks)",
        "hard_problem": "One mega-notebook; failure mid-way redoes everything.",
        "easy_approach": "Job tasks: bronze → silver → gold with depends_on; retry per task.",
        "analogy": "**Assembly line stations.** If frosting fails, don't re-bake the cake from flour — restart at frosting.",
        "theory": (
            "**WHAT:** Databricks Workflows multi-task jobs with dependencies.\n\n"
            "**WHY:** Isolate failures; clearer lineage in Runs UI.\n\n"
            "**HOW:** Separate notebooks or task parameters; shared `run_id` widget."
        ),
        "code": '''# --- Topic 46: job DAG story ---
tasks = [
    ("t1_bronze", None, "land + validate"),
    ("t2_silver", "t1_bronze", "clean + Event"),
    ("t3_gold", "t2_silver", "KPI + pivot"),
    ("t4_quality", "t3_gold", "assert + exit JSON"),
]
spark.createDataFrame(tasks, ["task", "depends_on", "role"]).show(truncate=False)
write_demo_table(spark.createDataFrame(tasks, ["task", "depends_on", "role"]), "teach46_job_dag")''',
    },
    {
        "num": "47",
        "group": "L",
        "title": "Purview / lineage habit (what did we just write?)",
        "hard_problem": "Nobody finds assets in Purview; ADF lineage looks empty.",
        "easy_approach": "Stable paths + table names; scan ADLS; search storage account not factory name.",
        "analogy": "**Library catalog card.** Book must have a spine label (path) before the card catalog helps anyone.",
        "theory": (
            "**WHAT:** Microsoft Purview catalogs assets and lineage.\n\n"
            "**WHY:** Governance + \"who eats this table?\"\n\n"
            "**HOW:** Shared lab: classic Purview portal; search `stshared*` paths; Data Curator on MI."
        ),
        "code": '''# --- Topic 47: lineage checklist ---
rows = [
    ("stable_path", BRONZE_PATH if "BRONZE_PATH" in dir() else f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/..."),
    ("search_hint", STORAGE_ACCOUNT),
    ("not_this", "searching ADF factory name alone often fails"),
    ("next_doc", "shared-adf-lab/docs/purview_portal_search_guide.md"),
]
spark.createDataFrame(rows, ["item", "value"]).show(truncate=False)
write_demo_table(spark.createDataFrame(rows, ["item", "value"]), "teach47_purview_hints")''',
    },
    {
        "num": "48",
        "group": "L",
        "title": "%run and shared helpers (don't paste auth 50 times)",
        "hard_problem": "Auth cell drifts across notebooks; one fix needs 12 edits.",
        "easy_approach": "One bronze_auth builder → generated notebooks; or `%run` a shared util notebook.",
        "analogy": "**Shared spice rack** in the kitchen — every chef uses the same salt cellar, not personal bags that expire.",
        "theory": (
            "**WHAT:** `%run ./utils` pulls another notebook into this one.\n\n"
            "**WHY:** DRY setup — auth, helpers, constants.\n\n"
            "**HOW:** This lab generates cells from `bronze_auth_cell.py` so learners see auth inline too."
        ),
        "code": '''# --- Topic 48: DRY setup ---
print("In Databricks you can: %run /Shared/day9/_utils_auth")
print("In this repo: day9/scripts/bronze_auth_cell.py → injected by builders")
print("STORAGE_ACCOUNT =", STORAGE_ACCOUNT)
print("DEMO_BASE =", DEMO_BASE)
write_demo_table(
    spark.createDataFrame([("bronze_auth_cell", "shared"), ("demo_table_helpers", "shared")], ["module", "scope"]),
    "teach48_dry",
)''',
    },
    {
        "num": "49",
        "group": "L",
        "title": "Interview story (explain your pipeline in 90 seconds)",
        "hard_problem": "Engineer freezes: lists APIs instead of business flow.",
        "easy_approach": "Narrate: source → bronze → silver/Event → gold → Job/ADF → quality → who consumes.",
        "analogy": "**Explain a recipe to a friend**, not the brand of oven heating element.",
        "theory": (
            "**WHAT:** Structured verbal story for interviews and stakeholder demos.\n\n"
            "**WHY:** Trust comes from clarity under time pressure.\n\n"
            "**HOW:** Practice aloud using FinLedger nouns (channels, returns, run_id)."
        ),
        "code": '''# --- Topic 49: 90-second script (print + recite) ---
script = [
    "1. Banks land CSVs in bronze via ADF.",
    "2. Databricks silver casts + dedupes; Event model unifies payments + returns.",
    "3. Gold aggregates by channel/day for finance.",
    "4. Job/ADF schedules nightly; quality asserts fail fast.",
    "5. Purview/SQL metadata help ops find owners.",
]
for line in script:
    print(line)
write_demo_table(spark.createDataFrame([(i + 1, s) for i, s in enumerate(script)], ["step", "line"]), "teach49_interview")''',
    },
    {
        "num": "50",
        "group": "L",
        "title": "Capstone checklist (you are ready when…)",
        "hard_problem": "\"I finished the course\" without proving skills.",
        "easy_approach": "Tick every box below on real cluster with FinLedger data.",
        "analogy": "**Driver's license road test** — not just reading the highway code.",
        "theory": (
            "**WHAT:** Validation checklist for Artem V&V style readiness.\n\n"
            "**WHY:** Verification (runs) ≠ Validation (learner can explain).\n\n"
            "**HOW:** Run teach-all + one 50-problem failure + event processors once."
        ),
        "code": '''# --- Topic 50: readiness checklist ---
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
print("Teach-all Topics 01–50 complete — practice aloud, then run 50_data_engineering_problems")''',
    },
]

TOPIC_GROUPS: list[tuple[str, str, list[str]]] = [
    ("A", "Spark fundamentals (start here)", [f"{i:02d}" for i in range(1, 6)]),
    ("B", "Medallion & cleansing", [f"{i:02d}" for i in range(6, 9)]),
    ("C", "Delta Lake power tools", [f"{i:02d}" for i in range(9, 13)]),
    ("D", "Zach Event model (multi-source)", [f"{i:02d}" for i in range(13, 16)]),
    ("E", "Reliability & quality", [f"{i:02d}" for i in range(16, 19)]),
    ("F", "Performance (hard → easy)", [f"{i:02d}" for i in range(19, 23)]),
    ("G", "SQL, Unity Catalog, secrets", [f"{i:02d}" for i in range(23, 27)]),
    ("H", "Jobs, widgets, ADF hooks", [f"{i:02d}" for i in range(27, 30)]),
    ("I", "Complicated ideas, simple stories", [f"{i:02d}" for i in range(30, 35)]),
    ("J", "Reshape & everyday pitfalls", [f"{i:02d}" for i in range(35, 41)]),
    ("K", "Lakehouse ops & handoff", [f"{i:02d}" for i in range(41, 46)]),
    ("L", "Jobs, governance, teach-ready", [f"{i:02d}" for i in range(46, 51)]),
]

assert len(TOPICS) == 50
assert all(t["num"] for t in TOPICS)
