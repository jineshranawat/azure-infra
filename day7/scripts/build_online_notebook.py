#!/usr/bin/env python3
"""Generate day7/notebooks/online_step_by_step.ipynb (100+ small cells)."""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "notebooks" / "online_step_by_step.ipynb"


def md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": [text]}


def code(lines: list[str]) -> dict:
    src = [ln + "\n" for ln in lines]
    if src:
        src[-1] = src[-1].rstrip("\n")
    return {
        "cell_type": "code",
        "metadata": {},
        "source": src,
        "outputs": [],
        "execution_count": None,
    }


def build_cells() -> list[dict]:
    c: list[dict] = []

    def section(title: str, body: str = "") -> None:
        c.append(md(f"## {title}\n\n{body}".strip()))

    # --- INTRO ---
    c.append(
        md(
            "# Day 7 — Online lab (120+ cells)\n\n"
            "**Colab:** File → Upload this notebook → Run top to bottom.\n\n"
            "**Rule:** Each code cell is 2–4 lines. Read the `#` comment on every line.\n\n"
            "**Path:** Python → pandas → Polars → PySpark → FinLedger end-to-end."
        )
    )

    section("Part 1 — Install packages (Colab / fresh Jupyter)")
    c.append(
        code(
            [
                "# Install FinLedger lab packages (skip if already installed)",
                "!pip install -q pandas polars pyspark",
            ]
        )
    )
    c.append(code(["# Confirm install finished", 'print("Step 1 done: packages installed")']))

    section("Part 2 — Python: variables and types")
    c.append(code(["# A string holds text — here a transaction id", 'txn_id = "TXN-10003"', 'print("txn_id:", txn_id)']))
    c.append(code(["# An integer counts whole numbers", "row_count = 5", 'print("rows in sample file:", row_count)']))
    c.append(code(["# A float holds decimal money amounts", "amount = 50000.00", 'print("amount GBP:", amount)']))
    c.append(code(["# A boolean is True or False — used in filters", "is_posted = True", 'print("is_posted:", is_posted)']))
    c.append(code(["# type() shows the Python type of a value", 'print(type(txn_id), type(amount), type(is_posted))']))

    section("Part 3 — Python: lists (many rows as a list)")
    c.append(code(["# A list keeps ordered items — channels we accept", 'channels = ["wire", "card", "fps"]', 'print(channels)']))
    c.append(code(["# Index 0 is the first item (wire)", 'print("first channel:", channels[0])']))
    c.append(code(["# -1 is the last item", 'print("last channel:", channels[-1])']))
    c.append(code(["# len() counts items in the list", 'print("how many channels:", len(channels))']))
    c.append(code(["# Append adds one item to the end", 'channels.append("chaps")', 'print("after append:", channels)']))
    c.append(code(["# List comprehension — build a new list from an old one", "upper = [ch.upper() for ch in channels[:3]]", "print(upper)"]))

    section("Part 4 — Python: dictionaries (one row as key→value)")
    c.append(
        code(
            [
                "# Dict maps column names to values — like one CSV row",
                'txn = {"transaction_id": "TXN-10003", "amount_gbp": "50000.00", "channel": "wire"}',
                'print(txn)',
            ]
        )
    )
    c.append(code(["# Read one field with square brackets", 'print("id:", txn["transaction_id"])']))
    c.append(code(["# .get() is safe if key might be missing", 'print("status:", txn.get("status", "unknown"))']))
    c.append(code(["# keys() and values() list field names and values", 'print(list(txn.keys()))', 'print(list(txn.values()))']))
    c.append(
        code(
            [
                "# List of dicts = many rows before pandas/Spark",
                "rows = [",
                '    {"transaction_id": "TXN-10001", "channel": "wire", "amount_gbp": "1250.50", "status": "posted"},',
                '    {"transaction_id": "TXN-10002", "channel": "card", "amount_gbp": "89.99", "status": "posted"},',
                '    {"transaction_id": "TXN-10003", "channel": "wire", "amount_gbp": "50000.00", "status": "pending"},',
                "]",
            ]
        )
    )
    c.append(code(["# How many rows in our mini dataset", "print(len(rows))"]))

    section("Part 5 — Python: functions (reusable rules)")
    c.append(
        code(
            [
                "# Define a function — same idea as clean_amount in Session 6",
                "def clean_amount(value):",
                "    try:",
                "        return float(value)",
                "    except (TypeError, ValueError):",
                "        return 0.0",
            ]
        )
    )
    c.append(code(["# Call the function with good and bad input", 'print(clean_amount("50000"))', 'print(clean_amount("oops"))']))
    c.append(code(["# Loop rows and clean amount_gbp one at a time", "for r in rows:", '    print(r["transaction_id"], clean_amount(r["amount_gbp"]))']))

    section("Part 6 — Python: if / else (business rules)")
    c.append(
        code(
            [
                "# Flag large wires for fraud monitoring",
                "amt = clean_amount(rows[2]['amount_gbp'])",
                'if amt >= 10000 and rows[2]["channel"] == "wire":',
                '    print("FRAUD WATCH:", rows[2]["transaction_id"])',
            ]
        )
    )
    c.append(
        code(
            [
                "# Keep only posted rows using a list comprehension",
                'posted = [r for r in rows if r["status"] == "posted"]',
                "print(len(posted), \"posted rows\")",
            ]
        )
    )

    section("Part 7 — Python: pathlib (paths without typos)")
    c.append(code(["# pathlib builds paths with / instead of string concat", "from pathlib import Path"]))
    c.append(code(["# Simulate lake folder layout from Session 3", 'run = Path("loaded") / "run=session3-lab"', 'print(run)']))
    c.append(code(["# File name inside the run folder", 'csv_name = run / "sample_transactions.csv"', "print(csv_name)"]))

    section("Part 8 — pandas: load and explore (local, eager)")
    c.append(code(["# pandas = in-memory table on your laptop", "import pandas as pd"]))
    c.append(code(["# Build DataFrame from list of dicts", "pdf = pd.DataFrame(rows)", "print(pdf)"]))
    c.append(code(["# .shape = (rows, columns)", "print(\"shape:\", pdf.shape)"]))
    c.append(code(["# .columns lists column names", "print(list(pdf.columns))"]))
    c.append(code(["# .dtypes shows types — amount_gbp is object (string)", "print(pdf.dtypes)"]))
    c.append(code(["# Select one column like a list", 'print(pdf["channel"])']))
    c.append(code(["# Filter rows — only posted", 'posted_pdf = pdf[pdf["status"] == "posted"]', "print(posted_pdf)"]))
    c.append(code(["# astype converts string column to float", 'pdf["amount"] = pdf["amount_gbp"].astype(float)', "print(pdf[[\"transaction_id\", \"amount\"]])"]))
    c.append(code(["# groupby sums amount per channel", 'totals = pdf.groupby("channel")["amount"].sum()', "print(totals)"]))

    section("Part 9 — Polars: same data, fast column engine")
    c.append(code(["# Polars = modern local DataFrame (Rust engine)", "import polars as pl"]))
    c.append(code(["# Create Polars DataFrame from same rows", "plf = pl.DataFrame(rows)", "print(plf)"]))
    c.append(code(["# height = row count", "print(\"rows:\", plf.height)"]))
    c.append(code(["# select picks columns", 'slim = plf.select("transaction_id", "channel", "status")', "print(slim)"]))
    c.append(code(["# filter keeps matching rows", 'posted_pl = plf.filter(pl.col("status") == "posted")', "print(posted_pl)"]))
    c.append(
        code(
            [
                "# with_columns adds typed amount column",
                'pl_typed = plf.with_columns(pl.col("amount_gbp").cast(pl.Float64).alias("amount"))',
                "print(pl_typed)",
            ]
        )
    )
    c.append(code(["# group_by + agg like SQL GROUP BY", 'by_ch = pl_typed.group_by("channel").agg(pl.col("amount").sum())', "print(by_ch)"]))

    section("Part 10 — Compare pandas vs Polars vs Spark (concept)")
    c.append(
        md(
            "| Tool | Runs on | When to use |\n"
            "|------|---------|-------------|\n"
            "| pandas | Your PC RAM | Quick checks, small CSV |\n"
            "| Polars | Your PC RAM | Fast local transforms |\n"
            "| PySpark | Cluster / Colab CPU | Lake scale, Databricks |"
        )
    )
    c.append(code(['print("Next: PySpark — same ideas, distributed lazy engine")']))

    section("Part 11 — PySpark: SparkSession (entry point)")
    c.append(code(["# SparkSession is the door to read/transform/write", "from pyspark.sql import SparkSession"]))
    c.append(
        code(
            [
                "# builder creates a local Spark for Colab/Jupyter",
                'spark = SparkSession.builder.appName("finledger-day7").master("local[*]").getOrCreate()',
                'print("Spark version:", spark.version)',
            ]
        )
    )
    c.append(code(["# Lower log noise so output is readable", 'spark.sparkContext.setLogLevel("WARN")']))

    section("Part 12 — PySpark: create DataFrame (our bronze sample)")
    c.append(
        code(
            [
                "# createDataFrame loads rows into a distributed lazy table",
                "data = [",
                '    ("TXN-10001", "wire", "1250.50", "posted"),',
                '    ("TXN-10002", "card", "89.99", "posted"),',
                '    ("TXN-10003", "wire", "50000.00", "pending"),',
                '    ("TXN-10004", "card", "INVALID", "posted"),',
                "]",
            ]
        )
    )
    c.append(
        code(
            [
                "# Column names define the schema",
                'cols = ["transaction_id", "channel", "amount_gbp", "status"]',
                "df = spark.createDataFrame(data, cols)",
            ]
        )
    )
    c.append(code(["# show() is an ACTION — prints sample rows", "df.show()"]))
    c.append(code(["# printSchema() shows names and types", "df.printSchema()"]))
    c.append(code(["# count() is an ACTION — runs the job", 'print("row count:", df.count())']))
    c.append(code(["# columns property lists names", "print(df.columns)"]))

    section("Part 13 — Lazy vs action (critical idea)")
    c.append(
        md(
            "**Transformation (lazy):** `select`, `filter`, `withColumn` — builds a plan.\n\n"
            "**Action (runs now):** `show`, `count`, `collect`, `write`."
        )
    )
    c.append(code(["# filter is LAZY — no work yet", 'step1 = df.filter("status = \'posted\'")', 'print("plan step added (lazy)")']))
    c.append(code(["# count is ACTION — executes filter + count", 'print("posted rows:", step1.count())']))
    c.append(code(["# show is ACTION — prints up to n rows", "step1.show(5)"]))

    section("Part 14 — select: pick columns")
    c.append(code(["# select keeps only the columns you name", 'slim = df.select("transaction_id", "channel", "amount_gbp", "status")']))
    c.append(code(["# show result — still lazy until show/count", "slim.show()"]))
    c.append(code(["# select with alias renames a column", 'from pyspark.sql import functions as F', 'renamed = slim.select(F.col("transaction_id").alias("txn_id"))', "renamed.show()"]))

    section("Part 15 — filter: keep rows")
    c.append(code(["# SQL string filter — easy to read", 'posted = df.filter("status = \'posted\'")', "posted.show()"]))
    c.append(code(["# Column API filter — preferred in production", 'posted2 = df.filter(F.col("status") == "posted")', "print(posted2.count())"]))
    c.append(code(["# Combine conditions with & and |", 'big_wire = df.filter((F.col("channel") == "wire") & (F.col("status") == "pending"))', "big_wire.show()"]))

    section("Part 16 — withColumn and cast (bronze → silver typing)")
    c.append(code(["# Bronze CSV stores amounts as strings", "df.select(\"amount_gbp\").printSchema()"]))
    c.append(code(["# withColumn adds/replaces a column", 'typed = df.withColumn("amount", F.col("amount_gbp").cast("double"))', "typed.printSchema()"]))
    c.append(code(["# Invalid text becomes null after cast", "typed.show()"]))
    c.append(code(["# isNull() finds bad rows for quarantine (Session 9)", "bad = typed.filter(F.col(\"amount\").isNull())", "bad.show()"]))
    c.append(code(["# Keep valid positive amounts", "good = typed.filter((F.col(\"amount\").isNotNull()) & (F.col(\"amount\") > 0))", "good.show()"]))

    section("Part 17 — dropDuplicates (one row per transaction_id)")
    c.append(
        code(
            [
                "# Add duplicate row to demo dedupe",
                'dup_data = data + [("TXN-10001", "wire", "1250.50", "posted")]',
                "df_dup = spark.createDataFrame(dup_data, cols)",
            ]
        )
    )
    c.append(code(['print("before dedupe:", df_dup.count())']))
    c.append(code(["# dropDuplicates keeps one row per key", 'deduped = df_dup.dropDuplicates(["transaction_id"])', 'print("after:", deduped.count())']))

    section("Part 18 — join: enrich with lookup table")
    c.append(
        code(
            [
                "# Small reference table: channel → risk level",
                'lookup = spark.createDataFrame([("wire","high"),("card","medium"),("fps","low")], ["channel","risk"])',
                "lookup.show()",
            ]
        )
    )
    c.append(code(["# left join keeps all transactions, adds risk", 'enriched = good.join(lookup, on="channel", how="left")', "enriched.show()"]))
    c.append(code(["# inner join keeps only rows with a match on both sides", 'inner = good.join(lookup, on="channel", how="inner")', "print(inner.count())"]))

    section("Part 19 — groupBy and agg (gold preview)")
    c.append(code(["# groupBy splits rows into buckets by channel", 'grouped = good.groupBy("channel")']))
    c.append(code(["# agg summarises each bucket", 'by_channel = grouped.agg(F.count("*").alias("txns"), F.sum("amount").alias("total_gbp"))', "by_channel.show()"]))
    c.append(code(["# orderBy sorts for reporting", 'ranked = by_channel.orderBy(F.desc("total_gbp"))', "ranked.show()"]))

    section("Part 20 — window: rank within channel")
    c.append(code(["# Window = calculate per group without collapsing rows", "from pyspark.sql.window import Window"]))
    c.append(code(["# Partition by channel, order by amount descending", 'w = Window.partitionBy("channel").orderBy(F.desc("amount"))']))
    c.append(code(["# row_number assigns 1,2,3 within each channel", 'ranked_rows = good.withColumn("rank_in_channel", F.row_number().over(w))', "ranked_rows.show()"]))
    c.append(code(["# Top 1 per channel", 'top1 = ranked_rows.filter(F.col("rank_in_channel") == 1)', "top1.show()"]))

    section("Part 21 — SQL API (same engine as DataFrame)")
    c.append(code(["# Register temp view to write SQL", 'good.createOrReplaceTempView("transactions")']))
    c.append(code(['# SQL SELECT — learners who prefer SQL', 'spark.sql("SELECT channel, COUNT(*) AS n FROM transactions GROUP BY channel").show()']))
    c.append(code(['# Filter in SQL', 'spark.sql("SELECT * FROM transactions WHERE amount > 1000").show()']))

    section("Part 22 — End-to-end bronze → silver → gold (in memory)")
    c.append(md("**Pipeline:** read → cast → filter good → dedupe → join → aggregate."))
    c.append(code(["# Step A: start from raw bronze-like df", "bronze = df"]))
    c.append(code(["# Step B: cast amount to double", 'silver_typed = bronze.withColumn("amount", F.col("amount_gbp").cast("double"))']))
    c.append(
        code(
            [
                "# Step C: split good vs quarantine",
                'silver_good = silver_typed.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))',
                'silver_bad = silver_typed.filter(F.col("amount").isNull() | (F.col("amount") <= 0))',
            ]
        )
    )
    c.append(code(['print("good:", silver_good.count(), "quarantine:", silver_bad.count())']))
    c.append(code(["# Step D: dedupe good rows", 'silver = silver_good.dropDuplicates(["transaction_id"])']))
    c.append(code(["# Step E: gold aggregate by channel", 'gold = silver.groupBy("channel").agg(F.sum("amount").alias("total_gbp"), F.count("*").alias("txns"))', "gold.show()"]))

    section("Part 23 — Read CSV from text (like a small file)")
    c.append(
        code(
            [
                "# Simulate reading CSV without uploading a file",
                'csv_text = "transaction_id,channel,amount_gbp\\nTXN-20001,wire,100.00\\nTXN-20002,card,50.00"',
            ]
        )
    )
    c.append(code(["# pandas read from string buffer", "from io import StringIO", "pdf2 = pd.read_csv(StringIO(csv_text))", "print(pdf2)"]))

    section("Part 24 — Common mistakes (what NOT to do)")
    c.append(md("- Do not `collect()` huge data on a cluster\n- Do not paste secrets in notebooks\n- Do not skip cast on bronze strings"))
    c.append(code(["# collect() pulls ALL rows to driver — OK only for tiny data", "small = good.limit(3).collect()", "print(len(small), \"rows on driver\")"]))

    section("Part 25 — Medallion paths (theory for Databricks)")
    c.append(
        md(
            "On **Databricks** you will read:\n\n"
            "```\n"
            "abfss://bronze@<account>.dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv\n"
            "```\n\n"
            "Colab cannot access your Azure lake without credentials — practice logic here, lake in Databricks `nb_04`."
        )
    )

    # --- EXPANDED DEEP DIVE (target 120+ cells) ---
    section("Part 26 — Python: tuples and sets")
    c.append(code(["# Tuple = fixed ordered pair (immutable)", 'pair = ("TXN-10001", "wire")', "print(pair[0], pair[1])"]))
    c.append(code(["# Set = unique values only", 'channels_seen = {"wire", "card", "wire", "fps"}', "print(channels_seen)"]))
    c.append(code(["# Set removes duplicates automatically", "print(len(channels_seen), \"unique channels\")"]))

    section("Part 27 — Python: loops with enumerate")
    c.append(code(["# enumerate gives index + row together", "for i, r in enumerate(rows):", '    print(i, r["transaction_id"])']))
    c.append(code(["# break stops early when condition met", "for r in rows:", '    if r["transaction_id"] == "TXN-10003":', '        print("found large wire row"); break']))

    section("Part 28 — Python: f-strings (readable messages)")
    c.append(code(['# f-string embeds variables inside text', 'msg = f"Txn {txn_id} amount {amount} GBP"', "print(msg)"]))

    section("Part 29 — pandas: head, describe, missing values")
    c.append(code(["# head() shows first n rows", "print(pdf.head(2))"]))
    c.append(code(["# describe() quick numeric summary", "print(pdf.describe(include=\"all\"))"]))
    c.append(code(["# Add a row with missing status to demo nulls", 'pdf.loc[len(pdf)] = {"transaction_id": "TXN-99999", "channel": "wire", "amount_gbp": None, "status": None}', "print(pdf.tail(1))"]))
    c.append(code(["# isna() finds missing cells", "print(pdf[\"status\"].isna())"]))
    c.append(code(["# dropna removes rows with missing values", "clean_pdf = pdf.dropna(subset=[\"status\"])", "print(len(clean_pdf))"]))

    section("Part 30 — Polars: more column expressions")
    c.append(code(["# pl.col references a column in Polars", "import polars as pl"]))
    c.append(code(['# with_columns can add multiple columns at once', 'pl2 = plf.with_columns(pl.col("channel").str.to_uppercase().alias("channel_up"))', "print(pl2)"]))
    c.append(code(["# sort by amount_gbp as string (bronze order wrong for math)", 'print(plf.sort("amount_gbp"))']))

    section("Part 31 — PySpark: distinct and limit")
    c.append(code(["# distinct() removes duplicate rows (all columns)", "print(df.distinct().count())"]))
    c.append(code(["# limit(n) keeps first n rows after shuffle (use with care)", "df.limit(2).show()"]))

    section("Part 32 — PySpark: orderBy ascending and descending")
    c.append(code(["# orderBy channel A→Z", 'df.orderBy("channel").show()']))
    c.append(code(["# orderBy amount_gbp descending needs cast first", 'tmp = df.withColumn("amount", F.col("amount_gbp").cast("double"))', "tmp.orderBy(F.desc(\"amount\")).show()"]))

    section("Part 33 — PySpark: when/otherwise (conditional column)")
    c.append(
        code(
            [
                "# when/otherwise = if/else as a new column",
                'flagged = good.withColumn("fraud_flag", F.when(F.col("amount") >= 10000, "WATCH").otherwise("OK"))',
                "flagged.show()",
            ]
        )
    )

    section("Part 34 — PySpark: coalesce (first non-null)")
    c.append(code(["# coalesce picks first non-null column value", 'with_backup = df.withColumn("amt", F.coalesce(F.col("amount_gbp"), F.lit("0")))', "with_backup.show()"]))

    section("Part 35 — PySpark: lit (constant column)")
    c.append(code(["# lit injects a constant value", 'tagged = df.withColumn("source_system", F.lit("finledger"))', "tagged.select(\"transaction_id\", \"source_system\").show()"]))

    section("Part 36 — PySpark: union two DataFrames")
    c.append(code(["# union stacks rows (same columns required)", "extra = spark.createDataFrame([(\"TXN-20001\", \"fps\", \"10.00\", \"posted\")], cols)", "combined = df.union(extra)", "print(combined.count())"]))

    section("Part 37 — PySpark: cache (reuse in many actions)")
    c.append(code(["# cache() stores in memory after first action", "cached = good.cache()"]))
    c.append(code(["# First count computes and caches", "print(cached.count())"]))
    c.append(code(["# Second count is faster (reuses cache)", "print(cached.count())"]))
    c.append(code(["# unpersist frees memory when done", "cached.unpersist()"]))

    section("Part 38 — PySpark: explain (see the plan)")
    c.append(code(["# explain() prints logical plan (debugging)", "good.filter(F.col(\"amount\") > 100).explain()"]))

    section("Part 39 — More filters on FinLedger sample")
    c.append(code(['# IN list — channel is wire or card', 'df.filter(F.col("channel").isin(["wire", "card"])).show()']))
    c.append(code(["# LIKE pattern — ids starting with TXN-100", 'df.filter(F.col("transaction_id").like("TXN-100%")).show()']))
    c.append(code(["# NOT equal", 'df.filter(F.col("status") != "posted").show()']))

    section("Part 40 — Aggregations: avg, min, max")
    c.append(
        code(
            [
                "# Multiple agg functions in one groupBy",
                "stats = good.groupBy(\"channel\").agg(",
                '    F.avg("amount").alias("avg_gbp"),',
                '    F.min("amount").alias("min_gbp"),',
                '    F.max("amount").alias("max_gbp"),',
                ")",
                "stats.show()",
            ]
        )
    )

    section("Part 41 — Window: sum running total (concept)")
    c.append(code(["# Unbounded window — running sum ordered by amount", "w2 = Window.partitionBy(\"channel\").orderBy(\"amount\").rowsBetween(Window.unboundedPreceding, Window.currentRow)"]))
    c.append(code(['running = good.withColumn("running_total", F.sum("amount").over(w2))', "running.show()"]))

    section("Part 42 — Join types side by side")
    c.append(code(["# right join — all lookup rows", "good.join(lookup, \"channel\", \"right\").show()"]))
    c.append(code(["# full outer — everything from both", "good.join(lookup, \"channel\", \"full\").show()"]))

    section("Part 43 — Silver quarantine pattern (Session 9 preview)")
    c.append(md("**FinLedger rule:** bad rows go to quarantine, pipeline keeps running."))
    c.append(code(["# Rebuild typed from full bronze df", "typed_full = df.withColumn(\"amount\", F.col(\"amount_gbp\").cast(\"double\"))"]))
    c.append(code(['silver_ok = typed_full.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))', 'silver_bad = typed_full.filter(F.col("amount").isNull() | (F.col("amount") <= 0))']))
    c.append(code(['print("silver_ok:", silver_ok.count(), "| quarantine:", silver_bad.count())']))
    c.append(code(["# Show quarantine row (INVALID amount)", "silver_bad.show()"]))

    section("Part 44 — Gold channel summary (Session 14 preview)")
    c.append(code(["# Gold = business metrics for reports", 'gold2 = silver_ok.groupBy("channel").agg(F.sum("amount").alias("total_gbp"), F.count("*").alias("txns"))']))
    c.append(code(["gold2.orderBy(F.desc(\"total_gbp\")).show()"]))

    section("Part 45 — Simulate pipeline parameters (widgets later)")
    c.append(code(["# run_id is a folder name in the lake — like a widget value", 'run_id = "session3-lab"', "print(run_id)"]))
    c.append(code(["# Build path string (Databricks uses abfss://)", 'bronze_path = f"abfss://bronze@account/loaded/run={run_id}/sample_transactions.csv"', "print(bronze_path)"]))

    section("Part 46 — pandas → Spark handoff concept")
    c.append(code(["# Sometimes convert pandas → Spark (small data only)", "pdf_small = pd.DataFrame(rows)", "spark_from_pandas = spark.createDataFrame(pdf_small)", "spark_from_pandas.show()"]))

    section("Part 47 — Check: posted count all three tools")
    c.append(code(['# pandas count', 'print("pandas posted:", len(pdf[pdf["status"]=="posted"]))']))
    c.append(code(['# Polars count', 'print("polars posted:", plf.filter(pl.col("status")=="posted").height)']))
    c.append(code(['# Spark count', 'print("spark posted:", df.filter("status = \'posted\'").count())']))

    section("Part 48 — Review: transformation chain one line at a time")
    c.append(code(["# One lazy step", "s1 = df"]))
    c.append(code(["s2 = s1.filter(\"status = 'posted'\")"]))
    c.append(code(["s3 = s2.withColumn(\"amount\", F.col(\"amount_gbp\").cast(\"double\"))"]))
    c.append(code(["s4 = s3.filter(F.col(\"amount\") > 0)"]))
    c.append(code(["s5 = s4.select(\"transaction_id\", \"channel\", \"amount\")"]))
    c.append(code(["# Single action at end", "s5.show()"]))

    section("Part 49 — Logging vs print (production habit)")
    c.append(code(["# logging survives in cluster logs; print is for demos", "import logging", 'logging.basicConfig(level=logging.INFO)', 'log = logging.getLogger("finledger")']))
    c.append(code(['log.info("processing %s rows", df.count())']))

    section("Part 50 — dataclass config (Session 6 pattern)")
    c.append(code(["# dataclass holds run settings in one object", "from dataclasses import dataclass"]))
    c.append(
        code(
            [
                "@dataclass",
                "class RunConfig:",
                '    run_id: str',
                '    storage_account: str = "stexample"',
            ]
        )
    )
    c.append(code(['cfg = RunConfig(run_id="session3-lab")', "print(cfg)"]))

    section("Part 51 — Final end-to-end script in tiny steps")
    c.append(md("Run these 8 cells in order — full bronze→gold logic."))
    c.append(code(["# 1 read bronze-like data", "b = df"]))
    c.append(code(["# 2 cast", 's = b.withColumn("amount", F.col("amount_gbp").cast("double"))']))
    c.append(code(["# 3 good rows", "g = s.filter(F.col(\"amount\").isNotNull() & (F.col(\"amount\") > 0))"]))
    c.append(code(["# 4 quarantine", "q = s.filter(F.col(\"amount\").isNull() | (F.col(\"amount\") <= 0))"]))
    c.append(code(["# 5 dedupe", 'g2 = g.dropDuplicates(["transaction_id"])']))
    c.append(code(["# 6 enrich", 'g3 = g2.join(lookup, "channel", "left")']))
    c.append(code(["# 7 gold", "gold_final = g3.groupBy(\"channel\").agg(F.sum(\"amount\").alias(\"total\"))"]))
    c.append(code(["# 8 show gold", "gold_final.show()", 'print("quarantine count:", q.count())']))

    section("Part 52 — Cleanup")
    c.append(code(["# Stop Spark to free memory", "spark.stop()", 'print("Spark stopped")']))
    c.append(
        md(
            "### Checkpoint\n"
            "- [ ] I ran every cell in order\n"
            "- [ ] I can explain lazy vs action\n"
            "- [ ] I can name select, filter, withColumn, join, groupBy, window\n"
            "- [ ] Next: Databricks `nb_04_read_bronze.py` for real `abfss://`"
        )
    )

    return c


def main() -> None:
    cells = build_cells()
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.12.0"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(nb, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUT} with {len(cells)} cells")


if __name__ == "__main__":
    main()
