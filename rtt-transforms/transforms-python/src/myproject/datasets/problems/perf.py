"""Performance & Scalability demonstrations (Problems 4,5,11,12,13,22,23,24,25,46).

Layout of this file:
  Each problem is a self-contained BLOCK with FOUR fixed sections in a fixed
  order, then the EXACT code that implements it directly below. No two
  concepts are mixed inside the same block. Read top-to-bottom.

  # ================ PROBLEM N - TITLE ================
  # WHAT IT DOES : ...
  # ANALOGY      : ...
  # INPUT        : ...
  # OUTPUT       : ...
  # ---------- CODE ----------
  <the actual code>
  # --------------------------

Quick map - which line solves which problem:
  P4  Adaptive Query Execution ................ spark.conf.set(...)
  P13 Column pruning + predicate pushdown ..... .select(...) / .filter(...)
  P22 Salting for skew ........................ withColumn("_salt", ...)
  P5  Broadcast join .......................... F.broadcast(dims)
  P24 Caching ................................. joined.cache()
  P11 Partitioning ............................ write_dataframe(partition_cols=)
  P12 File compaction ......................... final.coalesce(1)
  P25 Coalesce sizing ......................... final.coalesce(1)
  P23 Bucketing ............................... write_dataframe(bucket_cols=)
  P46 DuckDB in-process SQL ................... perf_duckdb_specialty()
"""
from __future__ import annotations

import duckdb
import pyspark.sql.functions as F
from transforms.api import transform, Input, Output

from myproject import paths


@transform.spark.using(
    clean=Input(paths.RTT_PATHWAYS_CLEAN),
    providers=Input(paths.PROVIDER_REFERENCE),
    perf_partitioned=Output(paths.PERF_TRUST_REGION),
    perf_bucketed=Output(paths.PERF_PATHWAY_BUCKETED),
)
def perf_spark_techniques(ctx, clean, providers, perf_partitioned, perf_bucketed):
    spark = ctx.spark_session

    # ==========================================================================
    # PROBLEM 4 - Adaptive Query Execution (AQE)
    # --------------------------------------------------------------------------
    # WHAT IT DOES : Lets Spark re-plan the query AT RUNTIME based on actual
    #                data statistics (partition sizes, join sizes, skew).
    # ANALOGY      : Google Maps re-routing mid-journey when it sees live
    #                traffic. The original route is only a guess until you
    #                start driving; AQE lets Spark change lanes when it sees
    #                the real shuffle "traffic".
    # INPUT        : Static plan uses 200 shuffle partitions no matter what.
    # OUTPUT       : Spark coalesces those 200 tiny partitions to ~8 sensibly
    #                sized ones; if a join key is skewed it splits that key
    #                into sub-partitions automatically. No code change needed.
    # ---------- CODE ----------
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # --------------------------

    # ==========================================================================
    # PROBLEM 13 - Column pruning + predicate pushdown
    # --------------------------------------------------------------------------
    # WHAT IT DOES : Only read the columns and rows you actually need, as
    #                early as possible, so Parquet can skip the rest.
    # ANALOGY      : Instead of photocopying the whole 400-page phonebook and
    #                THEN reading "Smith, John", you ask the librarian for
    #                page 231. Parquet's columnar layout makes this cheap.
    # INPUT        : rtt_pathways_clean has 16 columns, 1,205 rows.
    # OUTPUT       : Only 5 columns loaded; only non-null weeks_waited rows.
    #                  pathway_id | trust_code | specialty_name | weeks_waited | pathway_status
    #                  -----------+------------+----------------+--------------+---------------
    #                  PW00001    | TR005      | Cardiology     | 23           | Active
    #                  ...
    # ---------- CODE ----------
    facts = (
        clean.dataframe()
        .select("pathway_id", "trust_code", "specialty_name", "weeks_waited", "pathway_status")
        .filter(F.col("weeks_waited").isNotNull())
    )
    dims = providers.dataframe().select("trust_code", "trust_name", "region")
    # --------------------------

    # ==========================================================================
    # PROBLEM 22 - Salting for skew
    # --------------------------------------------------------------------------
    # WHAT IT DOES : Adds a "_salt" bucket (0..7) to a hot key so the
    #                downstream shuffle spreads that key across many
    #                partitions instead of piling onto one.
    # ANALOGY      : Airport passport control at 8am. Everyone is British and
    #                one queue is 400 people long. Split the British queue
    #                into 8 sub-queues by the last digit of the passport
    #                (the "salt"). Same total work, 8x parallelism.
    # INPUT        : TR005 = 60% of rows.
    #                  trust_code | count
    #                  -----------+------
    #                  TR005      | 723     <-- one Spark task carries all this
    #                  TR001..004 | ~120 each
    # OUTPUT       : Each hot key split across 8 salt buckets.
    #                  trust_code | _salt | count
    #                  -----------+-------+------
    #                  TR005      | 0     | 90     \  8 tasks now handle TR005
    #                  TR005      | 1     | 91      \ in parallel instead of 1
    #                  ...
    # ---------- CODE ----------
    n_salt = 8
    salted = facts.withColumn("_salt", F.pmod(F.hash(F.col("trust_code")), F.lit(n_salt)))
    # --------------------------

    # ==========================================================================
    # PROBLEM 5 - Broadcast join
    # --------------------------------------------------------------------------
    # WHAT IT DOES : Sends the SMALL side of the join (dims: 5 rows) to every
    #                executor, so the LARGE side (facts: 1,205 rows) never
    #                shuffles. Turns shuffle-hash-join into map-side join.
    # ANALOGY      : You have 5 recipe cards and 1,205 ingredients. Instead
    #                of gathering every ingredient into one kitchen and
    #                matching there (shuffle), photocopy the 5 recipe cards,
    #                hand one set to every chef, and match locally.
    # INPUT        : facts (1,205) + dims (5) would both shuffle by trust_code.
    # OUTPUT       : dims sits in every executor's memory (~200 bytes); facts
    #                stay put; join is local; zero shuffle of big side.
    #                Spark UI shows "BroadcastHashJoin" not "SortMergeJoin".
    # ---------- CODE ----------
    joined = salted.join(F.broadcast(dims), on="trust_code", how="left")
    # --------------------------

    # ==========================================================================
    # PROBLEM 24 - Caching (persist)
    # --------------------------------------------------------------------------
    # WHAT IT DOES : Materialises `joined` in memory once. Both outputs below
    #                (perf_partitioned + perf_bucketed) reuse the SAME
    #                joined DataFrame -- without .cache() Spark would
    #                recompute the join twice.
    # ANALOGY      : Baking two cakes from the same dough. Mix once (cache),
    #                split into two tins -- don't mix twice.
    # INPUT        : Two downstream writes on the same lazy plan.
    # OUTPUT       : First write triggers the join, keeps rows in memory;
    #                second write reads from the cache. Spark UI shows the
    #                second job's stages as "skipped" or "cached".
    # ---------- CODE ----------
    joined = joined.cache()
    # --------------------------

    # --------------------------------------------------------------------------
    # SUPPORTING P22 - two-stage salted aggregation.
    # Step 1: aggregate PER (trust, specialty, salt-bucket) -> partial counts.
    # Step 2: roll up across salt buckets -> final counts.
    # This is the correctness pattern you always pair with salting.
    # Not a new "problem" -- just the plumbing needed to make P22 valid.
    # --------------------------------------------------------------------------
    partial = joined.groupBy("region", "trust_name", "specialty_name", "_salt").agg(
        F.count("*").alias("n"),
        F.sum("weeks_waited").alias("sum_w"),
        F.sum(F.when(F.col("weeks_waited") <= 18, 1).otherwise(0)).alias("within18"),
    )
    final = (
        partial.groupBy("region", "trust_name", "specialty_name")
        .agg(
            F.sum("n").alias("total_pathways"),
            F.sum("sum_w").alias("sum_weeks"),
            F.sum("within18").alias("within_18w"),
        )
        .withColumn("avg_weeks_waited", F.round(F.col("sum_weeks") / F.col("total_pathways"), 1))
        .withColumn("pct_within_18w", F.round(F.col("within_18w") / F.col("total_pathways"), 4))
        .drop("sum_weeks")
    )

    # ==========================================================================
    # PROBLEM 11 - Partitioning by region
    # --------------------------------------------------------------------------
    # WHAT IT DOES : Writes each region into its own subfolder on disk.
    # ANALOGY      : Amazon warehouse aisles labelled "London / Midlands /
    #                North". A query for London pallets never walks into
    #                other aisles.
    # INPUT        : Unpartitioned parquet -> a filter on region scans every
    #                file.
    # OUTPUT       : .../rtt_perf_trust_region/
    #                  region=London/     part-0000.parquet
    #                  region=Midlands/   part-0000.parquet
    #                  region=North/      part-0000.parquet
    #                London query reads only 1 folder, not 3.
    # ==========================================================================
    # PROBLEM 12 - File compaction   +   PROBLEM 25 - Coalesce sizing
    # --------------------------------------------------------------------------
    # WHAT IT DOES : coalesce(1) merges the many tiny per-task files into
    #                ONE right-sized file per output partition.
    # ANALOGY      : Instead of 500 sticky notes on your desk, one clean
    #                page. Parquet readers hate tiny files -- each open()
    #                has fixed overhead. One file >> 500 tiny ones.
    # INPUT        : Without coalesce a 200-task job writes 200 ~1 KB files
    #                per region.
    # OUTPUT       : With coalesce(1): 1 file per region (~50 KB each).
    # NOTE ON SIZE : coalesce(1) is right ONLY because this output is tiny.
    #                For a real 100 GB table target ~128-512 MB per file
    #                with repartition(target_files), not coalesce(1).
    # ---------- CODE (P11 + P12 + P25 in one write call) ----------
    perf_partitioned.write_dataframe(final.coalesce(1), partition_cols=["region"])
    # --------------------------------------------------------------

    # ==========================================================================
    # PROBLEM 23 - Bucketing
    # --------------------------------------------------------------------------
    # WHAT IT DOES : Physically groups rows into a FIXED number of buckets
    #                by a hash of trust_code, sorted within each bucket.
    #                Future joins on trust_code against another table
    #                bucketed the SAME way avoid the shuffle entirely.
    # ANALOGY      : A pre-alphabetised card catalogue in a library. Two
    #                libraries that alphabetise the same way can merge
    #                catalogues by walking both A-drawers side by side --
    #                no sorting or shuffling needed. Partitioning groups
    #                cards "by year"; bucketing sub-sorts within each year.
    # INPUT        : 1,205 pathway rows, unbucketed. A future join on
    #                trust_code would require a full shuffle of both sides.
    # OUTPUT       : .../rtt_perf_pathway_bucketed/
    #                  bucket_00000.parquet  (rows where hash(trust_code)%8 == 0)
    #                  bucket_00001.parquet
    #                  ...
    #                  bucket_00007.parquet
    #                Co-located join with any other 8-bucket table on
    #                trust_code -> ZERO shuffle.
    # ---------- CODE ----------
    bucket_src = joined.select("pathway_id", "trust_code", "specialty_name", "weeks_waited", "region")
    perf_bucketed.write_dataframe(
        bucket_src, bucket_cols=["trust_code"], bucket_count=8, sort_by=["trust_code"]
    )
    # --------------------------


@transform.using(
    clean=Input(paths.RTT_PATHWAYS_CLEAN),
    out=Output(paths.PERF_SPECIALTY_DUCKDB),
)
def perf_duckdb_specialty(clean, out):
    """
    ==========================================================================
    PROBLEM 46 - DuckDB in-process SQL (no Spark needed)
    --------------------------------------------------------------------------
    WHAT IT DOES : Runs SQL directly against a Polars DataFrame in the SAME
                   Python process. DuckDB's "replacement scan" lets you
                   reference the Python variable `df` by name inside SQL.

    ANALOGY      : Spark = renting a fleet of trucks to move one sofa.
                   DuckDB = calling a mate with a hatchback.
                   For anything under a few million rows on one machine
                   DuckDB is 10-100x cheaper AND faster than Spark: no
                   cluster startup, no shuffle, no JVM<->Python overhead.

    INPUT        : clean.polars() -> 1,205 Polars rows.
                     pathway_id | trust_code | specialty_group | specialty_name | weeks_waited
                     -----------+------------+-----------------+----------------+-------------
                     PW00001    | TR005      | Surgery         | Cardiology     | 23
                     PW00002    | TR001      | Medicine        | Neurology      | 55
                     ...

    OUTPUT       : Aggregated by specialty.
                     specialty_group | specialty_name  | pathways | avg_weeks | breaches_18w
                     ----------------+-----------------+----------+-----------+-------------
                     Surgery         | Cardiology      | 145      | 24.3      | 87
                     Medicine        | Neurology       | 132      | 31.7      | 95
                     Surgery         | Orthopaedics    | 128      | 42.1      | 110
                     ...
    ==========================================================================
    """
    # ---------- CODE ----------
    df = clean.polars()  # noqa: F841 - referenced by name inside the DuckDB SQL below
    con = duckdb.connect()
    result = con.execute(
        """
        SELECT specialty_group,
               specialty_name,
               COUNT(*)                               AS pathways,
               ROUND(AVG(weeks_waited), 1)            AS avg_weeks,
               SUM(CASE WHEN weeks_waited > 18 THEN 1 ELSE 0 END) AS breaches_18w
        FROM df
        GROUP BY specialty_group, specialty_name
        ORDER BY pathways DESC
        """
    ).pl()
    out.write_table(result)
    # --------------------------
