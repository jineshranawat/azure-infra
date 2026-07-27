"""Performance & Scalability demonstrations (Problems 4,5,11,12,13,22,23,24,25,46).

The RTT data is tiny, so these are DEMONSTRATIONS of the Spark techniques Foundry
exposes natively (config + write API), not because the volume requires them. Where
the volume is genuinely small the lightweight Polars foundation already IS the right
answer (Problems 28,45) - and DuckDB covers small ad-hoc SQL (Problem 46).

Each technique is annotated with the problem number it addresses.
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

    # Problem 4 (AQE): let Spark adapt shuffle partitions, coalesce, and de-skew joins.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    # Problem 13 (column pruning + predicate pushdown): read only needed columns, filter early.
    facts = (
        clean.dataframe()
        .select("pathway_id", "trust_code", "specialty_name", "weeks_waited", "pathway_status")
        .filter(F.col("weeks_waited").isNotNull())
    )
    dims = providers.dataframe().select("trust_code", "trust_name", "region")

    # Problem 22 (salting): spread the hot TR005 key across buckets before the shuffle.
    n_salt = 8
    salted = facts.withColumn("_salt", F.pmod(F.hash(F.col("trust_code")), F.lit(n_salt)))

    # Problem 5 (broadcast join): broadcast the tiny dimension - no shuffle of the big side.
    joined = salted.join(F.broadcast(dims), on="trust_code", how="left")

    # Problem 24 (caching): the joined frame feeds both outputs, so cache it once.
    joined = joined.cache()

    # Two-stage salted aggregation: partial per salt bucket, then final roll-up.
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

    # Problems 11 + 12 + 25 (partitioning + compaction + coalesce sizing):
    # partition by region for pruning; coalesce(1) compacts the tiny output to one file.
    perf_partitioned.write_dataframe(final.coalesce(1), partition_cols=["region"])

    # Problem 23 (bucketing): bucket the fact grain by trust_code for repeated co-located joins.
    bucket_src = joined.select("pathway_id", "trust_code", "specialty_name", "weeks_waited", "region")
    perf_bucketed.write_dataframe(
        bucket_src, bucket_cols=["trust_code"], bucket_count=8, sort_by=["trust_code"]
    )


@transform.using(
    clean=Input(paths.RTT_PATHWAYS_CLEAN),
    out=Output(paths.PERF_SPECIALTY_DUCKDB),
)
def perf_duckdb_specialty(clean, out):
    """Problem 46: DuckDB in-process SQL over the small dataset (no Spark needed).

    DuckDB's replacement scan lets us query the Polars frame `df` by name directly.
    """
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
