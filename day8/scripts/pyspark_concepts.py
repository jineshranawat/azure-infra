"""Day 8 — print PySpark mental model (theory before transforms)."""

from __future__ import annotations


def print_pyspark_foundation() -> None:
    sections = [
        ("SparkSession", "Entry point. On Databricks: `spark` exists. Local: SparkSession.builder...getOrCreate()"),
        ("DataFrame", "Distributed table with named columns. Like pandas, but lazy and cluster-scale."),
        ("Schema", "Column names + types. Use printSchema() before transforms."),
        ("Transformation", "Lazy step: select, filter, withColumn, join, groupBy. Builds a plan."),
        ("Action", "Runs the plan: count, show, collect, write. Triggers cluster work."),
        ("functions (F)", "Column expressions: F.col, F.cast, F.sum, F.count. Import once per notebook."),
        ("Partition", "Chunk of data on a worker. groupBy/join shuffle partitions — affects performance."),
        ("Read", "spark.read.option('header',True).csv(path) or .parquet or .table('catalog.schema.name')"),
        ("Write", "df.write.mode('overwrite').parquet(path) — always an action."),
    ]
    print()
    print("PYSPARK FOUNDATION (read before Day 8 notebooks)")
    print("=" * 60)
    for title, body in sections:
        print(f"  {title}: {body}")
    print("=" * 60)
    print("Rule: chain transforms, call ONE action to see results.")
