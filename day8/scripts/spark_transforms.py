"""Day 8 — local PySpark transforms (small steps)."""

from __future__ import annotations

import shutil
from pathlib import Path


def _spark(root: Path):
  if not shutil.which("java"):
    raise SystemExit("JAVA not found — install Java 11+ then re-run orchestrate.cmd")
  from pyspark.sql import SparkSession

  csv_path = str((root / "data" / "sample_transactions.csv").resolve())
  spark = SparkSession.builder.appName("day8-local").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  df = spark.read.option("header", True).csv(csv_path)
  return spark, df


def demo_select_filter(root: Path) -> None:
  spark, df = _spark(root)
  slim = df.select("transaction_id", "channel", "amount_gbp")
  posted = slim.filter("status = 'posted'")
  print("posted rows:", posted.count())
  spark.stop()


def demo_withcolumn(root: Path) -> None:
  from pyspark.sql import functions as F

  spark, df = _spark(root)
  typed = df.withColumn("amount", F.col("amount_gbp").cast("double"))
  print("schema has amount:", "amount" in typed.columns)
  spark.stop()


def demo_groupby(root: Path) -> None:
  from pyspark.sql import functions as F

  spark, df = _spark(root)
  by_ch = df.groupBy("channel").agg(F.count("*").alias("n"))
  by_ch.show()
  spark.stop()


def main(root: Path) -> None:
  print("--- select + filter ---")
  demo_select_filter(root)
  print("--- withColumn + cast ---")
  demo_withcolumn(root)
  print("--- groupBy + agg ---")
  demo_groupby(root)
