"""Day 7 — local PySpark read (install Java + pyspark first)."""

from __future__ import annotations

import shutil
from pathlib import Path


def _java_ok() -> bool:
  return shutil.which("java") is not None


def main(root: Path) -> None:
  if not _java_ok():
    print("JAVA not found — install Java 11+ (adoptium.net) then re-run orchestrate.cmd")
    return

  from pyspark.sql import SparkSession

  csv_path = str((root / "data" / "sample_transactions.csv").resolve())
  spark = SparkSession.builder.appName("day7-local").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  df = spark.read.option("header", True).csv(csv_path)
  print("columns:", df.columns)
  print("rows:", df.count())

  df.show(3, truncate=False)
  spark.stop()
  print("Local Spark read OK.")
