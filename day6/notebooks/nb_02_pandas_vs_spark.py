# Databricks notebook source
# MAGIC %md
# MAGIC # Day 6 — Read CSV two ways: pandas vs PySpark
# MAGIC
# MAGIC **FinLedger Session 6** — same file, two APIs. You will use **PySpark** in production; **pandas** is handy for small samples and tests.

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "Storage account name")
dbutils.widgets.text("bronze_path", "", "Full abfss:// path (optional)")
dbutils.widgets.text("run_id", "session3-lab", "Run folder under bronze/loaded")

storage_account = dbutils.widgets.get("storage_account").strip()
bronze_path = dbutils.widgets.get("bronze_path").strip()
run_id = dbutils.widgets.get("run_id").strip()

if not bronze_path:
    if not storage_account:
        raise ValueError("Set bronze_path OR storage_account widget")
    bronze_path = (
        f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
        f"loaded/run={run_id}/sample_transactions.csv"
    )

print(f"Reading: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. pandas — loads everything into the driver (fine for small CSVs)

# COMMAND ----------

import pandas as pd

# Databricks can read abfss when the cluster has storage access
pdf = pd.read_csv(bronze_path)
print(f"pandas shape: {pdf.shape}")
display(pdf.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. PySpark — lazy, distributed (what we use at scale)

# COMMAND ----------

spark_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path)
)
print(f"Spark columns: {spark_df.columns}")
spark_df.printSchema()
display(spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Same transform in both worlds

# COMMAND ----------

import logging

log = logging.getLogger("finledger")


def clean_amount(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        log.warning("bad amount: %r", value)
        return 0.0


# pandas
pdf["amount_clean"] = pdf["amount_gbp"].apply(clean_amount)
display(pdf[["transaction_id", "amount_gbp", "amount_clean", "channel"]])

# COMMAND ----------

from pyspark.sql import functions as F

spark_clean = spark_df.withColumn(
    "amount_clean",
    F.when(F.col("amount_gbp").cast("double").isNotNull(), F.col("amount_gbp").cast("double")).otherwise(
        F.lit(0.0)
    ),
)
display(spark_clean.select("transaction_id", "amount_gbp", "amount_clean", "channel"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compare row counts (actions)

# COMMAND ----------

pandas_count = len(pdf)
spark_count = spark_df.count()
print(f"pandas rows: {pandas_count}  |  Spark rows: {spark_count}")
assert pandas_count == spark_count, "Counts should match for the same file"

# COMMAND ----------

# MAGIC %md
# MAGIC ### When to use which?
# MAGIC
# MAGIC | Tool | Good for | FinLedger rule |
# MAGIC |---|---|---|
# MAGIC | **pandas** | Quick peek, unit-test-sized data, local laptop | Session 6 learning + tests |
# MAGIC | **PySpark** | Lakehouse tables, millions of rows, Delta writes | Sessions 7+ and all production paths |
# MAGIC
# MAGIC **Checkpoint:** row counts match; TXN-10003 still shows £50,000 pending wire.

# COMMAND ----------

display(spark_df.filter("transaction_id = 'TXN-10003'"))
