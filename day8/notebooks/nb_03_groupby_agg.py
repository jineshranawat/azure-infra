# Databricks notebook source
# MAGIC %md
# MAGIC # Day 8 — `groupBy` and aggregates
# MAGIC
# MAGIC ## Theory
# MAGIC
# MAGIC - **`groupBy(cols)`** — split rows into buckets (e.g. by `channel`).
# MAGIC - **`.agg(...)`** — summarise each bucket: `count`, `sum`, `avg`, `min`, `max`.
# MAGIC - **Wide transformation** — may **shuffle** partitions (slower than filter).
# MAGIC - **`orderBy`** — sort result (often after agg for gold reports).

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", _storage_key)

# COMMAND ----------

from pyspark.sql import functions as F

path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv"
df = spark.read.option("header", True).csv(path)

# COMMAND ----------

clean = df.withColumn("amount", F.col("amount_gbp").cast("double"))
clean = clean.filter(F.col("amount") > 0)

# COMMAND ----------

by_channel = clean.groupBy("channel").agg(
    F.count("*").alias("txns"),
    F.sum("amount").alias("total_gbp"),
)

# COMMAND ----------

by_channel.orderBy(F.desc("total_gbp")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold preview — this shape lands in `finledger.gold` later

# COMMAND ----------

display(by_channel)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint
# MAGIC - [ ] One row per channel with `txns` and `total_gbp`
# MAGIC - [ ] Next: **nb_04_join_window**
