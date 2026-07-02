# Databricks notebook source
# MAGIC %md
# MAGIC # Day 8 — `withColumn` and `cast`
# MAGIC
# MAGIC ## Theory
# MAGIC
# MAGIC - **`withColumn(name, expr)`** — add or replace one column.
# MAGIC - **`.cast("double")`** — change type (CSV bronze is all strings).
# MAGIC - Invalid cast → `null` — filter with `F.col("amount").isNotNull()` or `> 0`.
# MAGIC
# MAGIC **FinLedger rule:** bad amounts become null → quarantine in Session 9.

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

typed = df.withColumn("amount", F.col("amount_gbp").cast("double"))
typed.printSchema()

# COMMAND ----------

valid = typed.filter(F.col("amount") > 0)
print("valid rows:", valid.count())

# COMMAND ----------

bad = typed.filter(F.col("amount").isNull())
print("null/bad cast rows:", bad.count())

# COMMAND ----------

display(valid.select("transaction_id", "channel", "amount", "status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint
# MAGIC - [ ] Schema shows `amount: double`
# MAGIC - [ ] Next: **nb_03_groupby_agg**
