# Databricks notebook source
# MAGIC %md
# MAGIC # Day 8 — `select` and `filter`
# MAGIC
# MAGIC ## Theory
# MAGIC
# MAGIC - **`select`** — choose columns (projection). Narrow transformation — no shuffle.
# MAGIC - **`filter`** (alias `where`) — keep rows matching a condition. Also narrow.
# MAGIC - Both are **lazy** — chain them, then call `.count()` or `.show()` once.
# MAGIC
# MAGIC **pandas equivalent:** `df[["a","b"]]` and `df[df.status=="posted"]`

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", _storage_key)

# COMMAND ----------

run_id = "session3-lab"
path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/loaded/run={run_id}/sample_transactions.csv"
df = spark.read.option("header", True).csv(path)

# COMMAND ----------

slim = df.select("transaction_id", "channel", "amount_gbp", "status")
slim.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter with string SQL expression

# COMMAND ----------

posted = slim.filter("status = 'posted'")
print("posted rows:", posted.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter with `F.col` (preferred in production)

# COMMAND ----------

from pyspark.sql import functions as F

posted_f = slim.filter(F.col("status") == "posted")
posted_f.show(3)

# COMMAND ----------

display(posted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint
# MAGIC - [ ] `select` = columns; `filter` = rows
# MAGIC - [ ] Next: **nb_02_withcolumn_cast**
