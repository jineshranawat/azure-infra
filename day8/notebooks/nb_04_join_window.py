# Databricks notebook source
# MAGIC %md
# MAGIC # Day 8 — dedupe, join, window
# MAGIC
# MAGIC ## Theory
# MAGIC
# MAGIC - **`dropDuplicates(keys)`** — keep one row per key (silver dedupe).
# MAGIC - **`join(other, on=..., how=...)`** — enrich rows. `how`: inner, left, right, full.
# MAGIC - **Window** — calculate per-group rank without collapsing rows (unlike groupBy).

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", _storage_key)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv"
df = spark.read.option("header", True).csv(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dropDuplicates`

# COMMAND ----------

deduped = df.dropDuplicates(["transaction_id"])
print("before:", df.count(), "after:", deduped.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## `join` — lookup table (demo)

# COMMAND ----------

lookup = spark.createDataFrame(
    [("wire", "high"), ("card", "medium"), ("fps", "low")],
    ["channel", "risk"],
)
enriched = deduped.join(lookup, on="channel", how="left")
enriched.select("transaction_id", "channel", "risk").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window — rank by amount within channel
# MAGIC
# MAGIC `row_number()` = unique rank (1,2,3). `rank()` allows ties (1,1,3).

# COMMAND ----------

typed = enriched.withColumn("amount", F.col("amount_gbp").cast("double"))
w = Window.partitionBy("channel").orderBy(F.desc("amount"))

# COMMAND ----------

ranked = typed.withColumn("rank_in_channel", F.row_number().over(w))
ranked.select("channel", "transaction_id", "amount", "rank_in_channel").show(10)

# COMMAND ----------

display(ranked.filter(F.col("rank_in_channel") <= 3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day 8 complete
# MAGIC - [ ] All eight core transforms practised
# MAGIC - [ ] Session 9 — write cleansed rows to silver + quarantine bad rows
