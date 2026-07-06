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
RUN_ID = "session3-lab"

def _read_bronze_transactions(spark, account, key, run_id):
    path = f"abfss://bronze@{account}.dfs.core.windows.net/loaded/run={run_id}/sample_transactions.csv"
    try:
        spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
        f = spark.read.option("header", True).option("inferSchema", True).csv(path)
        f.limit(1).count()
        return f, path
    except Exception:
        pass
    try:
        f = spark.read.option("header", True).option("inferSchema", True).csv(path)
        f.limit(1).count()
        return f, path
    except Exception:
        pass
    from io import StringIO
    import pandas as pd
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "azure-storage-file-datalake", "-q"])
        from azure.storage.filedatalake import DataLakeServiceClient
    rel = f"loaded/run={run_id}/sample_transactions.csv"
    c = DataLakeServiceClient(f"https://{account}.dfs.core.windows.net", credential=key)
    raw = c.get_file_system_client("bronze").get_file_client(rel).download_file().readall().decode("utf-8")
    return spark.createDataFrame(pd.read_csv(StringIO(raw))), path

df, path = _read_bronze_transactions(spark, STORAGE_ACCOUNT, _storage_key, RUN_ID)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
