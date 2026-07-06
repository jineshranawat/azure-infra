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
