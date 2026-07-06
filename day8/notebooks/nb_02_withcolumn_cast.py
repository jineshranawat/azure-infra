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
