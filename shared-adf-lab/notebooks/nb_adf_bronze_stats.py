# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze CSV stats — copy-then-notebook lab
# MAGIC
# MAGIC **Pipeline:** `pl_db_05_copy_then_notebook`
# MAGIC
# MAGIC ADF **Copy** lands CSV on bronze; this notebook reads it via **abfss** and returns row count.

# COMMAND ----------

dbutils.widgets.text("run_id", "session3-lab")
dbutils.widgets.text("triggered_by", "manual")
dbutils.widgets.text("bronze_path", "incoming/run=session3-lab/sample_transactions.csv")
dbutils.widgets.text("storage_account", "")

run_id = dbutils.widgets.get("run_id")
triggered_by = dbutils.widgets.get("triggered_by")
bronze_rel = dbutils.widgets.get("bronze_path").lstrip("/")
storage_account = dbutils.widgets.get("storage_account")

if not storage_account:
    try:
        storage_account = dbutils.secrets.get(scope="finledger", key="storage-account")
    except Exception:
        storage_account = "stsharedqgr7mj"

abfss_uri = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{bronze_rel}"
print("Reading:", abfss_uri)

# COMMAND ----------

df = spark.read.option("header", True).csv(abfss_uri)
row_count = df.count()
channels = [r.channel for r in df.select("channel").distinct().collect()]
print(f"Rows: {row_count}")
print(f"Channels: {channels}")

# COMMAND ----------

import json

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "ok",
            "run_id": run_id,
            "triggered_by": triggered_by,
            "bronze_path": bronze_rel,
            "row_count": row_count,
            "channels": channels,
        }
    )
)
