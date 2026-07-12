# Databricks notebook source
# MAGIC %md
# MAGIC # ADF complete integration — bronze → silver (master notebook)
# MAGIC
# MAGIC **Triggered by:** `pl_db_12_end_to_end_integration` (ADF Copy → this notebook)
# MAGIC
# MAGIC **Covers:** secret scope `finledger`, **abfss** read, Delta silver write, JSON exit for ADF Monitor.
# MAGIC
# MAGIC **Prerequisite:** `orchestrate.cmd --setup-databricks-integration` once per workspace.

# COMMAND ----------

dbutils.widgets.text("run_id", "session3-lab")
dbutils.widgets.text("triggered_by", "adf-integration")
dbutils.widgets.text("bronze_path", "loaded/run=session3-lab/sample_transactions.csv")
dbutils.widgets.text("silver_path", "cleaned/run=session3-lab/adf_integration_delta")
dbutils.widgets.text("storage_account", "")

run_id = dbutils.widgets.get("run_id")
triggered_by = dbutils.widgets.get("triggered_by")
bronze_rel = dbutils.widgets.get("bronze_path").lstrip("/")
silver_rel = dbutils.widgets.get("silver_path").strip("/")
storage_account = dbutils.widgets.get("storage_account")

SCOPE = "finledger"
if not storage_account:
    try:
        storage_account = dbutils.secrets.get(scope=SCOPE, key="storage-account")
    except Exception:
        storage_account = "stsharedqgr7mj"

print("ADF COMPLETE INTEGRATION")
print("  run_id       :", run_id)
print("  triggered_by :", triggered_by)
print("  bronze_path  :", bronze_rel)
print("  silver_path  :", silver_rel)
print("  storage      :", storage_account)

# COMMAND ----------

bronze_uri = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{bronze_rel}"
silver_uri = f"abfss://silver@{storage_account}.dfs.core.windows.net/{silver_rel}"

try:
  storage_key = dbutils.secrets.get(scope=SCOPE, key="storage-key")
  spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
  print("Storage key loaded from secret scope finledger")
except Exception as exc:
  print("Secret scope note (run orchestrate --setup-databricks-integration):", exc)

# COMMAND ----------

from pyspark.sql import functions as F

source_df = spark.read.option("header", True).csv(bronze_uri)
row_count = source_df.count()

silver_df = (
    source_df
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("adf_run_id", F.lit(run_id))
    .withColumn("adf_triggered_by", F.lit(triggered_by))
)

silver_df.write.format("delta").mode("overwrite").save(silver_uri)
silver_count = spark.read.format("delta").load(silver_uri).count()

print(f"Bronze rows read : {row_count}")
print(f"Silver Delta rows: {silver_count}")
print(f"Silver location  : {silver_uri}")

# COMMAND ----------

import json

dbutils.notebook.exit(
    json.dumps(
        {
            "status": "ok",
            "run_id": run_id,
            "triggered_by": triggered_by,
            "bronze_path": bronze_rel,
            "silver_path": silver_rel,
            "bronze_row_count": row_count,
            "silver_row_count": silver_count,
            "silver_uri": silver_uri,
        }
    )
)
