# Databricks notebook source

# MAGIC %md
# MAGIC # ADF master — Databricks medallion + governance publish
# MAGIC
# MAGIC Called by `pl_db_14_databricks_medallion_governance` in shared-adf-lab.
# MAGIC Writes Purview manifest + SQL metadata export to `audit/governance/`.

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()
dbutils.widgets.text("run_id", "session3-lab", "Pipeline run id")
dbutils.widgets.text("triggered_by", "manual", "Triggered by (adf or manual)")
RUN_ID = dbutils.widgets.get("run_id").strip() or "session3-lab"
TRIGGERED_BY = dbutils.widgets.get("triggered_by").strip() or "manual"

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

try:
    spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", _storage_key)
except Exception:
    pass

GOVERNANCE_BASE = f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net/governance"
bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/loaded/run={RUN_ID}/sample_transactions.csv"
silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/transactions/run={RUN_ID}"
gold_path = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/daily_channel_summary"

raw = spark.read.option("header", True).option("inferSchema", True).csv(bronze_path)
typed = raw.withColumn("amount", F.col("amount_gbp").cast("double"))
good = typed.filter(F.col("amount").isNotNull() & (F.col("amount") > 0)).dropDuplicates(["transaction_id"])
good.write.format("delta").mode("overwrite").save(silver_path)
gold = good.groupBy("channel").agg(F.count("*").alias("txns"), F.round(F.sum("amount"), 2).alias("total_gbp"))
gold.write.format("delta").mode("overwrite").save(gold_path)

now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
manifest = {
    "catalog": "finledger-governance",
    "run_id": RUN_ID,
    "triggered_by": TRIGGERED_BY,
    "generated_utc": now,
    "data_assets": [
        {"qualified_name": bronze_path, "layer": "bronze"},
        {"qualified_name": silver_path, "layer": "silver"},
        {"qualified_name": gold_path, "layer": "gold"},
    ],
    "lineage_edges": [
        {"from": "bronze", "to": "silver", "pipeline": "pl_db_14_databricks_medallion_governance"},
        {"from": "silver", "to": "gold", "pipeline": "pl_db_14_databricks_medallion_governance"},
    ],
}
manifest_path = f"{GOVERNANCE_BASE}/adf_databricks_governance_manifest.json"
dbutils.fs.mkdirs(GOVERNANCE_BASE)
dbutils.fs.put(manifest_path, json.dumps(manifest, indent=2), overwrite=True)

summary = {
    "run_id": RUN_ID,
    "bronze_rows": raw.count(),
    "silver_rows": good.count(),
    "gold_channels": gold.count(),
    "manifest": manifest_path,
}
dbutils.notebook.exit(json.dumps(summary))
