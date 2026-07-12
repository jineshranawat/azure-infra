# Databricks notebook source
# MAGIC %md
# MAGIC # ADF hello notebook — triggered by Azure Data Factory
# MAGIC
# MAGIC **Purpose:** Prove ADF **Databricks Notebook** activity can pass parameters and run code on a job cluster.
# MAGIC
# MAGIC **Linked service:** `ls_databricks_shared` in `adf-shared-*`
# MAGIC
# MAGIC **Pipelines:** `pl_07_databricks_notebook`, `pl_10_parent_chain`

# COMMAND ----------

dbutils.widgets.text("run_id", "session3-lab")
dbutils.widgets.text("triggered_by", "manual")
dbutils.widgets.text("source_file", "")

run_id = dbutils.widgets.get("run_id")
triggered_by = dbutils.widgets.get("triggered_by")
source_file = dbutils.widgets.get("source_file")

print("=" * 60)
print("ADF NOTEBOOK ACTIVITY — SUCCESS PATH")
print("  run_id       :", run_id)
print("  triggered_by :", triggered_by)
if source_file:
    print("  source_file  :", source_file)
print("=" * 60)

# COMMAND ----------

# Optional: prove secrets scope works (same as session-3 labs)
try:
    scope = "finledger"
    acct = dbutils.secrets.get(scope=scope, key="storage-account")
    print("Storage account from secret scope:", acct)
except Exception as exc:
    print("Secret scope note (run session-3 --setup-secrets if needed):", exc)

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"status": "ok", "run_id": run_id, "triggered_by": triggered_by, "source_file": source_file or None}))
