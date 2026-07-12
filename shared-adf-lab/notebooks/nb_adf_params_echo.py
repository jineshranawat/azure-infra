# Databricks notebook source
# MAGIC %md
# MAGIC # ADF parameters echo — expression-builder lab
# MAGIC
# MAGIC **Pipelines:** `pl_db_02` … `pl_db_08` pass **dynamic** `base_parameters` from ADF expressions.
# MAGIC
# MAGIC **Widgets** must match ADF **Databricks Notebook** activity parameter names exactly.

# COMMAND ----------

dbutils.widgets.text("run_id", "session3-lab")
dbutils.widgets.text("triggered_by", "manual")
dbutils.widgets.text("source_file", "")
dbutils.widgets.text("lab_tag", "")

run_id = dbutils.widgets.get("run_id")
triggered_by = dbutils.widgets.get("triggered_by")
source_file = dbutils.widgets.get("source_file")
lab_tag = dbutils.widgets.get("lab_tag")

print("ADF → Databricks parameter mapping")
print("  run_id       :", run_id)
print("  triggered_by :", triggered_by)
print("  source_file  :", source_file or "(empty)")
print("  lab_tag      :", lab_tag or "(empty)")

# COMMAND ----------

import json

result = {
    "status": "ok",
    "run_id": run_id,
    "triggered_by": triggered_by,
    "source_file": source_file or None,
    "lab_tag": lab_tag or None,
}
dbutils.notebook.exit(json.dumps(result))
