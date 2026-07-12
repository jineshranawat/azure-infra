# Databricks notebook source
# MAGIC %md
# MAGIC # ADF Databricks Job task (Preview) — `pl_db_11`
# MAGIC
# MAGIC Lightweight task notebook for **Databricks Job activity (Preview)** in ADF.
# MAGIC Deployed as workspace job `adf-finledger-integration-job` by `databricks_infra.py`.

# COMMAND ----------

dbutils.widgets.text("run_id", "session3-lab")
dbutils.widgets.text("triggered_by", "adf-job-preview")
dbutils.widgets.text("job_note", "")

run_id = dbutils.widgets.get("run_id")
triggered_by = dbutils.widgets.get("triggered_by")
job_note = dbutils.widgets.get("job_note")

print("ADF DATABRICKS JOB ACTIVITY (PREVIEW)")
print("  run_id       :", run_id)
print("  triggered_by :", triggered_by)
print("  job_note     :", job_note or "(empty)")

# COMMAND ----------

import json
from datetime import datetime, timezone

result = {
    "status": "ok",
    "integration": "adf-databricks-job-preview",
    "run_id": run_id,
    "triggered_by": triggered_by,
    "job_note": job_note or None,
    "completed_utc": datetime.now(timezone.utc).isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
