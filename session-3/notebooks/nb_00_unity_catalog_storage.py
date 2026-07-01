# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Unity Catalog external locations (optional)
# MAGIC
# MAGIC Only if workspace has an **access connector** from orchestrate.cmd.
# MAGIC
# MAGIC Most learners: run **`nb_00_setup_credentials`** instead (storage key in secrets).

# COMMAND ----------

# MAGIC %run ./_storage_auth

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "Storage account")
dbutils.widgets.text("access_connector_id", "", "From orchestrate.cmd output")
dbutils.widgets.text("run_id", "session3-lab", "Run id")

storage_account = finledger_resolve_storage_account(
    dbutils.widgets.get("storage_account").strip()
)
access_connector_id = dbutils.widgets.get("access_connector_id").strip()
run_id = dbutils.widgets.get("run_id").strip()

if not access_connector_id:
    raise ValueError("Set access_connector_id — or use nb_00_setup_credentials instead")

spark.sql(
    f"""
CREATE STORAGE CREDENTIAL IF NOT EXISTS `finledger-storage`
WITH (AZURE_MANAGED_IDENTITY (ACCESS_CONNECTOR_ID = '{access_connector_id}'))
"""
)

for container in ["bronze", "silver", "gold"]:
    url = finledger_abfss(storage_account, container) + "/"
    loc = f"finledger-{container}"
    spark.sql(
        f"CREATE EXTERNAL LOCATION IF NOT EXISTS `{loc}` URL '{url}' "
        f"WITH (STORAGE CREDENTIAL `finledger-storage`)"
    )
    spark.sql(f"GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `{loc}` TO `account users`")
    print(f"OK: {loc}")

probe = finledger_abfss(storage_account, "bronze", f"loaded/run={run_id}/") + "/"
for f in dbutils.fs.ls(probe):
    print(f"  {f.name}")
