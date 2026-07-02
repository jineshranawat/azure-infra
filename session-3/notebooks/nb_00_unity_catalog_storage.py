# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Unity Catalog setup (catalog, schemas, external locations)
# MAGIC
# MAGIC Run **once** if workspace has an **access connector** from orchestrate.cmd.
# MAGIC
# MAGIC Creates `finledger` catalog + bronze/silver/gold schemas + external locations.
# MAGIC Lab notebooks register tables on top of this (or use storage-key auth alone).

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT_SECRET = "storage-account"
STORAGE_KEY_SECRET = "storage-key"
CATALOG = "finledger"

try:
    STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key=STORAGE_ACCOUNT_SECRET).strip()
    _storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=STORAGE_KEY_SECRET).strip()
except Exception as exc:
    raise RuntimeError(
        "Secrets missing. On your PC run: cd session-3 && orchestrate.cmd --setup-secrets"
    ) from exc

for _host in (
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net",
    f"{STORAGE_ACCOUNT}.blob.core.windows.net",
):
    spark.conf.set(f"fs.azure.account.key.{_host}", _storage_key)

print(f"Secrets OK — scope={SECRET_SCOPE} account={STORAGE_ACCOUNT}")

# COMMAND ----------

dbutils.widgets.text("access_connector_id", "", "From orchestrate.cmd output")
dbutils.widgets.text("run_id", "session3-lab", "Run id")

access_connector_id = dbutils.widgets.get("access_connector_id").strip()
run_id = dbutils.widgets.get("run_id").strip()

if not access_connector_id:
    raise ValueError("Set access_connector_id widget")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION ''")
for _layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_layer}")
    print(f"Schema: {CATALOG}.{_layer}")

# COMMAND ----------

spark.sql(
    f"""
CREATE STORAGE CREDENTIAL IF NOT EXISTS `finledger-storage`
WITH (AZURE_MANAGED_IDENTITY (ACCESS_CONNECTOR_ID = '{access_connector_id}'))
"""
)

for container in ["bronze", "silver", "gold"]:
    url = f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    loc = f"finledger-{container}"
    spark.sql(
        f"CREATE EXTERNAL LOCATION IF NOT EXISTS `{loc}` URL '{url}' "
        f"WITH (STORAGE CREDENTIAL `finledger-storage`)"
    )
    spark.sql(f"GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `{loc}` TO `account users`")
    print(f"OK: {loc}")

probe = (
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"loaded/run={run_id}/"
)
for f in dbutils.fs.ls(probe):
    print(f"  {f.name}")

print("Unity Catalog infrastructure ready — run nb_01 … nb_04 to register tables.")
