# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Read Bronze from ADLS Gen2
# MAGIC
# MAGIC **Prerequisite:** `orchestrate.cmd --setup-secrets` once (scope `finledger`).
# MAGIC
# MAGIC Cell 1 loads **`storage-account`** and **`storage-key`** from secrets — no hardcoded credentials.
# MAGIC
# MAGIC After this lab, bronze data is also registered in **Unity Catalog** (`finledger.bronze.sample_transactions`).

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

if not STORAGE_ACCOUNT or not _storage_key:
    raise ValueError("Empty secret — re-run session-3\\orchestrate.cmd --setup-secrets")

for _host in (
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net",
    f"{STORAGE_ACCOUNT}.blob.core.windows.net",
):
    spark.conf.set(f"fs.azure.account.key.{_host}", _storage_key)

print(f"Secrets OK — scope={SECRET_SCOPE} account={STORAGE_ACCOUNT}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION ''")
for _layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_layer}")
print(f"Unity Catalog: {CATALOG}.bronze | {CATALOG}.silver | {CATALOG}.gold")

# COMMAND ----------

dbutils.widgets.text("bronze_path", "", "Full abfss path OR leave empty")
dbutils.widgets.text("run_id", "session3-lab", "Run folder id")

run_id = dbutils.widgets.get("run_id").strip()
bronze_path = dbutils.widgets.get("bronze_path").strip()

if not bronze_path:
    bronze_path = (
        f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
        f"loaded/run={run_id}/sample_transactions.csv"
    )

bronze_folder = (
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"loaded/run={run_id}/"
)

print(f"Reading: {bronze_path}")

# COMMAND ----------

bronze_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path)
)

row_count = bronze_df.count()
print(f"Bronze rows: {row_count}")

display(bronze_df)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

display(bronze_df.filter("transaction_id = 'TXN-10003'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register bronze in Unity Catalog (Catalog UI)

# COMMAND ----------

BRONZE_TABLE = f"{CATALOG}.bronze.sample_transactions"

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
USING CSV
OPTIONS (header 'true', inferSchema 'true')
LOCATION '{bronze_folder}'
"""
)

try:
    spark.sql(f"REFRESH TABLE {BRONZE_TABLE}")
except Exception:
    pass

for grant_sql in (
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.bronze TO `account users`",
    f"GRANT SELECT ON TABLE {BRONZE_TABLE} TO `account users`",
):
    try:
        spark.sql(grant_sql)
    except Exception as exc:
        print(f"Grant skipped: {exc}")

print(f"Unity Catalog table: {BRONZE_TABLE}")
display(spark.sql(f"SELECT * FROM {BRONZE_TABLE} LIMIT 5"))
