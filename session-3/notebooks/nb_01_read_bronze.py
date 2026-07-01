# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Read Bronze from ADLS Gen2
# MAGIC
# MAGIC **Prerequisite:** run **`nb_00_setup_credentials`** once (or Single-user cluster + `auth_mode=none`).
# MAGIC
# MAGIC After setup, you only set **`run_id`** (and optional `bronze_path`) — credentials load from secrets automatically.

# COMMAND ----------

# MAGIC %run ./_storage_auth

# COMMAND ----------

dbutils.widgets.dropdown(
    "auth_mode",
    "auto",
    ["auto", "none", "access_connector"],
    "auto = use saved secrets | none = Single-user cluster",
)
dbutils.widgets.text(
    "storage_account",
    "",
    "Optional if saved by nb_00_setup_credentials",
)
dbutils.widgets.text("bronze_path", "", "Full abfss path OR leave empty")
dbutils.widgets.text("run_id", "session3-lab", "Run folder id")

storage_account = finledger_configure_storage(
    storage_account=dbutils.widgets.get("storage_account").strip(),
    auth_mode=dbutils.widgets.get("auth_mode").strip(),
)

bronze_path = dbutils.widgets.get("bronze_path").strip()
run_id = dbutils.widgets.get("run_id").strip()

# COMMAND ----------

if not bronze_path:
    bronze_path = finledger_abfss(
        storage_account,
        "bronze",
        f"loaded/run={run_id}/sample_transactions.csv",
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
