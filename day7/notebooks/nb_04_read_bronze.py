# Databricks notebook source
# MAGIC %md
# MAGIC # Day 7 — Read bronze from the lake
# MAGIC
# MAGIC **Practical:** load Session 3 CSV from ADLS and peek at Unity Catalog.

# COMMAND ----------

SECRET_SCOPE = "finledger"
CATALOG = "finledger"

STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()

for _host in (f"{STORAGE_ACCOUNT}.dfs.core.windows.net", f"{STORAGE_ACCOUNT}.blob.core.windows.net"):
    spark.conf.set(f"fs.azure.account.key.{_host}", _storage_key)

print(f"account={STORAGE_ACCOUNT}")

# COMMAND ----------

dbutils.widgets.text("run_id", "session3-lab", "Run folder")
run_id = dbutils.widgets.get("run_id").strip()

bronze_path = (
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"loaded/run={run_id}/sample_transactions.csv"
)

# COMMAND ----------

bronze_df = spark.read.option("header", True).option("inferSchema", True).csv(bronze_path)

# COMMAND ----------

print("rows:", bronze_df.count())

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog table (if Session 3 registered it)

# COMMAND ----------

BRONZE_TABLE = f"{CATALOG}.bronze.sample_transactions"

try:
    uc_count = spark.table(BRONZE_TABLE).count()
    print(f"{BRONZE_TABLE}: {uc_count} rows")
except Exception as exc:
    print(f"UC table not ready — run session-3 nb_01 ({exc})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint
# MAGIC - [ ] Bronze CSV read from `abfss://` without errors
# MAGIC - [ ] Schema shows `amount_gbp` as string (cast in Day 8)
# MAGIC - [ ] Ready for **Day 8** transforms
