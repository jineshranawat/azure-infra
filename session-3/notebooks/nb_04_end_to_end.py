# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — End-to-end Bronze → Silver → Gold (ADF notebook activity)
# MAGIC
# MAGIC **Prerequisite:** `orchestrate.cmd --setup-secrets` once. Cell 1 loads account + key from scope `finledger`.
# MAGIC
# MAGIC Full medallion run + Unity Catalog tables:
# MAGIC - `finledger.silver.transactions`
# MAGIC - `finledger.gold.daily_channel_summary`

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT_SECRET = "storage-account"
STORAGE_KEY_SECRET = "storage-key"
CATALOG = "finledger"
SILVER_TABLE = f"{CATALOG}.silver.transactions"
GOLD_TABLE = f"{CATALOG}.gold.daily_channel_summary"

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

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

dbutils.widgets.text("bronze_path", "", "Bronze CSV path")
dbutils.widgets.text("silver_path", "", "Silver Delta folder")
dbutils.widgets.text("gold_path", "", "Gold Delta folder")
dbutils.widgets.text("quarantine_path", "", "Quarantine folder")
dbutils.widgets.text("run_id", "session3-lab", "Run id")

run_id = dbutils.widgets.get("run_id").strip()
bronze_path = dbutils.widgets.get("bronze_path").strip()
silver_path = dbutils.widgets.get("silver_path").strip()
gold_path = dbutils.widgets.get("gold_path").strip()
quarantine_path = dbutils.widgets.get("quarantine_path").strip()

if not bronze_path:
    bronze_path = (
        f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
        f"loaded/run={run_id}/sample_transactions.csv"
    )
if not silver_path:
    silver_path = (
        f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/transactions"
    )
if not gold_path:
    gold_path = (
        f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/daily_channel_summary"
    )
if not quarantine_path:
    quarantine_path = (
        f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
        f"quarantine/run={run_id}"
    )

# COMMAND ----------

bronze_df = spark.read.option("header", True).csv(bronze_path)
bronze_count = bronze_df.count()
print(f"Bronze rows: {bronze_count}")

# COMMAND ----------

cleaned = (
    bronze_df.withColumn("transaction_id", F.trim(F.col("transaction_id")))
    .withColumn("channel", F.lower(F.trim(F.col("channel"))))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .withColumn("amount_gbp", F.col("amount_gbp").cast(DoubleType()))
    .withColumn("value_date", F.to_date(F.col("value_date")))
    .withColumn("is_high_value", F.col("amount_gbp") >= 1000)
    .withColumn("ingested_run_id", F.lit(run_id))
)

valid = cleaned.filter(F.col("amount_gbp").isNotNull())
quarantine = cleaned.filter(F.col("amount_gbp").isNull())

(
    valid.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path)
)

if quarantine.count() > 0:
    quarantine.write.mode("overwrite").option("header", True).csv(quarantine_path)

silver_count = valid.count()
print(f"Silver rows: {silver_count}")

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
USING DELTA
LOCATION '{silver_path}'
"""
)

try:
    spark.sql(f"REFRESH TABLE {SILVER_TABLE}")
except Exception:
    pass

silver_df = spark.table(SILVER_TABLE)

gold_df = (
    silver_df.groupBy("value_date", "channel")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount_gbp").alias("total_amount_gbp"),
        F.sum(F.when(F.col("is_high_value"), 1).otherwise(0)).alias("high_value_count"),
    )
)

(
    gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path)
)

gold_count = gold_df.count()
print(f"Gold rows: {gold_count}")

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE}
USING DELTA
LOCATION '{gold_path}'
"""
)

try:
    spark.sql(f"REFRESH TABLE {GOLD_TABLE}")
except Exception:
    pass

for grant_sql in (
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`",
    f"GRANT SELECT ON TABLE {SILVER_TABLE} TO `account users`",
    f"GRANT SELECT ON TABLE {GOLD_TABLE} TO `account users`",
):
    try:
        spark.sql(grant_sql)
    except Exception as exc:
        print(f"Grant skipped: {exc}")

print(f"Unity Catalog: {SILVER_TABLE}, {GOLD_TABLE}")

# COMMAND ----------

dbutils.notebook.exit(
    f'{{"bronze_rows": {bronze_count}, "silver_rows": {silver_count}, '
    f'"gold_rows": {gold_count}, "run_id": "{run_id}", '
    f'"silver_table": "{SILVER_TABLE}", "gold_table": "{GOLD_TABLE}"}}'
)
