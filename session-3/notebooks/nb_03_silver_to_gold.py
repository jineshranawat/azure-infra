# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver → Gold (business aggregates)
# MAGIC
# MAGIC **Prerequisite:** `orchestrate.cmd --setup-secrets` once. Cell 1 loads account + key from scope `finledger`.
# MAGIC
# MAGIC Reads **`finledger.silver.transactions`** and registers **`finledger.gold.daily_channel_summary`**.

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

dbutils.widgets.text("silver_path", "", "Silver Delta folder")
dbutils.widgets.text("gold_path", "", "Gold Delta folder")

silver_path = dbutils.widgets.get("silver_path").strip()
gold_path = dbutils.widgets.get("gold_path").strip()

if not silver_path:
    silver_path = (
        f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/transactions"
    )
if not gold_path:
    gold_path = (
        f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/daily_channel_summary"
    )

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
USING DELTA
LOCATION '{silver_path}'
"""
)

try:
    silver_df = spark.table(SILVER_TABLE)
except Exception:
    silver_df = spark.read.format("delta").load(silver_path)

# COMMAND ----------

gold_df = (
    silver_df.groupBy("value_date", "channel")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount_gbp").alias("total_amount_gbp"),
        F.sum(F.when(F.col("is_high_value"), 1).otherwise(0)).alias("high_value_count"),
        F.sum(F.when(F.col("status") == "pending", 1).otherwise(0)).alias("pending_count"),
    )
    .orderBy("value_date", "channel")
)

display(gold_df)

# COMMAND ----------

(
    gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path)
)

print(f"Gold Delta written to storage: {gold_path}")

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
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `account users`",
    f"GRANT SELECT ON TABLE {GOLD_TABLE} TO `account users`",
):
    try:
        spark.sql(grant_sql)
    except Exception as exc:
        print(f"Grant skipped: {exc}")

print(f"Unity Catalog table: {GOLD_TABLE}")
display(spark.table(GOLD_TABLE))

# COMMAND ----------

display(
    silver_df.filter(
        (F.col("status") == "pending") & (F.col("amount_gbp") >= 10000)
    ).select("transaction_id", "account_id", "amount_gbp", "channel", "status")
)
