# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — End-to-end Bronze → Silver → Gold (ADF notebook activity)

# COMMAND ----------

# MAGIC %run ./_storage_auth

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

dbutils.widgets.dropdown("auth_mode", "auto", ["auto", "none", "access_connector"], "Auth")
dbutils.widgets.text("storage_account", "", "Optional if nb_00_setup done")
dbutils.widgets.text("bronze_path", "", "Bronze CSV path")
dbutils.widgets.text("silver_path", "", "Silver Delta folder")
dbutils.widgets.text("gold_path", "", "Gold Delta folder")
dbutils.widgets.text("quarantine_path", "", "Quarantine folder")
dbutils.widgets.text("run_id", "session3-lab", "Run id")

storage_account = finledger_configure_storage(
    storage_account=dbutils.widgets.get("storage_account").strip(),
    auth_mode=dbutils.widgets.get("auth_mode").strip(),
)

run_id = dbutils.widgets.get("run_id").strip()
bronze_path = dbutils.widgets.get("bronze_path").strip()
silver_path = dbutils.widgets.get("silver_path").strip()
gold_path = dbutils.widgets.get("gold_path").strip()
quarantine_path = dbutils.widgets.get("quarantine_path").strip()

if not bronze_path:
    bronze_path = finledger_abfss(
        storage_account, "bronze", f"loaded/run={run_id}/sample_transactions.csv"
    )
if not silver_path:
    silver_path = finledger_abfss(storage_account, "silver", "transactions")
if not gold_path:
    gold_path = finledger_abfss(storage_account, "gold", "daily_channel_summary")
if not quarantine_path:
    quarantine_path = finledger_abfss(
        storage_account, "silver", f"quarantine/run={run_id}"
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

silver_df = spark.read.format("delta").load(silver_path)

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

dbutils.notebook.exit(
    f'{{"bronze_rows": {bronze_count}, "silver_rows": {silver_count}, '
    f'"gold_rows": {gold_count}, "run_id": "{run_id}"}}'
)
