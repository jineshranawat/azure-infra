# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver → Gold (business aggregates)

# COMMAND ----------

# MAGIC %run ./_storage_auth

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.dropdown("auth_mode", "auto", ["auto", "none", "access_connector"], "Auth")
dbutils.widgets.text("storage_account", "", "Optional if nb_00_setup done")
dbutils.widgets.text("silver_path", "", "Silver Delta folder")
dbutils.widgets.text("gold_path", "", "Gold Delta folder")

storage_account = finledger_configure_storage(
    storage_account=dbutils.widgets.get("storage_account").strip(),
    auth_mode=dbutils.widgets.get("auth_mode").strip(),
)

silver_path = dbutils.widgets.get("silver_path").strip()
gold_path = dbutils.widgets.get("gold_path").strip()

if not silver_path:
    silver_path = finledger_abfss(storage_account, "silver", "transactions")
if not gold_path:
    gold_path = finledger_abfss(storage_account, "gold", "daily_channel_summary")

# COMMAND ----------

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

print(f"Gold written: {gold_path}")

# COMMAND ----------

display(
    silver_df.filter(
        (F.col("status") == "pending") & (F.col("amount_gbp") >= 10000)
    ).select("transaction_id", "account_id", "amount_gbp", "channel", "status")
)
