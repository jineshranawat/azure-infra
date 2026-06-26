# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver → Gold (business aggregates)
# MAGIC
# MAGIC **Gold** = business-ready tables for dashboards and regulators.
# MAGIC
# MAGIC FinLedger need: *daily totals and counts by payment channel*, plus high-value flags.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("silver_path", "", "Silver Delta abfss folder")
dbutils.widgets.text("gold_path", "", "Gold Delta abfss folder")

silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")

storage_account = "stYOURLEARNERHASH"  # <-- change once per learner

if not silver_path:
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/transactions"
if not gold_path:
    gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/daily_channel_summary"

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold aggregation — one row per (date, channel)

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

print(f"Gold Delta written to {gold_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business check — pending high-value wires (fraud queue)

# COMMAND ----------

display(
    silver_df.filter(
        (F.col("status") == "pending") & (F.col("amount_gbp") >= 10000)
    ).select("transaction_id", "account_id", "amount_gbp", "channel", "status")
)
