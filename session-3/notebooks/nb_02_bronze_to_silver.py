# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze → Silver (cleanse, cast, quarantine)

# COMMAND ----------

# MAGIC %run ./_storage_auth

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

dbutils.widgets.dropdown("auth_mode", "auto", ["auto", "none", "access_connector"], "Auth")
dbutils.widgets.text("storage_account", "", "Optional if nb_00_setup done")
dbutils.widgets.text("bronze_path", "", "Bronze CSV abfss path")
dbutils.widgets.text("silver_path", "", "Silver Delta abfss folder")
dbutils.widgets.text("quarantine_path", "", "Quarantine folder")
dbutils.widgets.text("run_id", "session3-lab", "Run id")

storage_account = finledger_configure_storage(
    storage_account=dbutils.widgets.get("storage_account").strip(),
    auth_mode=dbutils.widgets.get("auth_mode").strip(),
)

run_id = dbutils.widgets.get("run_id").strip()
bronze_path = dbutils.widgets.get("bronze_path").strip()
silver_path = dbutils.widgets.get("silver_path").strip()
quarantine_path = dbutils.widgets.get("quarantine_path").strip()

if not bronze_path:
    bronze_path = finledger_abfss(
        storage_account, "bronze", f"loaded/run={run_id}/sample_transactions.csv"
    )
if not silver_path:
    silver_path = finledger_abfss(storage_account, "silver", "transactions")
if not quarantine_path:
    quarantine_path = finledger_abfss(
        storage_account, "silver", f"quarantine/run={run_id}"
    )

print("Bronze:", bronze_path)
print("Silver:", silver_path)

# COMMAND ----------

raw = spark.read.option("header", True).csv(bronze_path)

messy_path = finledger_abfss(
    storage_account, "bronze", f"incoming/transactions/{run_id}/transactions_messy.csv"
)

try:
    messy = spark.read.option("header", True).csv(messy_path)
    raw = raw.unionByName(messy, allowMissingColumns=True)
    print("Joined messy feed")
except Exception as exc:
    print(f"Messy feed not found (OK): {exc}")

# COMMAND ----------

cleaned = (
    raw.withColumn("transaction_id", F.trim(F.col("transaction_id")))
    .withColumn("account_id", F.trim(F.col("account_id")))
    .withColumn("channel", F.lower(F.trim(F.col("channel"))))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .withColumn("amount_gbp_raw", F.col("amount_gbp"))
    .withColumn("amount_gbp", F.col("amount_gbp").cast(DoubleType()))
    .withColumn("value_date", F.to_date(F.col("value_date")))
    .withColumn("is_high_value", F.col("amount_gbp") >= 1000)
    .withColumn("ingested_run_id", F.lit(run_id))
)

valid = cleaned.filter(F.col("amount_gbp").isNotNull())
quarantine = cleaned.filter(F.col("amount_gbp").isNull())

print(f"Valid: {valid.count()}, Quarantined: {quarantine.count()}")

# COMMAND ----------

display(valid.orderBy("value_date", "transaction_id"))

# COMMAND ----------

(
    valid.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path)
)

if quarantine.count() > 0:
    quarantine.write.mode("overwrite").option("header", True).csv(quarantine_path)

print("Silver Delta written.")

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path)
display(silver_df)
silver_df.groupBy("channel").count().show()
