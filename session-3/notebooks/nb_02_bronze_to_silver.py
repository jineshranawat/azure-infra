# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze → Silver (cleanse, cast, quarantine)
# MAGIC
# MAGIC **Why Databricks here?** ADF copied raw files; Spark can **filter bad rows**, **cast types**,
# MAGIC and **write Delta** with ACID guarantees — too heavy for a copy activity alone.
# MAGIC
# MAGIC | Step | What | Why |
# MAGIC |---|---|---|
# MAGIC | Read bronze | Raw CSV | Immutable landing zone |
# MAGIC | Clean | Trim, cast `amount_gbp` to double | Reporting needs numeric types |
# MAGIC | Quarantine | Rows where amount is not numeric | Data quality gate before silver |
# MAGIC | Write Delta | `silver/transactions` | ACID + schema enforcement |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType

dbutils.widgets.text("bronze_path", "", "Bronze CSV abfss path")
dbutils.widgets.text("silver_path", "", "Silver Delta abfss folder")
dbutils.widgets.text("quarantine_path", "", "Quarantine CSV abfss folder")
dbutils.widgets.text("run_id", "session3-lab", "Run id")

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")
quarantine_path = dbutils.widgets.get("quarantine_path")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------

storage_account = "stYOURLEARNERHASH"  # <-- change once per learner

if not bronze_path:
    bronze_path = (
        f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
        f"loaded/run={run_id}/sample_transactions.csv"
    )
if not silver_path:
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/transactions"
if not quarantine_path:
    quarantine_path = (
        f"abfss://silver@{storage_account}.dfs.core.windows.net/"
        f"quarantine/run={run_id}"
    )

print("Bronze:", bronze_path)
print("Silver:", silver_path)
print("Quarantine:", quarantine_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read bronze (messy feed optional — use transactions_messy for extra rows)

# COMMAND ----------

raw = spark.read.option("header", True).csv(bronze_path)

# Also read messy file for cleanse demo (invalid amount on TXN-20003)
messy_path = bronze_path.replace("sample_transactions.csv", "../incoming/transactions/")
messy_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    f"incoming/transactions/{run_id}/transactions_messy.csv"
)

try:
    messy = spark.read.option("header", True).csv(messy_path)
    raw = raw.unionByName(messy, allowMissingColumns=True)
    print("Joined messy feed for cleanse demo")
except Exception as exc:
    print(f"Messy feed not found (OK for minimal lab): {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations (lazy until write)

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

# Quarantine: cast failed (NULL amount after cast from non-numeric)
valid = cleaned.filter(F.col("amount_gbp").isNotNull())
quarantine = cleaned.filter(F.col("amount_gbp").isNull())

print(f"Valid rows: {valid.count()}, Quarantined: {quarantine.count()}")

# COMMAND ----------

display(valid.orderBy("value_date", "transaction_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver as Delta (overwrite for lab idempotency)

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

# MAGIC %md
# MAGIC ## Read back Silver Delta (proof)

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path)
display(silver_df)

silver_df.groupBy("channel").count().show()
