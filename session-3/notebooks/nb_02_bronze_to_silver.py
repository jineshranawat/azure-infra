# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze → Silver (cleanse, cast, quarantine)
# MAGIC
# MAGIC **Prerequisite:** `orchestrate.cmd --setup-secrets` once. Cell 1 loads account + key from scope `finledger`.
# MAGIC
# MAGIC Writes Delta to ADLS **and** registers **`finledger.silver.transactions`** in Unity Catalog.

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT_SECRET = "storage-account"
STORAGE_KEY_SECRET = "storage-key"
CATALOG = "finledger"
SILVER_TABLE = f"{CATALOG}.silver.transactions"

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

dbutils.widgets.text("bronze_path", "", "Bronze CSV abfss path")
dbutils.widgets.text("silver_path", "", "Silver Delta abfss folder")
dbutils.widgets.text("quarantine_path", "", "Quarantine folder")
dbutils.widgets.text("run_id", "session3-lab", "Run id")

run_id = dbutils.widgets.get("run_id").strip()
bronze_path = dbutils.widgets.get("bronze_path").strip()
silver_path = dbutils.widgets.get("silver_path").strip()
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
if not quarantine_path:
    quarantine_path = (
        f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
        f"quarantine/run={run_id}"
    )

print("Bronze:", bronze_path)
print("Silver:", silver_path)

# COMMAND ----------

raw = spark.read.option("header", True).csv(bronze_path)

messy_path = (
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"incoming/transactions/{run_id}/transactions_messy.csv"
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

print("Silver Delta written to storage.")

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

for grant_sql in (
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.silver TO `account users`",
    f"GRANT SELECT ON TABLE {SILVER_TABLE} TO `account users`",
):
    try:
        spark.sql(grant_sql)
    except Exception as exc:
        print(f"Grant skipped: {exc}")

print(f"Unity Catalog table: {SILVER_TABLE}")

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)
display(silver_df)
silver_df.groupBy("channel").count().show()
