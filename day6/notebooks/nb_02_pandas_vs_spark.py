# Databricks notebook source
# MAGIC %md
# MAGIC # Day 6 — Read CSV two ways: pandas vs PySpark
# MAGIC
# MAGIC **Prerequisite:** `session-3\orchestrate.cmd --setup-secrets` once (scope `finledger`).
# MAGIC
# MAGIC Cell 1 loads **`storage-account`** and **`storage-key`** from scope `finledger` (same as Session 3).
# MAGIC
# MAGIC Reads the **same bronze file** Session 3 landed (`session3-lab` run folder).

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT_SECRET = "storage-account"
STORAGE_KEY_SECRET = "storage-key"
CATALOG = "finledger"
BRONZE_TABLE = f"{CATALOG}.bronze.sample_transactions"

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

dbutils.widgets.text("bronze_path", "", "Full abfss:// path (optional)")
dbutils.widgets.text("run_id", "session3-lab", "Run folder under bronze/loaded")

bronze_path = dbutils.widgets.get("bronze_path").strip()
run_id = dbutils.widgets.get("run_id").strip()

if not bronze_path:
    bronze_path = (
        f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
        f"loaded/run={run_id}/sample_transactions.csv"
    )

print(f"Bronze file: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. pandas — loads everything into the driver (fine for small CSVs)

# COMMAND ----------

import pandas as pd

pdf = pd.read_csv(bronze_path)
print(f"pandas shape: {pdf.shape}")
display(pdf.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. PySpark — lazy, distributed (what we use at scale)
# MAGIC
# MAGIC Prefer the **Unity Catalog** table from Session 3 `nb_01` when it exists; otherwise read the CSV path.

# COMMAND ----------

try:
    spark_df = spark.table(BRONZE_TABLE)
    print(f"Spark source: Unity Catalog {BRONZE_TABLE}")
except Exception:
    spark_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(bronze_path)
    )
    print(f"Spark source: {bronze_path}")

print(f"Spark columns: {spark_df.columns}")
spark_df.printSchema()
display(spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Same transform in both worlds

# COMMAND ----------

import logging

log = logging.getLogger("finledger")


def clean_amount(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        log.warning("bad amount: %r", value)
        return 0.0


pdf["amount_clean"] = pdf["amount_gbp"].apply(clean_amount)
display(pdf[["transaction_id", "amount_gbp", "amount_clean", "channel"]])

# COMMAND ----------

from pyspark.sql import functions as F

spark_clean = spark_df.withColumn(
    "amount_clean",
    F.when(F.col("amount_gbp").cast("double").isNotNull(), F.col("amount_gbp").cast("double")).otherwise(
        F.lit(0.0)
    ),
)
display(spark_clean.select("transaction_id", "amount_gbp", "amount_clean", "channel"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compare row counts (actions)

# COMMAND ----------

pandas_count = len(pdf)
spark_count = spark_df.count()
print(f"pandas rows: {pandas_count}  |  Spark rows: {spark_count}")
assert pandas_count == spark_count, "Counts should match for the same file"

# COMMAND ----------

# MAGIC %md
# MAGIC ### When to use which?
# MAGIC
# MAGIC | Tool | Good for | FinLedger rule |
# MAGIC |---|---|---|
# MAGIC | **pandas** | Quick peek, unit-test-sized data, local laptop | Session 6 learning + tests |
# MAGIC | **PySpark** | Lakehouse tables, millions of rows, Delta writes | Sessions 7+ and all production paths |
# MAGIC
# MAGIC **Checkpoint:** row counts match; TXN-10003 still shows £50,000 pending wire.

# COMMAND ----------

display(spark_df.filter("transaction_id = 'TXN-10003'"))
