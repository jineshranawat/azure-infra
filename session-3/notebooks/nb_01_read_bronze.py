# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Read Bronze from ADLS Gen2
# MAGIC
# MAGIC **FinLedger Session 3** — read the CSV `orchestrate.cmd` landed in bronze.
# MAGIC
# MAGIC | Concept | Why |
# MAGIC |---|---|
# MAGIC | `abfss://` | Secure path to ADLS Gen2 from Spark (no mount needed) |
# MAGIC | Lazy read | `spark.read` builds a plan; nothing moves until an **action** (`count`, `show`) |
# MAGIC | Widgets | Paste path from `orchestrate.cmd` — no code edit needed |

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "Storage account name (e.g. stjineshfqdcgg)")
dbutils.widgets.text("bronze_path", "", "Full abfss path OR leave empty to build from widgets")
dbutils.widgets.text("run_id", "session3-lab", "Run folder id")

storage_account = dbutils.widgets.get("storage_account").strip()
bronze_path = dbutils.widgets.get("bronze_path").strip()
run_id = dbutils.widgets.get("run_id").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve storage path
# MAGIC
# MAGIC **Preferred:** run `session-3\orchestrate.cmd` and paste the printed **Bronze read** line into the `bronze_path` widget.
# MAGIC
# MAGIC **Or** fill `storage_account` widget only (e.g. `stjineshfqdcgg`).

# COMMAND ----------

if not bronze_path:
    if not storage_account:
        raise ValueError(
            "Set bronze_path widget (from orchestrate.cmd) OR storage_account widget"
        )
    bronze_path = (
        f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
        f"loaded/run={run_id}/sample_transactions.csv"
    )

print(f"Reading: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV (transformation = lazy until action)

# COMMAND ----------

bronze_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path)
)

row_count = bronze_df.count()
print(f"Bronze rows: {row_count}")

display(bronze_df)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify FinLedger fraud row — TXN-10003 (£50k pending wire)

# COMMAND ----------

display(bronze_df.filter("transaction_id = 'TXN-10003'"))
