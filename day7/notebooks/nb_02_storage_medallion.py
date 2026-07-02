# Databricks notebook source
# MAGIC %md
# MAGIC # Day 7 — Storage & medallion layout
# MAGIC
# MAGIC **Theory first:** where FinLedger files live before we `spark.read` them.

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT_SECRET = "storage-account"
STORAGE_KEY_SECRET = "storage-key"

try:
    STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key=STORAGE_ACCOUNT_SECRET).strip()
    _storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=STORAGE_KEY_SECRET).strip()
except Exception as exc:
    raise RuntimeError("Run session-3\\orchestrate.cmd --setup-secrets") from exc

for _host in (f"{STORAGE_ACCOUNT}.dfs.core.windows.net", f"{STORAGE_ACCOUNT}.blob.core.windows.net"):
    spark.conf.set(f"fs.azure.account.key.{_host}", _storage_key)

print(f"Storage account: {STORAGE_ACCOUNT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADLS Gen2 in one slide
# MAGIC
# MAGIC - **ADLS Gen2** = Azure Data Lake Storage with **real folders** (HNS).
# MAGIC - **Medallion:** bronze (raw) → silver (clean) → gold (metrics).
# MAGIC - **abfss://** = Spark’s path to a container — no copy into DBFS.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path anatomy
# MAGIC
# MAGIC ```
# MAGIC abfss://bronze@<account>.dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv
# MAGIC          ^^^^^^  ^^^^^^^^                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# MAGIC          layer   storage account                 folder + file
# MAGIC ```

# COMMAND ----------

bronze_root = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
silver_root = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
gold_root = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

print("bronze:", bronze_root)
print("silver:", silver_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run folder (Session 3 landed data here)

# COMMAND ----------

dbutils.widgets.text("run_id", "session3-lab", "Run folder id")
run_id = dbutils.widgets.get("run_id").strip()

bronze_csv = f"{bronze_root}loaded/run={run_id}/sample_transactions.csv"
print(bronze_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access tiers (cost — one line each)
# MAGIC
# MAGIC - **Hot** — frequent reads (bronze landing).
# MAGIC - **Cool** — older archives (audit).
# MAGIC - **LRS** — three copies in one UK region (our Class-1 default).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auth recap
# MAGIC
# MAGIC Cell 1 read **`storage-key`** from scope `finledger` — same as Session 3. **Never** paste keys in notebook cells.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint
# MAGIC - [ ] I can point at bronze / silver / gold in Storage Explorer
# MAGIC - [ ] I can read an `abfss://` path out loud
# MAGIC - [ ] Next: **nb_03_spark_concepts** — DataFrame & lazy evaluation
