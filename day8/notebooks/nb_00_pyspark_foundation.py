# Databricks notebook source
# MAGIC %md
# MAGIC # Day 8 — PySpark foundation (read this first)
# MAGIC
# MAGIC **Before transforms:** this notebook makes PySpark **fully clear**.
# MAGIC
# MAGIC **Order:** Day 7 nb_03 → **this notebook** → nb_01 … nb_04.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Local setup (your PC, not Databricks)
# MAGIC
# MAGIC ```text
# MAGIC cd d:\azure
# MAGIC python -m venv .venv
# MAGIC .venv\Scripts\python.exe -m pip install -r day8\requirements.txt
# MAGIC cd day8
# MAGIC orchestrate.cmd
# MAGIC ```
# MAGIC
# MAGIC Installs **pandas**, **Polars**, **PySpark** into one isolated venv.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — pandas & Polars recap (local DataFrames)
# MAGIC
# MAGIC | | pandas | Polars | PySpark |
# MAGIC |---|--------|--------|---------|
# MAGIC | Runs on | Your PC RAM | Your PC RAM | Cluster (or local[*]) |
# MAGIC | Speed | Good | Very fast | Scales out |
# MAGIC | API | `df["col"]` | `df.select("col")` | `df.select("col")` |
# MAGIC
# MAGIC **Same CSV** in `day8/data/` — three tools, one FinLedger file.

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()
RUN_ID = "session3-lab"

def _read_bronze_transactions(spark, account, key, run_id):
    path = f"abfss://bronze@{account}.dfs.core.windows.net/loaded/run={run_id}/sample_transactions.csv"
    try:
        spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
        f = spark.read.option("header", True).option("inferSchema", True).csv(path)
        f.limit(1).count()
        return f, path
    except Exception:
        pass
    try:
        f = spark.read.option("header", True).option("inferSchema", True).csv(path)
        f.limit(1).count()
        return f, path
    except Exception:
        pass
    from io import StringIO
    import pandas as pd
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "azure-storage-file-datalake", "-q"])
        from azure.storage.filedatalake import DataLakeServiceClient
    rel = f"loaded/run={run_id}/sample_transactions.csv"
    c = DataLakeServiceClient(f"https://{account}.dfs.core.windows.net", credential=key)
    raw = c.get_file_system_client("bronze").get_file_client(rel).download_file().readall().decode("utf-8")
    return spark.createDataFrame(pd.read_csv(StringIO(raw))), path

df, path = _read_bronze_transactions(spark, STORAGE_ACCOUNT, _storage_key, RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — PySpark module map
# MAGIC
# MAGIC | Import | Purpose |
# MAGIC |--------|---------|
# MAGIC | `from pyspark.sql import SparkSession` | Create `spark` (local only) |
# MAGIC | `from pyspark.sql import functions as F` | Column math: col, cast, sum |
# MAGIC | `from pyspark.sql.types import ...` | Explicit schemas |
# MAGIC | `from pyspark.sql.window import Window` | Rank, running totals |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("F and Window imported.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Column expressions (not Python loops)
# MAGIC
# MAGIC **Wrong:** `for row in df.collect(): ...` (pulls all data to driver)
# MAGIC
# MAGIC **Right:** `df.withColumn("x", F.col("y") * 2)` (runs on cluster)

# COMMAND ----------

# df loaded above (Serverless-safe auth)
demo = df.select(
    F.col("transaction_id"),
    F.col("channel"),
    F.upper(F.col("channel")).alias("channel_upper"),
)
demo.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Transformation chain (all lazy)

# COMMAND ----------

step1 = df.filter(F.col("status") == "posted")
step2 = step1.select("transaction_id", "channel", "amount_gbp")
print("Two lazy steps — no execution yet.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Actions you will use daily
# MAGIC
# MAGIC | Action | What it does | When |
# MAGIC |--------|--------------|------|
# MAGIC | `.show(n)` | Print n rows | Debug |
# MAGIC | `.count()` | Row count | Validate |
# MAGIC | `.printSchema()` | Types | After read |
# MAGIC | `display(df)` | Grid in Databricks | Explore |
# MAGIC | `.write...` | Save to lake | Bronze→silver |

# COMMAND ----------

print("rows after filter+select:", step2.count())

# COMMAND ----------

step2.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — SQL vs DataFrame API (same engine)
# MAGIC
# MAGIC Register a temp view → write SQL → Spark runs the same optimiser.

# COMMAND ----------

df.createOrReplaceTempView("bronze_txn")

# COMMAND ----------

spark.sql("SELECT channel, COUNT(*) AS n FROM bronze_txn GROUP BY channel").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — The eight transforms (Day 8 notebooks)
# MAGIC
# MAGIC 1. `select` — nb_01  
# MAGIC 2. `filter` — nb_01  
# MAGIC 3. `withColumn` + cast — nb_02  
# MAGIC 4. `groupBy` + `agg` — nb_03  
# MAGIC 5. `orderBy` — nb_03  
# MAGIC 6. `dropDuplicates` — nb_04  
# MAGIC 7. `join` — nb_04  
# MAGIC 8. `window` — nb_04

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Common mistakes
# MAGIC
# MAGIC | Mistake | Fix |
# MAGIC |---------|-----|
# MAGIC | Calling `.collect()` on big data | Use `.show()` or `.filter` first |
# MAGIC | Comparing column wrong | Use `F.col("a") == F.col("b")`, not `==` alone |
# MAGIC | Forgetting cast | Bronze strings need `.cast("double")` |
# MAGIC | Action inside loop | Build one pipeline, one `.write` at end |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Foundation complete
# MAGIC - [ ] I ran local `day8\orchestrate.cmd` on my PC
# MAGIC - [ ] I understand lazy chain + single action
# MAGIC - [ ] Start **nb_01_select_filter**
