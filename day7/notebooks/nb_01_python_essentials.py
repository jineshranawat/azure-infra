# Databricks notebook source
# MAGIC %md
# MAGIC # Day 7 — Python essentials (one folder, no “advanced” split)
# MAGIC
# MAGIC **Before Spark:** you need a few Python habits — short cells, one idea each.
# MAGIC
# MAGIC **Local laptop (not this cluster):** `cd day7` → `orchestrate.cmd` creates a **venv** at repo root and installs **PySpark**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Virtual environment — what & why (your PC)
# MAGIC
# MAGIC | Step | Command (Windows) | Why |
# MAGIC |------|-------------------|-----|
# MAGIC | Create venv | `python -m venv .venv` | Isolates packages per project |
# MAGIC | Activate | `.venv\Scripts\activate` | `orchestrate.cmd` does this for you |
# MAGIC | Install | `pip install -r day7\requirements.txt` | Gets PySpark without touching global Python |
# MAGIC
# MAGIC **Analogy:** venv = a **toolbox** only for FinLedger — not every project on your laptop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Variables and lists

# COMMAND ----------

channels = ["wire", "card", "fps"]
print("channels:", channels)
print("first:", channels[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dictionaries — one row of data

# COMMAND ----------

txn = {"transaction_id": "TXN-10003", "amount_gbp": "50000.00"}
print("id:", txn["transaction_id"])
print("amount:", txn["amount_gbp"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Functions — reusable rules

# COMMAND ----------

def clean_amount(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

# COMMAND ----------

print(clean_amount("50000"), clean_amount("oops"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `pathlib` — paths without typos

# COMMAND ----------

from pathlib import Path

run_folder = Path("loaded") / "run=session3-lab"
csv_name = run_folder / "sample_transactions.csv"
print(csv_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Imports you will see in every PySpark notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Before Spark: pandas and Polars concepts
# MAGIC
# MAGIC - **pandas**: local, in-memory DataFrame (small/medium files).
# MAGIC - **Polars**: local DataFrame engine focused on speed.
# MAGIC - **Spark**: distributed DataFrame for big data and clusters.

# COMMAND ----------

import pandas as pd
import polars as pl

print("pandas", pd.__version__, "| polars", pl.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. PySpark imports (next step after pandas/Polars)

# COMMAND ----------

from pyspark.sql import functions as F

print("F and types imported — ready for Day 8 transforms.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint
# MAGIC - [ ] I can explain what a venv is (local PC)
# MAGIC - [ ] I can compare pandas vs Polars vs Spark in one sentence
# MAGIC - [ ] I can read `txn["amount_gbp"]` and `clean_amount("oops")`
# MAGIC - [ ] Next: **nb_02_storage_medallion** — ADLS paths
