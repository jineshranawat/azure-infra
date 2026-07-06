# Databricks notebook source

# MAGIC %md
# MAGIC # File formats & secret scope — simple FinLedger guide
# MAGIC
# MAGIC **Two topics in one short notebook:**
# MAGIC 1. How to store **secrets** in scope `finledger` (safe storage keys)
# MAGIC 2. **File formats** you meet on the lake: CSV, JSON, Parquet, Delta
# MAGIC
# MAGIC **Cells:** small theory + diagram + one code example each.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — Secret scope `finledger`

# COMMAND ----------

# MAGIC %md
# MAGIC #### How secrets reach your notebook
# MAGIC
# MAGIC ```
# MAGIC Azure Portal          .env laptop           Databricks
# MAGIC ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
# MAGIC │ storage     │      │ orchestrate │      │ scope:      │
# MAGIC │ key1 copy   │─────►│ .cmd setup  │─────►│ finledger   │
# MAGIC └─────────────┘      └─────────────┘      └──────┬──────┘
# MAGIC                                                   │
# MAGIC                                                   ▼
# MAGIC                                          dbutils.secrets.get()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### A.1 — What is a secret scope?

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC A **secret scope** is a named vault inside Databricks (scope `finledger`) that holds keys like `storage-account` and `storage-key`.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Storage keys must **never** appear in notebook code or git — they leak in logs and version control.

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Notebooks call `dbutils.secrets.get(scope, key)` at runtime. Databricks decrypts the value on the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Hotel safe:** you keep the PIN in the safe, not written on the room door.

# COMMAND ----------

print("Scope name we use in every lab: finledger")

# COMMAND ----------

# MAGIC %md
# MAGIC ### A.2 — Step-by-step: add secrets (Windows, recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC | Step | Where | What to do |
# MAGIC |------|--------|------------|
# MAGIC | **1** | Azure Portal | Storage account → **Access keys** → copy **key1** |
# MAGIC | **2** | Repo `.env` | Add `STORAGE_ACCOUNT_KEY=...` and `DATABRICKS_TOKEN=...` |
# MAGIC | **3** | Command Prompt | `cd session-3` then `orchestrate.cmd --setup-secrets` |
# MAGIC | **4** | Databricks | Run cell A.4 below to verify `dbutils.secrets.get` works |
# MAGIC
# MAGIC **Get DATABRICKS_TOKEN:** Workspace → your name → **User settings** → **Developer** → **Access tokens** → **Generate new token**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Secret setup flow
# MAGIC
# MAGIC ```
# MAGIC Windows laptop (one time)
# MAGIC ─────────────────────────
# MAGIC 1. Paste key1 into d:\azure\.env
# MAGIC 2. cd session-3
# MAGIC 3. orchestrate.cmd --setup-secrets
# MAGIC    → creates scope finledger
# MAGIC    → stores storage-account + storage-key
# MAGIC
# MAGIC Databricks notebook (every lab)
# MAGIC ───────────────────────────────
# MAGIC dbutils.secrets.get("finledger", "storage-key")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### A.3 — CLI commands (trainer laptop)

# COMMAND ----------

# MAGIC %md
# MAGIC Run these in PowerShell after setting `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in `.env`.

# COMMAND ----------

# List scopes
# d:\azure\.venv\Scripts\databricks.exe secrets list-scopes

# List key names (not values) in finledger
# d:\azure\.venv\Scripts\databricks.exe secrets list --scope finledger

# Put/update a secret (trainer only)
# databricks secrets put --scope finledger --key storage-account --string-value stsharedqgr7mj

print("Trainer runs CLI on laptop — learners only verify in notebook")

# COMMAND ----------

# MAGIC %md
# MAGIC ### A.4 — Verify secrets in this notebook

# COMMAND ----------

# MAGIC %md
# MAGIC If this cell works, your scope is ready for all FinLedger labs.

# COMMAND ----------

SECRET_SCOPE = "finledger"

STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key")

# Prove key exists — Databricks redacts the value if you print it directly
print("Scope          :", SECRET_SCOPE)
print("Storage account:", STORAGE_ACCOUNT)
print("Key loaded     :", "yes" if len(_storage_key) > 10 else "NO — run orchestrate.cmd --setup-secrets")

# COMMAND ----------

# Optional: list bronze folder (needs spark.conf on classic cluster)
try:
    spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        _storage_key,
    )
    root = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    for item in dbutils.fs.ls(root):
        print(" bronze/", item.name)
except Exception as e:
    print("Storage list skipped (Serverless may need different auth):", str(e)[:80])

# COMMAND ----------

# MAGIC %md
# MAGIC ### A.5 — If `dbutils.secrets.put` fails in notebook
# MAGIC
# MAGIC Many workspaces **block** writing secrets from notebooks (`SecretsHandler has no attribute put`).
# MAGIC
# MAGIC **Fix:** always use **orchestrate.cmd --setup-secrets** on your Windows PC — not `dbutils.secrets.put` in a cell.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — File formats on the data lake

# COMMAND ----------

# MAGIC %md
# MAGIC #### Which format when?
# MAGIC
# MAGIC ```
# MAGIC Need human-readable ingest?     ──► CSV  (bronze)
# MAGIC  Need API / nested data?         ──► JSON (bronze)
# MAGIC  Need fast columnar analytics?   ──► Parquet
# MAGIC  Need ACID lakehouse tables?     ──► Delta (silver/gold)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### FinLedger medallion → format
# MAGIC
# MAGIC ```
# MAGIC Sources ──► CSV bronze ──► Delta silver ──► Delta gold
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Quick comparison table
# MAGIC
# MAGIC ```
# MAGIC Format      | Best for              | FinLedger layer
# MAGIC ------------|----------------------|------------------
# MAGIC CSV         | raw ingest, humans   | bronze
# MAGIC JSON        | APIs, semi-structured| bronze (optional)
# MAGIC Parquet     | fast columnar files  | intermediate
# MAGIC Delta       | ACID + time travel   | silver, gold
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### B.1 — CSV (Comma-Separated Values)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC **CSV** = text file, one row per line, columns separated by commas. First row often has column names.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Sources (banks, ERP exports) still deliver CSV. Easy for humans to open in Excel.

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Bronze layer lands CSV unchanged. Spark: `spark.read.option('header', True).csv(path)`.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Paper ledger:** simple, universal, not great for huge scale or strict types.

# COMMAND ----------

bronze_csv = (
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/loaded/run=session3-lab/sample_transactions.csv"
)
df_csv = spark.read.option("header", True).option("inferSchema", True).csv(bronze_csv)
df_csv.printSchema()
df_csv.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### B.2 — JSON (JavaScript Object Notation)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC **JSON** = nested text format `{ "key": "value" }`. Common for REST APIs and event payloads.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Some feeds arrive as JSON lines (one JSON object per row).

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Spark: `spark.read.json(path)` infers schema from structure. Good for semi-structured bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Post-it stack:** flexible shape, harder to query than flat tables without flattening.

# COMMAND ----------

# Demo JSON in memory (real lake path would be abfss://bronze@.../events.json)
df_json = spark.read.json(
    spark.sparkContext.parallelize([
        '{"transaction_id":"TXN-1","amount_gbp":"100.00","channel":"wire"}',
    ])
)
df_json.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### B.3 — Parquet (columnar files)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC **Parquet** = binary columnar format. Stores data by column (not row), compressed — fast for analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC Much smaller and faster than CSV for large scans. No transaction log — just files in a folder.

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Spark: `spark.read.parquet(path)` and `.write.parquet(path)`. Used inside Delta tables as the data files.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Filing cabinet by column:** pull only the columns you need — less IO.

# COMMAND ----------

# Parquet write/read demo (temp folder on cluster — production silver uses Delta)
tmp = "dbfs:/tmp/finledger_parquet_demo"
df_csv.select("transaction_id", "channel", "amount_gbp").write.mode("overwrite").parquet(tmp)
df_parquet = spark.read.parquet(tmp)
print("Parquet rows:", df_parquet.count())
df_parquet.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### B.4 — Delta Lake (Parquet + transaction log)

# COMMAND ----------

# MAGIC %md
# MAGIC **WHAT**
# MAGIC
# MAGIC **Delta** = Parquet data files + `_delta_log/` folder. Adds **ACID**, schema enforcement, **time travel**.

# COMMAND ----------

# MAGIC %md
# MAGIC **WHY**
# MAGIC
# MAGIC FinLedger **silver** and **gold** use Delta — not plain CSV or loose Parquet folders.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐       ┌─────────────────────┐
# MAGIC │  Parquet / CSV      │       │     Delta Lake      │
# MAGIC ├─────────────────────┤       ├─────────────────────┤
# MAGIC │ files only          │       │ Parquet files       │
# MAGIC │ no transaction log  │       │ + _delta_log        │
# MAGIC │ schema drift risk   │       │ ACID + time travel  │
# MAGIC └─────────────────────┘       └─────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **HOW**
# MAGIC
# MAGIC Write: `.format('delta').save(path)`  Read: `spark.read.format('delta').load(path)`.

# COMMAND ----------

# MAGIC %md
# MAGIC **ANALOGY**
# MAGIC
# MAGIC **Parquet + bank ledger:** every change is a numbered version you can audit or restore.

# COMMAND ----------

silver_path = (
    f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/transactions/run=session3-lab"
)
# Read Delta if Session 9 already wrote it; else show message
try:
    df_delta = spark.read.format("delta").load(silver_path)
    print("Delta silver rows:", df_delta.count())
    df_delta.show(3)
except Exception:
    print("Silver Delta not written yet — run Session 9 notebook first")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inside a Delta table folder
# MAGIC
# MAGIC ```
# MAGIC my_table/
# MAGIC  ├── part-00001.parquet  ◄── data
# MAGIC  ├── part-00002.parquet
# MAGIC  └── _delta_log/
# MAGIC      ├── 000...000.json   ◄── version 0
# MAGIC      └── 000...001.json   ◄── version 1
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### B.5 — Other formats (awareness only)
# MAGIC
# MAGIC | Format | Typical use |
# MAGIC |--------|-------------|
# MAGIC | **Avro** | Kafka schemas, row-based with schema embedded |
# MAGIC | **ORC** | Hive legacy, columnar (less common in new Azure lakes) |
# MAGIC | **XML** | Old enterprise feeds — parse then convert to Delta |
# MAGIC
# MAGIC **FinLedger rule:** bronze = **CSV** (today) → silver/gold = **Delta**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B.6 — Side-by-side row count

# COMMAND ----------

# MAGIC %md
# MAGIC Same logical data — different formats, different layers.

# COMMAND ----------

print("CSV bronze rows :", df_csv.count())
print("Format lesson complete — use CSV ingest, Delta for curated tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist

# COMMAND ----------

# MAGIC %md
# MAGIC **Secrets**
# MAGIC - [ ] Ran `orchestrate.cmd --setup-secrets` on Windows
# MAGIC - [ ] Cell A.4 shows storage account name
# MAGIC - [ ] Never committed keys to git
# MAGIC
# MAGIC **Formats**
# MAGIC - [ ] Can explain CSV vs Parquet vs Delta in one sentence each
# MAGIC - [ ] Know bronze = CSV, silver/gold = Delta
# MAGIC - [ ] **Next:** Session 9 notebook — write silver Delta + quarantine

# COMMAND ----------
