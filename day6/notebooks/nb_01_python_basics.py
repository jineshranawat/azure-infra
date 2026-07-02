# Databricks notebook source
# MAGIC %md
# MAGIC # Day 6 — Python for data engineers
# MAGIC
# MAGIC **Prerequisite:** `session-3\orchestrate.cmd --setup-secrets` once (scope `finledger`).
# MAGIC
# MAGIC Cell 1 loads **`storage-account`** and **`storage-key`** from scope `finledger` (same as Session 3).

# COMMAND ----------

SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT_SECRET = "storage-account"
STORAGE_KEY_SECRET = "storage-key"
CATALOG = "finledger"

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

# MAGIC %md
# MAGIC ## 1. Variables, lists, dicts

# COMMAND ----------

channels = ["wire", "card", "fps", "wire"]
unique_channels = sorted(set(channels))
print(f"Channels seen: {unique_channels}")

txn = {
    "transaction_id": "TXN-10003",
    "amount_gbp": "50000.00",
    "channel": "wire",
    "status": "pending",
}
print(f"Fraud watch: {txn['transaction_id']} £{txn['amount_gbp']} ({txn['status']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Functions — `clean_amount`

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("finledger")


def clean_amount(value):
    """Return a float, or 0.0 if the cell is blank/garbage."""
    if value is None:
        log.warning("bad amount: %r", value)
        return 0.0
    try:
        amount = float(value)
    except (TypeError, ValueError):
        log.warning("bad amount: %r", value)
        return 0.0
    if amount < 0:
        log.warning("negative amount rejected: %s", amount)
        return 0.0
    return amount


print(clean_amount("50000"), clean_amount("oops"), clean_amount(None))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `dataclass` — run configuration

# COMMAND ----------

from dataclasses import dataclass


@dataclass(frozen=True)
class RunConfig:
    run_date: str
    storage_account: str

    def bronze_csv_abfss(self) -> str:
        return (
            f"abfss://bronze@{self.storage_account}.dfs.core.windows.net/"
            f"loaded/run={self.run_date}/sample_transactions.csv"
        )


dbutils.widgets.text("run_date", "session3-lab", "Run folder id")

cfg = RunConfig(
    run_date=dbutils.widgets.get("run_date").strip(),
    storage_account=STORAGE_ACCOUNT,
)
print(cfg)
print(f"Bronze path: {cfg.bronze_csv_abfss()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `pathlib` — build paths without string mistakes

# COMMAND ----------

from pathlib import Path

run_folder = Path("loaded") / f"run={cfg.run_date}"
csv_name = run_folder / "sample_transactions.csv"
print(f"Relative lake path: {csv_name}")
print(f"Parts: {list(csv_name.parts)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Apply `clean_amount` to a list of dicts (preview of silver logic)

# COMMAND ----------

sample_rows = [
    {"transaction_id": "TXN-20001", "amount_gbp": "1250.50", "channel": "wire"},
    {"transaction_id": "TXN-20003", "amount_gbp": "INVALID", "channel": "wire"},
]

for row in sample_rows:
    cleaned = clean_amount(row["amount_gbp"])
    flag = "QUARANTINE" if cleaned == 0.0 and row["amount_gbp"] not in (None, "", "0") else "OK"
    print(f"{row['transaction_id']}: {row['amount_gbp']!r} -> {cleaned} [{flag}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Peek at Session 3 Unity Catalog bronze (if nb_01 ran)

# COMMAND ----------

BRONZE_TABLE = f"{CATALOG}.bronze.sample_transactions"

try:
  row_count = spark.table(BRONZE_TABLE).count()
  print(f"Unity Catalog {BRONZE_TABLE}: {row_count} rows (from Session 3 nb_01)")
except Exception as exc:
  print(f"{BRONZE_TABLE} not registered yet — run session-3 nb_01 first ({exc})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint
# MAGIC
# MAGIC - [ ] `orchestrate.cmd --setup-secrets` ran (scope `finledger`)
# MAGIC - [ ] You can explain why `clean_amount` returns `0.0` for `INVALID`
# MAGIC - [ ] `RunConfig` prints your `abfss://` bronze path
# MAGIC - [ ] Next notebook — read the same CSV with pandas vs Spark
