# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — One-time credential setup (fallback)
# MAGIC
# MAGIC **Preferred:** run on your Windows PC:
# MAGIC
# MAGIC ```text
# MAGIC cd session-3
# MAGIC orchestrate.cmd --setup-secrets
# MAGIC ```
# MAGIC
# MAGIC Use this notebook only if `dbutils.secrets.put` works in your workspace.

# COMMAND ----------

SCOPE = "finledger"
ACCOUNT_KEY = "storage-account"
SECRET_KEY = "storage-key"

dbutils.widgets.text("storage_account", "", "Storage account name")
dbutils.widgets.text(
    "storage_account_key",
    "",
    "Paste key1 only if CLI setup failed",
)

storage_account = dbutils.widgets.get("storage_account").strip()
storage_account_key = dbutils.widgets.get("storage_account_key").strip()

# COMMAND ----------

if not hasattr(dbutils.secrets, "put"):
    raise RuntimeError(
        "dbutils.secrets.put is not available. "
        "Run orchestrate.cmd --setup-secrets on your Windows PC instead."
    )

if not storage_account:
    raise ValueError("Set storage_account widget")

if not storage_account_key:
    raise ValueError("Paste storage key OR use orchestrate.cmd --setup-secrets")

try:
    dbutils.secrets.put(scope=SCOPE, key=ACCOUNT_KEY, string_value=storage_account)
    dbutils.secrets.put(scope=SCOPE, key=SECRET_KEY, string_value=storage_account_key)
    print(f"Saved {SCOPE}/{ACCOUNT_KEY} and {SECRET_KEY}")
except Exception as exc:
    raise RuntimeError(
        f"Could not write secrets. Run orchestrate.cmd --setup-secrets on your PC.\n{exc}"
    ) from exc

# COMMAND ----------

STORAGE_ACCOUNT = dbutils.secrets.get(scope=SCOPE, key=ACCOUNT_KEY).strip()
_storage_key = dbutils.secrets.get(scope=SCOPE, key=SECRET_KEY).strip()
for _host in (
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net",
    f"{STORAGE_ACCOUNT}.blob.core.windows.net",
):
    spark.conf.set(f"fs.azure.account.key.{_host}", _storage_key)

bronze_root = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
for item in dbutils.fs.ls(bronze_root):
    print(f"  {item.name}")
print("SETUP COMPLETE")
