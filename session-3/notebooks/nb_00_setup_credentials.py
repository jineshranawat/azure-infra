# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — One-time credential setup
# MAGIC
# MAGIC **Preferred (Windows):** secrets are saved from your laptop — notebook `put` is blocked on many workspaces.
# MAGIC
# MAGIC ```text
# MAGIC cd session-3
# MAGIC orchestrate.cmd --setup-secrets
# MAGIC ```
# MAGIC
# MAGIC Add to repo-root `.env` first (never commit):
# MAGIC - `STORAGE_ACCOUNT_KEY` = Portal → Storage → Access keys → key1
# MAGIC - `DATABRICKS_TOKEN` = Databricks → User settings → Developer → Access token
# MAGIC - `DATABRICKS_HOST` = optional (orchestrate auto-detects)
# MAGIC
# MAGIC See **SECRET-SCOPE-SETUP.md** in the repo.

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "Storage account (e.g. stjineshfqdcgg)")
dbutils.widgets.text(
    "storage_account_key",
    "",
    "Only if CLI setup failed — paste key1 once",
)

storage_account = dbutils.widgets.get("storage_account").strip()
storage_account_key = dbutils.widgets.get("storage_account_key").strip()

if not storage_account:
    raise ValueError("Set storage_account widget")

SCOPE = "finledger"
ACCOUNT_KEY = "storage-account"
SECRET_KEY = "storage-key"

# COMMAND ----------

# dbutils.secrets.put is disabled on many Shared / serverless runtimes
if not hasattr(dbutils.secrets, "put"):
    raise RuntimeError(
        "dbutils.secrets.put is not available in this workspace. "
        "Use orchestrate.cmd --setup-secrets on your Windows PC instead (see notebook header)."
    )

if not storage_account_key:
    raise ValueError(
        "Paste storage key OR use orchestrate.cmd --setup-secrets on your laptop"
    )

try:
    dbutils.secrets.put(scope=SCOPE, key=ACCOUNT_KEY, string_value=storage_account)
    dbutils.secrets.put(scope=SCOPE, key=SECRET_KEY, string_value=storage_account_key)
    print(f"Saved {SCOPE}/{ACCOUNT_KEY} and {SECRET_KEY}")
except Exception as exc:
    raise RuntimeError(
        f"Could not write secrets. Run on your PC: session-3\\orchestrate.cmd --setup-secrets\n{exc}"
    ) from exc

# COMMAND ----------

# MAGIC %run ./_storage_auth

# COMMAND ----------

account = finledger_configure_storage(auth_mode="auto")
bronze_root = finledger_abfss(account, "bronze") + "/"
for item in dbutils.fs.ls(bronze_root):
    print(f"  {item.name}")
print("SETUP COMPLETE")
