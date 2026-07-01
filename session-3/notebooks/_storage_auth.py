# Databricks notebook source
# MAGIC %md
# MAGIC # _storage_auth — shared ADLS auth (imported via `%run`)
# MAGIC
# MAGIC **One-time setup:** run **`nb_00_setup_credentials`** once — saves account + key to Databricks secrets.
# MAGIC
# MAGIC After that, every notebook uses **`auth_mode=auto`** (default): probes storage, loads secrets if needed — **no key to paste again**.
# MAGIC
# MAGIC | `auth_mode` | When |
# MAGIC |---|---|
# MAGIC | **`auto`** (default) | Probe first; use saved secrets if needed |
# MAGIC | `none` | Single-user cluster only |
# MAGIC | `access_connector` | Unity Catalog external locations (nb_00) |

# COMMAND ----------

FINLEDGER_SECRET_SCOPE = "finledger"
FINLEDGER_KEY_SECRET = "storage-key"
FINLEDGER_ACCOUNT_SECRET = "storage-account"


def _clear_stale_storage_confs(account: str) -> None:
    stale = [
        "fs.azure.account.key",
        "spark.hadoop.fs.azure.account.key",
        f"fs.azure.account.key.{account}",
        f"spark.hadoop.fs.azure.account.key.{account}",
    ]
    for key in stale:
        try:
            spark.conf.unset(key)
        except Exception:
            pass


def finledger_abfss(storage_account: str, container: str, path: str = "") -> str:
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    if path:
        return f"{base}/{path.lstrip('/')}"
    return base


def _probe_bronze(account: str) -> bool:
    """Return True if we can list the bronze container (auth already works)."""
    try:
        dbutils.fs.ls(finledger_abfss(account, "bronze") + "/")
        return True
    except Exception:
        return False


def finledger_resolve_storage_account(widget_value: str = "") -> str:
    """Widget first, then secret saved by nb_00_setup_credentials."""
    account = (widget_value or "").strip()
    if account:
        return account
    try:
        account = dbutils.secrets.get(
            scope=FINLEDGER_SECRET_SCOPE, key=FINLEDGER_ACCOUNT_SECRET
        ).strip()
        print(f"Using storage account from secret {FINLEDGER_SECRET_SCOPE}/{FINLEDGER_ACCOUNT_SECRET}")
        return account
    except Exception:
        raise ValueError(
            "Set storage_account widget OR run nb_00_setup_credentials once "
            f"(saves {FINLEDGER_ACCOUNT_SECRET} secret)"
        )


def _load_storage_key_from_secret() -> str:
    return dbutils.secrets.get(
        scope=FINLEDGER_SECRET_SCOPE, key=FINLEDGER_KEY_SECRET
    ).strip()


def configure_storage_account_key(account: str, key: str) -> None:
    if not account:
        raise ValueError("storage_account is required")
    if not key:
        raise ValueError("storage account key is empty")

    key = key.strip()
    _clear_stale_storage_confs(account)

    for conf_key in (
        f"fs.azure.account.key.{account}.dfs.core.windows.net",
        f"fs.azure.account.key.{account}.blob.core.windows.net",
    ):
        spark.conf.set(conf_key, key)


def finledger_configure_storage(
    storage_account: str = "",
    auth_mode: str = "auto",
    storage_account_key: str = "",
) -> str:
    """
    Idempotent ADLS auth. Returns resolved storage account name.
    Safe to call at the top of every notebook — skips work if already connected.
    """
    account = finledger_resolve_storage_account(storage_account)
    mode = (auth_mode or "auto").strip().lower()

    if mode == "none":
        print(f"auth_mode=none — cluster identity for {account}")
        return account

    if mode == "access_connector":
        print("auth_mode=access_connector — assuming Unity Catalog external locations exist")
        return account

    # auto / storage_key — skip if bronze is already reachable on this cluster
    if _probe_bronze(account):
        print(f"Storage already accessible for {account} — no re-auth needed")
        return account

    # Need key: secret (saved once) or one-off widget override
    if storage_account_key and storage_account_key.strip():
        key = storage_account_key.strip()
        print("Using storage_account_key widget (override — prefer nb_00_setup_credentials)")
    else:
        try:
            key = _load_storage_key_from_secret()
            print(f"Loaded key from secret {FINLEDGER_SECRET_SCOPE}/{FINLEDGER_KEY_SECRET}")
        except Exception as exc:
            raise ValueError(
                f"Cannot access ADLS for {account}. Run **nb_00_setup_credentials** once, "
                f"or use Single-user cluster with auth_mode=none. ({exc})"
            ) from exc

    configure_storage_account_key(account, key)

    if not _probe_bronze(account):
        raise RuntimeError(
            f"Storage key configured but bronze probe still failed for {account}. "
            "Try Single-user cluster (auth_mode=none) or rotate key in nb_00_setup_credentials."
        )

    print(f"Storage auth OK for {account}")
    return account
