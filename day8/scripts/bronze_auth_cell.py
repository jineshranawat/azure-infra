"""Cell 0 source for Databricks notebooks — ADLS bronze read (Serverless-safe)."""

# Spark Connect / Serverless blocks spark.conf.set(fs.azure.account.key.*).
# This cell tries: (1) classic key config, (2) RBAC passthrough, (3) driver SDK fallback.

BRONZE_AUTH_CELL = '''# Cell 0 — Load FinLedger bronze (Serverless + classic cluster safe)
SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()
RUN_ID = "session3-lab"


def _read_bronze_transactions(spark, account: str, key: str, run_id: str):
    """Read bronze CSV; works when spark.conf account keys are blocked (Spark Connect)."""
    path = (
        f"abfss://bronze@{account}.dfs.core.windows.net"
        f"/loaded/run={run_id}/sample_transactions.csv"
    )

    # 1) Classic attached cluster — storage account key in Spark config
    try:
        spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "storage_key_spark_conf"
    except Exception:
        pass

    # 2) Azure RBAC / credential passthrough — no key in config
    try:
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "azure_rbac_passthrough"
    except Exception:
        pass

    # 3) Driver SDK fallback (training CSV is small) — Serverless / Spark Connect
    from io import StringIO
    import pandas as pd

    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        import subprocess
        import sys
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "azure-storage-file-datalake", "-q"]
        )
        from azure.storage.filedatalake import DataLakeServiceClient

    rel = f"loaded/run={run_id}/sample_transactions.csv"
    client = DataLakeServiceClient(
        account_url=f"https://{account}.dfs.core.windows.net",
        credential=key,
    )
    raw = (
        client.get_file_system_client("bronze")
        .get_file_client(rel)
        .download_file()
        .readall()
        .decode("utf-8")
    )
    pdf = pd.read_csv(StringIO(raw))
    return spark.createDataFrame(pdf), path, "driver_sdk"


bronze, BRONZE_PATH, BRONZE_AUTH_MODE = _read_bronze_transactions(
    spark, STORAGE_ACCOUNT, _storage_key, RUN_ID
)
df = bronze

print("Storage     :", STORAGE_ACCOUNT)
print("Bronze path :", BRONZE_PATH)
print("Auth mode   :", BRONZE_AUTH_MODE)
print("Bronze rows :", bronze.count())'''
