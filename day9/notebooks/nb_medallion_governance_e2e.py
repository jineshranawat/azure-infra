# Databricks notebook source

# MAGIC %md
# MAGIC # Medallion + SQL metadata + Purview — end-to-end (Databricks)
# MAGIC
# MAGIC **Goal:** One notebook runs the full **bronze → silver → gold** medallion pipeline, exports a **SQL metadata registry** for ADF/Azure SQL, and writes a **Purview governance manifest** covering all teaching assets.
# MAGIC
# MAGIC **Deploy:** `release-medallion-governance.cmd` or `deploy-shared-lab.cmd`
# MAGIC
# MAGIC **ADF alignment:** `shared-adf-lab` folder `13-governance-purview` (`pl_gov_06`)

# COMMAND ----------

# =============================================================================
# CELL 0 — Bronze ingest (shared FinLedger estate)
# =============================================================================
SECRET_SCOPE = "finledger"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key").strip()
# Define widgets first — safe for Run in Studio AND ADF/job base_parameters (ADF overrides values)
dbutils.widgets.text("run_id", "session3-lab", "Pipeline run id")
RUN_ID = dbutils.widgets.get("run_id").strip() or "session3-lab"


def _read_bronze_transactions(spark, account: str, key: str, run_id: str):
    path = (
        f"abfss://bronze@{account}.dfs.core.windows.net"
        f"/loaded/run={run_id}/sample_transactions.csv"
    )
    try:
        spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "storage_key_spark_conf"
    except Exception:
        pass
    try:
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "azure_rbac_passthrough"
    except Exception:
        pass
    from io import StringIO
    import pandas as pd
    from azure.storage.filedatalake import DataLakeServiceClient

    rel = f"loaded/run={run_id}/sample_transactions.csv"
    client = DataLakeServiceClient(
        account_url=f"https://{account}.dfs.core.windows.net", credential=key
    )
    raw = (
        client.get_file_system_client("bronze")
        .get_file_client(rel)
        .download_file()
        .readall()
        .decode("utf-8")
    )
    return spark.createDataFrame(pd.read_csv(StringIO(raw))), path, "driver_sdk"


bronze, BRONZE_PATH, BRONZE_AUTH_MODE = _read_bronze_transactions(
    spark, STORAGE_ACCOUNT, _storage_key, RUN_ID
)
print("Bronze:", BRONZE_PATH, "| rows:", bronze.count(), "| auth:", BRONZE_AUTH_MODE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance helpers + medallion pipeline

# COMMAND ----------

# Inline helpers (self-contained notebook — no Workspace file dependency)
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

GOVERNANCE_AUDIT_BASE = f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net/governance"
MANIFEST_PATH = f"{GOVERNANCE_AUDIT_BASE}/databricks_governance_manifest.json"
SQL_EXPORT_PATH = f"{GOVERNANCE_AUDIT_BASE}/de_table_registry.csv"
LINEAGE_EDGES: list[dict[str, str]] = []


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def register_lineage_edge(upstream: str, downstream: str, pipeline: str) -> None:
    LINEAGE_EDGES.append(
        {"upstream": upstream, "downstream": downstream, "pipeline": pipeline, "recorded_utc": _now_utc()}
    )


def medallion_paths(run_id: str) -> dict[str, str]:
    return {
        "bronze_loaded": (
            f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/loaded/run={run_id}/sample_transactions.csv"
        ),
        "silver_transactions": (
            f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/transactions/run={run_id}"
        ),
        "gold_summary": f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/daily_channel_summary",
        "quarantine": f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net/quarantine/run={run_id}",
    }


def run_medallion_pipeline(run_id: str) -> dict:
    paths = medallion_paths(run_id)
    raw = spark.read.option("header", True).option("inferSchema", True).csv(paths["bronze_loaded"])
    typed = raw.withColumn("amount", F.col("amount_gbp").cast("double"))
    good = typed.filter(F.col("amount").isNotNull() & (F.col("amount") > 0)).dropDuplicates(["transaction_id"])
    bad = typed.filter(F.col("amount").isNull() | (F.col("amount") <= 0))
    good.write.format("delta").mode("overwrite").save(paths["silver_transactions"])
    if bad.count() > 0:
        bad.write.format("delta").mode("overwrite").save(paths["quarantine"])
    gold = good.groupBy("channel").agg(F.count("*").alias("txns"), F.round(F.sum("amount"), 2).alias("total_gbp"))
    gold.write.format("delta").mode("overwrite").save(paths["gold_summary"])
    register_lineage_edge("bronze_loaded", "silver_transactions", "nb_medallion_governance_e2e")
    register_lineage_edge("silver_transactions", "gold_summary", "nb_medallion_governance_e2e")
    return {
        "run_id": run_id,
        "bronze_rows": raw.count(),
        "silver_rows": good.count(),
        "quarantine_rows": bad.count(),
        "gold_channels": gold.count(),
        "paths": paths,
        "completed_utc": _now_utc(),
    }


def write_purview_manifest(run_id: str, metrics: dict) -> str:
    paths = medallion_paths(run_id)
    body = {
        "catalog": "finledger-governance",
        "run_id": run_id,
        "generated_utc": _now_utc(),
        "purview_concepts": ["data_map", "data_assets", "lineage", "glossary", "classifications", "ownership", "scan"],
        "data_assets": [
            {"qualified_name": paths["bronze_loaded"], "layer": "bronze", "glossary_term": "Medallion.Bronze"},
            {"qualified_name": paths["silver_transactions"], "layer": "silver", "glossary_term": "Medallion.Silver"},
            {"qualified_name": paths["gold_summary"], "layer": "gold", "glossary_term": "Medallion.Gold"},
        ],
        "lineage_edges": LINEAGE_EDGES,
        "medallion_metrics": metrics,
        "notebook": "medallion_governance_e2e",
    }
    dbutils.fs.mkdirs(GOVERNANCE_AUDIT_BASE)
    dbutils.fs.put(MANIFEST_PATH, json.dumps(body, indent=2), overwrite=True)
    return MANIFEST_PATH


def export_sql_metadata_model(run_id: str, metrics: dict) -> str:
    paths = medallion_paths(run_id)
    rows = [
        (run_id, "medallion", paths["bronze_loaded"], "bronze", "FinLedger.Internal", "Medallion.Bronze", "READY", _now_utc()),
        (run_id, "medallion", paths["silver_transactions"], "silver", "FinLedger.Internal", "Medallion.Silver", "READY", _now_utc()),
        (run_id, "medallion", paths["gold_summary"], "gold", "FinLedger.Internal", "Medallion.Gold", "READY", _now_utc()),
    ]
    schema = StructType(
        [
            StructField("run_id", StringType()),
            StructField("problem_num", StringType()),
            StructField("qualified_name", StringType()),
            StructField("medallion_layer", StringType()),
            StructField("classification", StringType()),
            StructField("glossary_term", StringType()),
            StructField("status", StringType()),
            StructField("updated_utc", StringType()),
        ]
    )
    export_df = spark.createDataFrame(rows, schema)
    csv_lines = ["run_id,problem_num,qualified_name,medallion_layer,classification,glossary_term,status,updated_utc"]
    csv_lines.extend(",".join(f'"{c}"' for c in r) for r in rows)
    dbutils.fs.put(SQL_EXPORT_PATH, "\n".join(csv_lines), overwrite=True)
    export_df.write.format("delta").mode("append").option("mergeSchema", "true").save(
        f"{GOVERNANCE_AUDIT_BASE}/de_table_registry_delta"
    )
    run_log = spark.createDataFrame(
        [(run_id, metrics["bronze_rows"], metrics["silver_rows"], metrics["gold_channels"], metrics["completed_utc"], "Succeeded")],
        ["run_id", "bronze_rows", "silver_rows", "gold_channels", "completed_utc", "status"],
    )
    run_log.write.format("delta").mode("append").save(f"{GOVERNANCE_AUDIT_BASE}/de_pipeline_run_log")
    return SQL_EXPORT_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run medallion + publish governance artifacts

# COMMAND ----------

metrics = run_medallion_pipeline(RUN_ID)
manifest = write_purview_manifest(RUN_ID, metrics)
sql_export = export_sql_metadata_model(RUN_ID, metrics)

summary = {
    "run_id": RUN_ID,
    "medallion": metrics,
    "purview_manifest": manifest,
    "sql_export": sql_export,
    "lineage_edges": len(LINEAGE_EDGES),
}
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
