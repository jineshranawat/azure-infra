"""Medallion + SQL metadata + Purview helpers for Day 9 / 50-problems notebooks."""

GOVERNANCE_HELPERS = '''# =============================================================================
# GOVERNANCE HELPERS — medallion, SQL metadata export, Purview manifest
# =============================================================================
# WHAT: Shared functions for lineage, catalog comments, Purview asset registry.
# WHY:  Problems 32/33/48/50 + capstone reuse one implementation (DRY).
# HOW:  Writes audit/governance/* on ADLS; optional UC COMMENT/TAGS when allowed.
# =============================================================================
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

GOVERNANCE_AUDIT_BASE = (
    f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net/governance"
)
MANIFEST_PATH = f"{GOVERNANCE_AUDIT_BASE}/databricks_governance_manifest.json"
SQL_EXPORT_PATH = f"{GOVERNANCE_AUDIT_BASE}/de_table_registry.csv"
LINEAGE_LOG_PATH = f"{GOVERNANCE_AUDIT_BASE}/lineage_edges.json"

PURVIEW_CONCEPTS = [
    "data_map",
    "data_assets",
    "lineage",
    "glossary",
    "classifications",
    "ownership",
    "scan",
]

MEDALLION_GLOSSARY = {
    "FinLedger.Transaction": "Retail payment transaction ingested from partner files.",
    "Medallion.Bronze": "Raw immutable landing layer — CSV in ADLS bronze.",
    "Medallion.Silver": "Cleansed conforming layer — typed Delta, quarantine split.",
    "Medallion.Gold": "Business aggregates for reporting and regulatory KPIs.",
}

CLASSIFICATIONS = {
    "FinLedger.Internal": "Non-PII operational payment data.",
    "MICROSOFT.PERSONAL.NAME": "customer_name if present in extended feeds.",
}

LINEAGE_EDGES: list[dict[str, str]] = []


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def medallion_paths(run_id: str) -> dict[str, str]:
    """Canonical ADLS paths — align with Session 13 + shared-adf-lab governance."""
    return {
        "bronze_loaded": (
            f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
            f"/loaded/run={run_id}/sample_transactions.csv"
        ),
        "silver_transactions": (
            f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
            f"/transactions/run={run_id}"
        ),
        "silver_cleaned_csv": (
            f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
            f"/cleaned/run={run_id}/cleaned_transactions.csv"
        ),
        "gold_aggregates": (
            f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
            f"/aggregates/run={run_id}/channel_totals.csv"
        ),
        "gold_summary": (
            f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"
            f"/daily_channel_summary"
        ),
        "quarantine": (
            f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net"
            f"/quarantine/run={run_id}"
        ),
    }


def run_medallion_pipeline(run_id: str) -> dict:
    """Bronze CSV → silver Delta → gold aggregates. Returns metrics dict."""
    paths = medallion_paths(run_id)
    raw = spark.read.option("header", True).option("inferSchema", True).csv(paths["bronze_loaded"])
    typed = raw.withColumn("amount", F.col("amount_gbp").cast("double"))
    good = typed.filter(F.col("amount").isNotNull() & (F.col("amount") > 0)).dropDuplicates(
        ["transaction_id"]
    )
    bad = typed.filter(F.col("amount").isNull() | (F.col("amount") <= 0))

    good.write.format("delta").mode("overwrite").save(paths["silver_transactions"])
    if bad.count() > 0:
        bad.write.format("delta").mode("overwrite").save(paths["quarantine"])

    gold = good.groupBy("channel").agg(
        F.count("*").alias("txns"),
        F.round(F.sum("amount"), 2).alias("total_gbp"),
    )
    gold.write.format("delta").mode("overwrite").save(paths["gold_summary"])
    gold.coalesce(1).write.mode("overwrite").option("header", True).csv(
        paths["gold_aggregates"].rsplit("/", 1)[0]
    )

    metrics = {
        "run_id": run_id,
        "bronze_rows": raw.count(),
        "silver_rows": good.count(),
        "quarantine_rows": bad.count(),
        "gold_channels": gold.count(),
        "paths": paths,
        "completed_utc": _now_utc(),
    }
    register_lineage_edge("bronze_loaded", "silver_transactions", "medallion_bronze_to_silver")
    register_lineage_edge("silver_transactions", "gold_summary", "medallion_silver_to_gold")
    return metrics


def register_lineage_edge(upstream: str, downstream: str, pipeline: str) -> None:
    """Accumulate lineage edges for Purview manifest (Problem 32)."""
    edge = {
        "upstream": upstream,
        "downstream": downstream,
        "pipeline": pipeline,
        "recorded_utc": _now_utc(),
    }
    LINEAGE_EDGES.append(edge)
    print(f"LINEAGE: {upstream} -> {downstream} ({pipeline})")


def apply_table_documentation(table_name: str, layer: str, problem_num: str) -> None:
    """UC COMMENT + TAGS when write enabled (Problems 33, 48)."""
    if not UC_WRITE_ENABLED:
        print(f"SKIP docs for {table_name} (UC write disabled)")
        return
    full = f"{UC_TABLE}.{table_name}"
    try:
        spark.sql(
            f"COMMENT ON TABLE {full} IS "
            f"'FinLedger demo — Problem {problem_num}, layer={layer}. Owner: data-platform.'"
        )
        spark.sql(
            f"ALTER TABLE {full} SET TAGS ("
            f"'domain' = 'payments', "
            f"'classification' = 'internal', "
            f"'medallion_layer' = '{layer}', "
            f"'problem_id' = '{problem_num}'"
            f")"
        )
        print(f"DOCUMENTED: {full}")
    except Exception as exc:
        print(f"DOC SKIP {full}:", str(exc)[:120])


def problem_asset_record(
    problem_num: str,
    problem_name: str,
    solution: str,
    table_ref: str,
    layer: str = "demo",
) -> dict:
    """One Purview-style data asset row for the 50-problems inventory."""
    return {
        "qualified_name": table_ref,
        "problem_num": problem_num,
        "problem": problem_name,
        "solution": solution,
        "layer": layer,
        "glossary_term": "FinLedger.Transaction",
        "classification": "FinLedger.Internal",
        "owner": "data-platform",
        "source": "databricks_50_problems",
    }


def build_problem_inventory() -> list[dict]:
    """Map every prob* table written this run into a governance inventory."""
    inventory: list[dict] = []
    if UC_WRITE_ENABLED:
        for row in spark.sql(f"SHOW TABLES IN {UC_TABLE}").collect():
            name = row.tableName
            if not name.startswith("prob"):
                continue
            num = name[4:6] if len(name) >= 6 and name[4:6].isdigit() else "00"
            inventory.append(
                problem_asset_record(
                    num,
                    f"prob_{name}",
                    "50_problems_lab",
                    f"{UC_TABLE}.{name}",
                    layer="demo",
                )
            )
    else:
        for item in dbutils.fs.ls(DEMO_BASE):
            name = item.name.rstrip("/")
            if not name.startswith("prob"):
                continue
            inventory.append(
                problem_asset_record(
                    "00",
                    name,
                    "50_problems_lab",
                    item.path,
                    layer="demo",
                )
            )
    return inventory


def write_purview_manifest(
    run_id: str,
    medallion_metrics: dict | None = None,
    extra_assets: list[dict] | None = None,
) -> str:
    """Write Purview-ready manifest JSON to audit container (all 50 problems + medallion)."""
    paths = medallion_paths(run_id)
    medallion_assets = [
        {
            "qualified_name": paths["bronze_loaded"],
            "layer": "bronze",
            "type": "azure_datalake_gen2_path",
            "glossary_term": "Medallion.Bronze",
        },
        {
            "qualified_name": paths["silver_transactions"],
            "layer": "silver",
            "type": "delta_lake_path",
            "glossary_term": "Medallion.Silver",
        },
        {
            "qualified_name": paths["gold_summary"],
            "layer": "gold",
            "type": "delta_lake_path",
            "glossary_term": "Medallion.Gold",
        },
    ]
    problem_assets = build_problem_inventory()
    body = {
        "catalog": "finledger-governance",
        "run_id": run_id,
        "generated_utc": _now_utc(),
        "purview_concepts": PURVIEW_CONCEPTS,
        "glossary_terms": [
            {"name": k, "definition": v} for k, v in MEDALLION_GLOSSARY.items()
        ],
        "classifications": [
            {"name": k, "applies_to": v} for k, v in CLASSIFICATIONS.items()
        ],
        "data_assets": medallion_assets + problem_assets + (extra_assets or []),
        "lineage_edges": LINEAGE_EDGES
        + [
            {"from_layer": "bronze", "to_layer": "silver", "pipeline": "databricks_medallion"},
            {"from_layer": "silver", "to_layer": "gold", "pipeline": "databricks_medallion"},
            {
                "from_layer": "demo",
                "to_layer": "silver",
                "pipeline": "50_problems_teaching_outputs",
            },
        ],
        "medallion_metrics": medallion_metrics or {},
        "adf_purview_link": "shared-adf-lab/pl_gov_06_master_medallion_governance",
    }
    dbutils.fs.mkdirs(GOVERNANCE_AUDIT_BASE)
    dbutils.fs.put(MANIFEST_PATH, json.dumps(body, indent=2), overwrite=True)
    dbutils.fs.put(LINEAGE_LOG_PATH, json.dumps(LINEAGE_EDGES, indent=2), overwrite=True)
    print("Purview manifest:", MANIFEST_PATH)
    print("  assets:", len(body["data_assets"]), "| lineage edges:", len(body["lineage_edges"]))
    return MANIFEST_PATH


def export_sql_metadata_model(run_id: str, medallion_metrics: dict | None = None) -> str:
    """Export de_table_registry CSV for Azure SQL metadata-driven modelling."""
    rows: list[tuple] = []
    for asset in build_problem_inventory():
        rows.append(
            (
                run_id,
                asset["problem_num"],
                asset["qualified_name"],
                asset["layer"],
                asset["classification"],
                asset["glossary_term"],
                "READY",
                _now_utc(),
            )
        )
    paths = medallion_paths(run_id)
    for layer, qn in (
        ("bronze", paths["bronze_loaded"]),
        ("silver", paths["silver_transactions"]),
        ("gold", paths["gold_summary"]),
    ):
        rows.append(
            (run_id, "medallion", qn, layer, "FinLedger.Internal", f"Medallion.{layer.title()}", "READY", _now_utc())
        )

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
    export_path = f"{GOVERNANCE_AUDIT_BASE}/de_table_registry/run={run_id}"
    export_df.coalesce(1).write.mode("overwrite").option("header", True).csv(export_path)

    # Single-file CSV for SQL bulk seed / ADF Copy
    csv_lines = [
        "run_id,problem_num,qualified_name,medallion_layer,classification,glossary_term,status,updated_utc"
    ]
    for r in rows:
        csv_lines.append(",".join(f'"{c}"' for c in r))
    csv_body = "\\n".join(csv_lines)
    dbutils.fs.put(SQL_EXPORT_PATH, csv_body, overwrite=True)

    registry_delta = f"{GOVERNANCE_AUDIT_BASE}/de_table_registry_delta"
    export_df.write.format("delta").mode("append").option("mergeSchema", "true").save(registry_delta)

    if medallion_metrics:
        run_log = spark.createDataFrame(
            [
                (
                    run_id,
                    medallion_metrics.get("bronze_rows", 0),
                    medallion_metrics.get("silver_rows", 0),
                    medallion_metrics.get("gold_channels", 0),
                    medallion_metrics.get("completed_utc", _now_utc()),
                    "Succeeded",
                )
            ],
            ["run_id", "bronze_rows", "silver_rows", "gold_channels", "completed_utc", "status"],
        )
        run_log.write.format("delta").mode("append").save(f"{GOVERNANCE_AUDIT_BASE}/de_pipeline_run_log")

    print("SQL metadata export:", SQL_EXPORT_PATH)
    print("  registry rows:", len(rows))
    return SQL_EXPORT_PATH


def run_governance_capstone(run_id: str) -> dict:
    """End-to-end: medallion + Purview manifest + SQL metadata export."""
    print("=" * 70)
    print("GOVERNANCE CAPSTONE — medallion + SQL metadata + Purview")
    print("=" * 70)
    metrics = run_medallion_pipeline(run_id)
    if UC_WRITE_ENABLED:
        for row in spark.sql(f"SHOW TABLES IN {UC_TABLE}").collect():
            if row.tableName.startswith("prob"):
                num = row.tableName[4:6] if row.tableName[4:6].isdigit() else "00"
                apply_table_documentation(row.tableName, "demo", num)
    manifest = write_purview_manifest(run_id, metrics)
    sql_export = export_sql_metadata_model(run_id, metrics)
    summary = {
        "run_id": run_id,
        "medallion": metrics,
        "purview_manifest": manifest,
        "sql_export": sql_export,
        "problem_tables": len(build_problem_inventory()),
        "lineage_edges": len(LINEAGE_EDGES),
    }
    print("CAPSTONE COMPLETE:", json.dumps({k: v for k, v in summary.items() if k != "medallion"}, indent=2))
    return summary


print("Governance helpers loaded — medallion_paths(), run_governance_capstone()")'''

GOVERNANCE_CAPSTONE_MD = """## Part K — Medallion + SQL metadata + Purview capstone

After all **50 problems**, this section ties every `prob*` output to:

1. **Medallion pipeline** — bronze → silver Delta → gold aggregates on ADLS  
2. **SQL metadata model** — `de_table_registry.csv` on `audit/governance/` for ADF/Azure SQL  
3. **Purview governance** — manifest JSON with glossary, classifications, lineage, and all 50 problem assets  

**ADF integration:** `shared-adf-lab` folder `13-governance-purview` (`pl_gov_06`) reads the same audit paths.

**CI/CD:** `release-medallion-governance.cmd` from repo root builds, deploys, and seeds SQL registry."""

GOVERNANCE_CAPSTONE_CODE = '''import json

capstone = run_governance_capstone(RUN_ID)
dbutils.notebook.exit(json.dumps(capstone, default=str))'''
