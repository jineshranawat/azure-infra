#!/usr/bin/env python3
"""Generate nb_finledger_event_processors.py — Zach Event lab, ABFSS-only on classic.

Same Cell 0 bronze auth as 50-problems. Does NOT inject demo_table_helpers
(those abfss mkdirs fail on Serverless). Lake writes are abfss:// only.

Run:  python day9/scripts/build_finledger_event_processors_notebook.py
Deploy + verify: python scripts/run_finledger_event_processors.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from bronze_auth_cell import BRONZE_AUTH_CELL
from notebook_helpers import code, finish, md

OUT = ROOT.parent / "notebooks" / "nb_finledger_event_processors.py"


def build() -> Path:
    parts: list[str] = [
        "# Databricks notebook source",
        "# MAGIC %md",
        "# MAGIC # FinLedger Event processors v4 (ABFSS + classic cluster)",
        "# MAGIC",
        "# MAGIC ### STOP — read this first",
        "# MAGIC",
        "# MAGIC 1. Top-right compute must be the shared **classic** cluster (NOT **Serverless**).",
        "# MAGIC 2. Serverless **cannot** write `abfss://` with the lab storage key — that is the red error you saw.",
        "# MAGIC 3. Click compute → detach Serverless → attach classic → **Run all** from the top.",
        "# MAGIC",
        "# MAGIC Same Cell 0 secrets as `/Shared/day9/50_data_engineering_problems`.",
        "# MAGIC Lake outputs: `abfss://silver|gold@…` only — **no DBFS**, no demo mkdirs on ADLS.",
        "# MAGIC",
        "# MAGIC | | |",
        "# MAGIC |:---|:---|",
        "# MAGIC | **Verify** | `python scripts/run_finledger_event_processors.py` |",
        "# MAGIC | **Transcript** | `day9/docs/finledger_event_processors_transcript.md` |",
        "",
        "# COMMAND ----------",
        "",
    ]

    md(
        parts,
        """## 0. Roadmap (Zach → FinLedger)

| Step | Concept | In this notebook |
|------|---------|------------------|
| 1 | Metrics first | cash by day, refunds vs payments |
| 2 | Data modeling | one `Event` schema |
| 3 | Processors | txn + returns |
| 4 | Quality | drop null/bad amounts |
| 5 | Distributed compute | PySpark + **abfss Delta** |
| 6 | Orchestration | widget `run_id` |
| 7 | Tools last | Delta / SQL pivot |

**Analogy:** Event = shipping label. Processors = factories. Runner = warehouse truck → CFO pivot.""",
    )

    md(
        parts,
        """## Cell 0 — Bronze (identical to 50-problems)

`finledger` secret scope. Next cells refuse Serverless for lake writes.""",
    )

    parts.extend(BRONZE_AUTH_CELL.strip().splitlines())
    parts.extend(["", "# COMMAND ----------", ""])

    code(
        parts,
        '''# Widget + light setup (NO abfss mkdirs — those fail on Serverless)
dbutils.widgets.text("run_id", "session3-lab", "Pipeline run id")
_w = dbutils.widgets.get("run_id").strip() or "session3-lab"
if _w != RUN_ID:
    print("Reloading bronze for widget run_id=", _w)
    RUN_ID = _w
    bronze, BRONZE_PATH, BRONZE_AUTH_MODE = _read_bronze_transactions(
        spark, STORAGE_ACCOUNT, _storage_key, RUN_ID
    )
    df = bronze

WRITTEN_TABLES = []
UC_WRITE_ENABLED = False
UC_TABLE = ""
CATALOG = "none"

# Optional UC / hive mirrors — never create catalogs on ADLS managed locations
try:
    names = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
except Exception:
    names = []
print("Catalogs visible:", names)

for catalog in ("main", "hive_metastore"):
    if catalog not in names and catalog != "hive_metastore":
        continue
    full_schema = f"{catalog}.demo_problems" if catalog != "hive_metastore" else "demo_problems"
    try:
        if catalog == "hive_metastore":
            spark.sql("CREATE DATABASE IF NOT EXISTS demo_problems")
            probe = "demo_problems._zach_uc_probe"
        else:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
            probe = f"{full_schema}._zach_uc_probe"
        spark.createDataFrame([("ok",)], ["ping"]).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(probe)
        spark.sql(f"DROP TABLE IF EXISTS {probe}")
        UC_WRITE_ENABLED = True
        CATALOG = catalog
        UC_TABLE = full_schema if catalog != "hive_metastore" else "demo_problems"
        print("UC/hive mirror ENABLED →", UC_TABLE)
        break
    except Exception as exc:
        print(f"  mirror skip {catalog}:", str(exc)[:120])

print("Active RUN_ID =", RUN_ID, "| Auth =", BRONZE_AUTH_MODE)
print("Lake mode     = ABFSS-only (classic cluster required)")''',
    )

    md(
        parts,
        """## Lake paths — ABFSS only

```text
abfss://silver@{account}/zach_events/run={run_id}/
abfss://gold@{account}/zach_daily_aggregates/run={run_id}/
abfss://gold@{account}/zach_pivoted/run={run_id}/
```

If the next cell raises **SERVERLESS / ABFSS blocked**: change compute to classic, then Run all.""",
    )

    code(
        parts,
        '''# =============================================================================
# ABFSS-only lake helpers (v4)
# =============================================================================
from io import StringIO

import pandas as pd
from pyspark.sql import functions as F

BRONZE_TXN_REL = f"loaded/run={RUN_ID}/sample_transactions.csv"
BRONZE_RETURNS_REL = "incoming/returns/returns_raw.csv"

SILVER_EVENTS_ABFSS = (
    f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/zach_events/run={RUN_ID}"
)
GOLD_DAILY_ABFSS = (
    f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/zach_daily_aggregates/run={RUN_ID}"
)
GOLD_PIVOT_ABFSS = (
    f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/zach_pivoted/run={RUN_ID}"
)

for _junk in (
    "dbfs:/FileStore/demo_50_problems/zach_events",
    "dbfs:/FileStore/demo_50_problems/zach_daily",
    "dbfs:/FileStore/demo_50_problems/zach_pivoted",
    f"dbfs:/FileStore/finledger_event_processors/{RUN_ID}",
):
    try:
        dbutils.fs.rm(_junk, True)
    except Exception:
        pass


def _abfss(container: str, rel: str) -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{rel}"


def _read_abfss_csv(container: str, rel: str):
    """Same order as 50-problems Cell 0: RBAC → key (unset on fail) → driver SDK."""
    path = _abfss(container, rel)

    try:
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "azure_rbac_passthrough"
    except Exception as exc1:
        print(f"  RBAC read failed ({container}/{rel}): {str(exc1)[:120]}")

    try:
        spark.conf.set(
            f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
            _storage_key,
        )
        frame = spark.read.option("header", True).option("inferSchema", True).csv(path)
        frame.limit(1).count()
        return frame, path, "storage_key_spark_conf"
    except Exception as exc2:
        print(f"  Account-key read failed: {str(exc2)[:120]}")
        _unset_account_key(STORAGE_ACCOUNT)

    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        import subprocess
        import sys

        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "azure-storage-file-datalake", "-q"]
        )
        from azure.storage.filedatalake import DataLakeServiceClient

    client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=_storage_key,
    )
    raw = (
        client.get_file_system_client(container)
        .get_file_client(rel)
        .download_file()
        .readall()
        .decode("utf-8")
    )
    frame = spark.createDataFrame(pd.read_csv(StringIO(raw)))
    return frame, path, "driver_sdk"


def _try_set_account_key_for_writes() -> bool:
    """Enable ABFSS Delta writes (RBAC or classic storage key)."""
    probe = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_zach_write_probe"
    try:
        spark.createDataFrame([("ok",)], ["ping"]).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(probe)
        print("ABFSS write probe: OK (RBAC or existing conf)")
        return True
    except Exception as exc1:
        print("ABFSS write probe without key:", str(exc1)[:120])
    try:
        spark.conf.set(
            f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
            _storage_key,
        )
        spark.createDataFrame([("ok",)], ["ping"]).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(probe)
        print("ABFSS write probe: OK (storage_key)")
        return True
    except Exception as exc2:
        print("ABFSS write probe with key failed:", str(exc2)[:160])
        _unset_account_key(STORAGE_ACCOUNT)
        return False


ABFSS_WRITE_OK = _try_set_account_key_for_writes()
if not ABFSS_WRITE_OK:
    raise RuntimeError(
        "SERVERLESS / ABFSS blocked. "
        "You are on compute that cannot write abfss:// to the FinLedger lake. "
        "FIX: Top-right → detach Serverless → attach classic cluster "
        "0703-105931-31juyffm → Run all from Cell 0. "
        "This lab does not use DBFS."
    )


def write_lab_delta(df, abfss_path: str) -> str:
    """Write Delta to canonical ABFSS path only."""
    if not str(abfss_path).startswith("abfss://"):
        raise ValueError(f"Refusing non-ABFSS path: {abfss_path}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(abfss_path)
    print("Wrote ABFSS:", abfss_path)
    WRITTEN_TABLES.append(abfss_path)
    return abfss_path


def write_uc_mirror(df, table_name: str):
    """Optional UC/hive table when permitted — never DBFS."""
    if not UC_WRITE_ENABLED:
        print(f"UC mirror skip ({table_name})")
        return None
    full_name = f"{UC_TABLE}.{table_name}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    print("UC mirror :", full_name)
    WRITTEN_TABLES.append(full_name)
    return full_name


print("Canonical silver:", SILVER_EVENTS_ABFSS)
print("ABFSS_WRITE_OK   :", ABFSS_WRITE_OK)
print("UC mirrors       :", UC_WRITE_ENABLED, UC_TABLE or "-")''',
    )

    md(parts, "## 1. Event schema")
    code(
        parts,
        '''from collections import namedtuple

from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

Event = namedtuple(
    "Event",
    ["source", "metric_name", "timestamp", "metric_value", "content"],
)

EVENT_SCHEMA = StructType(
    [
        StructField("source", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("content", StringType(), True),
    ]
)

print("Event fields:", Event._fields)''',
    )

    md(
        parts,
        """## 2. Load bronze + returns + processors

Transactions = Cell 0 `bronze` (already loaded). Returns = same ABFSS read chain.""",
    )
    code(
        parts,
        '''txn_df = bronze
BRONZE_TXN = BRONZE_PATH
TXN_AUTH = BRONZE_AUTH_MODE
print("Transactions auth:", TXN_AUTH, "| rows:", txn_df.count())

print("Loading returns...")
try:
    returns_df, BRONZE_RETURNS, RET_AUTH = _read_abfss_csv("bronze", BRONZE_RETURNS_REL)
    print("  auth:", RET_AUTH, "| rows:", returns_df.count())
except Exception as exc:
    print("  RETURNS SKIP:", str(exc)[:160])
    returns_df = None
    BRONZE_RETURNS = _abfss("bronze", BRONZE_RETURNS_REL)
    RET_AUTH = "skipped"


def txn_processor(raw, source: str = "transactions"):
    return raw.select(
        F.lit(source).alias("source"),
        F.lit("payment_amount").alias("metric_name"),
        F.to_timestamp(F.col("value_date")).alias("timestamp"),
        F.col("amount_gbp").cast("double").alias("metric_value"),
        F.col("transaction_id").alias("content"),
    )


def txn_channel_processor(raw, source: str = "transactions"):
    return raw.select(
        F.lit(source).alias("source"),
        F.concat(F.lit("txn_count_"), F.lower(F.col("channel"))).alias("metric_name"),
        F.to_timestamp(F.col("value_date")).alias("timestamp"),
        F.lit(1.0).alias("metric_value"),
        F.col("transaction_id").alias("content"),
    )


def returns_processor(raw, source: str = "returns"):
    return raw.select(
        F.lit(source).alias("source"),
        F.lit("refund_amount").alias("metric_name"),
        F.to_timestamp(F.col("return_date")).alias("timestamp"),
        F.col("refund_gbp").cast("double").alias("metric_value"),
        F.concat_ws("|", F.col("return_id"), F.col("reason")).alias("content"),
    )


PROCESSORS = {
    "transactions_payments": {"fn": txn_processor, "df": txn_df},
    "transactions_counts": {"fn": txn_channel_processor, "df": txn_df},
}
if returns_df is not None:
    PROCESSORS["returns"] = {"fn": returns_processor, "df": returns_df}

print("Registered processors:", list(PROCESSORS.keys()))''',
    )

    md(
        parts,
        """## 3. Runner — Event timeline → silver (ABFSS)""",
    )
    code(
        parts,
        '''def run_everything():
    frames = []
    for name, cfg in PROCESSORS.items():
        print(f"Processor: {name}")
        try:
            part = cfg["fn"](cfg["df"]).select(
                "source", "metric_name", "timestamp", "metric_value", "content"
            )
            frames.append(part)
            print(f"  rows: {part.count()}")
        except Exception as exc:
            print(f"  SKIP {name}: {exc}")

    if not frames:
        raise RuntimeError(
            "No processors produced data. Fix: deploy-shared-lab.cmd; attach classic cluster."
        )

    events = frames[0]
    for more in frames[1:]:
        events = events.unionByName(more)

    good = events.filter(
        F.col("metric_value").isNotNull()
        & (F.col("metric_value") > 0)
        & F.col("timestamp").isNotNull()
    )
    print("Timeline good:", good.count())
    write_lab_delta(good, SILVER_EVENTS_ABFSS)
    write_uc_mirror(good.limit(500), "zach_events_preview")
    return good


events = run_everything()
display(events.limit(20))''',
    )

    md(parts, "## 4. Daily aggregates → gold (ABFSS)")
    code(
        parts,
        '''daily = (
    events.withColumn("event_date", F.to_date("timestamp"))
    .groupBy("source", "metric_name", "event_date")
    .agg(F.sum("metric_value").alias("metric_value"), F.count("*").alias("event_count"))
)
write_lab_delta(daily, GOLD_DAILY_ABFSS)
write_uc_mirror(daily, "zach_daily_aggregates")
display(daily.orderBy("event_date", "metric_name").limit(30))''',
    )

    md(parts, "## 5. Pivot + net_cash_gbp (ABFSS)")
    code(
        parts,
        '''pivoted = (
    daily.groupBy("event_date")
    .pivot("metric_name")
    .sum("metric_value")
    .orderBy("event_date")
)
cols = [c for c in pivoted.columns if c != "event_date"]
if "payment_amount" in cols and "refund_amount" in cols:
    pivoted = pivoted.withColumn(
        "net_cash_gbp",
        F.coalesce(F.col("payment_amount"), F.lit(0.0))
        - F.coalesce(F.col("refund_amount"), F.lit(0.0)),
    )
write_lab_delta(pivoted, GOLD_PIVOT_ABFSS)
write_uc_mirror(pivoted, "zach_pivoted")
display(pivoted)''',
    )

    md(parts, "## 6. SQL check")
    code(
        parts,
        '''pivoted.createOrReplaceTempView("zach_pivoted")
spark.sql("SELECT * FROM zach_pivoted ORDER BY event_date").show(20, truncate=False)''',
    )

    md(parts, "## 7. Quality gate + exit")
    code(
        parts,
        '''def assert_quality(df):
    n = df.count()
    assert n > 0, "pivoted gold is empty"
    assert df.filter(F.col("event_date").isNull()).count() == 0, "null event_date"
    print("quality OK:", n, "days")


assert_quality(pivoted)

import json

summary = {
    "run_id": RUN_ID,
    "pattern": "eczachly_event_processor_runner_pivot_v4_abfss",
    "txn_auth": TXN_AUTH,
    "returns_auth": RET_AUTH if "RET_AUTH" in dir() else "n/a",
    "abfss_write_ok": ABFSS_WRITE_OK,
    "lake_paths": [SILVER_EVENTS_ABFSS, GOLD_DAILY_ABFSS, GOLD_PIVOT_ABFSS],
    "sources": list(PROCESSORS.keys()),
    "written": list(WRITTEN_TABLES),
    "status": "Succeeded",
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))''',
    )

    md(
        parts,
        """## 8. Student troubleshooting

| Symptom | Fix |
|---------|-----|
| Top-right says **Serverless** | Detach → attach classic `0703-105931-31juyffm` → Run all |
| `SERVERLESS / ABFSS blocked` | Same — classic required for lake writes |
| `SparkKeyProviderException` / mkdirs | Refresh **v4** (setup no longer calls ADLS mkdirs) |
| Secrets / bronze fail | Run `50_data_engineering_problems` Cell 0 first |
| Returns SKIP | `deploy-shared-lab.cmd` |

**Verify:** `python scripts/run_finledger_event_processors.py`""",
    )

    OUT.write_text("\n".join(finish(parts)) + "\n", encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
