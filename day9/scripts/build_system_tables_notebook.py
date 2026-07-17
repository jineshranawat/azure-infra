#!/usr/bin/env python3
"""Generate nb_system_tables_master.py — complete Databricks system tables lab.

Novice + architect. Heavy analogies. Soft-try every table (ACL-safe).
Synthetic samples when a table is denied so class never blocks.

Run:    python day9/scripts/build_system_tables_notebook.py
Deploy: python scripts/deploy_system_tables_lab.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from notebook_helpers import code, finish, md

OUT = ROOT.parent / "notebooks" / "nb_system_tables_master.py"

# Full catalog (Azure Databricks docs, Jul 2026) — path, schema, retention, scope, one-liner
CATALOG: list[tuple[str, str, str, str, str]] = [
    # access
    ("system.access.audit", "access", "365d", "regional(+global acct)", "Who did what, when (security CCTV)"),
    ("system.access.table_lineage", "access", "365d", "regional", "Which table read/wrote which table"),
    ("system.access.column_lineage", "access", "365d", "regional", "Column-level read/write lineage"),
    ("system.access.workspaces_latest", "access", "indefinite", "global", "All workspaces in the account"),
    ("system.access.inbound_network", "access", "30d", "regional", "Denied inbound access (ingress)"),
    ("system.access.outbound_network", "access", "365d", "regional", "Denied outbound internet"),
    ("system.access.clean_room_events", "access", "365d", "regional", "Clean room collaboration events"),
    ("system.access.assistant_events", "access", "365d", "regional", "Genie / assistant message events"),
    # billing
    ("system.billing.usage", "billing", "365d", "global", "Billable usage (DBUs / SKUs)"),
    ("system.billing.list_prices", "billing", "indefinite", "global", "Historical SKU list prices"),
    # compute
    ("system.compute.clusters", "compute", "365d", "regional", "Cluster config history (SCD2)"),
    ("system.compute.node_timeline", "compute", "90d", "regional", "CPU/mem utilization over time"),
    ("system.compute.node_types", "compute", "indefinite", "regional", "Available VM / node SKUs"),
    ("system.compute.warehouses", "compute", "365d", "regional", "SQL warehouse config history"),
    ("system.compute.warehouse_events", "compute", "365d", "regional", "Warehouse start/stop/scale events"),
    ("system.compute.instance_pools", "compute", "365d", "regional", "Instance pool config history"),
    ("system.compute.instance_events", "compute", "365d", "regional", "Classic VM instance state changes"),
    # lakeflow
    ("system.lakeflow.jobs", "lakeflow", "365d+", "regional", "Job definitions (SCD2)"),
    ("system.lakeflow.job_tasks", "lakeflow", "365d+", "regional", "Job task definitions"),
    ("system.lakeflow.job_run_timeline", "lakeflow", "365d", "regional", "Job run start/end/status"),
    ("system.lakeflow.job_task_run_timeline", "lakeflow", "365d", "regional", "Task-level run timeline"),
    ("system.lakeflow.pipelines", "lakeflow", "365d+", "regional", "Lakeflow / DLT pipelines"),
    ("system.lakeflow.pipeline_update_timeline", "lakeflow", "365d", "regional", "Pipeline update runs"),
    ("system.lakeflow.zerobus_stream", "lakeflow", "365d", "regional", "Zerobus ingest stream events (beta)"),
    ("system.lakeflow.zerobus_ingest", "lakeflow", "365d", "regional", "Zerobus ingest records (beta)"),
    # query
    ("system.query.history", "query", "365d", "regional", "SQL warehouse + serverless query log"),
    # storage
    ("system.storage.predictive_optimization_operations_history", "storage", "180d", "regional", "Predictive optimization ops"),
    ("system.storage.table_auto_upgrade_operations_history", "storage", "180d", "regional", "UC auto-upgrade ops"),
    # sharing / marketplace
    ("system.sharing.materialization_history", "sharing", "365d", "regional", "Delta Sharing materializations"),
    ("system.marketplace.listing_funnel_events", "marketplace", "365d", "regional", "Marketplace listing funnel"),
    ("system.marketplace.listing_access_events", "marketplace", "365d", "regional", "Marketplace get-data events"),
    # ml / serving / ai
    ("system.mlflow.experiments_latest", "mlflow", "180d", "regional", "MLflow experiments"),
    ("system.mlflow.runs_latest", "mlflow", "180d", "regional", "MLflow runs"),
    ("system.mlflow.run_metrics_history", "mlflow", "180d", "regional", "MLflow metric timeseries"),
    ("system.serving.served_entities", "serving", "365d", "regional", "Model serving entities"),
    ("system.serving.endpoint_usage", "serving", "90d", "regional", "Serving token usage"),
    ("system.ai_gateway.usage", "ai_gateway", "365d", "regional", "AI Gateway request metrics"),
    ("system.ai_gateway.external_model_spend", "ai_gateway", "365d", "regional", "External model $ spend"),
    # quality / classification
    ("system.data_classification.results", "data_classification", "13mo", "regional", "Sensitive data detections"),
    ("system.data_quality_monitoring.table_results", "data_quality_monitoring", "indefinite", "regional", "DQ freshness/completeness"),
    # replication
    ("system.replication.states", "replication", "365d", "global", "Managed DR replication (private)"),
]


def build() -> Path:
    parts: list[str] = [
        "# Databricks notebook source",
        "# MAGIC %md",
        "# MAGIC # Databricks System Tables — Complete Master Lab",
        "# MAGIC",
        "# MAGIC **One notebook to understand every major system table** — step by step,",
        "# MAGIC with kitchen / hotel analogies for novices and NFR notes for architects.",
        "# MAGIC",
        "# MAGIC | | |",
        "# MAGIC |:---|:---|",
        "# MAGIC | Path | `/Shared/day9/system_tables_master` |",
        "# MAGIC | Needs | Unity Catalog workspace + grants on `system` |",
        "# MAGIC | Style | Soft-try every table — ACL deny → synthetic sample |",
        "# MAGIC | Docs | [system_tables_master.md](../docs/system_tables_master.md) |",
        "# MAGIC | Official ref | Azure Databricks *System tables reference* |",
        "",
        "# COMMAND ----------",
        "",
    ]

    # ========== 0 BIG PICTURE ==========
    md(
        parts,
        """## 0. The big picture (read first)

### Master analogy — the **bank hotel**

Imagine Databricks is a **giant hotel** where FinLedger data lives.

| Hotel thing | Databricks thing | System table role |
|-------------|------------------|-------------------|
| Front desk guest log | Who logged in / ran what | `system.access.audit` |
| Room service tickets | SQL queries | `system.query.history` |
| Kitchen shift roster | Jobs & pipelines | `system.lakeflow.*` |
| Stove inventory | Clusters / warehouses | `system.compute.*` |
| Gas & electricity bill | DBUs / SKUs | `system.billing.*` |
| Recipe family tree | Lineage | `system.access.*_lineage` |
| Hotel directory | Catalog of tables | `system.information_schema` |
| Security camera (denied doors) | Network policy denials | `inbound_network` / `outbound_network` |
| Spa / AI lounge spend | Serving / AI Gateway | `system.serving.*` / `ai_gateway.*` |

**Novice:** System tables are **read-only diaries** Databricks writes for you — you query them like any other table.  
**Architect:** Account-wide (often **regional**) observability store in catalog `system`, governed by Unity Catalog. Not real-time; refreshed through the day. Free retention ~30–365 days (varies). Do **not** export casually (sensitive ops data).

```text
                    ┌─────────────────────────────┐
                    │   catalog: system           │
                    │  (hotel central archive)    │
                    └─────────────┬───────────────┘
          ┌───────────┬───────────┼───────────┬───────────┐
          ▼           ▼           ▼           ▼           ▼
       access      billing     compute     lakeflow     query
      (security)   (money)    (stoves)    (shifts)   (tickets)
          │           │           │           │           │
          └───────────┴───────────┴───────────┴───────────┘
                    also: storage, sharing, marketplace,
                    mlflow, serving, ai_gateway,
                    data_classification, data_quality_monitoring,
                    replication, information_schema
```""",
    )

    md(
        parts,
        """## 0b. Rules every learner must know

| Rule | Why it matters |
|------|----------------|
| **Unity Catalog required** | System catalog only visible from UC-enabled workspace |
| **Grants** | Need `USE CATALOG` on `system`, `USE SCHEMA` + `SELECT` on schemas |
| **Read-only** | You never `INSERT` into system tables |
| **Not real-time** | Event may appear minutes/hours later |
| **Selective queries** | Broad scans → *“returned too much data”* — always filter by date |
| **Regional vs global** | Jobs/compute = often regional; billing = global |
| **SCD2 configs** | `jobs`, `clusters`, `warehouses` keep history of *config changes* |
| **Timelines** | `*_timeline` tables = immutable *run* history |
| **information_schema** | Different beast — SQL standard metadata, not ops telemetry |
| **Deprecated** | `system.operational_data` / `system.lineage` = empty — use `access.*_lineage` |
| **Streaming** | Use `skipChangeCommits=true`; don't lag > ~7 days (VACUUM) |

### Fix: `PERMISSION_DENIED` / no `BROWSE` on Catalog `system`

This is **not a notebook bug**. Unity Catalog blocks `system` until an **account admin + metastore admin** grants access.

**Who can fix it:** only a user who is **both** Databricks **Account Admin** and **Metastore Admin** (lab `.env` token usually is *not* enough).

**Portal (recommended):**
1. Open https://accounts.azuredatabricks.net (Account console) → **Data** / Metastores → your metastore  
2. Confirm your user is **Metastore admin**  
3. In the workspace → **SQL Editor** (any small SQL warehouse) run the grants below  
4. Re-run this notebook — `OK` instead of `SKIP`

**SQL grants (trainer / account admin):**
```sql
GRANT USE CATALOG ON CATALOG system TO `account users`;
GRANT BROWSE ON CATALOG system TO `account users`;

GRANT USE SCHEMA ON SCHEMA system.access TO `account users`;
GRANT SELECT ON SCHEMA system.access TO `account users`;
GRANT USE SCHEMA ON SCHEMA system.billing TO `account users`;
GRANT SELECT ON SCHEMA system.billing TO `account users`;
GRANT USE SCHEMA ON SCHEMA system.compute TO `account users`;
GRANT SELECT ON SCHEMA system.compute TO `account users`;
GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `account users`;
GRANT SELECT ON SCHEMA system.lakeflow TO `account users`;
GRANT USE SCHEMA ON SCHEMA system.query TO `account users`;
GRANT SELECT ON SCHEMA system.query TO `account users`;
-- optional: storage, sharing, marketplace, mlflow, serving, ai_gateway, ...
```

**Repo helper (same SQL via API):** `python scripts\\grant_system_tables_access.py`  
(fails with *not an account admin* until the right admin runs it)

**While waiting:** keep running the notebook — every `SKIP` shows a **synthetic sample** so teaching continues.""",
    )

    # ========== SETUP ==========
    md(
        parts,
        """## 1. Setup — soft explorer (never crash the class)

We build one helper: try a real system table; on ACL/error, show a **tiny synthetic** frame so teaching continues.

If you see `BROWSE on Catalog 'system'` — jump back to **0b Fix** (grants).""",
    )
    code(
        parts,
        '''# SETUP — soft explorer + permission self-check
from pyspark.sql import functions as F
from datetime import date, timedelta
import json

dbutils.widgets.text("lookback_days", "7", "Days to scan (keep small)")
LOOKBACK = int(dbutils.widgets.get("lookback_days") or "7")
SINCE = (date.today() - timedelta(days=LOOKBACK)).isoformat()

HITS: list[str] = []
MISSES: list[dict] = []
ACCESS_OK = False


def try_table(label: str, sql: str, synthetic_rows=None, synthetic_cols=None):
    """Query a system table; fall back to synthetic sample on any error."""
    try:
        df = spark.sql(sql)
        HITS.append(label)
        print("OK  ", label, "| cols =", len(df.columns))
        display(df.limit(20))
        return df
    except Exception as e:
        msg = str(e)[:220]
        MISSES.append({"table": label, "error": msg})
        print("SKIP", label, "→", msg)
        if "PERMISSION_DENIED" in msg or "BROWSE" in msg or "UnauthorizedAccess" in msg:
            print("     → FIX: account/metastore admin must GRANT BROWSE + USE CATALOG on system (see section 0b)")
        if synthetic_rows is not None:
            sdf = spark.createDataFrame(synthetic_rows, synthetic_cols)
            print("     (synthetic sample for teaching)")
            display(sdf)
            return sdf
        return None


def peek_schema(label: str):
    """DESCRIBE TABLE if allowed."""
    try:
        display(spark.sql(f"DESCRIBE TABLE {label}"))
        print("OK describe", label)
    except Exception as e:
        print("SKIP describe", label, "→", str(e)[:120])


# Quick gate: can we see catalog system at all?
try:
    spark.sql("SHOW SCHEMAS IN system").limit(5).collect()
    ACCESS_OK = True
    print("ACCESS_OK = True  (system catalog is readable)")
except Exception as e:
    ACCESS_OK = False
    print("ACCESS_OK = False")
    print("Reason:", str(e)[:240])
    print("Continue anyway — synthetic samples keep the lesson usable.")

print("lookback_days =", LOOKBACK, "| since =", SINCE)
print("Tip: Catalog Explorer → system → expand schemas (needs BROWSE)")''',
    )

    md(
        parts,
        """## 2. Map the hotel — list what *this* metastore actually shows

**Novice:** First open the filing cabinet and see which drawers exist.  
**Architect:** Schemas appear as Databricks enables them; Private Preview tables may be empty.""",
    )
    code(
        parts,
        '''# STEP 2 — discover schemas + tables under catalog system
try_table(
    "system schemas",
    "SHOW SCHEMAS IN system",
    synthetic_rows=[
        ("access",), ("billing",), ("compute",), ("lakeflow",), ("query",),
        ("information_schema",), ("storage",), ("sharing",),
    ],
    synthetic_cols=["databaseName"],
)

# List tables in the most common schemas
for sch in ["access", "billing", "compute", "lakeflow", "query", "information_schema"]:
    try_table(
        f"tables in system.{sch}",
        f"SHOW TABLES IN system.{sch}",
        synthetic_rows=[(sch, "example_table", False)],
        synthetic_cols=["database", "tableName", "isTemporary"],
    )''',
    )

    # ========== FULL MAP ==========
    # Build markdown table of full catalog
    map_lines = [
        "## 3. Complete map — every major system table (Jul 2026)",
        "",
        "Official list can grow. This lab covers the **public/GA + preview** set used in Azure Databricks docs.",
        "",
        "| Full path | Schema | Retention | Scope | Plain English |",
        "|-----------|--------|-----------|-------|---------------|",
    ]
    for path, sch, ret, scope, plain in CATALOG:
        map_lines.append(f"| `{path}` | {sch} | {ret} | {scope} | {plain} |")
    map_lines.extend(
        [
            "",
            "Plus **`system.information_schema.*`** (SQL standard metadata — catalogs, schemas, tables, columns, privileges).",
            "",
            "**Also remember:** billing + list_prices are free to query; many previews are free during preview.",
        ]
    )
    md(parts, "\n".join(map_lines))

    code(
        parts,
        '''# STEP 3 — printable checklist DataFrame (pin during demos)
catalog_rows = [
'''
        + ",\n".join(
            f'    ("{p}", "{s}", "{r}", "{sc}", "{pl}")' for p, s, r, sc, pl in CATALOG
        )
        + '''
]
catalog_df = spark.createDataFrame(
    catalog_rows,
    ["table_path", "schema_name", "retention", "scope", "plain_english"],
)
display(catalog_df.orderBy("schema_name", "table_path"))
print("Mapped tables =", catalog_df.count())''',
    )

    # ========== ACCESS ==========
    md(
        parts,
        """## 4. Schema `system.access` — security cameras & guest book

### Analogy
Hotel **CCTV + guest register + recipe family tree**.

| Table | Analogy | Architect use |
|-------|---------|---------------|
| `audit` | CCTV who opened which door | SOC2, who dropped a table, login anomalies |
| `table_lineage` | Recipe: dish ← ingredients | Impact analysis before DROP |
| `column_lineage` | Which spice went into which sauce | PII column blast radius |
| `workspaces_latest` | List of hotel buildings | Multi-workspace inventory |
| `inbound_network` | Denied front-door entries | Ingress policy tuning |
| `outbound_network` | Denied “call home” attempts | Egress / data exfil attempts |
| `clean_room_events` | Shared vault visit log | Clean room governance |
| `assistant_events` | Concierge chat log | Genie usage / prompt audit |""",
    )
    code(
        parts,
        '''# STEP 4a — audit (who did what)
try_table(
    "system.access.audit",
    f"""
    SELECT event_time, action_name, user_identity, request_params, response
    FROM system.access.audit
    WHERE event_date >= DATE('{SINCE}')
    ORDER BY event_time DESC
    LIMIT 50
    """,
    synthetic_rows=[
        (f"{SINCE}T10:00:00", "createTable", "alice@bank.com", "finledger.silver.txn", "SUCCESS"),
        (f"{SINCE}T11:00:00", "getToken", "bob@bank.com", "workspace", "SUCCESS"),
        (f"{SINCE}T12:00:00", "deleteTable", "carol@bank.com", "tmp.scratch", "SUCCESS"),
    ],
    synthetic_cols=["event_time", "action_name", "user_identity", "request_params", "response"],
)

# Common actions to look for in class
print("Hunt for: createTable, deleteTable, setAcl, generateTemporaryToken, createCluster")''',
    )
    code(
        parts,
        '''# STEP 4b — table + column lineage
try_table(
    "system.access.table_lineage",
    f"""
    SELECT event_time, source_table_full_name, target_table_full_name,
           event_type, created_by
    FROM system.access.table_lineage
    WHERE event_date >= DATE('{SINCE}')
    LIMIT 40
    """,
    synthetic_rows=[
        (f"{SINCE}T09:00:00", "finledger.bronze.txn", "finledger.silver.txn", "READ", "alice"),
        (f"{SINCE}T09:05:00", "finledger.silver.txn", "finledger.gold.channel", "WRITE", "alice"),
    ],
    synthetic_cols=["event_time", "source_table_full_name", "target_table_full_name", "event_type", "created_by"],
)

try_table(
    "system.access.column_lineage",
    f"""
    SELECT event_time, source_table_full_name, source_column_name,
           target_table_full_name, target_column_name
    FROM system.access.column_lineage
    WHERE event_date >= DATE('{SINCE}')
    LIMIT 40
    """,
    synthetic_rows=[
        (f"{SINCE}T09:05:00", "finledger.silver.txn", "amount", "finledger.gold.channel", "total_gbp"),
    ],
    synthetic_cols=["event_time", "source_table_full_name", "source_column_name",
                    "target_table_full_name", "target_column_name"],
)''',
    )
    code(
        parts,
        '''# STEP 4c — workspaces + network denials + extras
try_table(
    "system.access.workspaces_latest",
    "SELECT * FROM system.access.workspaces_latest LIMIT 20",
    synthetic_rows=[("dbw-shared-qgr7mj", "eastus", "RUNNING")],
    synthetic_cols=["workspace_name", "region", "status"],
)
try_table(
    "system.access.inbound_network",
    f"SELECT * FROM system.access.inbound_network WHERE event_date >= DATE('{SINCE}') LIMIT 20",
    synthetic_rows=[(f"{SINCE}T08:00:00", "DENIED", "203.0.113.10", "ingress_policy")],
    synthetic_cols=["event_time", "action", "source_ip", "policy"],
)
try_table(
    "system.access.outbound_network",
    f"SELECT * FROM system.access.outbound_network WHERE event_date >= DATE('{SINCE}') LIMIT 20",
)
try_table(
    "system.access.clean_room_events",
    f"SELECT * FROM system.access.clean_room_events WHERE event_date >= DATE('{SINCE}') LIMIT 10",
)
try_table(
    "system.access.assistant_events",
    f"SELECT * FROM system.access.assistant_events WHERE event_date >= DATE('{SINCE}') LIMIT 10",
)''',
    )

    # ========== BILLING ==========
    md(
        parts,
        """## 5. Schema `system.billing` — the gas & electricity bill

### Analogy
Monthly **utility bill** for the hotel: which stove (SKU) burned how many units (DBUs).

| Table | Analogy | Architect use |
|-------|---------|---------------|
| `usage` | Meter readings | Cost by day, SKU, job, cluster tags |
| `list_prices` | Tariff card | Convert usage → estimated USD |

**Join pattern (architect):** `usage` ⋈ `list_prices` on SKU + date → approximate `$`.

**Novice tip:** Fast kitchen that burns money overnight (idle clusters) is still a failed restaurant.""",
    )
    code(
        parts,
        '''# STEP 5a — billable usage
usage = try_table(
    "system.billing.usage",
    f"""
    SELECT usage_date, sku_name, usage_unit,
           SUM(usage_quantity) AS qty
    FROM system.billing.usage
    WHERE usage_date >= DATE('{SINCE}')
    GROUP BY usage_date, sku_name, usage_unit
    ORDER BY usage_date DESC, qty DESC
    LIMIT 100
    """,
    synthetic_rows=[
        (date.today() - timedelta(days=i), "JOBS_COMPUTE", "DBU", float(3 + i % 5))
        for i in range(LOOKBACK)
    ],
    synthetic_cols=["usage_date", "sku_name", "usage_unit", "qty"],
)

# Peek common attribution columns when live
peek_schema("system.billing.usage")''',
    )
    code(
        parts,
        '''# STEP 5b — list prices + simple cost chart
try_table(
    "system.billing.list_prices",
    """
    SELECT * FROM system.billing.list_prices
    ORDER BY price_start_time DESC
    LIMIT 30
    """,
    synthetic_rows=[("JOBS_COMPUTE", "DBU", 0.15, "USD")],
    synthetic_cols=["sku_name", "usage_unit", "pricing", "currency"],
)

try:
    import matplotlib.pyplot as plt
    if usage is not None:
        pdf = usage.toPandas()
        dcol = "usage_date" if "usage_date" in pdf.columns else pdf.columns[0]
        qcol = "qty" if "qty" in pdf.columns else pdf.columns[-1]
        g = pdf.groupby(dcol, as_index=False)[qcol].sum().sort_values(dcol)
        fig, ax = plt.subplots(figsize=(9, 3.5))
        ax.bar(g[dcol].astype(str), g[qcol])
        ax.set_title("Usage quantity by day (cost NFR)")
        ax.set_ylabel(qcol)
        fig.autofmt_xdate()
        fig.tight_layout()
        display(fig)
except Exception as e:
    print("chart skipped:", str(e)[:100])

print("Architect: tag jobs/clusters so usage.custom_tags explain FinLedger vs playground")''',
    )

    # ========== COMPUTE ==========
    md(
        parts,
        """## 6. Schema `system.compute` — the kitchen equipment

### Analogy
Inventory of **stoves** (clusters), **banquet halls** (SQL warehouses), **spare pans** (pools), and how hot they ran.

| Table | Analogy | Architect use |
|-------|---------|---------------|
| `clusters` | Stove models over time (SCD2) | Rightsizing, abandon detection |
| `node_timeline` | Thermometer readings | CPU idle vs busy; waste |
| `node_types` | Catalog of stove sizes | Hardware picker |
| `warehouses` | Banquet-hall config history | SQL warehouse inventory |
| `warehouse_events` | Open/close/scale party hall | Autostop validation |
| `instance_pools` | Pre-warmed spare pans | Pool waste / hit rate |
| `instance_events` | Individual burner on/off | Classic VM lifecycle |

```text
Cluster (stove)
  └─ nodes over time  → node_timeline (CPU%, mem%)
SQL warehouse (hall)
  └─ start / stop / scale → warehouse_events
```""",
    )
    code(
        parts,
        '''# STEP 6a — clusters + node timeline
try_table(
    "system.compute.clusters",
    """
    SELECT * FROM system.compute.clusters
    ORDER BY change_time DESC NULLS LAST
    LIMIT 30
    """,
    synthetic_rows=[
        ("0703-105931-31juyffm", "class-shared", "STANDARD", "2", "TERMINATED"),
    ],
    synthetic_cols=["cluster_id", "cluster_name", "cluster_source", "worker_count", "state"],
)

try_table(
    "system.compute.node_timeline",
    f"""
    SELECT * FROM system.compute.node_timeline
    WHERE start_time >= TIMESTAMP('{SINCE}')
    LIMIT 40
    """,
    synthetic_rows=[
        (f"{SINCE}T10:00:00", "driver", 12.5, 40.0),
        (f"{SINCE}T10:05:00", "worker", 55.0, 62.0),
    ],
    synthetic_cols=["start_time", "node_type", "cpu_user_percent", "mem_used_percent"],
)

try_table(
    "system.compute.node_types",
    "SELECT * FROM system.compute.node_types LIMIT 30",
)''',
    )
    code(
        parts,
        '''# STEP 6b — warehouses, pools, instance events
try_table(
    "system.compute.warehouses",
    "SELECT * FROM system.compute.warehouses LIMIT 20",
    synthetic_rows=[("wh-sql-shared", "SERVERLESS", "2X-Small", "RUNNING")],
    synthetic_cols=["warehouse_id", "warehouse_type", "cluster_size", "state"],
)
try_table(
    "system.compute.warehouse_events",
    f"""
    SELECT * FROM system.compute.warehouse_events
    WHERE event_time >= TIMESTAMP('{SINCE}')
    LIMIT 30
    """,
)
try_table(
    "system.compute.instance_pools",
    "SELECT * FROM system.compute.instance_pools LIMIT 15",
)
try_table(
    "system.compute.instance_events",
    f"""
    SELECT * FROM system.compute.instance_events
    WHERE event_time >= TIMESTAMP('{SINCE}')
    LIMIT 30
    """,
)''',
    )

    # ========== LAKEFLOW ==========
    md(
        parts,
        """## 7. Schema `system.lakeflow` — kitchen shifts (Jobs & Pipelines)

### Analogy
**Staff roster** (what jobs exist) vs **timesheets** (when they actually worked).

| Table | Analogy | Architect use |
|-------|---------|---------------|
| `jobs` | Recipe card for a shift (SCD2) | Inventory, drift, tags |
| `job_tasks` | Steps on the recipe card | Task graph / dependencies |
| `job_run_timeline` | Clock-in / clock-out for whole shift | SLA, fail rate, duration |
| `job_task_run_timeline` | Clock per cook station | Find slow task |
| `pipelines` | Continuous conveyor recipes | DLT / SDP inventory |
| `pipeline_update_timeline` | Each conveyor batch | Pipeline health |
| `zerobus_*` | Special loading dock (beta) | Zerobus ingest observability |

**Critical join:** `job_run_timeline` ⋈ `billing.usage` (via job/run ids in usage metadata) → **$ per job**.

```text
jobs (definition)
  └─ job_tasks
job_run_timeline (each run)
  └─ job_task_run_timeline (each task in that run)
```""",
    )
    code(
        parts,
        '''# STEP 7a — jobs + tasks (definitions)
try_table(
    "system.lakeflow.jobs",
    "SELECT * FROM system.lakeflow.jobs LIMIT 25",
    synthetic_rows=[
        ("job-1001", "finledger_nightly", '{"team":"de"}', True),
    ],
    synthetic_cols=["job_id", "name", "tags", "delete_time_is_null"],
)
try_table(
    "system.lakeflow.job_tasks",
    "SELECT * FROM system.lakeflow.job_tasks LIMIT 25",
)''',
    )
    code(
        parts,
        '''# STEP 7b — run timelines (execution)
try_table(
    "system.lakeflow.job_run_timeline",
    f"""
    SELECT * FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= TIMESTAMP('{SINCE}')
    ORDER BY period_start_time DESC
    LIMIT 40
    """,
    synthetic_rows=[
        ("job-1001", "run-55", f"{SINCE}T02:00:00", f"{SINCE}T02:18:00", "SUCCESS"),
        ("job-1001", "run-56", f"{SINCE}T02:00:00", f"{SINCE}T02:45:00", "FAILED"),
    ],
    synthetic_cols=["job_id", "run_id", "period_start_time", "period_end_time", "result_state"],
)
try_table(
    "system.lakeflow.job_task_run_timeline",
    f"""
    SELECT * FROM system.lakeflow.job_task_run_timeline
    WHERE period_start_time >= TIMESTAMP('{SINCE}')
    LIMIT 40
    """,
)
try_table(
    "system.lakeflow.pipelines",
    "SELECT * FROM system.lakeflow.pipelines LIMIT 15",
)
try_table(
    "system.lakeflow.pipeline_update_timeline",
    f"""
    SELECT * FROM system.lakeflow.pipeline_update_timeline
    WHERE period_start_time >= TIMESTAMP('{SINCE}')
    LIMIT 20
    """,
)
try_table("system.lakeflow.zerobus_stream", "SELECT * FROM system.lakeflow.zerobus_stream LIMIT 5")
try_table("system.lakeflow.zerobus_ingest", "SELECT * FROM system.lakeflow.zerobus_ingest LIMIT 5")''',
    )

    # ========== QUERY ==========
    md(
        parts,
        """## 8. Schema `system.query` — room-service tickets

### Analogy
Every **order ticket** to the SQL kitchen (warehouses + serverless notebooks/jobs).

| Table | Analogy | Architect use |
|-------|---------|---------------|
| `history` | Ticket: waiter, dish, cook time, error | Slow query hunt, noisy neighbour, warehouse rightsizing |

**Novice:** “Who ran a 40-minute SELECT at 2am?”  
**Architect:** Filter by `statement_type`, duration, `compute.type` (`WAREHOUSE` / `SERVERLESS_COMPUTE`), user, error.

**Note:** Classic all-purpose cluster notebook SQL may **not** always appear here the same way — warehouse/serverless is the sweet spot.""",
    )
    code(
        parts,
        '''# STEP 8 — query history
try_table(
    "system.query.history",
    f"""
    SELECT start_time, end_time, statement_text, statement_type,
           executed_by, status, total_duration_ms, error_message, compute
    FROM system.query.history
    WHERE start_time >= TIMESTAMP('{SINCE}')
    ORDER BY start_time DESC
    LIMIT 40
    """,
    synthetic_rows=[
        (f"{SINCE}T10:00:00", f"{SINCE}T10:00:02", "SELECT channel, sum(amount)", "SELECT",
         "alice", "FINISHED", 2000, None, "WAREHOUSE"),
        (f"{SINCE}T11:00:00", f"{SINCE}T11:05:00", "MERGE INTO silver", "MERGE",
         "job", "FINISHED", 300000, None, "SERVERLESS_COMPUTE"),
    ],
    synthetic_cols=["start_time", "end_time", "statement_text", "statement_type",
                    "executed_by", "status", "total_duration_ms", "error_message", "compute"],
)
peek_schema("system.query.history")''',
    )

    # ========== INFORMATION_SCHEMA ==========
    md(
        parts,
        """## 9. `system.information_schema` — the library card catalog

### Analogy
Not CCTV. Not the gas bill. This is the **library index**: what books (tables) exist, which shelves (schemas), which pages (columns), who may borrow (privileges).

**Different from other system tables:** SQL-standard information schema over Unity Catalog objects.

| Common view | Question it answers |
|-------------|---------------------|
| `catalogs` | Which catalogs exist? |
| `schemata` | Which schemas? |
| `tables` | Which tables/views? |
| `columns` | Column names + types |
| `table_privileges` / `schema_privileges` | Who can SELECT? |
| `volumes` / `routines` | UC volumes, functions |

**FinLedger use:** generate documentation, find tables without owners, audit open grants.""",
    )
    code(
        parts,
        '''# STEP 9 — information_schema essentials
try_table(
    "system.information_schema.catalogs",
    "SELECT * FROM system.information_schema.catalogs LIMIT 20",
)
try_table(
    "system.information_schema.schemata",
    "SELECT catalog_name, schema_name FROM system.information_schema.schemata LIMIT 40",
)
try_table(
    "system.information_schema.tables",
    """
    SELECT table_catalog, table_schema, table_name, table_type
    FROM system.information_schema.tables
    LIMIT 40
    """,
)
try_table(
    "system.information_schema.columns",
    """
    SELECT table_catalog, table_schema, table_name, column_name, data_type, ordinal_position
    FROM system.information_schema.columns
    LIMIT 40
    """,
)
try_table(
    "system.information_schema.table_privileges",
    "SELECT * FROM system.information_schema.table_privileges LIMIT 30",
)''',
    )

    # ========== STORAGE ==========
    md(
        parts,
        """## 10. Schema `system.storage` — automatic housekeepers

### Analogy
Night crew that **compacts closets** and **upgrades locks** on rooms without waking guests.

| Table | Analogy | Architect use |
|-------|---------|---------------|
| `predictive_optimization_operations_history` | Auto OPTIMIZE/VACUUM-style ops | Prove lake maintenance ran |
| `table_auto_upgrade_operations_history` | Auto feature upgrades on UC tables | Change audit for managed tables |""",
    )
    code(
        parts,
        '''# STEP 10 — storage maintenance history
try_table(
    "system.storage.predictive_optimization_operations_history",
    "SELECT * FROM system.storage.predictive_optimization_operations_history LIMIT 25",
)
try_table(
    "system.storage.table_auto_upgrade_operations_history",
    "SELECT * FROM system.storage.table_auto_upgrade_operations_history LIMIT 25",
)''',
    )

    # ========== SHARING / MARKETPLACE ==========
    md(
        parts,
        """## 11. Sharing & Marketplace — lending books to other hotels

### Analogy
**Inter-library loan** (Delta Sharing) and a **public bookstore window** (Marketplace).

| Table | Analogy |
|-------|---------|
| `system.sharing.materialization_history` | When a shared view was physically prepared for a borrower |
| `system.marketplace.listing_funnel_events` | Shoppers browsing your listing |
| `system.marketplace.listing_access_events` | Shoppers who actually took the data |""",
    )
    code(
        parts,
        '''# STEP 11 — sharing + marketplace
try_table(
    "system.sharing.materialization_history",
    "SELECT * FROM system.sharing.materialization_history LIMIT 20",
)
try_table(
    "system.marketplace.listing_funnel_events",
    "SELECT * FROM system.marketplace.listing_funnel_events LIMIT 15",
)
try_table(
    "system.marketplace.listing_access_events",
    "SELECT * FROM system.marketplace.listing_access_events LIMIT 15",
)''',
    )

    # ========== ML / AI ==========
    md(
        parts,
        """## 12. MLflow, Serving, AI Gateway — the hotel spa & AI lounge

### Analogy
Spa treatments (ML experiments), room-service robots (model serving), and a concierge AI that calls external chefs (AI Gateway).

| Schema.table | Analogy | Architect use |
|--------------|---------|---------------|
| `mlflow.experiments_latest` | Spa treatment menus | Experiment inventory |
| `mlflow.runs_latest` | Each spa appointment | Run metadata |
| `mlflow.run_metrics_history` | Heart-rate chart during treatment | Metric trends |
| `serving.served_entities` | Which robots are on duty | Endpoint inventory |
| `serving.endpoint_usage` | Tokens / calls per request | Serving cost & load |
| `ai_gateway.usage` | Concierge AI request log | Latency, routing |
| `ai_gateway.external_model_spend` | External chef invoices | $ to OpenAI/etc. |""",
    )
    code(
        parts,
        '''# STEP 12 — mlflow / serving / ai_gateway
for label, sql in [
    ("system.mlflow.experiments_latest", "SELECT * FROM system.mlflow.experiments_latest LIMIT 15"),
    ("system.mlflow.runs_latest", "SELECT * FROM system.mlflow.runs_latest LIMIT 15"),
    ("system.mlflow.run_metrics_history", "SELECT * FROM system.mlflow.run_metrics_history LIMIT 15"),
    ("system.serving.served_entities", "SELECT * FROM system.serving.served_entities LIMIT 15"),
    ("system.serving.endpoint_usage", f"SELECT * FROM system.serving.endpoint_usage WHERE event_date >= DATE('{SINCE}') LIMIT 15"),
    ("system.ai_gateway.usage", f"SELECT * FROM system.ai_gateway.usage WHERE event_date >= DATE('{SINCE}') LIMIT 15"),
    ("system.ai_gateway.external_model_spend", "SELECT * FROM system.ai_gateway.external_model_spend LIMIT 15"),
]:
    try_table(label, sql)''',
    )

    # ========== DQ / CLASSIFICATION / REPLICATION ==========
    md(
        parts,
        """## 13. Data quality, classification, replication

### Analogy
| Area | Analogy |
|------|---------|
| `data_classification.results` | Metal detector — “this column looks like PII/PAN” |
| `data_quality_monitoring.table_results` | Health inspection — freshness & completeness scores |
| `replication.states` | Twin hotel in another city (managed DR) — private preview |

**Architect:** Classification + lineage together = “where does PAN flow?”""",
    )
    code(
        parts,
        '''# STEP 13 — classification, DQ, replication
try_table(
    "system.data_classification.results",
    "SELECT * FROM system.data_classification.results LIMIT 25",
    synthetic_rows=[("finledger.silver.txn", "card_pan", "PAYMENT_CARD", 0.96)],
    synthetic_cols=["table_name", "column_name", "class_name", "score"],
)
try_table(
    "system.data_quality_monitoring.table_results",
    "SELECT * FROM system.data_quality_monitoring.table_results LIMIT 25",
    synthetic_rows=[("finledger.gold.channel", "FRESHNESS", "PASS", "ok")],
    synthetic_cols=["table_name", "check_type", "status", "detail"],
)
try_table(
    "system.replication.states",
    "SELECT * FROM system.replication.states LIMIT 10",
)''',
    )

    # ========== JOIN RECIPES ==========
    md(
        parts,
        """## 14. Architect recipes — joins that answer real questions

### 14.1 Cost per job (money × shifts)
```text
billing.usage  ──(job_id / run_id in usage metadata)──►  lakeflow.job_run_timeline
```

### 14.2 Slow warehouse queries (tickets × banquet hall)
```text
query.history  ──warehouse_id──►  compute.warehouses
```

### 14.3 Blast radius (cameras × recipe tree)
```text
access.audit (deleteTable)  +  access.table_lineage (downstream)
```

### 14.4 Idle waste (stoves × thermometer)
```text
compute.clusters  +  compute.node_timeline (low CPU for hours)
```

### 14.5 ADF correlation (your run_id)
ADF Monitor duration vs `lakeflow.job_run_timeline` vs tagged `billing.usage` — same business `run_id` in job params/tags.

**Novice takeaway:** One table rarely answers “why are we late and expensive?” — **join two diaries**.""",
    )
    code(
        parts,
        '''# STEP 14 — safe recipe sketches (run only if both sides OK)
# A) Top SKUs last week (always useful)
try_table(
    "recipe: top SKUs",
    f"""
    SELECT sku_name, usage_unit, SUM(usage_quantity) AS qty
    FROM system.billing.usage
    WHERE usage_date >= DATE('{SINCE}')
    GROUP BY sku_name, usage_unit
    ORDER BY qty DESC
    LIMIT 20
    """,
)

# B) Failed job runs (if lakeflow granted)
try_table(
    "recipe: failed job runs",
    f"""
    SELECT job_id, run_id, result_state, period_start_time, period_end_time
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= TIMESTAMP('{SINCE}')
      AND result_state IN ('FAILED', 'TIMED_OUT', 'CANCELED', 'ERROR')
    ORDER BY period_start_time DESC
    LIMIT 30
    """,
)

# C) Longest queries (if query history granted)
try_table(
    "recipe: longest queries",
    f"""
    SELECT executed_by, total_duration_ms, statement_type,
           substr(statement_text, 1, 120) AS sql_preview
    FROM system.query.history
    WHERE start_time >= TIMESTAMP('{SINCE}')
    ORDER BY total_duration_ms DESC NULLS LAST
    LIMIT 20
    """,
)''',
    )

    # ========== STREAMING + GOTCHAS ==========
    md(
        parts,
        """## 15. Streaming system tables & gotchas

### Streaming (advanced)
```python
(
  spark.readStream
    .option("skipChangeCommits", "true")
    .table("system.billing.usage")
)
```
- System tables delete/VACUUM old versions — lagging streams break after ~7 days.
- Prefer scheduled micro-batches / `AvailableNow` on newer runtimes.

### Gotchas checklist
| Gotcha | Fix |
|--------|-----|
| “Too much data” | Filter `usage_date` / `event_date` / `start_time` |
| Empty table | Feature not used, or Private Preview, or wrong region |
| Missing schema | Not enabled / no grant |
| No fresh rows | Not real-time — wait |
| Schema changed | New columns appear — use `SELECT *` or schema evolution on copies |
| Exported CSV of audit | Security risk — keep inside platform |
| Expecting classic cluster SQL in `query.history` | Prefer warehouse/serverless for this log |""",
    )
    code(
        parts,
        '''# STEP 15 — streaming demo is optional (do NOT leave running in class)
print("Streaming example (do not start unbounded in shared lab):")
print('spark.readStream.option("skipChangeCommits","true").table("system.billing.usage")')
print("Prefer batch SELECT with date filters for teaching.")''',
    )

    # ========== DECISION TREE ==========
    md(
        parts,
        """## 16. Decision tree — which table do I open?

```text
What do you need?
│
├─ Money / DBUs? ──────────────────── billing.usage (+ list_prices)
├─ Who changed / accessed what? ───── access.audit
├─ Where did this table come from? ── access.table_lineage / column_lineage
├─ Job SLA / failures? ────────────── lakeflow.job_run_timeline (+ job_tasks)
├─ Cluster waste / sizing? ────────── compute.clusters + node_timeline
├─ SQL warehouse behaviour? ───────── compute.warehouses + warehouse_events
├─ Slow SQL text? ─────────────────── query.history
├─ What tables exist / grants? ────── information_schema.*
├─ PII discovery? ─────────────────── data_classification.results
├─ Freshness incidents? ───────────── data_quality_monitoring.table_results
├─ Model / GenAI cost? ────────────── serving.* / ai_gateway.* / mlflow.*
└─ Sharing to partners? ───────────── sharing.* / marketplace.*
```

**Pair with:** `/Shared/day9/perf_databricks_adf_lab` (NFR) and `/Shared/day9/spark_dag_problems_lab` (Spark DAG smells).""",
    )

    # ========== SCORECARD ==========
    md(
        parts,
        """## 17. Workshop scorecard

Fill during class — prove you can *navigate*, not memorize every column.""",
    )
    code(
        parts,
        '''# STEP 17 — scorecard + coverage report
score = spark.createDataFrame(
    [
        ("Find system catalog in Explorer", "done/skip", ""),
        ("Query billing.usage with date filter", "done/skip", ""),
        ("Explain audit vs lineage", "done/skip", ""),
        ("Name SCD2 vs timeline tables", "done/skip", ""),
        ("Join idea: cost per job", "done/skip", ""),
        ("Know information_schema ≠ ops telemetry", "done/skip", ""),
        ("List 3 gotchas", "done/skip", ""),
    ],
    ["skill", "status", "notes"],
)
display(score)

print("=== COVERAGE ===")
print("OK hits :", len(HITS))
for h in HITS:
    print("  ✓", h)
print("SKIP    :", len(MISSES))
for m in MISSES[:30]:
    print("  ·", m["table"], "→", m["error"][:80])''',
    )

    md(
        parts,
        """## 18. Wrap — 30-second scripts

**Novice:**  
“System tables are Databricks’ own diaries in catalog `system`. I use billing for the bill, audit for who did what, lakeflow for job shifts, compute for stoves, query history for SQL tickets, and information_schema for the library index.”

**Architect:**  
“UC-governed, mostly regional ops store with ~365d retention. Separate config SCD2 from run timelines. Always date-filter. Join usage to jobs for unit economics. Lineage + classification for blast radius. Never treat as real-time SIEM.”

**Re-run deploy:** `python scripts\\deploy_system_tables_lab.py`""",
    )
    code(
        parts,
        '''# EXIT
summary = {
    "lab": "system_tables_master",
    "lookback_days": LOOKBACK,
    "hits": len(HITS),
    "misses": len(MISSES),
    "hit_labels": HITS,
    "status": "Succeeded",
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))''',
    )

    OUT.write_text("\n".join(finish(parts)) + "\n", encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
