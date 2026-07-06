"""ASCII box diagrams — render in every Databricks cell (no Mermaid)."""

# ── Medallion & lake ─────────────────────────────────────────────────────────

BOX_MEDALLION = """
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   SOURCES    │     │    BRONZE    │     │    SILVER    │
│  CSV, APIs   │────►│  raw as land │────►│ cast, dedupe │
└──────────────┘     └──────┬───────┘     └──────┬───────┘
                            │                    │
                            │              ┌─────▼──────┐
                            │              │    GOLD    │
                            │              │ aggregates │
                            │              └────────────┘
                            │
                     ┌──────▼───────┐
                     │  QUARANTINE  │
                     │ audit/bad    │
                     └──────────────┘"""

BOX_ADLS_CONTAINERS = """
┌─────────────────── stsharedqgr7mj ───────────────────┐
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
│  │ bronze  │  │ silver  │  │  gold   │  │  audit  │ │
│  │ raw CSV │  │ Delta   │  │ reports │  │quarantine│ │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘ │
└──────────────────────────────────────────────────────┘"""

# ── Session 9 ────────────────────────────────────────────────────────────────

BOX_SESSION09_OVERVIEW = """
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Day 8     │     │  Session 9  │     │ WRITE lake  │
│ transform   │────►│ cast+split  │────►│ silver+audit│
│ in memory   │     │ good / bad  │     │   Delta     │
└─────────────┘     └─────────────┘     └─────────────┘"""

BOX_CAST_FLOW = """
┌─────────────────┐
│ Bronze CSV      │
│ amount_gbp TEXT │
└────────┬────────┘
         │  .cast("double")
         ▼
┌─────────────────┐
│ amount DOUBLE   │
└────────┬────────┘
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌────────────┐
│ SILVER │ │ QUARANTINE │
│ good   │ │ null/<=0   │
└────────┘ └────────────┘"""

BOX_CAST_DETAIL = """
  amount_gbp          cast           result
┌───────────┐    ┌──────────┐    ┌──────────────┐
│ "1250.50" │───►│ to double│───►│ 1250.50  OK  │
└───────────┘    └──────────┘    └──────────────┘
┌───────────┐    ┌──────────┐    ┌──────────────┐
│ "INVALID" │───►│ to double│───►│ NULL → audit │
└───────────┘    └──────────┘    └──────────────┘"""

BOX_QUARANTINE = """
         ┌──────────────┐
         │  All rows    │
         └──────┬───────┘
                │ quality rules
         ┌──────┴───────┐
         ▼              ▼
┌─────────────┐  ┌─────────────┐
│ silver_good │  │ silver_bad  │
│ abfss://    │  │ abfss://    │
│ silver/...  │  │ audit/...   │
└─────────────┘  └─────────────┘
  pipeline OK      ops reviews"""

BOX_WRITE_ACTION = """
 LAZY (no cluster work yet)          ACTION (runs now)
┌────────────────────────┐         ┌────────────────────────┐
│ filter                 │         │ .write.format("delta") │
│ select                 │   ───►  │ .save(abfss://...)     │
│ withColumn             │         │ folders on ADLS        │
└────────────────────────┘         └────────────────────────┘"""

BOX_SILVER_WRITE_SEQUENCE = """
 Notebook          Spark cluster           ADLS silver
┌─────────┐       ┌─────────────┐       ┌──────────────┐
│ write() │──────►│ executors   │──────►│ part-*.pq    │
└─────────┘       │ run tasks   │       │ _delta_log/  │
┌─────────┐       └─────────────┘       └──────┬───────┘
│ read()  │◄─────────────────────────────────┘
└─────────┘       verify row count"""

BOX_DEAD_LETTER = """
┌──────────────────────────────────────────┐
│           Pipeline keeps running         │
└─────────────────┬────────────────────────┘
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
┌───────────────┐   ┌───────────────┐
│ Good → silver │   │ Bad → audit   │
└───────────────┘   └───────┬───────┘
                            ▼
                    ┌───────────────┐
                    │ Ops fixes src │
                    └───────────────┘"""

# ── Delta ────────────────────────────────────────────────────────────────────

BOX_DELTA_VS_PARQUET = """
┌─────────────────────┐       ┌─────────────────────┐
│  Parquet / CSV      │       │     Delta Lake      │
├─────────────────────┤       ├─────────────────────┤
│ files only          │       │ Parquet files       │
│ no transaction log  │       │ + _delta_log        │
│ schema drift risk   │       │ ACID + time travel  │
└─────────────────────┘       └─────────────────────┘"""

BOX_DELTA_LOG = """
 my_table/
 ├── part-00001.parquet  ◄── data
 ├── part-00002.parquet
 └── _delta_log/
     ├── 000...000.json   ◄── version 0
     └── 000...001.json   ◄── version 1"""

BOX_WRITE_MODES = """
┌────────────┬────────────────────────────────────┐
│ overwrite  │ replace snapshot (batch default)   │
├────────────┼────────────────────────────────────┤
│ append     │ add new files                      │
├────────────┼────────────────────────────────────┤
│ error      │ fail if path exists                │
├────────────┼────────────────────────────────────┤
│ ignore     │ skip if path exists                │
└────────────┴────────────────────────────────────┘"""

BOX_TIME_TRAVEL = """
 v0 ──────► v1 ──────► v2 ──────► RESTORE to v1
first      re-run     mistake      roll back"""

BOX_SNAPSHOT_SAFETY = """
 1. Job writes bad data        ──►  version 3 (bad)
 2. Engineer DESCRIBE HISTORY  ──►  see v0, v1, v2, v3
 3. RESTORE TO VERSION 2       ──►  table back to good state"""

# ── MERGE & pipeline ─────────────────────────────────────────────────────────

BOX_MERGE = """
 Target Delta          Today's file         Result
┌─────────────┐       ┌─────────────┐      ┌─────────────┐
│ yesterday   │       │ updates     │      │ MERGE ON    │
│ silver rows │──────►│ snapshot    │─────►│ txn_id      │
└─────────────┘       └─────────────┘      └──────┬──────┘
                                                  │
                              ┌───────────────────┴────────┐
                              ▼                            ▼
                       matched UPDATE              new INSERT"""

BOX_INCREMENTAL = """
 Day 1 file ──MERGE──►┐
                       ├──► Silver (one row per txn_id)
 Day 2 file ──MERGE──►┘"""

BOX_RUN_MEDALLION = """
 run_date
    │
    ▼
┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐
│ bronze │──►│ cast   │──►│ dedupe │──►│ silver │
└────────┘   └────────┘   └────────┘   └───┬────┘
                                             ▼
                                        ┌────────┐
                                        │  gold  │
                                        └────────┘"""

BOX_PIPELINE_E2E = """
 1 Read bronze ──► 2 Cast ──► 3 Quarantine split
        │
        ▼
 4 Dedupe ──► 5 Join ──► 6 groupBy ──► 7 Gold report"""

BOX_GOLD_ROLLUP = """
 Silver rows ──► groupBy channel+day ──► pivot (optional)
                        │
                        ▼
                 partitionBy day ──► gold Delta table"""

# ── Orchestration ────────────────────────────────────────────────────────────

BOX_SQL_VS_PYSPARK = """
 ┌──────────────┐     ┌──────────────────┐
 │ PySpark API  │────►│                  │
 └──────────────┘     │    Catalyst      │──► executors
 ┌──────────────┐     │    optimizer     │
 │  %sql / SQL  │────►│                  │
 └──────────────┘     └──────────────────┘"""

BOX_UNITY_CATALOG = """
 finledger (catalog)
 ├── bronze (schema)
 ├── silver (schema)
 └── gold (schema)
      └── channel_daily (table)
            └── GRANT SELECT → analysts"""

BOX_ORCHESTRATE = """
 ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────────┐
 │     ADF     │────►│ Databricks  │────►│  Notebook   │────►│ ADLS     │
 │  schedule   │     │    Job      │     │run_medallion│     │ Delta    │
 └─────────────┘     └─────────────┘     └─────────────┘     └──────────┘"""

# ── Secrets & formats ────────────────────────────────────────────────────────

BOX_SECRET_SCOPE = """
 Azure Portal          .env laptop           Databricks
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│ storage     │      │ orchestrate │      │ scope:      │
│ key1 copy   │─────►│ .cmd setup  │─────►│ finledger   │
└─────────────┘      └─────────────┘      └──────┬──────┘
                                                  │
                                                  ▼
                                         dbutils.secrets.get()"""

BOX_FORMAT_PICKER = """
 Need human-readable ingest?     ──► CSV  (bronze)
 Need API / nested data?         ──► JSON (bronze)
 Need fast columnar analytics?   ──► Parquet
 Need ACID lakehouse tables?     ──► Delta (silver/gold)"""

BOX_FORMAT_LAYERS = """
 Sources ──► CSV bronze ──► Delta silver ──► Delta gold"""

# Aliases used by build scripts (replace old mermaid names)
MEDALLION = BOX_MEDALLION
CAST_FLOW = BOX_CAST_FLOW
QUARANTINE_FLOW = BOX_QUARANTINE
WRITE_ACTION = BOX_WRITE_ACTION
DEAD_LETTER = BOX_DEAD_LETTER
DELTA_VS_PARQUET = BOX_DELTA_VS_PARQUET
DELTA_LOG = BOX_DELTA_LOG
WRITE_MODES = BOX_WRITE_MODES
TIME_TRAVEL = BOX_TIME_TRAVEL
SNAPSHOT_SAFETY = BOX_SNAPSHOT_SAFETY
MERGE_FLOW = BOX_MERGE
INCREMENTAL_BATCH = BOX_INCREMENTAL
RUN_MEDALLION = BOX_RUN_MEDALLION
PIPELINE_E2E = BOX_PIPELINE_E2E
GOLD_ROLLUP = BOX_GOLD_ROLLUP
SQL_VS_PYSPARK = BOX_SQL_VS_PYSPARK
UNITY_CATALOG = BOX_UNITY_CATALOG
ORCHESTRATE_STACK = BOX_ORCHESTRATE
SECRET_SCOPE_FLOW = BOX_SECRET_SCOPE
FORMAT_PICKER = BOX_FORMAT_PICKER
FORMAT_LAYERS = BOX_FORMAT_LAYERS
SESSION09_OVERVIEW = BOX_SESSION09_OVERVIEW
ADLS_CONTAINERS = BOX_ADLS_CONTAINERS
CAST_DETAIL = BOX_CAST_DETAIL
SILVER_WRITE_SEQUENCE = BOX_SILVER_WRITE_SEQUENCE
