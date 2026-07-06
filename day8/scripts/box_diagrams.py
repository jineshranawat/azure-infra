"""ASCII box diagrams for Day 8 master notebook (no Mermaid)."""

BOX_SPARK_OVERVIEW = """
 You (notebook)              Driver                 Executors
┌──────────────┐         ┌──────────────┐       ┌──────────────┐
│ df.filter()  │────────►│ build plan   │──────►│ partition A  │
│ df.groupBy() │         │ schedule     │       │ partition B  │
└──────────────┘         └──────┬───────┘       └──────┬───────┘
                                └──────────────────────┘
                                         │
                                         ▼
                                  result to driver"""

BOX_DRIVER_EXECUTOR = """
 ┌─────────────┐
 │   DRIVER    │  sends tasks
 │ coordinator │
 └──────┬──────┘
        ├──────────────────┐
        ▼                  ▼
 ┌─────────────┐    ┌─────────────┐
 │ EXECUTOR 1  │    │ EXECUTOR 2  │
 │ partition 1 │    │ partition 2 │
 └─────────────┘    └─────────────┘"""

BOX_PARTITIONS = """
 DataFrame (1000 rows)
 ┌────┬────┬────┬────┐
 │ P1 │ P2 │ P3 │ P4 │  ◄── each partition = one task
 └────┴────┴────┴────┘"""

BOX_LAZY_ACTION = """
 filter  ──► select ──► withColumn     (lazy — nothing runs)
                           │
                           ▼
                      count() / show()  (ACTION — cluster runs)"""

BOX_CATALYST = """
 DataFrame API ──┐
                 ├──► Logical plan ──► Catalyst ──► Physical plan ──► Executors
 SQL ────────────┘"""

BOX_SHUFFLE = """
 Before                    Shuffle                 After
 Exec1: wire,card    ──►   network move    ──►   Exec1: all wire
 Exec2: wire,fps                              Exec2: card+fps"""

BOX_INGEST_TRANSFORM = """
 ┌────────┐     ┌────────────┐     ┌─────────────┐
 │ INGEST │────►│ TRANSFORM  │────►│ ORCHESTRATE │
 │ abfss  │     │ PySpark    │     │ ADF / Jobs  │
 └────────┘     └────────────┘     └─────────────┘"""

BOX_BRONZE_READ = """
 Notebook ──► secret scope ──► spark.read.csv ──► ADLS bronze ──► DataFrame"""

BOX_NARROW_WIDE = """
 NARROW (cheap)              WIDE (shuffle)
 filter, select              groupBy, join
 same partition              data moves across executors"""

BOX_MEDALLION = """
 Sources ──► BRONZE ──► SILVER ──► GOLD
                  └──► QUARANTINE"""

# Aliases for build script
SPARK_OVERVIEW = BOX_SPARK_OVERVIEW
DRIVER_EXECUTOR = BOX_DRIVER_EXECUTOR
PARTITIONS = BOX_PARTITIONS
LAZY_ACTION = BOX_LAZY_ACTION
CATALYST = BOX_CATALYST
SHUFFLE = BOX_SHUFFLE
INGEST_TRANSFORM_ORCHESTRATE = BOX_INGEST_TRANSFORM
BRONZE_READ_FLOW = BOX_BRONZE_READ
MEDALLION = BOX_MEDALLION
PIPELINE_E2E = """
 bronze ──► cast ──► quarantine ──► dedupe ──► join ──► gold"""
CAST_FLOW = """
 bronze STRING ──► cast double ──► good / quarantine"""
JOIN_FLOW = """
 silver + lookup ──► join on channel ──► enriched rows"""
GROUPBY_FLOW = """
 rows ──► shuffle by channel ──► sum/count per channel"""
WINDOW_FLOW = """
 groupBy = 1 row per key  |  Window = keep all rows + rank column"""

ASCII_LAZY = """
 filter()   →  (nothing yet)
 select()   →  (nothing yet)
 count()    →  RUN full plan"""

ASCII_MEDALLION = """
 Sources
    │
    ▼
 BRONZE ──► SILVER ──► GOLD
              │
              ▼
         QUARANTINE"""

ASCII_NARROW_WIDE = BOX_NARROW_WIDE
