"""Mermaid + ASCII flow diagrams for master PySpark notebook (one per concept)."""

# ── Part 0: How Spark thinks ─────────────────────────────────────────────────

SPARK_OVERVIEW = """flowchart LR
    subgraph You["Your notebook"]
        CODE["df.filter().groupBy()"]
    end
    subgraph Driver["Driver JVM"]
        PLAN["Build DAG plan"]
        SCHED["Schedule stages"]
    end
    subgraph Cluster["Executor JVMs"]
        E1["Executor 1\\npartition A"]
        E2["Executor 2\\npartition B"]
    end
    CODE --> PLAN --> SCHED
    SCHED --> E1
    SCHED --> E2
    E1 --> RESULT["Result to driver"]
    E2 --> RESULT"""

DRIVER_EXECUTOR = """flowchart TB
    D["Driver\\n(coordinator)"]
    E1["Executor 1"]
    E2["Executor 2"]
    P1["Partition 1"]
    P2["Partition 2"]
    D -->|"send tasks"| E1
    D -->|"send tasks"| E2
    E1 --> P1
    E2 --> P2
    P1 -->|"metrics / sample"| D
    P2 -->|"metrics / sample"| D"""

PARTITIONS = """flowchart LR
    DF["DataFrame\\n1000 rows"]
    P1["Partition 1\\n~250 rows"]
    P2["Partition 2\\n~250 rows"]
    P3["Partition 3\\n~250 rows"]
    P4["Partition 4\\n~250 rows"]
    DF --> P1 & P2 & P3 & P4
    T1["Task 1"] --> P1
    T2["Task 2"] --> P2
    T3["Task 3"] --> P3
    T4["Task 4"] --> P4"""

LAZY_ACTION = """flowchart TD
    T1["filter (lazy)"] --> T2["select (lazy)"]
    T2 --> T3["withColumn (lazy)"]
    T3 --> WAIT["Plan recorded —\\nno data moved yet"]
    WAIT --> A["count() ACTION"]
    A --> RUN["Cluster executes\\nentire chain once"]"""

CATALYST = """flowchart LR
    API["DataFrame API\\nor SQL"]
    LOG["Logical plan"]
    CAT["Catalyst\\noptimizer"]
    PHYS["Physical plan"]
    EXEC["Executors run"]
    API --> LOG --> CAT --> PHYS --> EXEC"""

SHUFFLE = """flowchart TB
    subgraph Before["Before shuffle"]
        E1["Exec 1: wire, card"]
        E2["Exec 2: wire, fps"]
    end
    subgraph Shuffle["Shuffle exchange"]
        NET["Network + disk\\nrows grouped by key"]
    end
    subgraph After["After shuffle"]
        E3["Exec 1: all wire"]
        E4["Exec 2: all card/fps"]
    end
    E1 & E2 --> NET --> E3 & E4"""

MEDALLION = """flowchart LR
    SRC["Sources\\nCSV, APIs"] --> BRZ["BRONZE\\nraw as landed"]
    BRZ --> SLV["SILVER\\ncast, dedupe, QC"]
    SLV --> GLD["GOLD\\nbusiness metrics"]
    SLV --> Q["QUARANTINE\\nbad rows"]"""

INGEST_TRANSFORM_ORCHESTRATE = """flowchart LR
    I["INGEST\\nread abfss"] --> T["TRANSFORM\\nPySpark"]
    T --> O["ORCHESTRATE\\nADF / Jobs"]
    I -.->|"today"| T
    T -.->|"Session 4+"| O"""

BRONZE_READ_FLOW = """sequenceDiagram
    participant NB as Notebook
    participant SC as Secret scope
    participant ADLS as ADLS bronze
    participant SP as Spark
    NB->>SC: get storage-account + key
    NB->>SP: read.csv(abfss://bronze/...)
    SP->>ADLS: read file blocks
    ADLS-->>SP: CSV partitions
    SP-->>NB: DataFrame bronze"""

CAST_FLOW = """flowchart TD
    B["Bronze: amount_gbp STRING"]
    C["cast to double"]
    G["Silver good: amount > 0"]
    Q["Quarantine: null or <= 0"]
    B --> C --> G
    C --> Q"""

GROUPBY_FLOW = """flowchart TD
  R["Rows with channel"] --> S["SHUFFLE by channel"]
  S --> W["wire bucket"]
  S --> C["card bucket"]
  S --> F["fps bucket"]
  W --> AGG["sum, count, avg"]
  C --> AGG
  F --> AGG"""

JOIN_FLOW = """flowchart LR
    SIL["Silver transactions"]
    LKP["Lookup risk table"]
    SIL --> J["join on channel"]
    LKP --> J
    J --> OUT["Enriched rows\\ntxn + risk"]"""

WINDOW_FLOW = """flowchart TB
    subgraph GroupBy["groupBy — collapses"]
        G1["channel wire"] --> ONE["1 row per channel"]
    end
    subgraph Window["Window — keeps all rows"]
        W1["row 1 rank=1"]
        W2["row 2 rank=2"]
        W3["row 3 rank=3"]
    end"""

PIPELINE_E2E = """flowchart TD
    S1["1 Read bronze"] --> S2["2 Cast amount"]
    S2 --> S3["3 Split good / quarantine"]
    S3 --> S4["4 Dedupe"]
    S4 --> S5["5 Join lookup"]
    S5 --> S6["6 groupBy channel"]
    S6 --> S7["7 Gold report"]"""

# ── ASCII flows (render everywhere) ──────────────────────────────────────────

ASCII_LAZY = """Notebook cell          Spark cluster
─────────────          ─────────────
df.filter(...)    →    (nothing yet)
df.select(...)    →    (nothing yet)
df.count()        →    RUN full plan now"""

ASCII_MEDALLION = """Sources
   │
   ▼
┌─────────┐    ┌─────────┐    ┌─────────┐
│ BRONZE  │───►│ SILVER  │───►│  GOLD   │
│ raw CSV │    │ typed   │    │ totals  │
└─────────┘    └────┬────┘    └─────────┘
                    │
                    ▼
              ┌───────────┐
              │QUARANTINE │
              └───────────┘"""

ASCII_NARROW_WIDE = """NARROW (cheap)              WIDE (shuffle)
filter, select              groupBy, join
same partition              data moves across executors
     │                              │
     ▼                              ▼
  local task                   network exchange"""
