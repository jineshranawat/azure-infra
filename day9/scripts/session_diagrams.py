"""Re-export box diagrams for Day 9+ notebooks (no Mermaid)."""

from box_diagrams import *  # noqa: F403

# Keep ASCII text tables used in notebooks
ASCII_FORMATS = """Format      | Best for              | FinLedger layer
------------|----------------------|------------------
CSV         | raw ingest, humans   | bronze
JSON        | APIs, semi-structured| bronze (optional)
Parquet     | fast columnar files  | intermediate
Delta       | ACID + time travel   | silver, gold"""

ASCII_QUARANTINE = """Bronze row
    │
    ▼ cast
 amount valid? ──no──► audit/quarantine/
    │
   yes
    ▼
 silver/transactions/"""

ASCII_DELTA_LOG = """my_table/
  part-00001.parquet
  part-00002.parquet
  _delta_log/
    00000000000000000000.json  ← version 0
    00000000000000000001.json  ← version 1"""

ASCII_MEDALLION = """Sources
   │
   ▼
 BRONZE ──► SILVER ──► GOLD
              │
              ▼
         QUARANTINE"""
