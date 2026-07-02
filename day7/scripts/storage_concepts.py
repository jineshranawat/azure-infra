"""Day 7 — explain storage + medallion paths (theory printed for class)."""

from __future__ import annotations


def print_storage_concepts(storage_account: str = "st<learner><hash>") -> None:
  print()
  print("ADLS Gen2 = Azure Data Lake Storage (hierarchical folders).")
  print("Medallion layout:")
  print(f"  bronze : abfss://bronze@{storage_account}.dfs.core.windows.net/")
  print(f"  silver : abfss://silver@{storage_account}.dfs.core.windows.net/")
  print(f"  gold   : abfss://gold@{storage_account}.dfs.core.windows.net/")
  print()
  print("abfss:// = Spark path to read files without copying into DBFS.")
  print("Session 3 landed bronze at: .../loaded/run=session3-lab/sample_transactions.csv")
