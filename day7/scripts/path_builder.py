"""Build FinLedger abfss paths — same logic as notebooks."""

from __future__ import annotations


def bronze_container(storage_account: str) -> str:
    return f"abfss://bronze@{storage_account}.dfs.core.windows.net/"


def bronze_csv_path(storage_account: str, run_id: str, filename: str = "sample_transactions.csv") -> str:
    base = bronze_container(storage_account)
    return f"{base}loaded/run={run_id}/{filename}"
