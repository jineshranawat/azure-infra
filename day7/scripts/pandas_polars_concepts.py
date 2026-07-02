"""Day 7 — pandas + Polars on sample CSV before Spark."""

from __future__ import annotations

from pathlib import Path


def main(root: Path) -> None:
    import pandas as pd
    import polars as pl

    csv_path = root / "data" / "sample_transactions.csv"
    pdf = pd.read_csv(csv_path)
    plf = pl.read_csv(csv_path)

    print("Step 1: venv created by orchestrate.cmd at repo root (.venv).")
    print("Step 2 pandas rows:", len(pdf), "| cols:", list(pdf.columns)[:3])
    print("Step 3 polars rows:", plf.height, "| cols:", plf.columns[:3])
    print("Step 4: next block is PySpark foundation (nb_03), then read bronze.")
