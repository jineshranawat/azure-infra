"""Day 8 — recap pandas/Polars before PySpark transforms."""

from __future__ import annotations

from pathlib import Path


def main(root: Path) -> None:
    import pandas as pd
    import polars as pl

    csv_path = root / "data" / "sample_transactions.csv"
    pdf = pd.read_csv(csv_path)
    plf = pl.read_csv(csv_path)

    print("Step 1 recap: venv + pip install (done by orchestrate.cmd).")
    print("Step 2 pandas rows:", len(pdf), "| columns:", list(pdf.columns)[:3])
    print("Step 3 polars rows:", plf.height, "| columns:", plf.columns[:3])
    print("Step 4 Spark next: same CSV, distributed DataFrame on cluster.")
