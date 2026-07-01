"""Read FinLedger CSV from disk with pathlib + stdlib csv (no Spark required)."""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from finledger_config import RunConfig
from transforms import clean_amount, summarise_channel_totals

log = logging.getLogger("finledger")


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    """Load a headered CSV into a list of row dicts."""
    if not path.is_file():
        raise FileNotFoundError(f"CSV not found: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def enrich_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add a cleaned amount column — same rule as later silver notebook."""
    enriched: list[dict[str, Any]] = []
    for row in rows:
        cleaned = dict(row)
        cleaned["amount_clean"] = clean_amount(row.get("amount_gbp"))
        enriched.append(cleaned)
    return enriched


def main(session_root: Path, run_date: str = "session6-lab") -> None:
    cfg = RunConfig(run_date=run_date, learner="local")
    csv_path = cfg.local_data_path("sample_transactions.csv", session_root)
    log.info("Reading local CSV: %s", csv_path)

    rows = read_csv_rows(csv_path)
    log.info("Row count: %d", len(rows))

    for row in rows[:3]:
        txn = row.get("transaction_id", "?")
        raw = row.get("amount_gbp", "?")
        cleaned = clean_amount(row.get("amount_gbp"))
        print(f"  {txn}: raw={raw!r} -> cleaned={cleaned}")

    totals = summarise_channel_totals(rows)
    print("\nChannel totals (GBP):")
    for channel, total in sorted(totals.items(), key=lambda item: -item[1]):
        print(f"  {channel:8s} {total:>12,.2f}")

    messy_path = cfg.local_data_path("transactions_messy.csv", session_root)
    if messy_path.is_file():
        messy = read_csv_rows(messy_path)
        bad = [r for r in messy if clean_amount(r.get("amount_gbp")) == 0.0]
        print(f"\nMessy feed: {len(messy)} rows, {len(bad)} would be quarantined (amount=0 after clean)")
