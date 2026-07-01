"""Pure Python transforms — testable without Spark (Day 6 foundation)."""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger("finledger")


def clean_amount(value: Any) -> float:
    """Return a float amount, or 0.0 if the cell is blank or not numeric."""
    if value is None:
        log.warning("bad amount: %r", value)
        return 0.0
    try:
        amount = float(value)
    except (TypeError, ValueError):
        log.warning("bad amount: %r", value)
        return 0.0
    if amount < 0:
        log.warning("negative amount rejected: %s", amount)
        return 0.0
    return amount


def is_valid_txn_id(txn_id: str) -> bool:
    """FinLedger transaction ids start with TXN- and have a numeric suffix."""
    if not txn_id or not isinstance(txn_id, str):
        return False
    if not txn_id.startswith("TXN-"):
        return False
    suffix = txn_id[4:]
    return suffix.isdigit()


def summarise_channel_totals(rows: list[dict[str, Any]], amount_key: str = "amount_gbp") -> dict[str, float]:
    """Sum amounts by channel — same logic we later express in PySpark groupBy."""
    totals: dict[str, float] = {}
    for row in rows:
        channel = str(row.get("channel", "unknown"))
        amount = clean_amount(row.get(amount_key))
        totals[channel] = totals.get(channel, 0.0) + amount
    return totals
