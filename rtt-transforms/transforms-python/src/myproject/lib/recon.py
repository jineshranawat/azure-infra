"""Reconciliation helpers: prove no silent row loss across a split.

Solves (support): source-to-target reconciliation (integration checks) so that
clean + quarantine == raw, guaranteeing rows are quarantined, never dropped.
"""
from __future__ import annotations


def reconcile_counts(source_count: int, *partition_counts: int) -> bool:
    """True iff the partitions sum exactly to the source count (no silent drop)."""
    return sum(partition_counts) == source_count


def variance(expected: int, actual: int) -> int:
    """Signed difference actual - expected (0 == reconciled)."""
    return actual - expected
