"""Schema-contract / drift detection (Problem 9).

Compares an incoming column set against a declared contract to detect renamed,
new, or missing columns before they silently corrupt downstream joins.
"""
from __future__ import annotations

from typing import Dict, Iterable, List


def detect_drift(actual_columns: Iterable[str], expected_columns: Iterable[str]) -> Dict[str, List[str]]:
    """Return {'missing': [...], 'unexpected': [...]} versus the expected contract."""
    actual = list(actual_columns)
    expected = list(expected_columns)
    actual_set, expected_set = set(actual), set(expected)
    return {
        "missing": [c for c in expected if c not in actual_set],
        "unexpected": [c for c in actual if c not in expected_set],
    }


def is_contract_satisfied(actual_columns: Iterable[str], required_columns: Iterable[str]) -> bool:
    """True iff every required column is present (missing a required column is fatal)."""
    actual_set = set(actual_columns)
    return all(c in actual_set for c in required_columns)
