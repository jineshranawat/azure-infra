"""SAE (data-minimisation) helper.

Drops IG-approved columns before Silver leaves the layer, driven by the
sae_column_rules reference table (metadata-driven, NOT hard-coded). This is the
legal/PII constraint: sex, age_at_referral and source_load_ts must not leave Silver.

Solves (support): Problem 6/48 support and the SAE minimisation stage.
"""
from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple


def columns_to_drop(rules: Iterable[dict], dataset_name: str) -> List[str]:
    """Sorted, de-duplicated column names to drop for a given dataset_name."""
    cols = {
        r["column_name"]
        for r in rules
        if r.get("dataset_name") == dataset_name and r.get("column_name")
    }
    return sorted(cols)


def apply_minimisation(all_columns: Sequence[str],
                       drop_columns: Iterable[str]) -> Tuple[List[str], List[str]]:
    """Split all_columns into (kept, dropped), preserving original order."""
    drop = set(drop_columns)
    kept = [c for c in all_columns if c not in drop]
    dropped = [c for c in all_columns if c in drop]
    return kept, dropped


def assert_minimised(remaining_columns: Iterable[str], must_be_absent: Iterable[str]) -> bool:
    """True iff none of the must_be_absent columns remain (post-condition check)."""
    remaining = set(remaining_columns)
    return not (remaining & set(must_be_absent))
