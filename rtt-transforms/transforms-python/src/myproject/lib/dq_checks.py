"""Pure data-quality predicates, reused by transforms and unit tests.

Solves (support): Problems 1, 8, 34, 35, 41, 42 (dedupe, nulls, orphans,
range/domain, cross-field, PK uniqueness).
"""
from __future__ import annotations

import datetime as _dt
from typing import Hashable, Iterable, List, Optional


def is_null_or_blank(value) -> bool:
    """True for None or an all-whitespace string."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def in_range(value: Optional[float], low: float, high: float) -> Optional[bool]:
    """Inclusive range test. None if value is None."""
    if value is None:
        return None
    return low <= value <= high


def find_duplicate_keys(keys: Iterable[Hashable]) -> List[Hashable]:
    """Keys appearing more than once, in first-seen order."""
    counts = {}
    order = []
    for k in keys:
        if k not in counts:
            order.append(k)
        counts[k] = counts.get(k, 0) + 1
    return [k for k in order if counts[k] > 1]


def orphan_keys(fk_values: Iterable[Hashable], valid_keys: Iterable[Hashable]) -> List[Hashable]:
    """FK values absent from the valid-PK set (first-seen order, de-duplicated)."""
    valid = set(valid_keys)
    out: List[Hashable] = []
    seen = set()
    for v in fk_values:
        if v not in valid and v not in seen:
            out.append(v)
            seen.add(v)
    return out


def referral_not_after(referral: Optional[_dt.date], snapshot: Optional[_dt.date]) -> Optional[bool]:
    """Cross-field/temporal rule: a referral cannot post-date the extract snapshot.

    True = valid (referral <= snapshot). None if either date is unknown.
    """
    if referral is None or snapshot is None:
        return None
    return referral <= snapshot
