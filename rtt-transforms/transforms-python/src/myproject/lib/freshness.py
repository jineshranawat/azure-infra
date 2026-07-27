"""Data freshness / staleness (Problem 43).

RTT is a monthly national return; a source that has not refreshed within the
expected window is a data-availability incident that Data Health must surface.
"""
from __future__ import annotations

import datetime as _dt
from typing import Optional


def days_since(latest: Optional[_dt.date], as_of: _dt.date) -> Optional[int]:
    """Whole days between the latest observed load date and `as_of`. None if unknown."""
    if latest is None:
        return None
    return (as_of - latest).days


def is_stale(latest: Optional[_dt.date], as_of: _dt.date, max_age_days: int) -> Optional[bool]:
    """True if the data is older than max_age_days. None if the latest date is unknown."""
    d = days_since(latest, as_of)
    if d is None:
        return None
    return d > max_age_days
