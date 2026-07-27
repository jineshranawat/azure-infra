"""Unit tests for lib.freshness (Problem 43)."""
import datetime as dt

from myproject.lib import freshness as F


def test_days_since():
    assert F.days_since(dt.date(2025, 1, 31), dt.date(2025, 2, 10)) == 10
    assert F.days_since(None, dt.date(2025, 2, 10)) is None


def test_is_stale():
    as_of = dt.date(2025, 3, 20)
    assert F.is_stale(dt.date(2025, 1, 31), as_of, 45) is True   # 48d > 45 -> stale (negative)
    assert F.is_stale(dt.date(2025, 3, 1), as_of, 45) is False   # 19d <= 45 -> fresh
    assert F.is_stale(None, as_of, 45) is None
