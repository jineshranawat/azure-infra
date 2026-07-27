"""Unit tests for lib.dq_checks (Problems 1, 8, 34, 35, 41, 42 support)."""
import datetime as dt

from myproject.lib import dq_checks as Q


def test_is_null_or_blank():
    assert Q.is_null_or_blank(None) is True
    assert Q.is_null_or_blank("") is True
    assert Q.is_null_or_blank("   ") is True
    assert Q.is_null_or_blank("x") is False
    assert Q.is_null_or_blank(0) is False


def test_in_range():
    assert Q.in_range(5, 0, 10) is True
    assert Q.in_range(-1, 0, 10) is False   # negative: below range
    assert Q.in_range(11, 0, 10) is False   # negative: above range
    assert Q.in_range(None, 0, 10) is None


def test_find_duplicate_keys():
    assert Q.find_duplicate_keys(["a", "b", "a", "c", "b"]) == ["a", "b"]
    assert Q.find_duplicate_keys(["a", "b", "c"]) == []


def test_orphan_keys():
    valid = ["TR001", "TR002", "TR003", "TR004", "TR005"]
    # negative: TR999/TR888 are orphans not present in the reference PK set
    assert Q.orphan_keys(["TR001", "TR999", "TR888", "TR999"], valid) == ["TR999", "TR888"]
    assert Q.orphan_keys(["TR001", "TR002"], valid) == []


def test_referral_not_after():
    snap = dt.date(2025, 1, 31)
    assert Q.referral_not_after(dt.date(2024, 9, 25), snap) is True
    # negative: a referral dated after the extract snapshot is impossible
    assert Q.referral_not_after(dt.date(2030, 1, 1), snap) is False
    assert Q.referral_not_after(None, snap) is None
