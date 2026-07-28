"""Unit tests for lib.advanced_patterns (A1..A11) - positive + negative cases."""
import datetime as dt

import pytest

from myproject.lib import advanced_patterns as A


def test_scd1_latest():
    recs = [{"k": "x", "v": "a", "t": 1}, {"k": "x", "v": "b", "t": 3},
            {"k": "y", "v": "c", "t": 2}, {"k": "x", "v": "old", "t": 2}]
    out = {r["k"]: r["v"] for r in A.scd1_latest(recs, "k", "t")}
    assert out == {"x": "b", "y": "c"}          # newest per key wins


def test_cdc_with_tombstones():
    cur = {"1": "A", "2": "A", "3": "A"}
    prev = {"1": "A", "2": "B", "4": "A"}
    ops = A.classify_cdc_with_tombstones(cur, prev, tombstones=["3"])
    assert ops == {"1": "UNCHANGED", "2": "UPDATE", "3": "DELETE", "4": "DELETE"}


def test_content_dedupe():
    rows = [{"pk": 1, "a": "x", "b": "y"}, {"pk": 2, "a": "x", "b": "y"}, {"pk": 3, "a": "z", "b": "y"}]
    out = A.dedupe_by_hash(rows, ["a", "b"])
    assert [r["pk"] for r in out] == [1, 3]      # pk2 is a content duplicate of pk1
    assert A.content_hash(rows[0], ["a", "b"]) == A.content_hash(rows[1], ["a", "b"])


def test_profile_column():
    p = A.profile_column([1, 2, 2, None, 5])
    assert (p["count"], p["nulls"], p["distinct"], p["min"], p["max"], p["null_pct"]) == (5, 1, 3, 1, 5, 0.2)
    e = A.profile_column([])                     # negative: empty column
    assert e["count"] == 0 and e["null_pct"] is None and e["min"] is None


def test_tokenize_preserving():
    t = A.tokenize_preserving("NHS1000580", "salt")
    assert t != "NHS1000580" and len(t) == len("NHS1000580")
    assert t[:3].isupper() and t[3:].isdigit()   # character classes preserved
    assert t == A.tokenize_preserving("NHS1000580", "salt")     # deterministic
    assert A.tokenize_preserving("NHS1000580", "other") != t    # key-dependent
    assert A.tokenize_preserving(None, "salt") is None


def test_within_allowed_lateness():
    wm = dt.date(2025, 1, 31)
    assert A.within_allowed_lateness(dt.date(2025, 1, 20), wm, 15) is True   # 11d late, allowed
    assert A.within_allowed_lateness(dt.date(2025, 1, 1), wm, 15) is False   # 30d late (negative)
    assert A.within_allowed_lateness(dt.date(2025, 2, 5), wm, 15) is True    # future vs watermark
    assert A.within_allowed_lateness(None, wm, 15) is None


def test_reconcile_by_partition():
    ok = A.reconcile_by_partition({"TR001": 10, "TR002": 5}, {"TR001": 10, "TR002": 5})
    assert ok["balanced"] is True
    bad = A.reconcile_by_partition({"TR001": 10, "TR002": 5}, {"TR001": 9, "TR002": 6})
    assert bad["balanced"] is False and bad["variances"]["TR001"] == -1 and bad["variances"]["TR002"] == 1


def test_merge_schemas():
    assert A.merge_schemas(["a", "b"], ["b", "c", "d"]) == ["a", "b", "c", "d"]


def test_backoff_schedule():
    assert A.backoff_schedule(4, base=1, factor=2, cap=30) == [1, 2, 4, 8]
    assert A.backoff_schedule(6, base=1, factor=2, cap=8) == [1, 2, 4, 8, 8, 8]   # capped
    assert A.backoff_schedule(0) == []
    with pytest.raises(ValueError):
        A.backoff_schedule(-1)
    with pytest.raises(ValueError):
        A.backoff_schedule(3, factor=0.5)


def test_as_of_join_pick():
    v = [(dt.date(2024, 1, 1), "v1"), (dt.date(2024, 6, 1), "v2"), (dt.date(2025, 1, 1), "v3")]
    assert A.as_of_join_pick(dt.date(2024, 3, 1), v) == "v1"
    assert A.as_of_join_pick(dt.date(2024, 6, 1), v) == "v2"
    assert A.as_of_join_pick(dt.date(2025, 5, 1), v) == "v3"
    assert A.as_of_join_pick(dt.date(2023, 1, 1), v) is None    # before any version


def test_idempotency_key():
    k1 = A.idempotency_key(["src", "batch7", "PW001"])
    assert k1 == A.idempotency_key(["src", "batch7", "PW001"]) and len(k1) == 24
    assert A.idempotency_key(["src", "batch8", "PW001"]) != k1
