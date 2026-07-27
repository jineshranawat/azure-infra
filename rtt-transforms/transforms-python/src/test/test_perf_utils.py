"""Unit tests for lib.perf_utils (Problems 22 salting, 25 partition sizing)."""
import pytest

from myproject.lib import perf_utils as P


def test_salt_bucket_range_and_determinism():
    vals = [P.salt_bucket(f"TR{i:03d}", 8) for i in range(50)]
    assert all(0 <= v < 8 for v in vals)
    assert P.salt_bucket("TR005", 8) == P.salt_bucket("TR005", 8)  # deterministic
    assert P.salt_bucket(None, 8) == 0


def test_salt_bucket_rejects_bad_n():
    with pytest.raises(ValueError):
        P.salt_bucket("x", 0)


def test_target_partitions():
    assert P.target_partitions(0) == 1
    assert P.target_partitions(1_000_000) == 1
    assert P.target_partitions(1_000_001) == 2
    assert P.target_partitions(2_500_000, 1_000_000) == 3
    with pytest.raises(ValueError):
        P.target_partitions(100, 0)
