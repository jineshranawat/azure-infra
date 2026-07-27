"""Crafted multi-batch proofs of the incremental/streaming logic (lib.scd)."""
import polars as pl
import pytest

from myproject.lib import scd as SCD


def test_dedupe_latest_keeps_newest_per_key():
    # Two events for PW1 (late-arriving older one must lose) - Problems 15, 20.
    df = pl.DataFrame({
        "pathway_id": ["PW1", "PW1", "PW2"],
        "pathway_status": ["Active", "Discharged", "Active"],
        "event_ts": ["2025-01-01", "2025-02-01", "2025-01-15"],
    })
    out = SCD.dedupe_latest(df, "pathway_id", "event_ts").sort("pathway_id")
    assert out["pathway_status"].to_list() == ["Discharged", "Active"]


def test_dedupe_latest_is_idempotent():
    # Problem 19: running the merge twice yields the same result.
    df = pl.DataFrame({"k": ["a", "a"], "v": [1, 2], "ts": ["1", "2"]})
    once = SCD.dedupe_latest(df, "k", "ts")
    twice = SCD.dedupe_latest(once, "k", "ts")
    assert once.to_dicts() == twice.to_dicts()


def test_high_water_mark():
    assert SCD.high_water_mark(["2025-01-01", "2025-03-01", "2025-02-01"]) == "2025-03-01"
    assert SCD.high_water_mark([None, None]) is None
    assert SCD.high_water_mark([]) is None


def test_classify_cdc_all_change_types():
    current = pl.DataFrame({"pathway_id": ["PW1", "PW2", "PW3"],
                            "pathway_status": ["Active", "Active", "Active"]})
    previous = pl.DataFrame({"pathway_id": ["PW1", "PW2", "PW4"],
                             "pathway_status": ["Active", "Discharged", "Active"]})
    feed = SCD.classify_cdc(current, previous, "pathway_id", ["pathway_status"])
    got = {r["pathway_id"]: r["change_type"] for r in feed.to_dicts()}
    assert got == {"PW1": "UNCHANGED", "PW2": "UPDATE", "PW3": "INSERT", "PW4": "DELETE"}


def test_build_scd2_effective_dating():
    obs = [
        {"key": "PW1", "value": "Active", "snapshot_date": "2025-01-01"},
        {"key": "PW1", "value": "Active", "snapshot_date": "2025-02-01"},  # unchanged -> merged
        {"key": "PW1", "value": "Discharged", "snapshot_date": "2025-03-01"},
    ]
    segments = SCD.build_scd2(obs)
    assert segments == [
        {"key": "PW1", "value": "Active", "valid_from": "2025-01-01",
         "valid_to": "2025-03-01", "is_current": False},
        {"key": "PW1", "value": "Discharged", "valid_from": "2025-03-01",
         "valid_to": None, "is_current": True},
    ]


def test_rolling_sum():
    assert SCD.rolling_sum([1, 1, 1, 1], 2) == [1, 2, 2, 2]
    assert SCD.rolling_sum([5, 0, 3], 3) == [5, 5, 8]
    with pytest.raises(ValueError):
        SCD.rolling_sum([1, 2], 0)
