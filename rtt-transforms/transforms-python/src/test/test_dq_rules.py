"""Crafted-frame proof of the Silver quarantine policy (Problems 1,8,34,35,41,42)."""
import datetime as dt

import polars as pl

from myproject.lib import dq_rules as R

SNAP = dt.date(2025, 1, 31)
VALID = ["TR001", "TR002", "TR003", "TR004", "TR005"]


def _frame(rows):
    return pl.DataFrame(
        rows,
        schema={
            "pathway_id": pl.Utf8, "trust_code": pl.Utf8, "specialty_code": pl.Utf8,
            "weeks_waited": pl.Int64, "referral_date": pl.Date,
        },
        orient="row",
    )


def test_valid_row_passes():
    df = _frame([("PW1", "TR001", "SP04", 20, dt.date(2024, 6, 1))])
    out = R.add_reject_reason(df, VALID, SNAP, 520)
    assert out["reject_reason"].to_list() == [None]


def test_every_reject_reason_fires():
    rows = [
        (None, "TR001", "SP04", 10, dt.date(2024, 1, 1)),     # null_pathway_id
        ("PWX", "TR001", "SP04", 10, dt.date(2024, 1, 1)),    # first PWX -> valid
        ("PWX", "TR001", "SP04", 11, dt.date(2024, 1, 2)),    # duplicate_pathway_id
        ("PW2", "TR999", "SP04", 10, dt.date(2024, 1, 1)),    # orphan_trust_code
        ("PW3", "TR001", None, 10, dt.date(2024, 1, 1)),      # unmapped_specialty
        ("PW4", "TR001", "SP04", None, dt.date(2024, 1, 1)),  # weeks_waited_unparseable
        ("PW5", "TR001", "SP04", -5, dt.date(2024, 1, 1)),    # weeks_waited_negative
        ("PW6", "TR001", "SP04", 9999, dt.date(2024, 1, 1)),  # weeks_waited_out_of_range
        ("PW7", "TR001", "SP04", 10, None),                   # referral_date_unparseable
        ("PW8", "TR001", "SP04", 10, dt.date(2030, 1, 1)),    # referral_date_in_future
    ]
    out = R.add_reject_reason(_frame(rows), VALID, SNAP, 520)
    assert out["reject_reason"].to_list() == [
        "null_pathway_id", None, "duplicate_pathway_id", "orphan_trust_code",
        "unmapped_specialty", "weeks_waited_unparseable", "weeks_waited_negative",
        "weeks_waited_out_of_range", "referral_date_unparseable", "referral_date_in_future",
    ]
