"""Reusable Polars quarantine policy for Silver (Problems 1, 8, 34, 35, 41, 42).

`add_reject_reason` encodes the full quarantine policy in ONE tested place, so the
exact same logic runs in the clean_silver transform and in unit tests with crafted
frames. reject_reason is null for rows that pass every rule; non-null names the
first rule the row violated (order = precedence).
"""
from __future__ import annotations

import datetime as dt

import polars as pl


def add_reject_reason(df: pl.DataFrame, valid_trusts, snapshot: dt.date, max_weeks: int) -> pl.DataFrame:
    """Add `_is_dup_pk` and `reject_reason` to a typed+joined pathways frame.

    Expected columns: pathway_id (Utf8), trust_code (Utf8), specialty_code (Utf8;
    null if unmapped), weeks_waited (Int; null if unparseable), referral_date (Date;
    null if unparseable). Rows must already be in their canonical order (first
    occurrence of a pathway_id wins for de-duplication).
    """
    valid_trusts = list(valid_trusts)
    df = df.with_columns((~pl.col("pathway_id").is_first_distinct()).alias("_is_dup_pk"))
    reason = (
        pl.when(pl.col("pathway_id").is_null() | (pl.col("pathway_id").str.strip_chars() == ""))
          .then(pl.lit("null_pathway_id"))
        .when(pl.col("_is_dup_pk")).then(pl.lit("duplicate_pathway_id"))
        .when(~pl.col("trust_code").is_in(valid_trusts)).then(pl.lit("orphan_trust_code"))
        .when(pl.col("specialty_code").is_null()).then(pl.lit("unmapped_specialty"))
        .when(pl.col("weeks_waited").is_null()).then(pl.lit("weeks_waited_unparseable"))
        .when(pl.col("weeks_waited") < 0).then(pl.lit("weeks_waited_negative"))
        .when(pl.col("weeks_waited") > max_weeks).then(pl.lit("weeks_waited_out_of_range"))
        .when(pl.col("referral_date").is_null()).then(pl.lit("referral_date_unparseable"))
        .when(pl.col("referral_date") > snapshot).then(pl.lit("referral_date_in_future"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )
    return df.with_columns(reason.alias("reject_reason"))
