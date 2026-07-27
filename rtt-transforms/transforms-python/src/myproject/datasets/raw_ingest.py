"""Bronze: type the raw all-string RTT landing table.

Problem 6 (ingest-time schema/type validation): casts referral_date / weeks_waited /
age_at_referral / source_load_ts using the tested lib.canonicalise parsers.
Unparseable values become NULL and are flagged; Bronze keeps every row (rows are
quarantined later in Silver, never silently dropped).
"""
from __future__ import annotations

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput, Check
from transforms import expectations as E

from myproject import paths
from myproject.lib import canonicalise as C


@transform.using(
    src=Input(paths.RTT_PATHWAYS_RAW),
    out=Output(
        paths.RTT_PATHWAYS_TYPED,
        checks=[
            Check(E.col("pathway_id").non_null(), "raw: pathway_id present", on_error="WARN"),
            Check(E.col("trust_code").non_null(), "raw: trust_code present", on_error="WARN"),
        ],
    ),
)
def compute(src: LightweightInput, out: LightweightOutput) -> None:
    df = src.polars()
    df = df.with_columns([
        pl.col("referral_date").alias("referral_date_str"),
        pl.col("weeks_waited").alias("weeks_waited_str"),
        pl.col("age_at_referral").alias("age_at_referral_str"),
    ]).with_columns([
        pl.col("referral_date").map_elements(C.parse_date, return_dtype=pl.Date).alias("referral_date"),
        pl.col("weeks_waited").map_elements(C.to_int, return_dtype=pl.Int64).alias("weeks_waited"),
        pl.col("age_at_referral").map_elements(C.to_int, return_dtype=pl.Int64).alias("age_at_referral"),
        pl.col("source_load_ts").map_elements(C.parse_date, return_dtype=pl.Date).alias("source_load_date"),
    ]).with_columns([
        (pl.col("weeks_waited").is_null()
         & pl.col("weeks_waited_str").is_not_null()
         & (pl.col("weeks_waited_str").str.strip_chars() != "")).alias("weeks_parse_failed"),
        (pl.col("referral_date").is_null()
         & pl.col("referral_date_str").is_not_null()
         & (pl.col("referral_date_str").str.strip_chars() != "")).alias("date_parse_failed"),
    ])
    out.write_table(df)
