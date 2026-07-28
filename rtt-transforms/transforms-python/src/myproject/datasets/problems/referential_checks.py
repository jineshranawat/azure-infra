"""Referential-integrity, cross-column and native-freshness expectations (were pending).

Implements documented Data Expectations the core build had not used:
- Foreign key  : E.col(..).is_in_foreign_col(E.dataset_ref(..).col(..)) - referential integrity
                 (experimental / expensive per docs -> WARN).
- Cross-column : E.col('referral_date').is_before_col('source_load_date') - temporal ordering.
- Freshness    : E.col('load_ts').is_after(<static bound>) on a parsed timestamp column.
All WARN: referential/temporal/freshness are advisory (docs severity strategy).

Input:  rtt_pathways_clean  (+ provider_reference, referenced by the FK expectation)
Output: rtt_clean_ri_checked (clean rows + a parsed `load_ts` timestamp column)
Reference: foundry/transforms-python/unit-tests (Foreign key / Cross-column / Timestamp).
"""
from __future__ import annotations

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput, Check
from transforms import expectations as E

from myproject import paths


@transform.using(
    clean=Input(paths.RTT_PATHWAYS_CLEAN),
    # provider_reference is declared as an input so the FK reference dataset is resolvable.
    providers=Input(paths.PROVIDER_REFERENCE),
    out=Output(
        paths.RTT_CLEAN_RI_CHECKED,
        checks=[
            # FOREIGN KEY (referential integrity): every trust_code must exist in
            # provider_reference.trust_code. Experimental + expensive -> WARN.
            Check(
                E.col("trust_code").is_in_foreign_col(
                    # dataset_ref takes the INPUT PARAMETER NAME ('providers'), not the path
                    E.dataset_ref("providers").col("trust_code")
                ),
                "ri: trust_code exists in provider_reference", on_error="WARN",
            ),
            # CROSS-COLUMN temporal: a referral cannot post-date the extract's load date.
            Check(
                E.col("referral_date").is_before_col("source_load_date"),
                "ri: referral_date before source_load_date", on_error="WARN",
            ),
            # NATIVE FRESHNESS: the parsed load timestamp is after a static floor.
            Check(
                E.col("load_ts").is_after("2020-01-01T00:00:00+0000"),
                "ri: load_ts after 2020", on_error="WARN",
            ),
        ],
    ),
)
def compute(clean: LightweightInput, providers: LightweightInput, out: LightweightOutput) -> None:
    # Parse the string watermark into a real Datetime so native timestamp expectations
    # have a correctly-typed target column.
    df = clean.polars().with_columns(
        pl.col("source_load_ts").str.to_datetime(strict=False).alias("load_ts")
    )
    out.write_table(df)
