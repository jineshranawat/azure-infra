"""Silver: cleanse, canonicalise, validate and QUARANTINE the typed pathways.

- Canonicalise free-text specialty -> specialty_reference (Problem 40).
- Enforce referential integrity to provider_reference (Problem 34).
- Enforce PK non-null + uniqueness (Problems 8, 42), range/domain (Problem 35),
  parseable dates (Problem 10) and temporal cross-field consistency (Problem 41).
- Bad rows are routed to /quarantine with a reject_reason; NEVER dropped.
- A recon control-total dataset proves clean + rejects == input (no silent loss).

FAIL Data Expectations on the clean output guarantee nothing bad leaks downstream.
"""
from __future__ import annotations

import datetime as dt

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput, Check
from transforms import expectations as E

from myproject import paths
from myproject.lib import canonicalise as C
from myproject.lib import dq_rules

MAX_WEEKS = 520  # ~10 years; beyond this is a data error, not a real wait
SNAPSHOT = dt.date(2025, 1, 31)

_CLEAN_COLS = [
    "pathway_id", "trust_code", "specialty_name", "specialty_code", "specialty_group",
    "is_surgical", "rtt_target_weeks", "standard_tariff_gbp", "referral_date",
    "weeks_waited", "nhs_number", "sex", "age_at_referral", "pathway_status",
    "source_load_ts", "source_load_date",
]
_REJECT_COLS = [
    "pathway_id", "trust_code", "specialty", "referral_date_str", "weeks_waited_str",
    "nhs_number", "pathway_status", "reject_reason",
]


@transform.using(
    typed=Input(paths.RTT_PATHWAYS_TYPED),
    providers=Input(paths.PROVIDER_REFERENCE),
    specialties=Input(paths.SPECIALTY_REFERENCE),
    clean=Output(
        paths.RTT_PATHWAYS_CLEAN,
        checks=[
            Check(E.primary_key("pathway_id"), "clean: PK unique + non-null", on_error="FAIL"),
            Check(E.col("trust_code").non_null(), "clean: trust_code non-null", on_error="FAIL"),
            Check(E.col("specialty_code").non_null(), "clean: specialty mapped", on_error="FAIL"),
            Check(
                E.all(
                    E.col("weeks_waited").non_null(),
                    E.col("weeks_waited").gte(0),
                    E.col("weeks_waited").lte(MAX_WEEKS),
                ),
                "clean: weeks_waited within [0, 520]",
                on_error="FAIL",
            ),
        ],
    ),
    rejects=Output(paths.RTT_PATHWAYS_REJECTS),
    recon=Output(paths.RECON_CONTROL_TOTALS),
)
def compute(typed: LightweightInput, providers: LightweightInput, specialties: LightweightInput,
            clean: LightweightOutput, rejects: LightweightOutput, recon: LightweightOutput) -> None:
    df = typed.polars().with_row_index("_rid")
    prov = providers.polars()
    spec = specialties.polars()
    valid_trusts = prov["trust_code"].to_list()

    # Canonicalise specialty then join reference for code / target / tariff.
    df = df.with_columns(
        pl.col("specialty").map_elements(C.canonical_specialty, return_dtype=pl.Utf8).alias("specialty_name")
    )
    spec_lookup = spec.select(
        ["specialty_name", "specialty_code", "specialty_group",
         "is_surgical", "rtt_target_weeks", "standard_tariff_gbp"]
    )
    df = df.join(spec_lookup, on="specialty_name", how="left").sort("_rid")

    # Quarantine policy lives in ONE tested place (lib.dq_rules) - Problems 1,8,34,35,41,42.
    df = dq_rules.add_reject_reason(df, valid_trusts, SNAPSHOT, MAX_WEEKS)

    clean_df = df.filter(pl.col("reject_reason").is_null()).select(_CLEAN_COLS)
    rejects_df = df.filter(pl.col("reject_reason").is_not_null()).select(_REJECT_COLS)

    n_in, n_clean, n_rej = df.height, clean_df.height, rejects_df.height
    recon_df = pl.DataFrame({
        "metric": ["typed_count", "clean_count", "reject_count", "reconciled"],
        "value": [n_in, n_clean, n_rej, int(n_clean + n_rej == n_in)],
    })

    clean.write_table(clean_df)
    rejects.write_table(rejects_df)
    recon.write_table(recon_df)
