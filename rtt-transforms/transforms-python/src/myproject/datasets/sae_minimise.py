"""Silver leaving-gate: apply IG data-minimisation (SAE) rules before data leaves Silver.

Legal/PII constraint: sex, age_at_referral and source_load_ts must not leave Silver.
The columns to drop are read from sae_column_rules (metadata-driven, NOT hard-coded),
so IG can change policy without a code change. A hard post-condition guard fails the
build if any banned column leaks, and an audit log records exactly what was removed.
"""
from __future__ import annotations

import datetime as dt

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput, Check
from transforms import expectations as E

from myproject import paths
from myproject.lib import sae_minimiser as S

SAE_DATASET_NAME = "rtt_pathways"
_MUST_BE_ABSENT = ["sex", "age_at_referral", "source_load_ts"]


@transform.using(
    clean=Input(paths.RTT_PATHWAYS_CLEAN),
    rules=Input(paths.SAE_COLUMN_RULES),
    minimised=Output(
        paths.RTT_PATHWAYS_MINIMISED,
        checks=[Check(E.primary_key("pathway_id"), "minimised: PK preserved", on_error="FAIL")],
    ),
    audit=Output(paths.SAE_AUDIT_LOG),
)
def compute(clean: LightweightInput, rules: LightweightInput,
            minimised: LightweightOutput, audit: LightweightOutput) -> None:
    df = clean.polars()
    rules_df = rules.polars()

    drop_cols = S.columns_to_drop(rules_df.to_dicts(), SAE_DATASET_NAME)
    drop_present = [c for c in drop_cols if c in df.columns]
    kept, dropped = S.apply_minimisation(df.columns, drop_present)
    minimised_df = df.select(kept)

    # Hard post-condition: banned columns must not survive. Fails the build if they do.
    if not S.assert_minimised(minimised_df.columns, _MUST_BE_ABSENT):
        raise ValueError(f"SAE minimisation failed: banned columns still present in {minimised_df.columns}")

    applied_ts = dt.datetime.now(dt.timezone.utc).isoformat()
    audit_df = (
        rules_df.filter(pl.col("dataset_name") == SAE_DATASET_NAME)
        .with_columns([
            pl.lit(applied_ts).alias("applied_ts"),
            pl.lit(paths.RTT_PATHWAYS_MINIMISED).alias("applied_to_dataset"),
            pl.col("column_name").is_in(dropped).alias("column_dropped"),
        ])
    )

    minimised.write_table(minimised_df)
    audit.write_table(audit_df)
