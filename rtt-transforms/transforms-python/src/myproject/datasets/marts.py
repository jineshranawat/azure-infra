"""Gold marts for the national return and for Clare (RTT ops manager).

- rtt_trust_specialty_perf: trust x specialty performance, incl. % within 18 weeks
  and whether the 92% constitutional standard is met.
- rtt_ptl: the Patient Tracking List - one row per ACTIVE (incomplete) pathway,
  longest waiters first, with RAG + breach flags for triage.
"""
from __future__ import annotations

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput, Check
from transforms import expectations as E

from myproject import paths
from myproject.lib.rtt_metrics import CONSTITUTIONAL_STANDARD

_PTL_COLS = [
    "pathway_id", "trust_code", "trust_name", "region", "specialty_name",
    "specialty_group", "referral_date", "weeks_waited", "wait_band", "rag_status",
    "is_breach_18w", "is_52w_breach", "is_65w_breach",
]


@transform.using(
    metrics=Input(paths.RTT_PATHWAYS_METRICS),
    providers=Input(paths.PROVIDER_REFERENCE),
    perf=Output(
        paths.RTT_TRUST_SPECIALTY_PERF,
        checks=[Check(E.col("total_pathways").gt(0), "perf: non-empty groups", on_error="WARN")],
    ),
    ptl=Output(
        paths.RTT_PTL,
        checks=[Check(E.primary_key("pathway_id"), "ptl: one row per active pathway", on_error="FAIL")],
    ),
)
def compute(metrics: LightweightInput, providers: LightweightInput,
            perf: LightweightOutput, ptl: LightweightOutput) -> None:
    df = metrics.polars()
    prov = providers.polars().select(["trust_code", "trust_name", "region", "trust_type"])
    df = df.join(prov, on="trust_code", how="left")

    perf_df = (
        df.group_by(["trust_code", "trust_name", "region", "specialty_name"])
        .agg([
            pl.len().alias("total_pathways"),
            pl.col("within_target").sum().alias("within_18w"),
            pl.col("is_breach_18w").sum().alias("breaches_18w"),
            pl.col("is_52w_breach").sum().alias("breaches_52w"),
            pl.col("is_65w_breach").sum().alias("breaches_65w"),
            pl.col("weeks_waited").mean().round(1).alias("avg_weeks_waited"),
            pl.col("weeks_waited").max().alias("max_weeks_waited"),
        ])
        .with_columns((pl.col("within_18w") / pl.col("total_pathways")).round(4).alias("pct_within_18w"))
        .with_columns((pl.col("pct_within_18w") >= CONSTITUTIONAL_STANDARD).alias("meets_constitutional_standard"))
        .sort(["trust_code", "specialty_name"])
    )

    ptl_df = (
        df.filter(pl.col("pathway_status") == "Active")
        .select(_PTL_COLS)
        .sort("weeks_waited", descending=True)
    )

    perf.write_table(perf_df)
    ptl.write_table(ptl_df)
