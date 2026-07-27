"""Silver metrics: RTT breach flags, wait bands and RAG status.

Reuses the tested lib.rtt_metrics functions verbatim (so the unit tests that prove
those functions also prove the pipeline column values). National definitions:
18-week target, 52/65-week reportable breaches.
"""
from __future__ import annotations

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput, Check
from transforms import expectations as E

from myproject import paths
from myproject.lib import rtt_metrics as M


@transform.using(
    minimised=Input(paths.RTT_PATHWAYS_MINIMISED),
    out=Output(
        paths.RTT_PATHWAYS_METRICS,
        checks=[
            Check(E.primary_key("pathway_id"), "metrics: PK unique", on_error="FAIL"),
            Check(E.col("wait_band").non_null(), "metrics: wait_band computed", on_error="FAIL"),
        ],
    ),
)
def compute(minimised: LightweightInput, out: LightweightOutput) -> None:
    df = minimised.polars().with_columns([
        pl.col("weeks_waited").map_elements(M.is_breach, return_dtype=pl.Boolean).alias("is_breach_18w"),
        pl.col("weeks_waited").map_elements(M.is_52_week_breach, return_dtype=pl.Boolean).alias("is_52w_breach"),
        pl.col("weeks_waited").map_elements(M.is_65_week_breach, return_dtype=pl.Boolean).alias("is_65w_breach"),
        pl.col("weeks_waited").map_elements(M.wait_band, return_dtype=pl.Utf8).alias("wait_band"),
        pl.col("weeks_waited").map_elements(M.rag_status, return_dtype=pl.Utf8).alias("rag_status"),
    ]).with_columns(
        (~pl.col("is_breach_18w")).alias("within_target")
    )
    out.write_table(df)
