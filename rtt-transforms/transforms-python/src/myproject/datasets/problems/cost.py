"""Cost Optimisation (Problems 26, 27, 28).

Right-sizing: this tiny aggregation runs on a single small node via @lightweight
(1 core / 2 GB) instead of a Spark cluster - the cheapest correct engine for the
volume (28). Lightweight also lets Foundry keep warm pools and scale to zero when
idle (26, 27). Contrast with problems/perf.py, which reaches for Spark only where a
distributed shuffle would genuinely be needed.
"""
from __future__ import annotations

import polars as pl
from transforms.api import transform, lightweight, Input, Output, LightweightInput, LightweightOutput

from myproject import paths


@lightweight(cpu_cores=1, memory_gb=2)
@transform(
    clean=Input(paths.RTT_PATHWAYS_CLEAN),
    out=Output(paths.COST_REGION_SUMMARY),
)
def cost_optimised_summary(clean: LightweightInput, out: LightweightOutput) -> None:
    df = clean.polars()
    summary = (
        df.group_by("trust_code")
        .agg(
            pl.len().alias("pathways"),
            pl.col("weeks_waited").mean().round(1).alias("avg_weeks"),
            pl.col("weeks_waited").max().alias("max_weeks"),
        )
        .sort("trust_code")
    )
    out.write_table(summary)
