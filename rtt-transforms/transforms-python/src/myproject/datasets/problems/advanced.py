"""Advanced patterns - live demo transform (A4: data profiling / observability).

Profiles rtt_pathways_clean into a per-column observability table using the tested
lib.advanced_patterns.profile_column. The other patterns (A1-A3, A5-A11) are pure and
proven in lib.advanced_patterns + test_advanced_patterns; this transform makes A4 visible
as an actual dataset so you can see column health at a glance.

Input:  rtt_pathways_clean (Silver)
Output: rtt_data_profile  -> one row per column: count, nulls, null_pct, distinct, min, max
"""
from __future__ import annotations

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput

from myproject import paths
from myproject.lib import advanced_patterns as A


@transform.using(
    clean=Input(paths.RTT_PATHWAYS_CLEAN),
    out=Output(paths.RTT_DATA_PROFILE),
)
def data_profile(clean: LightweightInput, out: LightweightOutput) -> None:
    df = clean.polars()
    rows = []
    for col in df.columns:
        p = A.profile_column(df[col].to_list())
        rows.append({
            "column": col,
            "count": p["count"],
            "nulls": p["nulls"],
            "null_pct": p["null_pct"],
            "distinct": p["distinct"],
            "min_value": None if p["min"] is None else str(p["min"]),
            "max_value": None if p["max"] is None else str(p["max"]),
        })
    out.write_table(pl.DataFrame(rows))
