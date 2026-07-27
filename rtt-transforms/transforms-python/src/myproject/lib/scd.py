"""Incremental / streaming primitives (Problems 14,15,18,19,20,21).

Pure + Polars helpers so the tricky delta logic is unit-tested with crafted
multi-batch fixtures, then reused verbatim from @incremental transforms.
"""
from __future__ import annotations

from collections import defaultdict
from typing import List, Sequence

import polars as pl


def dedupe_latest(df: pl.DataFrame, key, order_col: str) -> pl.DataFrame:
    """Keep the latest row per key by order_col (Problems 15 dedup, 20 late-arrival,
    19 idempotent merge: running twice yields the same result)."""
    return df.sort(order_col, descending=True).unique(subset=key, keep="first", maintain_order=True)


def high_water_mark(values: Sequence):
    """Max non-null value = the watermark to resume from (Problem 3)."""
    vals = [v for v in values if v is not None]
    return max(vals) if vals else None


def classify_cdc(current: pl.DataFrame, previous: pl.DataFrame, key: str, compare_cols) -> pl.DataFrame:
    """Change feed of [key, change_type] with INSERT/UPDATE/UNCHANGED/DELETE (Problem 14)."""
    compare_cols = list(compare_cols)
    prev_keys = previous[key].to_list() if previous.height else []
    cur_keys = current[key].to_list() if current.height else []

    prev_lookup = previous.select([key] + compare_cols).rename({c: f"{c}__prev" for c in compare_cols})
    joined = current.join(prev_lookup, on=key, how="left")
    changed = pl.any_horizontal([pl.col(c) != pl.col(f"{c}__prev") for c in compare_cols])
    upserts = joined.select([
        pl.col(key),
        pl.when(~pl.col(key).is_in(prev_keys)).then(pl.lit("INSERT"))
          .when(changed).then(pl.lit("UPDATE"))
          .otherwise(pl.lit("UNCHANGED")).alias("change_type"),
    ])
    deletes = previous.filter(~pl.col(key).is_in(cur_keys)).select([
        pl.col(key), pl.lit("DELETE").alias("change_type"),
    ])
    return pl.concat([upserts, deletes])


def build_scd2(observations: List[dict], key: str = "key", value: str = "value",
               date: str = "snapshot_date") -> List[dict]:
    """Effective-date a stream of observations into SCD Type 2 segments (Problem 18).

    Consecutive identical values are merged; the last segment per key is_current.
    """
    by_key = defaultdict(list)
    for o in observations:
        by_key[o[key]].append(o)

    out: List[dict] = []
    for k, obs in by_key.items():
        obs = sorted(obs, key=lambda x: x[date])
        segments: List[dict] = []
        for o in obs:
            if segments and segments[-1]["value"] == o[value]:
                continue  # value unchanged -> extend current segment
            segments.append({"key": k, "value": o[value], "valid_from": o[date]})
        for i, seg in enumerate(segments):
            seg["valid_to"] = segments[i + 1]["valid_from"] if i + 1 < len(segments) else None
            seg["is_current"] = i == len(segments) - 1
            out.append(seg)
    return out


def rolling_sum(values: Sequence[float], window: int) -> List[float]:
    """Simple trailing moving sum over an ordered series (Problem 21 windowing)."""
    if window <= 0:
        raise ValueError("window must be positive")
    out: List[float] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        out.append(sum(values[start:i + 1]))
    return out
