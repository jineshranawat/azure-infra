"""Incremental & Streaming demonstrations (Problems 2,3,14,15,18,19,20,21,36,37,44).

These use Foundry's @incremental framework. On a first build each runs as a
snapshot (by design); from the second build onward they process deltas. The
delta LOGIC is proven independently by unit tests over lib.scd (crafted multi-batch
fixtures) so correctness does not depend on triggering an incremental run here.
"""
from __future__ import annotations

import datetime as dt

import polars as pl
from transforms.api import transform, incremental, Input, Output, LightweightInput, LightweightOutput

from myproject import paths
from myproject.lib import scd as SCD


# Problems 2 (incremental append), 3 (watermark via 'added'), 37 (snapshot ref lookup).
@incremental(v2_semantics=True, snapshot_inputs=["specialties"])
@transform.using(
    raw=Input(paths.RTT_PATHWAYS_RAW),
    specialties=Input(paths.SPECIALTY_REFERENCE),
    out=Output(paths.INC_PATHWAYS_APPENDED),
)
def inc_pathways_appended(raw, specialties, out):
    added = raw.polars()                 # 'added': only new rows when incremental
    spec = specialties.polars()          # snapshot input: always the full reference
    surgical = spec.select(["specialty_name", "specialty_group"]).unique()
    enriched = added.join(surgical, left_on="specialty", right_on="specialty_name", how="left")
    out.write_table(enriched)            # default 'modify' appends the new batch


# Problems 14 (CDC change feed), 15 (latest-state), 20 (late/out-of-order via state compare).
@incremental(v2_semantics=True)
@transform.using(
    minimised=Input(paths.RTT_PATHWAYS_MINIMISED),
    out=Output(paths.CDC_CHANGE_FEED),
)
def cdc_change_feed(minimised, out):
    schema = {"pathway_id": pl.Utf8, "pathway_status": pl.Utf8, "change_type": pl.Utf8}
    current = minimised.polars("current").select(["pathway_id", "pathway_status"])
    try:
        previous = out.polars("previous", schema).select(["pathway_id", "pathway_status"])
    except Exception:
        # First build: the output has never been materialised (schemaless) -> no prior state.
        previous = pl.DataFrame(schema={"pathway_id": pl.Utf8, "pathway_status": pl.Utf8})
    changes = SCD.classify_cdc(current, previous, "pathway_id", ["pathway_status"])
    result = current.join(changes, on="pathway_id", how="left")
    out.set_mode("replace")              # output = current state annotated with change_type
    out.write_table(result)


# Problems 18 (SCD2 history), 36 (snapshot->incremental), 44 (backfill-safe append).
@incremental(v2_semantics=True, snapshot_inputs=["minimised"])
@transform.using(
    minimised=Input(paths.RTT_PATHWAYS_MINIMISED),
    out=Output(paths.STATUS_HISTORY),
)
def status_history(minimised, out):
    df = (
        minimised.polars()
        .select(["pathway_id", "pathway_status"])
        .with_columns(pl.lit(dt.date.today().isoformat()).alias("snapshot_date"))
    )
    out.write_table(df)                  # default 'modify' appends today's snapshot


# Problem 18 (effective dating) - derive SCD2 segments from the accumulated history.
@transform.using(
    history=Input(paths.STATUS_HISTORY),
    out=Output(paths.STATUS_SCD2),
)
def status_scd2(history: LightweightInput, out: LightweightOutput):
    records = [
        {"key": r["pathway_id"], "value": r["pathway_status"], "snapshot_date": r["snapshot_date"]}
        for r in history.polars().to_dicts()
    ]
    segments = SCD.build_scd2(records)
    empty = {"key": pl.Utf8, "value": pl.Utf8, "valid_from": pl.Utf8,
             "valid_to": pl.Utf8, "is_current": pl.Boolean}
    out.write_table(pl.DataFrame(segments) if segments else pl.DataFrame(schema=empty))


# Problem 21 (windowing) - top-5 longest waiters per trust via a window function.
@transform.using(
    metrics=Input(paths.RTT_PATHWAYS_METRICS),
    out=Output(paths.LONGEST_WAITERS_BY_TRUST),
)
def window_rank_longest_waiters(metrics: LightweightInput, out: LightweightOutput):
    df = metrics.polars()
    ranked = (
        df.with_columns(
            pl.col("weeks_waited").rank("ordinal", descending=True).over("trust_code").alias("wait_rank_in_trust")
        )
        .filter(pl.col("wait_rank_in_trust") <= 5)
        .sort(["trust_code", "wait_rank_in_trust"])
        .select(["trust_code", "pathway_id", "specialty_name", "weeks_waited", "wait_band", "wait_rank_in_trust"])
    )
    out.write_table(ranked)
