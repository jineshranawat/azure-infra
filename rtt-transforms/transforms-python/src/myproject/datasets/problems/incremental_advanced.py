"""Advanced @incremental patterns - implements documented features left partial.

Implements:
- semantic_version : bump the integer to force a one-off non-incremental rebuild
  (docs: foundry/data-integration/incremental).
- ctx.abort_job()  : the documented no-data pattern - skip the write so no empty
  transaction is committed and no downstream schedule is triggered
  (docs: foundry/data-integration/datasets - "use output.abort() for no-data scenarios").
- .with_resources(cpu_cores, memory_gb) : documented single-node resource sizing
  (docs: foundry/transforms-python/transforms).
- read mode 'added' : process only rows new since the last build (watermark behaviour).

Input:  rtt_pathways_raw   ('added' rows when running incrementally)
Output: rtt_pathways_delta (APPEND of each new batch, stamped with an idempotency key)
"""
from __future__ import annotations

import polars as pl
from transforms.api import transform, incremental, Input, Output, LightweightInput, LightweightOutput

from myproject import paths
from myproject.lib import advanced_patterns as A


# @incremental goes ABOVE @transform.using. semantic_version=1: increment to force a full
# rebuild once (e.g. after a logic change that must reprocess history).
@incremental(v2_semantics=True, semantic_version=1)
@transform.using(
    raw=Input(paths.RTT_PATHWAYS_RAW),
    out=Output(paths.RTT_PATHWAYS_DELTA),
).with_resources(cpu_cores=1, memory_gb=2)   # right-size this small single-node job (cost)
def compute(ctx, raw: LightweightInput, out: LightweightOutput) -> None:
    # INPUT: read mode defaults to 'added' under @incremental -> only new rows (watermark).
    added = raw.polars()

    # NO-DATA PATTERN: if there is nothing new, abort the job so we do NOT commit an empty
    # transaction (empty transactions still trigger downstream schedules - see docs gotcha).
    if added.height == 0:
        ctx.abort_job()
        return

    # A11 idempotency_key: stamp each row with a replay-safe key so re-processing the same
    # batch is exactly-once (deterministic key from source + primary key).
    added = added.with_columns(
        pl.col("pathway_id").map_elements(
            lambda p: A.idempotency_key(["raw", p]), return_dtype=pl.Utf8
        ).alias("idem_key")
    )

    # OUTPUT: default write mode is 'modify' under @incremental -> APPEND this batch.
    out.write_table(added)
