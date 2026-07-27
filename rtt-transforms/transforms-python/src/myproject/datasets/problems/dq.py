"""Data Quality & Consistency - dedicated monitors.

Foundation already covers: 1 (dedupe), 6 (type validation), 8 (nulls), 10 (dates),
34 (orphans), 35 (range), 40 (canonicalisation), 41 (cross-field), 42 (PK) via
raw_ingest + clean_silver (lib.dq_rules) with FAIL/WARN Data Expectations.

This module adds:
- Problem 9: schema-drift detection against a declared column contract (FAIL if a
  required column disappears; WARN if unexpected columns appear).
- Problem 43: data freshness / staleness monitor (WARN if source is stale).
"""
from __future__ import annotations

import datetime as dt

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput, Check
from transforms import expectations as E

from myproject import paths
from myproject.lib import schema_contract as SC
from myproject.lib import freshness as F

EXPECTED_RAW_COLUMNS = [
    "pathway_id", "trust_code", "specialty", "referral_date", "weeks_waited",
    "nhs_number", "sex", "age_at_referral", "pathway_status", "source_load_ts",
]
REQUIRED_RAW_COLUMNS = ["pathway_id", "trust_code", "specialty", "referral_date", "weeks_waited"]

# RTT is a monthly return; the reference "today" would be CURRENT_DATE in production.
AS_OF = dt.date(2026, 7, 26)
MAX_AGE_DAYS = 45


@transform.using(
    src=Input(paths.RTT_PATHWAYS_RAW),
    report=Output(
        paths.SCHEMA_DRIFT_REPORT,
        checks=[
            Check(E.col("missing_count").lte(0), "P9: no required columns missing", on_error="FAIL"),
            Check(E.col("unexpected_count").lte(0), "P9: no unexpected columns", on_error="WARN"),
        ],
    ),
)
def p9_schema_drift(src: LightweightInput, report: LightweightOutput) -> None:
    cols = src.polars(lazy=True).collect_schema().names()
    drift = SC.detect_drift(cols, EXPECTED_RAW_COLUMNS)
    report.write_table(pl.DataFrame({
        "checked_ts": [dt.datetime.now(dt.timezone.utc).isoformat()],
        "missing_columns": [", ".join(drift["missing"]) or None],
        "unexpected_columns": [", ".join(drift["unexpected"]) or None],
        "missing_count": [len(drift["missing"])],
        "unexpected_count": [len(drift["unexpected"])],
    }))


@transform.using(
    metrics=Input(paths.RTT_PATHWAYS_METRICS),
    report=Output(
        paths.FRESHNESS_REPORT,
        checks=[Check(E.col("days_since_latest").lte(MAX_AGE_DAYS), "P43: source data is fresh", on_error="WARN")],
    ),
)
def p43_freshness(metrics: LightweightInput, report: LightweightOutput) -> None:
    latest = metrics.polars().select(pl.col("source_load_date").max()).item()
    report.write_table(pl.DataFrame({
        "as_of": [AS_OF.isoformat()],
        "latest_source_load_date": [latest.isoformat() if latest is not None else None],
        "days_since_latest": [F.days_since(latest, AS_OF)],
        "max_age_days": [MAX_AGE_DAYS],
        "is_stale": [F.is_stale(latest, AS_OF, MAX_AGE_DAYS)],
    }))
