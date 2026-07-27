"""Security & Governance - in-pipeline PII protection (Problems 29, 31; supports 48).

Sources the metrics grain (breach flags/bands/RAG), pseudonymises nhs_number and
DROPS the raw value, producing a PII-free, analytics-rich dataset that backs the
operational Pathway/Patient Ontology objects. This complements the platform-native
controls documented in /docs/security.md: Markings (29), Restricted Views for
row-level access (30), property security (31), encrypted Data Connection secrets (47).
"""
from __future__ import annotations

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput, Check
from transforms import expectations as E

from myproject import paths
from myproject.lib import pii

_ONTOLOGY_COLS = [
    "patient_pseudo_id", "pathway_id", "trust_code", "specialty_name", "specialty_code",
    "specialty_group", "rtt_target_weeks", "referral_date", "weeks_waited", "pathway_status",
    "is_breach_18w", "is_52w_breach", "is_65w_breach", "wait_band", "rag_status",
]


@transform.using(
    metrics=Input(paths.RTT_PATHWAYS_METRICS),
    out=Output(
        paths.PATIENT_PSEUDONYMISED,
        checks=[Check(E.primary_key("pathway_id"), "patient: PK unique", on_error="FAIL")],
    ),
)
def pseudonymise_patient(metrics: LightweightInput, out: LightweightOutput) -> None:
    df = metrics.polars()
    result = (
        df.with_columns(
            pl.col("nhs_number").map_elements(pii.pseudonymise, return_dtype=pl.Utf8).alias("patient_pseudo_id")
        )
        .drop("nhs_number")
        .select(_ONTOLOGY_COLS)
    )
    # Hard post-condition: raw NHS number must never leave the pipeline.
    assert "nhs_number" not in result.columns, "raw nhs_number must not leave the pipeline"
    out.write_table(result)
