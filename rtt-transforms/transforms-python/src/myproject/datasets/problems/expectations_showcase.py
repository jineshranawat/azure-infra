"""Documented Data Expectations showcase - implements catalog features per Palantir docs.

Passes rtt_pathways_clean through unchanged and attaches a broad set of Data Expectations
from the documented catalog that the core pipeline did NOT already use: schema structure,
composite group-by uniqueness, membership (is_in), regex (rlike), conditional
(when/otherwise), column type (has_type), aggregate (null_percentage), row count
(group_by().count()) and column existence (exists).

Reference (read + implemented from): foundry/transforms-python/unit-tests
    -> "Data expectations" / "Expectation types quick reference".

Input:  rtt_pathways_clean (Silver)
Output: rtt_clean_certified (identical rows; the value is the build-enforced checks attached)
"""
from __future__ import annotations

import polars as pl
from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput, Check
from transforms import expectations as E

from myproject import paths

# Reusable composite (docs: E.all(...) = logical AND).
_VALID_WEEKS = E.all(
    E.col("weeks_waited").non_null(),
    E.col("weeks_waited").gte(0),
    E.col("weeks_waited").lte(520),
)


@transform.using(
    clean=Input(paths.RTT_PATHWAYS_CLEAN),
    out=Output(
        paths.RTT_CLEAN_CERTIFIED,
        checks=[
            # Schema structure  (docs: E.schema().contains({...}))
            Check(E.schema().contains({"pathway_id": pl.String, "trust_code": pl.String,
                                       "weeks_waited": pl.Int64}),
                  "certified: schema contract", on_error="FAIL"),
            # Composite uniqueness  (docs: E.group_by(...).is_unique())
            Check(E.group_by("pathway_id").is_unique(),
                  "certified: pathway_id unique", on_error="FAIL"),
            # Membership set  (docs: E.col().is_in(...) - note: FAILS on nulls)
            Check(E.col("trust_code").is_in("TR001", "TR002", "TR003", "TR004", "TR005"),
                  "certified: trust_code in reference set", on_error="FAIL"),
            # Regex  (docs: E.col().rlike(regex))
            Check(E.col("specialty_code").rlike(r"^SP\d{2}$"),
                  "certified: specialty_code format", on_error="FAIL"),
            # Conditional  (docs: E.when(cond, then).otherwise(else))
            Check(E.when(E.col("pathway_status").equals("Active"),
                         E.col("weeks_waited").gte(0)).otherwise(E.true()),
                  "certified: active pathway wait non-negative", on_error="FAIL"),
            # Composite range  (docs: E.all(...))
            Check(_VALID_WEEKS, "certified: weeks within [0,520]", on_error="FAIL"),
            # Column type  (docs: E.col().has_type(pl.X))
            Check(E.col("weeks_waited").has_type(pl.Int64),
                  "certified: weeks_waited is Int64", on_error="WARN"),
            # Aggregate null-percentage  (docs: E.col().null_percentage().lt(x))
            Check(E.col("nhs_number").null_percentage().lt(0.01),
                  "certified: nhs_number rarely null", on_error="WARN"),
            # Row count over whole dataset  (docs: E.group_by().count().gt(n))
            Check(E.group_by().count().gt(0), "certified: non-empty", on_error="WARN"),
            # Column existence  (docs: E.col().exists())
            Check(E.col("specialty_name").exists(),
                  "certified: specialty_name present", on_error="WARN"),
        ],
    ),
)
def certify(clean: LightweightInput, out: LightweightOutput) -> None:
    # Passthrough - the transform's value is the attached, build-time-enforced expectations.
    out.write_table(clean.polars())
