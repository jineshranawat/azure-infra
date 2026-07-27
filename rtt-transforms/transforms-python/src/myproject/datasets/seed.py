"""Seed transforms: materialise the 4 RTT source tables from the supplied extract.

In production these arrive via Data Connection syncs; here they are seeded
deterministically so the whole reference implementation is self-contained and
reproducible (Problem 19: idempotent/replayable seeding).

The raw pathway table is emitted as ALL-STRING columns (CSV-landing shape) and
mixes ~1,200 valid rows with a small, DOCUMENTED set of dirty rows so the
downstream typing / DQ / quarantine machinery can be demonstrated end-to-end.
"""
from __future__ import annotations

import datetime as dt
import random

import polars as pl
from transforms.api import transform, Output, LightweightOutput

from myproject import paths

# --------------------------------------------------------------------------
# Reference data (verbatim from the supplied extract)
# --------------------------------------------------------------------------
_PROVIDERS = [
    ("TR001", "Northfield University Hospital NHS Trust", "North West", "Acute", 820, "data.quality@northfield.nhs.uk"),
    ("TR002", "Westgate General Hospital NHS Trust", "South West", "Acute", 610, "data.quality@westgate.nhs.uk"),
    ("TR003", "Eastbrook District General NHS Trust", "East of England", "Acute", 540, "data.quality@eastbrook.nhs.uk"),
    ("TR004", "Southmead Health Services NHS Trust", "South East", "Acute", 390, "data.quality@southmead.nhs.uk"),
    ("TR005", "Central Mercia Teaching Hospitals NHS Trust", "Midlands", "Teaching", 1050, "data.quality@mercia.nhs.uk"),
]
_PROVIDER_COLS = ["trust_code", "trust_name", "region", "trust_type", "beds", "contact_email"]

_SPECIALTIES = [
    ("SP01", "Trauma and Orthopaedics", "Surgery", "Y", 18, 750),
    ("SP02", "General Surgery", "Surgery", "Y", 18, 800),
    ("SP03", "Ophthalmology", "Surgery", "Y", 18, 850),
    ("SP04", "Cardiology", "Medicine", "N", 18, 900),
    ("SP05", "Gastroenterology", "Medicine", "N", 18, 950),
    ("SP06", "Ear Nose and Throat", "Surgery", "Y", 18, 1000),
    ("SP07", "Urology", "Surgery", "Y", 18, 1050),
    ("SP08", "Neurology", "Medicine", "N", 18, 1100),
    ("SP09", "Dermatology", "Medicine", "N", 18, 1150),
    ("SP10", "Rheumatology", "Medicine", "N", 18, 1200),
]
_SPECIALTY_COLS = ["specialty_code", "specialty_name", "specialty_group",
                   "is_surgical", "rtt_target_weeks", "standard_tariff_gbp"]

_SAE_RULES = [
    ("rtt_pathways", "sex",
     "Biological sex not required for RTT programme metrics - remove to minimise data scope",
     "IG Lead", "2024-04-01"),
    ("rtt_pathways", "age_at_referral",
     "Age band (derived in DA) is sufficient for analysis - exact age not required at national level",
     "IG Lead", "2024-04-01"),
    ("rtt_pathways", "source_load_ts",
     "Internal watermark column - not needed by downstream consumers, replaced by lnc_load_ts",
     "Data Engineering Lead", "2024-04-01"),
]
_SAE_COLS = ["dataset_name", "column_name", "reason", "approved_by", "approved_date"]

_PATHWAY_COLS = ["pathway_id", "trust_code", "specialty", "referral_date", "weeks_waited",
                 "nhs_number", "sex", "age_at_referral", "pathway_status", "source_load_ts"]

# The 25 supplied sample rows (verbatim; ints coerced to str for the CSV-landing shape).
_SAMPLE_PATHWAYS = [
    ("PW00580", "TR004", "Neurology", "2024-09-25", 23, "NHS1000580", "F", 77, "Active", "2025-01-15T08:00:00"),
    ("PW00134", "TR002", "Trauma and Orthopaedics", "2024-04-05", 15, "NHS1000134", "F", 55, "Active", "2025-01-15T08:00:00"),
    ("PW00106", "TR001", "Ophthalmology", "2024-06-24", 59, "NHS1000106", "F", 27, "Active", "2025-01-15T08:00:00"),
    ("PW00771", "TR005", "Ear Nose and Throat", "2025-01-14", 14, "NHS1000771", "M", 35, "Discharged", "2025-01-15T08:00:00"),
    ("PW00900", "TR003", "General Surgery", "2025-01-13", 85, "NHS1000900", "M", 51, "Active", "2025-01-31T08:00:00"),
    ("PW00259", "TR002", "Rheumatology", "2024-04-25", 95, "NHS1000259", "M", 47, "Active", "2025-01-15T08:00:00"),
    ("PW00661", "TR003", "Gastroenterology", "2025-01-14", 38, "NHS1000661", "M", 49, "Active", "2025-01-15T08:00:00"),
    ("PW00251", "TR005", "General Surgery", "2024-03-22", 28, "NHS1000251", "F", 71, "Active", "2025-01-31T08:00:00"),
    ("PW00435", "TR003", "Trauma and Orthopaedics", "2024-02-03", 53, "NHS1000435", "F", 71, "Active", "2025-01-15T08:00:00"),
    ("PW00295", "TR004", "Trauma and Orthopaedics", "2023-11-01", 100, "NHS1000295", "F", 47, "Active", "2025-01-31T08:00:00"),
    ("PW00503", "TR004", "General Surgery", "2023-05-14", 34, "NHS1000503", "M", 24, "Active", "2025-01-31T08:00:00"),
    ("PW00155", "TR001", "Cardiology", "2024-02-12", 44, "NHS1000155", "M", 59, "Active", "2025-01-15T08:00:00"),
    ("PW00756", "TR002", "Cardiology", "2024-07-12", 50, "NHS1000756", "F", 62, "Active", "2025-01-31T08:00:00"),
    ("PW00037", "TR001", "General Surgery", "2024-08-30", 69, "NHS1000037", "F", 81, "Active", "2025-01-15T08:00:00"),
    ("PW00364", "TR004", "Rheumatology", "2024-04-16", 13, "NHS1000364", "M", 45, "Discharged", "2025-01-15T08:00:00"),
    ("PW00353", "TR004", "Trauma and Orthopaedics", "2023-12-08", 153, "NHS1000353", "F", 57, "Active", "2025-01-15T08:00:00"),
    ("PW00812", "TR002", "Gastroenterology", "2025-01-13", 98, "NHS1000812", "M", 55, "Active", "2025-01-31T08:00:00"),
    ("PW00095", "TR004", "Dermatology", "2024-03-07", 45, "NHS1000095", "F", 62, "Active", "2025-01-15T08:00:00"),
    ("PW00076", "TR001", "Ear Nose and Throat", "2024-10-21", 36, "NHS1000076", "F", 57, "Active", "2025-01-15T08:00:00"),
    ("PW00042", "TR005", "General Surgery", "2024-05-27", 30, "NHS1000042", "F", 41, "Active", "2025-01-15T08:00:00"),
    ("PW00233", "TR002", "Gastroenterology", "2023-09-09", 18, "NHS1000233", "F", 86, "Discharged", "2025-01-15T08:00:00"),
    ("PW00610", "TR003", "Rheumatology", "2023-04-19", 95, "NHS1000610", "F", 46, "Discharged", "2025-01-15T08:00:00"),
    ("PW00268", "TR001", "General Surgery", "2024-08-24", 34, "NHS1000268", "M", 45, "Active", "2025-01-15T08:00:00"),
    ("PW00538", "TR001", "Trauma and Orthopaedics", "2024-08-17", 148, "NHS1000538", "M", 66, "Active", "2025-01-15T08:00:00"),
    ("PW00613", "TR005", "Ophthalmology", "2024-08-09", 73, "NHS1000613", "M", 43, "Active", "2025-01-31T08:00:00"),
]

# Small, DOCUMENTED set of dirty rows. Each comment states which problem it exercises.
_SNAPSHOT_TS = "2025-01-31T08:00:00"
_DIRTY_PATHWAYS = [
    # --- dirty free-text specialty variants -> canonicalised into clean (Problem 40) ---
    ("PWD001", "TR001", "T&O", "2024-05-01", "20", "NHS1009001", "M", "40", "Active", _SNAPSHOT_TS),
    ("PWD002", "TR002", "gen surg", "2024-06-15", "30", "NHS1009002", "F", "52", "Active", _SNAPSHOT_TS),
    ("PWD003", "TR003", "ENT", "2024-07-20", "12", "NHS1009003", "M", "33", "Active", _SNAPSHOT_TS),
    ("PWD004", "TR004", "cardio", "2024-08-10", "60", "NHS1009004", "F", "70", "Active", _SNAPSHOT_TS),
    ("PWD005", "TR005", "  Ophthalmology  ", "2024-09-01", "25", "NHS1009005", "M", "61", "Active", _SNAPSHOT_TS),
    # --- blank specialty: cannot map -> quarantine (Problem 8/40) ---
    ("PWD006", "TR001", "", "2024-10-01", "19", "NHS1009006", "F", "44", "Active", _SNAPSHOT_TS),
    # --- orphan trust codes -> quarantine (Problem 34 referential integrity) ---
    ("PWD007", "TR999", "Cardiology", "2024-03-03", "33", "NHS1009007", "M", "50", "Active", _SNAPSHOT_TS),
    ("PWD008", "TR888", "Urology", "2024-02-02", "41", "NHS1009008", "F", "39", "Active", _SNAPSHOT_TS),
    # --- duplicate primary key (re-uses PW00580) -> quarantine dup (Problem 1/42) ---
    ("PW00580", "TR001", "Cardiology", "2024-01-01", "10", "NHS1000580", "M", "30", "Active", _SNAPSHOT_TS),
    # --- negative weeks_waited -> quarantine (Problem 35 range) ---
    ("PWD009", "TR002", "Neurology", "2024-04-04", "-5", "NHS1009009", "M", "48", "Active", _SNAPSHOT_TS),
    # --- absurd weeks_waited -> quarantine (Problem 35 domain) ---
    ("PWD010", "TR003", "Dermatology", "2024-05-05", "9999", "NHS1009010", "F", "55", "Active", _SNAPSHOT_TS),
    # --- non-numeric weeks_waited -> quarantine (Problem 6 type validation) ---
    ("PWD011", "TR004", "Urology", "2024-06-06", "N/A", "NHS1009011", "M", "60", "Active", _SNAPSHOT_TS),
    # --- unparseable referral_date -> quarantine (Problem 10 timestamp/format) ---
    ("PWD012", "TR005", "Cardiology", "not_a_date", "22", "NHS1009012", "F", "42", "Active", _SNAPSHOT_TS),
    # --- future referral_date -> quarantine (Problem 41 cross-field/temporal) ---
    ("PWD013", "TR001", "Neurology", "2030-01-01", "5", "NHS1009013", "M", "36", "Active", _SNAPSHOT_TS),
    # --- null pathway_id -> quarantine (Problem 8 mandatory / Problem 42 PK) ---
    (None, "TR002", "Cardiology", "2024-07-07", "28", "NHS1009014", "F", "47", "Active", _SNAPSHOT_TS),
]

# Number of synthetic valid rows to generate (25 verbatim + 1,175 = 1,200 valid).
_N_SYNTHETIC = 1175
_SNAPSHOT_DATE = dt.date(2025, 1, 31)


def _stringify(row):
    """Coerce every field to str (CSV-landing shape); None stays None."""
    return tuple(None if v is None else str(v) for v in row)


def _generate_valid_rows(n: int):
    """Deterministically generate n valid pathway rows (Problem 19: reproducible)."""
    rng = random.Random(42)
    # Weighted so TR005 dominates -> deliberate skew for the salting demo (Problem 22).
    trusts = ["TR005", "TR005", "TR005", "TR005", "TR001", "TR002", "TR003", "TR004"]
    specialties = [s[1] for s in _SPECIALTIES]
    statuses = ["Active"] * 8 + ["Discharged"] * 2
    rows = []
    for i in range(2000, 2000 + n):
        ref = _SNAPSHOT_DATE - dt.timedelta(days=rng.randint(0, 730))
        weeks = int(rng.triangular(0, 160, 15))  # skew low with a breach tail
        rows.append((
            f"PW{i:05d}", rng.choice(trusts), rng.choice(specialties), ref.isoformat(),
            str(weeks), f"NHS{1002000 + i}", rng.choice(["M", "F"]),
            str(rng.randint(18, 92)), rng.choice(statuses), _SNAPSHOT_TS,
        ))
    return rows


def _raw_pathway_rows():
    samples = [_stringify(r) for r in _SAMPLE_PATHWAYS]
    synthetic = _generate_valid_rows(_N_SYNTHETIC)
    return samples + synthetic + list(_DIRTY_PATHWAYS)


@transform.using(out=Output(paths.PROVIDER_REFERENCE))
def seed_provider_reference(out: LightweightOutput) -> None:
    out.write_table(pl.DataFrame(_PROVIDERS, schema=_PROVIDER_COLS, orient="row"))


@transform.using(out=Output(paths.SPECIALTY_REFERENCE))
def seed_specialty_reference(out: LightweightOutput) -> None:
    out.write_table(pl.DataFrame(_SPECIALTIES, schema=_SPECIALTY_COLS, orient="row"))


@transform.using(out=Output(paths.SAE_COLUMN_RULES))
def seed_sae_column_rules(out: LightweightOutput) -> None:
    out.write_table(pl.DataFrame(_SAE_RULES, schema=_SAE_COLS, orient="row"))


@transform.using(out=Output(paths.RTT_PATHWAYS_RAW))
def seed_rtt_pathways_raw(out: LightweightOutput) -> None:
    df = pl.DataFrame(
        _raw_pathway_rows(),
        schema={c: pl.Utf8 for c in _PATHWAY_COLS},
        orient="row",
    )
    out.write_table(df)
