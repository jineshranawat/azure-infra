"""Unit tests for lib.canonicalise (Problems 10 & 40)."""
import datetime as dt

import pytest

from myproject.lib import canonicalise as C


@pytest.mark.parametrize("dirty,expected", [
    ("Trauma and Orthopaedics", "Trauma and Orthopaedics"),
    ("T&O", "Trauma and Orthopaedics"),
    ("  trauma & orthopaedics ", "Trauma and Orthopaedics"),
    ("ENT", "Ear Nose and Throat"),
    ("gen surg", "General Surgery"),
    ("cardio", "Cardiology"),
    ("  Ophthalmology  ", "Ophthalmology"),
    ("NEURO", "Neurology"),
])
def test_canonical_specialty_maps_dirty_variants(dirty, expected):
    assert C.canonical_specialty(dirty) == expected


@pytest.mark.parametrize("bad", [None, "", "   ", "not a specialty", "xyz"])
def test_canonical_specialty_returns_none_for_unknown(bad):
    # Negative: unknown/blank text must NOT be silently mapped.
    assert C.canonical_specialty(bad) is None


@pytest.mark.parametrize("raw,expected", [
    ("2024-09-25", dt.date(2024, 9, 25)),
    ("25/09/2024", dt.date(2024, 9, 25)),
    ("25-09-2024", dt.date(2024, 9, 25)),
    ("2025-01-15T08:00:00", dt.date(2025, 1, 15)),
    ("2025-01-15 08:00:00", dt.date(2025, 1, 15)),
    (dt.date(2023, 5, 14), dt.date(2023, 5, 14)),
])
def test_parse_date_handles_multiple_formats(raw, expected):
    assert C.parse_date(raw) == expected


@pytest.mark.parametrize("bad", [None, "", "not_a_date", "2024-13-40", "31/02/2024"])
def test_parse_date_returns_none_for_bad(bad):
    # Negative: unparseable dates return None (routed to quarantine downstream).
    assert C.parse_date(bad) is None


@pytest.mark.parametrize("raw,expected", [("23", 23), (23, 23), ("  0 ", 0), ("-5", -5), ("100.0", 100)])
def test_to_int_ok(raw, expected):
    assert C.to_int(raw) == expected


@pytest.mark.parametrize("bad", [None, "", "N/A", "abc", "12.5", True])
def test_to_int_bad_returns_none(bad):
    # Negative: non-integer strings/floats/bools are rejected (type validation).
    assert C.to_int(bad) is None
