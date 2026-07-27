"""Pure helpers for canonicalising dirty RTT source fields.

Engine-agnostic (operate on plain Python values) so they can be unit-tested
deterministically and reused from Polars or PySpark transforms.

Solves (support): Problem 10 (timestamp/date format inconsistency),
Problem 40 (free-text specialty canonicalisation).
"""
from __future__ import annotations

import datetime as _dt
import re
from typing import Optional

# Canonical specialty names are exactly specialty_reference.specialty_name.
# Map many dirty free-text variants -> canonical specialty_name.
_SPECIALTY_SYNONYMS = {
    "trauma and orthopaedics": "Trauma and Orthopaedics",
    "trauma orthopaedics": "Trauma and Orthopaedics",
    "t and o": "Trauma and Orthopaedics",
    "t o": "Trauma and Orthopaedics",
    "ortho": "Trauma and Orthopaedics",
    "orthopaedics": "Trauma and Orthopaedics",
    "general surgery": "General Surgery",
    "gen surgery": "General Surgery",
    "gen surg": "General Surgery",
    "ophthalmology": "Ophthalmology",
    "eye": "Ophthalmology",
    "cardiology": "Cardiology",
    "cardio": "Cardiology",
    "gastroenterology": "Gastroenterology",
    "gastro": "Gastroenterology",
    "ear nose and throat": "Ear Nose and Throat",
    "ent": "Ear Nose and Throat",
    "urology": "Urology",
    "uro": "Urology",
    "neurology": "Neurology",
    "neuro": "Neurology",
    "dermatology": "Dermatology",
    "derm": "Dermatology",
    "rheumatology": "Rheumatology",
    "rheum": "Rheumatology",
}


def normalise_text(value: Optional[str]) -> Optional[str]:
    """Lowercase, expand '&', strip punctuation, collapse whitespace.

    Returns None for None/blank input.
    """
    if value is None:
        return None
    text = str(value).strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[.,/()\-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def canonical_specialty(value: Optional[str]) -> Optional[str]:
    """Map a dirty free-text specialty to a canonical specialty_name, else None."""
    key = normalise_text(value)
    if key is None:
        return None
    return _SPECIALTY_SYNONYMS.get(key)


# Accepted date input formats (RTT extracts vary by source system).
# UK day-first order is tried before US month-first for ambiguous values.
_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y")


def parse_date(value) -> Optional[_dt.date]:
    """Parse a date from several known formats; None if unparseable.

    Accepts date/datetime objects directly. ISO date-time strings
    (e.g. '2025-01-15T08:00:00') are truncated to their date component.
    """
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    if " " in text and ":" in text:  # 'YYYY-MM-DD HH:MM:SS'
        text = text.split(" ", 1)[0]
    for fmt in _DATE_FORMATS:
        try:
            return _dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def to_int(value) -> Optional[int]:
    """Best-effort integer cast; None if not a clean integer (type validation)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text == "":
        return None
    try:
        return int(text)
    except ValueError:
        try:
            f = float(text)
        except ValueError:
            return None
        return int(f) if f.is_integer() else None
