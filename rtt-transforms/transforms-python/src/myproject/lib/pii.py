"""PII pseudonymisation (Problems 29, 31).

Deterministic one-way pseudonymisation of nhs_number so downstream analytics can
group by patient WITHOUT exposing the real NHS number. This is the in-pipeline
control; it complements the platform-native controls (Markings on any dataset
still carrying nhs_number, Restricted Views for row-level access, property
security for column access) described in /docs/security.md.
"""
from __future__ import annotations

import hashlib
from typing import Optional


def pseudonymise(value, salt: str = "RTT-Programme") -> Optional[str]:
    """Stable 16-char pseudonym for a value; None stays None. Not reversible."""
    if value is None:
        return None
    digest = hashlib.sha256(f"{salt}:{value}".encode("utf-8")).hexdigest()
    return digest[:16]
