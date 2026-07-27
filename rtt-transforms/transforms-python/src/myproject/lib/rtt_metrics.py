"""Pure RTT clock metrics.

National definitions used verbatim:
- RTT incomplete-pathway target = 18 weeks.
- 52-week and 65-week waits are nationally reportable breaches.
- Constitutional standard = 92% of pathways within 18 weeks.

Solves (support): Problem 35 (range/domain), Problem 21 (windowed metrics),
and powers the metrics/marts stages + Ontology computed properties.
"""
from __future__ import annotations

from typing import Optional

RTT_TARGET_WEEKS = 18
REPORTABLE_52W = 52
REPORTABLE_65W = 65
CONSTITUTIONAL_STANDARD = 0.92


def is_breach(weeks_waited: Optional[int], target_weeks: int = RTT_TARGET_WEEKS) -> Optional[bool]:
    """True if the pathway has breached the RTT target. None if weeks unknown."""
    if weeks_waited is None:
        return None
    return weeks_waited > target_weeks


def is_52_week_breach(weeks_waited: Optional[int]) -> Optional[bool]:
    if weeks_waited is None:
        return None
    return weeks_waited >= REPORTABLE_52W


def is_65_week_breach(weeks_waited: Optional[int]) -> Optional[bool]:
    if weeks_waited is None:
        return None
    return weeks_waited >= REPORTABLE_65W


def wait_band(weeks_waited: Optional[int]) -> Optional[str]:
    """National-style RTT reporting band. None for null/negative (invalid) waits."""
    if weeks_waited is None or weeks_waited < 0:
        return None
    if weeks_waited <= 18:
        return "0-18"
    if weeks_waited <= 26:
        return "19-26"
    if weeks_waited <= 39:
        return "27-39"
    if weeks_waited <= 51:
        return "40-51"
    if weeks_waited <= 64:
        return "52-64"
    return "65+"


def rag_status(weeks_waited: Optional[int], target_weeks: int = RTT_TARGET_WEEKS) -> Optional[str]:
    """GREEN within target, AMBER over target but < 52w, RED at/over 52w."""
    if weeks_waited is None or weeks_waited < 0:
        return None
    if weeks_waited <= target_weeks:
        return "GREEN"
    if weeks_waited < REPORTABLE_52W:
        return "AMBER"
    return "RED"


def compliance_rate(within_target: int, total: int) -> Optional[float]:
    """Fraction (0..1) of pathways within the 18-week target. None if total<=0."""
    if total <= 0:
        return None
    return within_target / total


def meets_constitutional_standard(within_target: int, total: int,
                                  standard: float = CONSTITUTIONAL_STANDARD) -> Optional[bool]:
    rate = compliance_rate(within_target, total)
    if rate is None:
        return None
    return rate >= standard
