"""Unit tests for lib.rtt_metrics (Problems 35 & 21 support)."""
import pytest

from myproject.lib import rtt_metrics as M


@pytest.mark.parametrize("weeks,expected", [(0, False), (18, False), (19, True), (52, True), (200, True)])
def test_is_breach(weeks, expected):
    assert M.is_breach(weeks) is expected


def test_is_breach_none():
    assert M.is_breach(None) is None


@pytest.mark.parametrize("weeks,expected", [(51, False), (52, True), (65, True)])
def test_52_week_breach(weeks, expected):
    assert M.is_52_week_breach(weeks) is expected


@pytest.mark.parametrize("weeks,expected", [(64, False), (65, True), (100, True)])
def test_65_week_breach(weeks, expected):
    assert M.is_65_week_breach(weeks) is expected


@pytest.mark.parametrize("weeks,band", [
    (0, "0-18"), (18, "0-18"), (19, "19-26"), (26, "19-26"), (27, "27-39"),
    (39, "27-39"), (40, "40-51"), (51, "40-51"), (52, "52-64"), (64, "52-64"),
    (65, "65+"), (300, "65+"),
])
def test_wait_band(weeks, band):
    assert M.wait_band(weeks) == band


def test_wait_band_invalid():
    # Negative: null/negative waits produce no band (invalid, quarantined upstream).
    assert M.wait_band(None) is None
    assert M.wait_band(-1) is None


@pytest.mark.parametrize("weeks,rag", [
    (10, "GREEN"), (18, "GREEN"), (19, "AMBER"), (51, "AMBER"), (52, "RED"), (99, "RED"),
])
def test_rag_status(weeks, rag):
    assert M.rag_status(weeks) == rag


def test_compliance_and_constitutional_standard():
    assert M.compliance_rate(92, 100) == pytest.approx(0.92)
    assert M.compliance_rate(0, 0) is None  # negative: divide-by-zero guarded
    assert M.meets_constitutional_standard(92, 100) is True
    assert M.meets_constitutional_standard(91, 100) is False
