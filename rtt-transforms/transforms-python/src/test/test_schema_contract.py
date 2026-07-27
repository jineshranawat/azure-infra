"""Unit tests for lib.schema_contract (Problem 9)."""
from myproject.lib import schema_contract as SC

ACTUAL = ["pathway_id", "trust_code", "specialty", "referral_date", "weeks_waited",
          "nhs_number", "sex", "age_at_referral", "pathway_status", "source_load_ts"]
EXPECTED = list(ACTUAL)
REQUIRED = ["pathway_id", "trust_code", "specialty", "referral_date", "weeks_waited"]


def test_no_drift():
    d = SC.detect_drift(ACTUAL, EXPECTED)
    assert d["missing"] == [] and d["unexpected"] == []
    assert SC.is_contract_satisfied(ACTUAL, REQUIRED) is True


def test_missing_required_column_detected():
    actual = [c for c in ACTUAL if c != "weeks_waited"]
    d = SC.detect_drift(actual, EXPECTED)
    assert "weeks_waited" in d["missing"]
    assert SC.is_contract_satisfied(actual, REQUIRED) is False  # negative: fatal


def test_unexpected_new_column_detected():
    actual = ACTUAL + ["mystery_new_col"]
    d = SC.detect_drift(actual, EXPECTED)
    assert d["unexpected"] == ["mystery_new_col"]
    assert SC.is_contract_satisfied(actual, REQUIRED) is True  # drift present but not fatal
