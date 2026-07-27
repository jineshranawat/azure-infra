"""Unit tests for lib.sae_minimiser (metadata-driven data minimisation)."""
from myproject.lib import sae_minimiser as S

RULES = [
    {"dataset_name": "rtt_pathways", "column_name": "sex",
     "reason": "not required", "approved_by": "IG Lead"},
    {"dataset_name": "rtt_pathways", "column_name": "age_at_referral",
     "reason": "band sufficient", "approved_by": "IG Lead"},
    {"dataset_name": "rtt_pathways", "column_name": "source_load_ts",
     "reason": "internal watermark", "approved_by": "Data Engineering Lead"},
    {"dataset_name": "other_ds", "column_name": "foo", "approved_by": "IG Lead"},
]


def test_columns_to_drop_filters_by_dataset():
    assert S.columns_to_drop(RULES, "rtt_pathways") == ["age_at_referral", "sex", "source_load_ts"]
    assert S.columns_to_drop(RULES, "other_ds") == ["foo"]
    assert S.columns_to_drop(RULES, "missing") == []


def test_apply_minimisation_preserves_order():
    cols = ["pathway_id", "sex", "weeks_waited", "age_at_referral", "source_load_ts"]
    kept, dropped = S.apply_minimisation(cols, ["sex", "age_at_referral", "source_load_ts"])
    assert kept == ["pathway_id", "weeks_waited"]
    assert dropped == ["sex", "age_at_referral", "source_load_ts"]


def test_assert_minimised_positive_and_negative():
    assert S.assert_minimised(["pathway_id", "weeks_waited"], ["sex", "age_at_referral"]) is True
    # Negative: a banned column leaked through -> post-condition must fail.
    assert S.assert_minimised(["pathway_id", "sex"], ["sex"]) is False
