"""Unit tests for lib.recon (source-to-target reconciliation)."""
from myproject.lib import recon as R


def test_reconcile_counts():
    assert R.reconcile_counts(100, 80, 20) is True
    # negative: 80 + 19 != 100 -> silent row loss detected
    assert R.reconcile_counts(100, 80, 19) is False
    assert R.reconcile_counts(0) is True


def test_variance():
    assert R.variance(100, 100) == 0
    assert R.variance(100, 97) == -3
    assert R.variance(100, 103) == 3
