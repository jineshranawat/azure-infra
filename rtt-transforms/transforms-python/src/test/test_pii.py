"""Unit tests for lib.pii (Problems 29, 31)."""
from myproject.lib import pii


def test_pseudonymise_deterministic_and_not_raw():
    a = pii.pseudonymise("NHS1000580")
    assert a == pii.pseudonymise("NHS1000580")     # deterministic -> stable joins
    assert a != "NHS1000580"                        # not the raw value
    assert len(a) == 16
    assert pii.pseudonymise("NHS1000581") != a      # distinct patients differ
    assert pii.pseudonymise(None) is None


def test_pseudonymise_salt_changes_output():
    assert pii.pseudonymise("NHS1", salt="A") != pii.pseudonymise("NHS1", salt="B")
