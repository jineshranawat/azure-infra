"""Unit tests for Day 8 pure-Python filter rules (no Spark cluster)."""

from __future__ import annotations

import unittest


def amount_ok(amount_gbp: str) -> bool:
    try:
        return float(amount_gbp) > 0
    except (TypeError, ValueError):
        return False


class TestAmountOk(unittest.TestCase):
    def test_positive(self) -> None:
        self.assertTrue(amount_ok("1250.50"))

    def test_zero_string(self) -> None:
        self.assertFalse(amount_ok("0"))

    def test_invalid(self) -> None:
        self.assertFalse(amount_ok("INVALID"))


if __name__ == "__main__":
    unittest.main()
