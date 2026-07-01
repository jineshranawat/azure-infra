"""Unit tests for Day 6 pure-Python transforms."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from transforms import clean_amount, is_valid_txn_id, summarise_channel_totals  # noqa: E402


class TestCleanAmount(unittest.TestCase):
    def test_parses_numeric_string(self) -> None:
        self.assertEqual(clean_amount("50000"), 50000.0)

    def test_parses_float(self) -> None:
        self.assertEqual(clean_amount(1250.5), 1250.5)

    def test_garbage_returns_zero(self) -> None:
        self.assertEqual(clean_amount("oops"), 0.0)

    def test_none_returns_zero(self) -> None:
        self.assertEqual(clean_amount(None), 0.0)

    def test_negative_returns_zero(self) -> None:
        self.assertEqual(clean_amount(-10), 0.0)


class TestTxnId(unittest.TestCase):
    def test_valid_ids(self) -> None:
        self.assertTrue(is_valid_txn_id("TXN-10003"))

    def test_invalid_prefix(self) -> None:
        self.assertFalse(is_valid_txn_id("INV-10003"))


class TestChannelTotals(unittest.TestCase):
    def test_sums_by_channel(self) -> None:
        rows = [
            {"channel": "wire", "amount_gbp": "100"},
            {"channel": "card", "amount_gbp": "50"},
            {"channel": "wire", "amount_gbp": "25"},
        ]
        totals = summarise_channel_totals(rows)
        self.assertEqual(totals["wire"], 125.0)
        self.assertEqual(totals["card"], 50.0)


if __name__ == "__main__":
    unittest.main()
