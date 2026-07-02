"""Unit tests for Day 7 path builder."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from path_builder import bronze_csv_path, bronze_container  # noqa: E402


class TestBronzePaths(unittest.TestCase):
    def test_container(self) -> None:
        self.assertEqual(
            bronze_container("stjineshabc"),
            "abfss://bronze@stjineshabc.dfs.core.windows.net/",
        )

    def test_csv_path(self) -> None:
        path = bronze_csv_path("stjineshabc", "session3-lab")
        self.assertIn("loaded/run=session3-lab/sample_transactions.csv", path)
        self.assertTrue(path.startswith("abfss://bronze@"))


if __name__ == "__main__":
    unittest.main()
