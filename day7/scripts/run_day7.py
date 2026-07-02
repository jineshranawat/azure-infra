#!/usr/bin/env python3
"""Day 7 orchestrator — Python essentials + storage + local Spark."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _config import SESSION_ROOT, load_config  # noqa: E402
from local_spark_read import main as run_local_spark  # noqa: E402
from pandas_polars_concepts import main as run_pandas_polars_concepts  # noqa: E402
from python_essentials import main as run_python_essentials  # noqa: E402
from storage_concepts import print_storage_concepts  # noqa: E402

logger = logging.getLogger("day7")


def _parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description="Day 7 — Python + storage + local Spark")
  p.add_argument("--skip-spark", action="store_true", help="Skip local PySpark block")
  p.add_argument("--skip-tests", action="store_true")
  p.add_argument("--verbose", action="store_true")
  return p.parse_args()


def _run_unit_tests() -> None:
  import unittest

  tests_dir = SESSION_ROOT / "tests"
  suite = unittest.defaultTestLoader.discover(str(tests_dir), pattern="test_*.py")
  result = unittest.TextTestRunner(verbosity=2).run(suite)
  if not result.wasSuccessful():
    raise SystemExit(1)


def _print_notebook_steps(cfg) -> None:
  print()
  print("=" * 72)
  print("DAY 7 — DATABRICKS NOTEBOOKS (import one file at a time)")
  print("=" * 72)
  print("  nb_01_python_essentials.py   — venv + pandas/polars + Python basics")
  print("  nb_02_storage_medallion.py  — ADLS + abfss paths (theory)")
  print("  nb_03_spark_concepts.py      — FULL PySpark foundation (read before Day 8)")
  print("  nb_04_read_bronze.py        — read FinLedger bronze in cloud")
  print()
  print("Prerequisite: session-3\\orchestrate.cmd --setup-secrets")
  print(f"Learner: {cfg.learner}  |  RG: {cfg.resource_group}")
  print("Guide: day7/SESSION7-STUDENT-GUIDE.md")
  print("=" * 72)


def main() -> int:
  args = _parse_args()
  logging.basicConfig(
    level=logging.DEBUG if args.verbose else logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
    force=True,
  )
  cfg = load_config()
  logger.info("Day 7 lab — learner=%s", cfg.learner)

  print()
  print("--- Block A: Python essentials (local) ---")
  run_python_essentials(SESSION_ROOT)

  print()
  print("--- Block A2: pandas + Polars concepts (before Spark) ---")
  run_pandas_polars_concepts(SESSION_ROOT)

  print()
  print("--- Block B: Storage + medallion (theory) ---")
  print_storage_concepts(f"st{cfg.learner}<hash>")

  if not args.skip_spark:
    print()
    print("--- Block C: Local PySpark read ---")
    run_local_spark(SESSION_ROOT)

  if not args.skip_tests:
    print()
    print("--- Block D: Unit tests ---")
    _run_unit_tests()

  _print_notebook_steps(cfg)
  print()
  print("Day 7 orchestration complete.")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
