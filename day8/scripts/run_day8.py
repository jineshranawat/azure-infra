#!/usr/bin/env python3
"""Day 8 orchestrator — PySpark transformations (local + notebook guide)."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _config import SESSION_ROOT, load_config  # noqa: E402
from pandas_polars_concepts import main as run_pandas_polars  # noqa: E402
from pyspark_concepts import print_pyspark_foundation  # noqa: E402
from spark_transforms import main as run_transforms  # noqa: E402

logger = logging.getLogger("day8")


def _parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description="Day 8 — PySpark transforms")
  p.add_argument("--skip-spark", action="store_true")
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
  print("DAY 8 — DATABRICKS NOTEBOOKS")
  print("=" * 72)
  print("  nb_00_pyspark_foundation.py  — READ FIRST (venv, pandas, full PySpark theory)")
  print("  nb_01_select_filter.py")
  print("  nb_02_withcolumn_cast.py")
  print("  nb_03_groupby_agg.py")
  print("  nb_04_join_window.py")
  print()
  print("Prerequisite: session-3 secrets + Day 7 nb_04 read bronze")
  print(f"Learner: {cfg.learner}")
  print("Guide: day8/SESSION8-STUDENT-GUIDE.md")
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
  logger.info("Day 8 lab — learner=%s", cfg.learner)

  print()
  print("--- Block A: venv path recap (pandas + Polars on sample CSV) ---")
  run_pandas_polars(SESSION_ROOT)

  print()
  print("--- Block B: PySpark foundation (theory) ---")
  print_pyspark_foundation()

  if not args.skip_spark:
    import shutil

    print()
    print("--- Local PySpark transforms ---")
    if shutil.which("java"):
      run_transforms(SESSION_ROOT)
    else:
      # Java is only needed for the LOCAL Spark demo; notebooks run on
      # Databricks clusters which have their own JVM. Skip, don't fail.
      print("SKIP  Java 11+ not found on this PC - local Spark demo skipped.")
      print("      Install Java (e.g. https://adoptium.net) and re-run, or")
      print("      just run the Day 8 notebooks in Databricks (JVM included).")

  if not args.skip_tests:
    print()
    print("--- Unit tests ---")
    _run_unit_tests()

  _print_notebook_steps(cfg)
  print()
  print("Day 8 orchestration complete.")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
