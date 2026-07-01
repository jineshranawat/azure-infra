#!/usr/bin/env python3
"""Day 6 orchestrator — Python for data engineers (Session 6).

Runs local Python demos + unit tests, then prints Databricks notebook steps.

Typical run (from day6 folder):
    orchestrate.cmd
    orchestrate.cmd --verbose
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

print("Day 6 — loading Python packages...", flush=True)

from _config import SESSION_ROOT, load_config  # noqa: E402
from read_local import main as run_read_local  # noqa: E402

logger = logging.getLogger("day6")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Day 6 — Python for data engineers")
    parser.add_argument("--skip-tests", action="store_true", help="Skip unit tests")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _run_unit_tests() -> None:
    tests_dir = SESSION_ROOT / "tests"
    suite = unittest.defaultTestLoader.discover(str(tests_dir), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit("Unit tests failed — fix transforms.py before continuing")


def _print_databricks_steps(cfg) -> None:
    print()
    print("=" * 72)
    print("DAY 6 — DATABRICKS NOTEBOOKS (do these in the workspace UI)")
    print("=" * 72)
    print()
    print("1. Portal -> Databricks workspace -> Workspace -> Import")
    print("   Files: day6/notebooks/nb_01_python_basics.py")
    print("          day6/notebooks/nb_02_pandas_vs_spark.py")
    print()
    print("2. Attach a small cluster (or serverless) and run nb_01 top to bottom.")
    print("3. Run nb_02 - compares pandas read vs Spark read on the same CSV.")
    print()
    print("4. If Session 3 bronze exists, set the bronze_path widget from session-3 output.")
    print("   Otherwise use the bundled data/ CSV paths in the notebook.")
    print()
    print(f"Learner: {cfg.learner}  |  RG: {cfg.resource_group}  |  Region: {cfg.location}")
    print()
    print("Student guide: day6/SESSION6-STUDENT-GUIDE.md")
    print("Step-by-step:  day6/MANUAL-LAB.md")
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
    logger.info("Day 6 lab — learner=%s region=%s", cfg.learner, cfg.location)

    print()
    print("--- Block A: local CSV read (pathlib + csv) ---")
    run_read_local(SESSION_ROOT, run_date="session6-lab")

    if not args.skip_tests:
        print()
        print("--- Block B: unit tests (transforms.py) ---")
        _run_unit_tests()

    _print_databricks_steps(cfg)
    print()
    print("Day 6 orchestration complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
