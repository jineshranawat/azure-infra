#!/usr/bin/env python3
"""CI/CD release: medallion + SQL metadata + Purview governance (Day 9 + shared-adf-lab).

Usage (Windows):
    release-medallion-governance.cmd
    release-medallion-governance.cmd --skip-run
    release-medallion-governance.cmd --run-only
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DAY9_BUILD = REPO_ROOT / "day9" / "scripts" / "build_all_notebooks.py"
DEPLOY_LAB = REPO_ROOT / "deploy-shared-lab.cmd"
DEPLOY_ADF = REPO_ROOT / "shared-adf-lab" / "orchestrate.cmd"
RUN_50 = REPO_ROOT / "run-50-problems.cmd"

logger = logging.getLogger(__name__)


def _run(cmd: list[str] | str, *, cwd: Path | None = None) -> None:
    logger.info("RUN: %s", cmd if isinstance(cmd, str) else " ".join(cmd))
    if isinstance(cmd, str):
        subprocess.run(cmd, cwd=cwd or REPO_ROOT, check=True, shell=True)
    else:
        subprocess.run(cmd, cwd=cwd or REPO_ROOT, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Release medallion + governance module")
    parser.add_argument("--skip-build", action="store_true", help="Skip notebook rebuild")
    parser.add_argument("--skip-deploy", action="store_true", help="Skip Databricks + ADF deploy")
    parser.add_argument("--skip-run", action="store_true", help="Skip Databricks job submit (50 problems)")
    parser.add_argument("--run-only", action="store_true", help="Only submit medallion governance job")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(message)s")

    if args.run_only:
        if RUN_50.is_file():
            _run(str(RUN_50))
        return 0

    if not args.skip_build and DAY9_BUILD.is_file():
        _run([sys.executable, str(DAY9_BUILD)])

    if not args.skip_deploy:
        if DEPLOY_LAB.is_file():
            _run(str(DEPLOY_LAB))
        if DEPLOY_ADF.is_file():
            _run(f'"{DEPLOY_ADF}" --skip-sql')

    if not args.skip_run:
        medallion_runner = REPO_ROOT / "scripts" / "run_medallion_governance_job.py"
        if medallion_runner.is_file():
            _run([sys.executable, str(medallion_runner)])
        elif RUN_50.is_file():
            logger.warning("Medallion job runner missing — falling back to run-50-problems.cmd")
            _run(str(RUN_50))

    print()
    print("=" * 70)
    print("MEDALLION + GOVERNANCE RELEASE COMPLETE")
    print("=" * 70)
    print("  Databricks notebooks : /Shared/day9/medallion_governance_e2e")
    print("                        /Shared/day9/50_data_engineering_problems (capstone)")
    print("  Purview manifest     : audit/governance/databricks_governance_manifest.json")
    print("  SQL metadata export  : audit/governance/de_table_registry.csv")
    print("  ADF governance       : pl_gov_06_master_medallion_governance")
    print("  Re-run               : release-medallion-governance.cmd")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
