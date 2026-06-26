#!/usr/bin/env python3
"""Session 3 orchestrator — Databricks lakehouse lab (2 hours, azure.html Day 3).

Prerequisite: Class-1 + Databricks workspace deployed via repo root orchestrate.cmd

Typical run (from session-3 folder):
    orchestrate.cmd
    orchestrate.cmd --verify-storage   # after running notebooks in Databricks UI
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

print("Session 3 — loading Python packages (first run may take 20-40 seconds)...", flush=True)

from _config import SessionConfig, find_az, load_config  # noqa: E402

logger = logging.getLogger("session3")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Session 3 — Databricks bronze→silver→gold")
    parser.add_argument(
        "--verify-storage",
        action="store_true",
        help="Check silver/gold Delta tables exist (run after Databricks notebooks)",
    )
    parser.add_argument("--skip-upload", action="store_true", help="Skip bronze CSV upload")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _preflight_az(cfg: SessionConfig) -> None:
    logger.info("Checking Azure login...")
    result = subprocess.run(
        [
            find_az(),
            "account",
            "show",
            "--subscription",
            cfg.subscription_id,
            "-o",
            "json",
        ],
        check=False,
        text=True,
        capture_output=True,
        timeout=45,
    )
    if result.returncode != 0:
        raise SystemExit(
            "Azure CLI not logged in or wrong subscription. "
            "From repo root run: orchestrate.cmd"
        )
    account = json.loads(result.stdout)
    logger.info(
        "Azure OK — subscription %s (%s)",
        account.get("name", "?"),
        cfg.subscription_id[:8] + "...",
    )


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    if not args.verbose:
        for noisy in ("azure", "urllib3", "msrest"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    cfg = load_config()
    logger.info("Session 3 — Databricks lab — learner=%s rg=%s", cfg.learner, cfg.resource_group)

    _preflight_az(cfg)

    from discover import abfss_path, databricks_workspace_url, discover_estate  # noqa: E402
    from databricks_rbac import ensure_databricks_storage_rbac  # noqa: E402
    from bronze_prep import ensure_bronze_for_databricks  # noqa: E402

    logger.info("Discovering Class-1 resources in %s...", cfg.resource_group)
    estate = discover_estate(cfg)
    logger.info(
        "Storage: %s | Databricks: %s | ADF: %s",
        estate.storage_account,
        estate.databricks_workspace,
        estate.data_factory or "(none)",
    )

    logger.info("==> Phase 1: Databricks storage RBAC (SDK — fast, skips if no access connector)")
    ensure_databricks_storage_rbac(cfg, estate.storage_account, estate.databricks_workspace)

    paths: dict[str, str] = {}
    if not args.skip_upload:
        logger.info("==> Phase 2: Bronze prep (loaded CSV for Databricks read)")
        paths = ensure_bronze_for_databricks(cfg, estate.storage_account)
    else:
        logger.info("==> Phase 2: Skipped upload (--skip-upload)")

    run_id = paths.get("run_id", "session3-lab")
    bronze_loaded = paths.get(
        "loaded_abfss",
        abfss_path(
            estate.storage_account,
            "bronze",
            f"loaded/run={run_id}/sample_transactions.csv",
        ),
    )
    silver_delta = abfss_path(estate.storage_account, "silver", "transactions")
    gold_delta = abfss_path(estate.storage_account, "gold", "daily_channel_summary")

    logger.info("==> Phase 3: Databricks UI lab (you run notebooks in the workspace)")
    logger.info("")
    logger.info("  Storage account : %s", estate.storage_account)
    logger.info("  Workspace  : %s", databricks_workspace_url(cfg, estate.databricks_workspace))
    logger.info("  Open UI    : Azure Portal → Databricks workspace → Launch workspace")
    logger.info("  Bronze read: %s", bronze_loaded)
    logger.info("  Silver sink: %s", silver_delta)
    logger.info("  Gold sink  : %s", gold_delta)
    logger.info("  Notebooks  : session-3/notebooks/ (import or copy into workspace)")
    logger.info("  Student UI : UI-OVERVIEW.md → SESSION3-STUDENT-GUIDE.md → MANUAL-LAB.md")
    logger.info("")

    if args.verify_storage:
        from storage_verify import print_verify_report  # noqa: E402

        logger.info("==> Phase 4: Verify silver/gold Delta outputs")
        return print_verify_report(cfg, estate.storage_account)

    logger.info("==> Phase 4: Verification skipped (use --verify-storage after notebooks)")
    logger.info("")
    logger.info("Session 3 prep complete.")
    logger.info("  Re-run     : orchestrate.cmd  (idempotent)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
