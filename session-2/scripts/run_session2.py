#!/usr/bin/env python3
"""Session 2 orchestrator — ADF ingestion lab (2 hours, azure.html Day 2).

Prerequisite: Class-1 + ADF deployed via repo root orchestrate.cmd

Typical run (from session-2 folder):
    orchestrate.cmd
    orchestrate.cmd --run-pipeline   # optional: trigger ADF copy (activity run charge)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow imports from scripts/ when launched as module
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _config import load_config
from adf_pipeline import ensure_adf_artifacts, trigger_pipeline_run
from adf_rbac import ensure_adf_storage_rbac
from bronze_loader import upload_bronze_feed
from discover import discover_estate
from morning_check import print_morning_check
from watermark_store import write_watermark

logger = logging.getLogger("session2")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Session 2 — ADF orchestration & bronze ingest")
    parser.add_argument(
        "--run-pipeline",
        action="store_true",
        help="Trigger ADF copy pipeline after deploy (small activity-run cost)",
    )
    parser.add_argument("--skip-upload", action="store_true", help="Skip bronze CSV upload")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg = load_config()
    logger.info("Session 2 — ADF ingestion — learner=%s rg=%s", cfg.learner, cfg.resource_group)

    estate = discover_estate(cfg)
    logger.info("Storage: %s | ADF: %s", estate.storage_account, estate.data_factory)

    logger.info("==> Phase 1: ADF managed identity → storage RBAC")
    ensure_adf_storage_rbac(cfg, estate.storage_account, estate.data_factory)

    logger.info("==> Phase 2: Deploy ADF datasets + copy pipeline (SDK)")
    ensure_adf_artifacts(cfg, estate.storage_account, estate.data_factory)

    paths: dict[str, str] = {}
    if not args.skip_upload:
        logger.info("==> Phase 3: Bronze loader (incoming path)")
        paths = upload_bronze_feed(cfg, estate.storage_account)
        incoming_folder = f"incoming/transactions/{paths['run_id']}"
        loaded_folder = f"loaded/run={paths['run_id']}"
    else:
        logger.info("==> Phase 3: Skipped upload (--skip-upload)")
        incoming_folder = "incoming/transactions/manual"
        loaded_folder = "loaded/run=manual"

    if args.run_pipeline:
        logger.info("==> Phase 4: Trigger ADF pipeline run")
        trigger_pipeline_run(cfg, estate.data_factory, incoming_folder, loaded_folder)
    else:
        logger.info("==> Phase 4: Pipeline deploy only (use --run-pipeline to execute)")

    if paths:
        logger.info("==> Phase 5: Watermark control file")
        write_watermark(estate.storage_account, paths["run_id"], f"bronze/{loaded_folder}")

    logger.info("==> Phase 6: Morning check (pipeline run history)")
    print_morning_check(cfg, estate.data_factory)

    adf_url = (
        f"https://portal.azure.com/#@/resource/subscriptions/{cfg.subscription_id}"
        f"/resourceGroups/{cfg.resource_group}/providers/Microsoft.DataFactory"
        f"/factories/{estate.data_factory}/overview"
    )
    logger.info("")
    logger.info("Session 2 complete.")
    logger.info("  ADF portal : %s", adf_url)
    logger.info("  Re-run     : orchestrate.cmd  (idempotent)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
