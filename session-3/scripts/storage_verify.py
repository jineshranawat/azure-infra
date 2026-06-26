"""Verify silver/gold Delta outputs after Databricks notebook run."""

from __future__ import annotations

import logging

from azure.storage.filedatalake import DataLakeServiceClient

from _config import SessionConfig, get_credential

logger = logging.getLogger(__name__)

SILVER_DELTA_PREFIX = "transactions"
GOLD_DELTA_PREFIX = "daily_channel_summary"


def _list_prefix(fs_client, prefix: str) -> list[str]:
    paths: list[str] = []
    for item in fs_client.get_paths(path=prefix):
        paths.append(item.name)
    return paths


def verify_lakehouse_outputs(storage_account: str) -> dict[str, bool]:
    """Check whether silver and gold Delta folders contain _delta_log."""
    account_url = f"https://{storage_account}.dfs.core.windows.net"
    service = DataLakeServiceClient(account_url=account_url, credential=get_credential())

    checks: dict[str, bool] = {}
    for container, prefix, label in (
        ("silver", SILVER_DELTA_PREFIX, "silver_transactions"),
        ("gold", GOLD_DELTA_PREFIX, "gold_channel_summary"),
    ):
        fs = service.get_file_system_client(container)
        paths = _list_prefix(fs, prefix)
        has_delta = any("_delta_log" in p for p in paths)
        checks[label] = has_delta
        if has_delta:
            logger.info("%s: Delta table found at %s/%s", label, container, prefix)
        else:
            logger.warning(
                "%s: not found yet — run notebooks in Databricks first", label
            )
    return checks


def print_verify_report(cfg: SessionConfig, storage_account: str) -> int:
    checks = verify_lakehouse_outputs(storage_account)
    logger.info("")
    logger.info("Session 3 storage verification (learner=%s)", cfg.learner)
    for name, ok in checks.items():
        logger.info("  %-25s %s", name, "OK" if ok else "PENDING")
    return 0 if all(checks.values()) else 1
