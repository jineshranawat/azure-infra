"""Unity Catalog / ADLS access helpers for Session 3."""

from __future__ import annotations

import json
import logging
import subprocess
from typing import Any

from _config import SessionConfig, find_az

logger = logging.getLogger(__name__)

_AZ_TIMEOUT_SEC = 20


def _az_json(args: list[str]) -> dict[str, Any] | None:
    try:
        result = subprocess.run(
            [find_az(), *args, "-o", "json"],
            check=False,
            text=True,
            capture_output=True,
            timeout=_AZ_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def workspace_access_connector_id(cfg: SessionConfig, workspace_name: str) -> str | None:
    """Return access connector resource ID linked to the Databricks workspace, if any."""
    ws = _az_json(
        [
            "resource",
            "show",
            "--resource-group",
            cfg.resource_group,
            "--resource-type",
            "Microsoft.Databricks/workspaces",
            "--name",
            workspace_name,
        ]
    )
    if not ws:
        return None

    props = ws.get("properties") or {}
    connector_id = props.get("accessConnectorId")
    if connector_id:
        return connector_id

    nested = props.get("accessConnector") or {}
    if nested.get("id"):
        return nested["id"]

    resources = _az_json(["resource", "list", "--resource-group", cfg.resource_group]) or []
    for res in resources:
        if res.get("type") == "Microsoft.Databricks/accessConnectors":
            return res.get("id")
    return None


def print_storage_access_instructions(
    *,
    storage_account: str,
    access_connector_id: str | None,
    bronze_abfss: str,
) -> None:
    """Print learner-facing fix for USER_ISOLATION / abfss auth errors."""
    logger.info("")
    logger.info("=" * 72)
    logger.info("STORAGE ACCESS — read this if nb_01 fails on abfss://")
    logger.info("=" * 72)
    logger.info("")
    logger.info("OPTION A (fastest for class): Single-user cluster")
    logger.info("  Compute -> Create -> Access mode: Single user (your email)")
    logger.info("  Re-run nb_01 on that cluster. No Unity Catalog setup needed.")
    logger.info("")
    logger.info("OPTION B: Windows orchestrator (recommended if notebook put fails)")
    logger.info("  Add STORAGE_ACCOUNT_KEY + DATABRICKS_TOKEN to repo-root .env")
    logger.info("  cd session-3 && orchestrate.cmd --setup-secrets")
    logger.info("")
    logger.info("OPTION C: Shared cluster + storage account key in notebook widget")
    logger.info("  1. Import notebooks/nb_00_unity_catalog_storage.py")
    logger.info("  2. Widgets: auth_mode=storage_key, storage_account=%s", storage_account)
    logger.info("  3. Portal -> Storage -> Access keys -> copy key1")
    logger.info("     Store: databricks secrets put --scope finledger --key storage-key")
    logger.info("     OR paste key1 into storage_account_key widget (lab only)")
    logger.info("  4. Run all cells, then nb_01 on the SAME cluster")
    logger.info("")
    logger.info("OPTION C: Shared cluster + Unity Catalog (only if connector exists)")
    if access_connector_id:
        logger.info("  Access connector: %s", access_connector_id)
        logger.info("  nb_00: auth_mode=access_connector, paste connector ID above")
    else:
        logger.info("  Skipped — no access connector on this workspace")
    logger.info("")
    logger.info("Bronze path for nb_01 widget: %s", bronze_abfss)
    logger.info("=" * 72)
