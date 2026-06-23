"""Ensure ADF managed identity can write to ADLS (check-before-create RBAC)."""

from __future__ import annotations

import json
import logging
import subprocess

from _config import SessionConfig, find_az

logger = logging.getLogger(__name__)

STORAGE_BLOB_DATA_CONTRIBUTOR = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run([find_az(), *args], check=False, text=True, capture_output=True)


def ensure_adf_storage_rbac(
    cfg: SessionConfig,
    storage_account: str,
    data_factory: str,
) -> None:
    """Grant ADF system-assigned MI Storage Blob Data Contributor on the lake account."""
    show = _run(
        [
            "datafactory", "show",
            "--resource-group", cfg.resource_group,
            "--factory-name", data_factory,
        ]
    )
    if show.returncode != 0:
        raise RuntimeError(show.stderr or show.stdout)

    factory = json.loads(show.stdout)
    principal_id = factory.get("identity", {}).get("principalId")
    if not principal_id:
        raise RuntimeError(f"No managed identity on ADF {data_factory}")

    scope = (
        f"/subscriptions/{cfg.subscription_id}/resourceGroups/{cfg.resource_group}"
        f"/providers/Microsoft.Storage/storageAccounts/{storage_account}"
    )

    existing = _run(
        [
            "role", "assignment", "list",
            "--assignee-object-id", principal_id,
            "--scope", scope,
            "--query", "length(@)",
            "-o", "tsv",
        ]
    )
    if existing.stdout.strip() not in ("", "0"):
        logger.info("ADF managed identity already has storage RBAC — skip")
        return

    _run(
        [
            "role", "assignment", "create",
            "--role", "Storage Blob Data Contributor",
            "--assignee-object-id", principal_id,
            "--assignee-principal-type", "ServicePrincipal",
            "--scope", scope,
            "-o", "none",
        ]
    )
    logger.info("Assigned Storage Blob Data Contributor to ADF MI on %s", storage_account)
