"""Ensure Databricks workspace access connector MI can read/write ADLS (idempotent RBAC).

Uses Azure CLI `az resource show` only — no azure-mgmt-resource import (breaks on partial venvs).
"""

from __future__ import annotations

import json
import logging
import subprocess
import uuid

from azure.core.exceptions import HttpResponseError
from azure.mgmt.authorization import AuthorizationManagementClient

from _config import SessionConfig, find_az, get_credential

logger = logging.getLogger(__name__)

STORAGE_BLOB_DATA_CONTRIBUTOR = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"
_AZ_TIMEOUT_SEC = 20


def _role_definition_id(subscription_id: str, role_guid: str) -> str:
    return (
        f"/subscriptions/{subscription_id}/providers/"
        f"Microsoft.Authorization/roleDefinitions/{role_guid}"
    )


def _az_json(args: list[str]) -> dict | None:
    """Run az … -o json with short timeout; return None on failure (non-fatal)."""
    try:
        result = subprocess.run(
            [find_az(), *args, "-o", "json"],
            check=False,
            text=True,
            capture_output=True,
            timeout=_AZ_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        logger.info("Azure CLI timed out (%ss) — skip connector RBAC", _AZ_TIMEOUT_SEC)
        return None
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def _workspace_access_connector_principal(cfg: SessionConfig, workspace_name: str) -> str | None:
    """Return principal ID of the workspace access connector managed identity, if present."""
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
        logger.info("Workspace metadata unavailable — Class-1 user RBAC applies for abfss://")
        return None

    connector_id = (ws.get("properties") or {}).get("accessConnectorId")
    if not connector_id:
        logger.info("No access connector — interactive user RBAC from Class-1 applies")
        return None

    ac = _az_json(["resource", "show", "--ids", connector_id])
    if not ac:
        return None

    identity = ac.get("identity") or {}
    return identity.get("principalId")


def _ensure_role_on_scope(
    auth_client: AuthorizationManagementClient,
    *,
    scope: str,
    principal_id: str,
    subscription_id: str,
) -> None:
    assignment_name = str(
        uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{scope}:{principal_id}:{STORAGE_BLOB_DATA_CONTRIBUTOR}",
        )
    )
    try:
        auth_client.role_assignments.create(
            scope=scope,
            role_assignment_name=assignment_name,
            parameters={
                "role_definition_id": _role_definition_id(
                    subscription_id, STORAGE_BLOB_DATA_CONTRIBUTOR
                ),
                "principal_id": principal_id,
                "principal_type": "ServicePrincipal",
            },
        )
        logger.info("Assigned Storage Blob Data Contributor on %s", scope)
    except HttpResponseError as exc:
        code = exc.error.code if exc.error else ""
        if code in ("RoleAssignmentExists", "RoleAssignmentUpdateNotPermitted"):
            logger.info("Databricks access connector already has storage RBAC — skip")
            return
        raise


def ensure_databricks_storage_rbac(
    cfg: SessionConfig,
    storage_account: str,
    databricks_workspace: str,
) -> None:
    """Grant workspace access connector MI storage access when connector exists."""
    principal_id = _workspace_access_connector_principal(cfg, databricks_workspace)
    if not principal_id:
        return

    scope = (
        f"/subscriptions/{cfg.subscription_id}/resourceGroups/{cfg.resource_group}"
        f"/providers/Microsoft.Storage/storageAccounts/{storage_account}"
    )
    auth = AuthorizationManagementClient(get_credential(), cfg.subscription_id)
    _ensure_role_on_scope(
        auth,
        scope=scope,
        principal_id=principal_id,
        subscription_id=cfg.subscription_id,
    )
