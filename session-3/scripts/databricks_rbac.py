"""Ensure Databricks workspace access connector MI can read/write ADLS (idempotent RBAC).

Uses Azure Resource Manager SDK — NOT `az databricks workspace show` (hangs 90s+ on VDI).
"""

from __future__ import annotations

import logging
import uuid

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.resource import ResourceManagementClient

from _config import SessionConfig, get_credential

logger = logging.getLogger(__name__)

STORAGE_BLOB_DATA_CONTRIBUTOR = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"
_DATABRICKS_API = "2024-05-01"
_CONNECTOR_API = "2022-10-01"


def _role_definition_id(subscription_id: str, role_guid: str) -> str:
    return (
        f"/subscriptions/{subscription_id}/providers/"
        f"Microsoft.Authorization/roleDefinitions/{role_guid}"
    )


def _workspace_access_connector_principal(cfg: SessionConfig, workspace_name: str) -> str | None:
    """Return principal ID of the workspace access connector managed identity, if present."""
    client = ResourceManagementClient(get_credential(), cfg.subscription_id)
    try:
        ws = client.resources.get(
            resource_group_name=cfg.resource_group,
            resource_provider_namespace="Microsoft.Databricks",
            parent_resource_path="",
            resource_type="workspaces",
            resource_name=workspace_name,
            api_version=_DATABRICKS_API,
        )
    except ResourceNotFoundError:
        logger.info("Databricks workspace resource not found — skip connector RBAC")
        return None
    except HttpResponseError as exc:
        logger.info("Workspace read skipped (%s) — Class-1 user RBAC applies", exc.message)
        return None

    props = ws.properties or {}
    connector_id = props.get("accessConnectorId")
    if not connector_id:
        logger.info("No access connector — interactive user RBAC from Class-1 applies")
        return None

    try:
        ac = client.resources.get_by_id(connector_id, api_version=_CONNECTOR_API)
    except (ResourceNotFoundError, HttpResponseError):
        return None

    identity = getattr(ac, "identity", None)
    if identity and getattr(identity, "principal_id", None):
        return identity.principal_id
    return None


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
