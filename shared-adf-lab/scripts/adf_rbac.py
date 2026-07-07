"""RBAC for shared ADF — storage, Databricks, optional SQL."""

from __future__ import annotations

import logging
import uuid

from azure.core.exceptions import HttpResponseError
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient

from _config import SharedAdfConfig, get_credential

logger = logging.getLogger(__name__)

STORAGE_BLOB_DATA_CONTRIBUTOR = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"
DATABRICKS_CONTRIBUTOR = "b24988ac-6180-42a0-ab88-20f7382dd24c"  # Azure Contributor on workspace


def _role_id(subscription_id: str, guid: str) -> str:
    return f"/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleDefinitions/{guid}"


def _adf_principal_id(cfg: SharedAdfConfig, data_factory: str) -> str:
    adf = DataFactoryManagementClient(get_credential(), cfg.subscription_id)
    factory = adf.factories.get(cfg.resource_group, data_factory)
    pid = factory.identity.principal_id if factory.identity else None
    if not pid:
        raise RuntimeError(f"No managed identity on ADF {data_factory}")
    return pid


def _ensure_role(
    auth: AuthorizationManagementClient,
    *,
    scope: str,
    principal_id: str,
    role_guid: str,
    subscription_id: str,
    label: str,
) -> None:
    assignment_name = str(
        uuid.uuid5(uuid.NAMESPACE_DNS, f"{scope}:{principal_id}:{role_guid}")
    )
    try:
        auth.role_assignments.create(
            scope=scope,
            role_assignment_name=assignment_name,
            parameters={
                "role_definition_id": _role_id(subscription_id, role_guid),
                "principal_id": principal_id,
                "principal_type": "ServicePrincipal",
            },
        )
        logger.info("Assigned %s to ADF MI", label)
    except HttpResponseError as exc:
        code = exc.error.code if exc.error else ""
        if code in ("RoleAssignmentExists", "RoleAssignmentUpdateNotPermitted"):
            logger.info("%s already present — skip", label)
            return
        raise


def ensure_adf_rbac(
    cfg: SharedAdfConfig,
    estate_storage: str,
    data_factory: str,
    databricks_workspace: str = "",
) -> None:
    """Grant ADF managed identity access to lake + Databricks workspace."""
    principal_id = _adf_principal_id(cfg, data_factory)
    auth = AuthorizationManagementClient(get_credential(), cfg.subscription_id)
    sub = cfg.subscription_id
    rg = cfg.resource_group

    storage_scope = (
        f"/subscriptions/{sub}/resourceGroups/{rg}"
        f"/providers/Microsoft.Storage/storageAccounts/{estate_storage}"
    )
    _ensure_role(
        auth,
        scope=storage_scope,
        principal_id=principal_id,
        role_guid=STORAGE_BLOB_DATA_CONTRIBUTOR,
        subscription_id=sub,
        label="Storage Blob Data Contributor",
    )

    if databricks_workspace:
        dbw_scope = (
            f"/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Databricks/workspaces/{databricks_workspace}"
        )
        try:
            _ensure_role(
                auth,
                scope=dbw_scope,
                principal_id=principal_id,
                role_guid=DATABRICKS_CONTRIBUTOR,
                subscription_id=sub,
                label="Contributor on Databricks workspace",
            )
        except HttpResponseError as exc:
            logger.warning(
                "Databricks RBAC skipped (notebook pipelines may fail until fixed): %s",
                exc.message if exc.error else exc,
            )
