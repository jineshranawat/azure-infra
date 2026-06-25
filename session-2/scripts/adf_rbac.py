"""Ensure ADF managed identity can write to ADLS (check-before-create RBAC).

Uses Azure SDK (not `az datafactory show`) — CLI datafactory commands often hang
on classroom VDI / shared machines (90s+ timeout).
"""

from __future__ import annotations

import logging
import uuid

from azure.core.exceptions import HttpResponseError
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient

from _config import SessionConfig, get_credential

logger = logging.getLogger(__name__)

STORAGE_BLOB_DATA_CONTRIBUTOR = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"


def _role_definition_id(subscription_id: str, role_guid: str) -> str:
    return (
        f"/subscriptions/{subscription_id}/providers/"
        f"Microsoft.Authorization/roleDefinitions/{role_guid}"
    )


def _adf_principal_id(cfg: SessionConfig, data_factory: str) -> str:
    """Read ADF system-assigned managed identity via SDK (fast on VDI)."""
    adf = DataFactoryManagementClient(get_credential(), cfg.subscription_id)
    factory = adf.factories.get(cfg.resource_group, data_factory)
    identity = factory.identity
    principal_id = identity.principal_id if identity else None
    if not principal_id:
        raise RuntimeError(f"No managed identity on ADF {data_factory}")
    return principal_id


def _ensure_role_on_scope(
    auth_client: AuthorizationManagementClient,
    *,
    scope: str,
    principal_id: str,
    subscription_id: str,
) -> None:
    """Create Storage Blob Data Contributor if missing — idempotent by deterministic name."""
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
        logger.info("Assigned Storage Blob Data Contributor to ADF MI on %s", scope)
    except HttpResponseError as exc:
        code = exc.error.code if exc.error else ""
        if code in ("RoleAssignmentExists", "RoleAssignmentUpdateNotPermitted"):
            logger.info("ADF managed identity already has storage RBAC — skip")
            return
        raise


def ensure_adf_storage_rbac(
    cfg: SessionConfig,
    storage_account: str,
    data_factory: str,
) -> None:
    """Grant ADF system-assigned MI Storage Blob Data Contributor on the lake account."""
    logger.info("Checking ADF managed identity RBAC on %s (SDK)...", storage_account)
    principal_id = _adf_principal_id(cfg, data_factory)
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
