"""Provision the Class-1 Azure landing zone via azure-mgmt-* SDKs.

Build order (guardrail-first):
  1. Resource group + mandatory tags
  2. Consumption budget (£1/month alerts)
  3. Key Vault (RBAC, Standard tier)
  4. Storage account + medallion containers + lifecycle policy
  5. RBAC role assignments (KV Secrets Officer, Storage Blob Data Contributor)

Every operation is idempotent (create_or_update / begin_create_or_update).
Auth: DefaultAzureCredential only — no secrets in code.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
import uuid
from datetime import date, datetime, timezone
from typing import Any

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.consumption import ConsumptionManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.keyvault import KeyVaultManagementClient

# ── Constants tied to non-functional requirements ─────────────────────────────
ALLOWED_LOCATIONS = frozenset({"uksouth", "ukwest"})
MANDATORY_TAGS: dict[str, str] = {
    "env": "training",
    "costcentre": "boe-data-enablement",
    "data-class": "training-synthetic",
    "course": "azure-etl-boe",
    "class": "class-1",
    "auto-teardown": "nightly",
}
MEDALLION_CONTAINERS = ("bronze", "silver", "gold", "audit")
KV_SECRETS_OFFICER_ROLE = "b86a8fe4-44ce-4948-aee5-586e75ac9c75"
STORAGE_BLOB_DATA_CONTRIBUTOR_ROLE = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"

logger = logging.getLogger(__name__)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _reject_non_uk_region(location: str) -> None:
    """UK residency — reject any region outside uksouth / ukwest."""
    if location not in ALLOWED_LOCATIONS:
        raise ValueError(
            f"Location '{location}' is not permitted. Use uksouth or ukwest only."
        )


def _build_tags(owner_email: str) -> dict[str, str]:
    tags = dict(MANDATORY_TAGS)
    tags["owner"] = owner_email
    return tags


def _name_hash(subscription_id: str, resource_group: str, learner: str) -> str:
    """Deterministic 6-char hash — mirrors Bicep uniqueString()."""
    seed = f"{subscription_id}:{resource_group}:{learner}"
    digest = hashlib.sha256(seed.encode()).hexdigest()
    return digest[:6]


def _storage_account_name(learner: str, name_hash: str) -> str:
    name = f"st{learner.lower()}{name_hash}"
    if len(name) > 24:
        raise ValueError(f"Storage account name '{name}' exceeds 24 characters.")
    return name


def _key_vault_name(learner: str, name_hash: str) -> str:
    return f"kv-{learner}-{name_hash}"


def _budget_start_date() -> str:
    """First day of the current UTC month — matches orchestrate.py budget anchor."""
    today = datetime.now(timezone.utc).date()
    return date(today.year, today.month, 1).isoformat()


def _get_principal_object_id() -> str:
    """Resolve signed-in user object ID — mirrors orchestrate.py az CLI lookup."""
    import subprocess

    result = subprocess.run(
        ["az", "ad", "signed-in-user", "show", "--query", "id", "-o", "tsv"],
        capture_output=True,
        text=True,
        check=True,
    )
    principal_id = result.stdout.strip()
    if not principal_id:
        raise RuntimeError("Could not derive principal object ID from az CLI.")
    return principal_id


def _ensure_resource_group(
    resource_client: ResourceManagementClient,
    resource_group: str,
    location: str,
    tags: dict[str, str],
) -> None:
    logger.info("Step 1/5 — resource group + tags: %s", resource_group)
    resource_client.resource_groups.create_or_update(
        resource_group,
        {"location": location, "tags": tags},
    )


def _ensure_budget(
    consumption_client: ConsumptionManagementClient,
    subscription_id: str,
    resource_group: str,
    owner_email: str,
    learner: str,
) -> None:
    logger.info("Step 2/5 — consumption budget guardrail (before spend resources)")
    budget_name = f"budget-{learner}-class1"
    scope = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"

    budget = {
        "category": "Cost",
        "amount": 1,
        "time_grain": "Monthly",
        "time_period": {
            "start_date": _budget_start_date(),
            "end_date": "2030-12-31",
        },
        "filter": {
            "dimensions": {
                "name": "ResourceGroupName",
                "operator": "In",
                "values": [resource_group],
            }
        },
        "notifications": {
            "Actual_GreaterThan_50_Pct": {
                "enabled": True,
                "operator": "GreaterThan",
                "threshold": 50,
                "threshold_type": "Actual",
                "contact_emails": [owner_email],
            },
            "Forecast_GreaterThan_90_Pct": {
                "enabled": True,
                "operator": "GreaterThan",
                "threshold": 90,
                "threshold_type": "Forecasted",
                "contact_emails": [owner_email],
            },
        },
    }

    consumption_client.budgets.create_or_update(
        scope=scope,
        budget_name=budget_name,
        parameters=budget,
    )


def _ensure_key_vault(
    kv_client: KeyVaultManagementClient,
    subscription_id: str,
    resource_group: str,
    location: str,
    vault_name: str,
    tags: dict[str, str],
    tenant_id: str,
) -> Any:
    logger.info("Step 3/5 — Key Vault (RBAC mode, Standard tier): %s", vault_name)
    parameters = {
        "location": location,
        "tags": tags,
        "properties": {
            "tenant_id": tenant_id,
            "sku": {"family": "A", "name": "standard"},
            "enable_rbac_authorization": True,
            "enabled_for_deployment": False,
            "enabled_for_disk_encryption": False,
            "enabled_for_template_deployment": False,
            "enable_soft_delete": True,
            "soft_delete_retention_in_days": 7,
            "public_network_access": "Enabled",
        },
    }
    poller = kv_client.vaults.begin_create_or_update(
        resource_group_name=resource_group,
        vault_name=vault_name,
        parameters=parameters,
    )
    return poller.result()


def _ensure_storage_account(
    storage_client: StorageManagementClient,
    resource_group: str,
    location: str,
    account_name: str,
    tags: dict[str, str],
) -> Any:
    logger.info("Step 4/5 — storage account (StorageV2, Standard_LRS, HNS): %s", account_name)
    parameters = {
        "sku": {"name": "Standard_LRS"},
        "kind": "StorageV2",
        "location": location,
        "tags": tags,
        "properties": {
            "access_tier": "Hot",
            "is_hns_enabled": True,
            "allow_blob_public_access": False,
            "minimum_tls_version": "TLS1_2",
            "supports_https_traffic_only": True,
        },
    }
    poller = storage_client.storage_accounts.begin_create_or_update(
        resource_group_name=resource_group,
        account_name=account_name,
        parameters=parameters,
    )
    return poller.result()


def _ensure_blob_service_properties(
    storage_client: StorageManagementClient,
    resource_group: str,
    account_name: str,
) -> None:
    storage_client.blob_services.set_service_properties(
        resource_group_name=resource_group,
        account_name=account_name,
        parameters={
            "delete_retention_policy": {"enabled": True, "days": 7},
            "container_delete_retention_policy": {"enabled": True, "days": 7},
            # Versioning is incompatible with HNS (ADLS Gen2).
        },
    )


def _ensure_containers(
    storage_client: StorageManagementClient,
    resource_group: str,
    account_name: str,
) -> None:
    for container in MEDALLION_CONTAINERS:
        logger.debug("Ensuring container: %s", container)
        try:
            storage_client.blob_containers.create(
                resource_group_name=resource_group,
                account_name=account_name,
                container_name=container,
                blob_container={"properties": {"public_access": "None"}},
            )
        except (ResourceExistsError, HttpResponseError) as exc:
            if isinstance(exc, HttpResponseError) and exc.status_code != 409:
                raise
            logger.info("Container '%s' already exists — continuing", container)


def _ensure_lifecycle_policy(
    storage_client: StorageManagementClient,
    resource_group: str,
    account_name: str,
) -> None:
    policy = {
        "policy": {
            "rules": [
                {
                    "enabled": True,
                    "name": "tier-block-blobs-cool-30d",
                    "type": "Lifecycle",
                    "definition": {
                        "actions": {
                            "base_blob": {
                                "tier_to_cool": {
                                    "days_after_modification_greater_than": 30
                                }
                            }
                        },
                        "filters": {"blob_types": ["blockBlob"]},
                    },
                },
                {
                    "enabled": True,
                    "name": "delete-block-blobs-7d",
                    "type": "Lifecycle",
                    "definition": {
                        "actions": {
                            "base_blob": {
                                "delete": {
                                    "days_after_modification_greater_than": 7
                                }
                            }
                        },
                        "filters": {"blob_types": ["blockBlob"]},
                    },
                },
            ]
        }
    }
    storage_client.management_policies.create_or_update(
        resource_group_name=resource_group,
        account_name=account_name,
        management_policy_name="default",
        properties=policy,
    )


def _role_definition_id(subscription_id: str, role_guid: str) -> str:
    return (
        f"/subscriptions/{subscription_id}/providers/"
        f"Microsoft.Authorization/roleDefinitions/{role_guid}"
    )


def _ensure_role_assignment(
    auth_client: AuthorizationManagementClient,
    scope: str,
    role_guid: str,
    principal_id: str,
    subscription_id: str,
) -> None:
    role_definition_id = _role_definition_id(subscription_id, role_guid)
    assignment_name = str(
        uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{scope}:{principal_id}:{role_guid}",
        )
    )
    try:
        auth_client.role_assignments.create(
            scope=scope,
            role_assignment_name=assignment_name,
            parameters={
                "role_definition_id": role_definition_id,
                "principal_id": principal_id,
                "principal_type": "User",
            },
        )
        logger.info("Role assignment created on %s", scope)
    except HttpResponseError as exc:
        if exc.error and exc.error.code == "RoleAssignmentExists":
            logger.info("Role assignment already exists on %s — continuing", scope)
        else:
            raise


def _ensure_rbac(
    auth_client: AuthorizationManagementClient,
    subscription_id: str,
    resource_group: str,
    vault_name: str,
    account_name: str,
    principal_id: str,
) -> None:
    logger.info("Step 5/5 — data-plane RBAC (least privilege)")
    kv_scope = (
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.KeyVault/vaults/{vault_name}"
    )
    storage_scope = (
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Storage/storageAccounts/{account_name}"
    )
    _ensure_role_assignment(
        auth_client, kv_scope, KV_SECRETS_OFFICER_ROLE, principal_id, subscription_id
    )
    _ensure_role_assignment(
        auth_client,
        storage_scope,
        STORAGE_BLOB_DATA_CONTRIBUTOR_ROLE,
        principal_id,
        subscription_id,
    )


def provision(
    subscription_id: str,
    learner: str,
    owner_email: str,
    location: str,
    principal_id: str | None,
) -> dict[str, str]:
    _reject_non_uk_region(location)

    resource_group = f"rg-{learner}-class1"
    tags = _build_tags(owner_email)
    name_hash = _name_hash(subscription_id, resource_group, learner)
    account_name = _storage_account_name(learner, name_hash)
    vault_name = _key_vault_name(learner, name_hash)

    credential = DefaultAzureCredential()
    resource_client = ResourceManagementClient(credential, subscription_id)
    consumption_client = ConsumptionManagementClient(credential, subscription_id)
    kv_client = KeyVaultManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)
    auth_client = AuthorizationManagementClient(credential, subscription_id)

    if principal_id is None:
        principal_id = _get_principal_object_id()

    from azure.mgmt.resource.subscriptions import SubscriptionClient

    sub_client = SubscriptionClient(credential)
    subscription = sub_client.subscriptions.get(subscription_id)
    tenant_id = subscription.tenant_id

    _ensure_resource_group(resource_client, resource_group, location, tags)
    _ensure_budget(
        consumption_client, subscription_id, resource_group, owner_email, learner
    )
    _ensure_key_vault(
        kv_client,
        subscription_id,
        resource_group,
        location,
        vault_name,
        tags,
        tenant_id,
    )
    account = _ensure_storage_account(
        storage_client, resource_group, location, account_name, tags
    )
    _ensure_blob_service_properties(storage_client, resource_group, account_name)
    _ensure_containers(storage_client, resource_group, account_name)
    _ensure_lifecycle_policy(storage_client, resource_group, account_name)
    _ensure_rbac(
        auth_client,
        subscription_id,
        resource_group,
        vault_name,
        account_name,
        principal_id,
    )

    dfs_endpoint = account.primary_endpoints.dfs if account.primary_endpoints else ""
    return {
        "resource_group": resource_group,
        "storage_account_name": account_name,
        "key_vault_name": vault_name,
        "dfs_endpoint": dfs_endpoint or "",
        "location": location,
        "principal_object_id": principal_id,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Provision Class-1 Azure landing zone (idempotent, £0 SKUs)."
    )
    parser.add_argument(
        "--subscription-id",
        default=os.environ.get("AZURE_SUBSCRIPTION_ID"),
        help="Azure subscription ID (or set AZURE_SUBSCRIPTION_ID).",
    )
    parser.add_argument(
        "--learner",
        default=os.environ.get("LEARNER", "demo"),
        help="Short learner identifier for resource names.",
    )
    parser.add_argument(
        "--owner-email",
        default=os.environ.get("OWNER_EMAIL"),
        help="Owner email for tags and budget alerts.",
    )
    parser.add_argument(
        "--location",
        default=os.environ.get("LOCATION", "uksouth"),
        help="Azure region (uksouth or ukwest only).",
    )
    parser.add_argument(
        "--principal-id",
        default=None,
        help="Optional object ID; derived from signed-in user if omitted.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)

    if not args.subscription_id:
        logger.error("Subscription ID required: --subscription-id or AZURE_SUBSCRIPTION_ID")
        return 1
    if not args.owner_email:
        logger.error("Owner email required: --owner-email or OWNER_EMAIL")
        return 1

    try:
        result = provision(
            subscription_id=args.subscription_id,
            learner=args.learner,
            owner_email=args.owner_email,
            location=args.location,
            principal_id=args.principal_id,
        )
    except Exception:
        logger.exception("Provisioning failed")
        return 1

    for key, value in result.items():
        logger.info("%s: %s", key, value)
    return 0


if __name__ == "__main__":
    sys.exit(main())
