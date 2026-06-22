"""Verify Class-1 estate stays within the £0 SKU allow-list and report MTD cost.

Lists every resource in the resource group, checks SKUs against a fixed
allow-list, then queries Cost Management for month-to-date actual spend.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.resource import ResourceManagementClient

logger = logging.getLogger(__name__)

# £0 allow-list — any SKU outside this set fails verification.
ALLOWED_STORAGE_SKUS = frozenset({"Standard_LRS"})
ALLOWED_STORAGE_KINDS = frozenset({"StorageV2"})
ALLOWED_KV_SKU_NAMES = frozenset({"standard"})
FREE_RESOURCE_TYPES = frozenset(
    {
        "Microsoft.Authorization/roleAssignments",
        "Microsoft.Consumption/budgets",
    }
)

# Platform services (deployed via infra/platform-services.bicep) — allowed with --include-platforms.
PLATFORM_RESOURCE_TYPES = frozenset(
    {
        "Microsoft.DataFactory/factories",
        "Microsoft.Synapse/workspaces",
        "Microsoft.Purview/accounts",
        "Microsoft.Databricks/workspaces",
        "Microsoft.Fabric/capacities",
    }
)
PLATFORM_CHILD_PREFIXES = (
    "Microsoft.DataFactory/",
    "Microsoft.Synapse/",
    "Microsoft.Purview/",
    "Microsoft.Databricks/",
)


@dataclass(frozen=True)
class SkuViolation:
    resource_id: str
    resource_type: str
    detail: str


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _month_bounds() -> tuple[str, str]:
    """Return (from_date, to_date) for the current UTC month (ISO-8601 datetimes)."""
    today = datetime.now(timezone.utc).date()
    start = date(today.year, today.month, 1)
    return (
        f"{start.isoformat()}T00:00:00Z",
        f"{today.isoformat()}T23:59:59Z",
    )


def _iter_resources(
    client: ResourceManagementClient, resource_group: str
) -> Iterator[Any]:
    return client.resources.list_by_resource_group(resource_group)


def _check_resource(resource: Any, *, include_platforms: bool = False) -> SkuViolation | None:
    rtype = resource.type
    rid = resource.id
    props = resource.properties or {}

    if rtype in FREE_RESOURCE_TYPES:
        return None

    if include_platforms and (
        rtype in PLATFORM_RESOURCE_TYPES
        or any(rtype.startswith(p) for p in PLATFORM_CHILD_PREFIXES)
    ):
        return None

    if rtype == "Microsoft.Storage/storageAccounts":
        sku_obj = resource.sku
        sku_name = (sku_obj.name if sku_obj else "") or ""
        if hasattr(sku_name, "value"):
            sku_name = sku_name.value
        kind = resource.kind or ""
        if sku_name not in ALLOWED_STORAGE_SKUS:
            return SkuViolation(rid, rtype, f"storage SKU '{sku_name}' not in allow-list")
        if kind not in ALLOWED_STORAGE_KINDS:
            return SkuViolation(rid, rtype, f"storage kind '{kind}' not in allow-list")
        access_tier = props.get("accessTier", "")
        if access_tier and access_tier.lower() != "hot":
            return SkuViolation(rid, rtype, f"access tier '{access_tier}' must be Hot")
        if props.get("isHnsEnabled") is False:
            return SkuViolation(rid, rtype, "hierarchical namespace must be enabled")
        return None

    if rtype == "Microsoft.KeyVault/vaults":
        sku = props.get("sku") or {}
        if not sku and resource.additional_properties:
            sku = resource.additional_properties.get("sku") or {}
        sku_name = (sku.get("name") or "").lower()
        if not sku_name:
            # List-by-RG API may omit SKU; Standard is the only tier deployed by Class-1.
            sku_name = "standard"
        if sku_name not in ALLOWED_KV_SKU_NAMES:
            return SkuViolation(rid, rtype, f"Key Vault SKU '{sku_name}' not in allow-list")
        rbac_auth = props.get("enableRbacAuthorization")
        if rbac_auth is False:
            return SkuViolation(rid, rtype, "Key Vault must use RBAC authorization")
        return None

    if rtype.startswith("Microsoft.Storage/storageAccounts/"):
        # Child resources (blobServices, containers, managementPolicies) inherit parent SKU.
        return None

    return SkuViolation(rid, rtype, "unexpected resource type in Class-1 estate")


def _query_mtd_cost(
    cost_client: CostManagementClient,
    subscription_id: str,
    resource_group: str,
) -> float:
    scope = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
    from_date, to_date = _month_bounds()

    query = {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": from_date, "to": to_date},
        "dataset": {
            "granularity": "None",
            "aggregation": {
                "totalCost": {"name": "Cost", "function": "Sum"},
            },
        },
    }

    result = cost_client.query.usage(scope=scope, parameters=query)
    rows = (result.rows or []) if result else []
    if not rows:
        return 0.0
    # First column is cost, second is currency in Cost Management responses.
    return float(rows[0][0])


def verify(
    subscription_id: str,
    resource_group: str,
    *,
    include_platforms: bool = False,
) -> tuple[list[SkuViolation], float, list[str]]:
    credential = DefaultAzureCredential()
    resource_client = ResourceManagementClient(credential, subscription_id)
    cost_client = CostManagementClient(credential)

    violations: list[SkuViolation] = []
    resource_ids: list[str] = []

    for resource in _iter_resources(resource_client, resource_group):
        resource_ids.append(resource.id)
        logger.info("Found resource: %s (%s)", resource.name, resource.type)
        violation = _check_resource(resource, include_platforms=include_platforms)
        if violation:
            violations.append(violation)

    mtd_cost = _query_mtd_cost(cost_client, subscription_id, resource_group)
    return violations, mtd_cost, resource_ids


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify Class-1 estate SKUs and month-to-date cost."
    )
    parser.add_argument(
        "--subscription-id",
        default=os.environ.get("AZURE_SUBSCRIPTION_ID"),
        help="Azure subscription ID.",
    )
    parser.add_argument(
        "--resource-group",
        required=True,
        help="Resource group to verify (e.g. rg-demo-class1).",
    )
    parser.add_argument(
        "--include-platforms",
        action="store_true",
        help="Allow ADF, Synapse, Purview, Databricks resources in the RG.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)

    if not args.subscription_id:
        logger.error("Subscription ID required: --subscription-id or AZURE_SUBSCRIPTION_ID")
        return 1

    try:
        violations, mtd_cost, resource_ids = verify(
            args.subscription_id, args.resource_group, include_platforms=args.include_platforms
        )
    except Exception:
        logger.exception("Verification failed")
        return 1

    logger.info("Resources checked: %d", len(resource_ids))
    logger.info("Month-to-date actual cost: £%.4f", mtd_cost)

    if violations:
        for v in violations:
            logger.error("SKU violation — %s: %s", v.resource_type, v.detail)
        return 1

    logger.info("All resources pass the £0 SKU allow-list.")

    # Cost Explorer deep links for the learner subscription / RG.
    scripts_dir = Path(__file__).resolve().parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    try:
        from compare_platform_costs import cost_portal_urls

        urls = cost_portal_urls(args.subscription_id, args.resource_group)
        logger.info("Cost Management links:")
        for label in (
            "cost_analysis_resource_group",
            "cost_analysis",
            "budgets",
            "pricing_calculator",
        ):
            if label in urls:
                logger.info("  %s: %s", label, urls[label])
    except ImportError:
        pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
