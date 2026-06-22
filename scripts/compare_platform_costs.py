"""Compare idle/entry-level costs for Azure data platform services (UK regions).

Uses the public Azure Retail Prices API — no extra resources are deployed.
Class-1 landing zone (storage + Key Vault) remains the cheapest *deployed* estate.

Services compared:
  - Azure Data Factory (ADF)
  - Azure Databricks
  - Microsoft Purview
  - Azure Synapse Analytics (serverless SQL / SQL warehouse metering)
  - Azure SQL Database (SQL Server PaaS)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

RETAIL_API = "https://prices.azure.com/api/retail/prices"
DEFAULT_REGION = "uksouth"

# Friendly name -> Retail API serviceName (exact or partial match strategy in code)
PLATFORMS: list[dict[str, str]] = [
    {
        "id": "adf",
        "name": "Azure Data Factory",
        "service_name": "Azure Data Factory",
        "idle_note": "Factory control plane is free; pay per pipeline activity / IR hour when jobs run.",
    },
    {
        "id": "databricks",
        "name": "Azure Databricks",
        "service_name": "Azure Databricks",
        "idle_note": "Workspace may incur cost; clusters bill DBUs whenever running (not idle-cheap).",
    },
    {
        "id": "purview",
        "name": "Microsoft Purview",
        "service_name": "Microsoft Purview",
        "idle_note": "Governance catalog; Standard tier uses capacity units (minimum monthly footprint).",
    },
    {
        "id": "synapse_sql",
        "name": "Synapse SQL (serverless pool)",
        "service_name": "Azure Synapse Analytics",
        "idle_note": "Serverless SQL: no fixed fee; pay per TB scanned when queries run.",
    },
    {
        "id": "sql_database",
        "name": "Azure SQL Database",
        "service_name": "SQL Database",
        "idle_note": "Basic/DTU tiers have fixed monthly cost; serverless can pause but not £0 fixed.",
    },
    {
        "id": "class1_storage",
        "name": "Class-1 estate (Storage + KV)",
        "service_name": "Storage",
        "idle_note": "This repo's landing zone: no fixed monthly fee, pennies MTD for empty estate.",
    },
]


@dataclass(frozen=True)
class PriceSummary:
    platform_id: str
    display_name: str
    min_price: float
    max_price: float
    currency: str
    unit: str
    sample_meter: str
    meter_count: int
    idle_note: str
    has_fixed_monthly: bool


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )


def _fetch_retail_prices(service_name: str, region: str) -> list[dict[str, Any]]:
    """Paginate Azure Retail Prices API for a service in a region."""
    items: list[dict[str, Any]] = []
    filt = f"armRegionName eq '{region}' and serviceName eq '{service_name}'"
    url: str | None = f"{RETAIL_API}?$filter={urllib.parse.quote(filt)}"

    while url:
        req = urllib.request.Request(url, headers={"User-Agent": "azure-etl-boe-class1"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode())
        except urllib.error.URLError as exc:
            logger.warning("Retail API error for %s: %s", service_name, exc)
            break

        items.extend(body.get("Items", []))
        url = body.get("NextPageLink")

    return items


def _summarise_platform(platform: dict[str, str], region: str) -> PriceSummary:
    items = _fetch_retail_prices(platform["service_name"], region)

    if not items and platform["id"] == "class1_storage":
        # Also pull Key Vault meters for completeness
        items = _fetch_retail_prices("Key Vault", region)

    prices = [float(i.get("retailPrice", 0)) for i in items if i.get("retailPrice") is not None]
    currency = items[0].get("currencyCode", "GBP") if items else "GBP"

    if not prices:
        return PriceSummary(
            platform_id=platform["id"],
            display_name=platform["name"],
            min_price=0.0,
            max_price=0.0,
            currency=currency,
            unit="n/a",
            sample_meter="No public meters found for region (check portal calculator)",
            meter_count=0,
            idle_note=platform["idle_note"],
            has_fixed_monthly=platform["id"] in {"sql_database", "purview", "databricks"},
        )

    # Prefer lowest non-zero pay-as-you-go meter for ranking
    non_zero = [p for p in prices if p > 0]
    ranked = non_zero or prices
    cheapest_item = min(items, key=lambda i: float(i.get("retailPrice", 1e18)))

    unit = cheapest_item.get("unitOfMeasure", "unit")
    meter = cheapest_item.get("meterName", cheapest_item.get("productName", "unknown"))

    fixed_monthly = any(
        kw in (cheapest_item.get("meterName") or "").lower()
        or kw in (cheapest_item.get("skuName") or "").lower()
        for kw in ("month", "daily", "dtu", "vcore", "capacity")
    ) or platform["id"] in {"sql_database", "purview"}

    return PriceSummary(
        platform_id=platform["id"],
        display_name=platform["name"],
        min_price=min(ranked),
        max_price=max(prices),
        currency=currency,
        unit=unit,
        sample_meter=meter,
        meter_count=len(items),
        idle_note=platform["idle_note"],
        has_fixed_monthly=fixed_monthly,
    )


def compare(region: str = DEFAULT_REGION) -> list[PriceSummary]:
    return [_summarise_platform(p, region) for p in PLATFORMS]


def cost_portal_urls(subscription_id: str, resource_group: str | None = None) -> dict[str, str]:
    """Deep links to Azure Cost Management blades."""
    sub_path = urllib.parse.quote(f"/subscriptions/{subscription_id}", safe="")
    base = "https://portal.azure.com/#view/Microsoft_Azure_CostManagement"
    urls = {
        "cost_analysis": f"{base}/Menu/~/costanalysis/open/scope/{sub_path}",
        "budgets": "https://portal.azure.com/#view/Microsoft_Azure_CostManagement/BudgetsBlade",
        "cost_alerts": f"{base}/Menu/~/costalerts/open/scope/{sub_path}",
        "pricing_calculator": "https://azure.microsoft.com/en-gb/pricing/calculator/",
        "retail_api": RETAIL_API,
    }
    if resource_group:
        rg_path = urllib.parse.quote(
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}", safe=""
        )
        urls["cost_analysis_resource_group"] = (
            f"{base}/Menu/~/costanalysis/open/scope/{rg_path}"
        )
        urls["resource_group"] = (
            f"https://portal.azure.com/#@/resource/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}/overview"
        )
    return urls


def _training_rank(summaries: list[PriceSummary]) -> list[PriceSummary]:
    """Rank for *training idle* posture (lower is cheaper)."""

    def sort_key(s: PriceSummary) -> tuple[int, float]:
        # Class-1 storage estate wins for empty training deploy
        if s.platform_id == "class1_storage":
            return (0, 0.0)
        if s.platform_id == "adf":
            return (1, s.min_price)
        if s.platform_id == "synapse_sql":
            return (2, s.min_price)
        if s.has_fixed_monthly:
            return (4, s.min_price)
        if s.platform_id == "databricks":
            return (5, s.min_price)
        return (3, s.min_price)

    return sorted(summaries, key=sort_key)


def print_report(
    summaries: list[PriceSummary],
    *,
    subscription_id: str | None,
    resource_group: str | None,
    region: str,
) -> None:
    ranked = _training_rank(summaries)

    print("")
    print("=" * 72)
    print(f"Azure data platform cost comparison — region: {region}")
    print("Source: Azure Retail Prices API (list prices; your EA/CSP discount may differ)")
    print("=" * 72)
    print("")
    print(f"{'Rank':<5} {'Platform':<32} {'Min list':>10} {'Unit':<18} Fixed?")
    print("-" * 72)

    for i, s in enumerate(ranked, start=1):
        fixed = "yes" if s.has_fixed_monthly else "no*"
        price = f"{s.currency} {s.min_price:.4f}" if s.meter_count else "n/a"
        print(f"{i:<5} {s.display_name:<32} {price:>10} {s.unit[:18]:<18} {fixed}")

    print("")
    print("* no = consumption-only at rest for training; charges apply when used.")
    print("")
    print("Training idle ranking (cheapest first for Class-style labs):")
    for i, s in enumerate(ranked[:4], start=1):
        print(f"  {i}. {s.display_name} — {s.idle_note}")

    print("")
    print("Sample meters (lowest found):")
    for s in ranked:
        if s.meter_count:
            print(f"  - {s.display_name}: {s.sample_meter} ({s.currency} {s.min_price}/{s.unit})")

    if subscription_id:
        urls = cost_portal_urls(subscription_id, resource_group)
        print("")
        print("=" * 72)
        print("Cost Explorer / Cost Management links (your subscription)")
        print("=" * 72)
        for label, url in urls.items():
            print(f"  {label}: {url}")

    print("")
    print("Note: This repo deploys Class-1 (storage + Key Vault) only — not ADF/Databricks/")
    print("Purview/SQL — adding those would break the £0 fixed-fee guardrail in verify_cost.py.")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare Azure data platform list prices (UK).")
    parser.add_argument("--region", default=os.environ.get("LOCATION", DEFAULT_REGION))
    parser.add_argument("--subscription-id", default=os.environ.get("AZURE_SUBSCRIPTION_ID"))
    parser.add_argument("--resource-group", default=None)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)

    if args.region not in ("uksouth", "ukwest"):
        logger.warning("Region %s — comparison uses retail API; Class-1 requires UK only.", args.region)

    summaries = compare(args.region)

    if args.json:
        payload = {
            "region": args.region,
            "platforms": [s.__dict__ for s in summaries],
            "urls": cost_portal_urls(args.subscription_id, args.resource_group)
            if args.subscription_id
            else {},
        }
        print(json.dumps(payload, indent=2))
        return 0

    rg = args.resource_group
    if not rg and args.subscription_id:
        learner = os.environ.get("LEARNER", "demo")
        rg = f"rg-{learner}-class1"

    print_report(
        summaries,
        subscription_id=args.subscription_id,
        resource_group=rg,
        region=args.region,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
