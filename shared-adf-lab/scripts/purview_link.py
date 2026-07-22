"""Link Microsoft Purview to shared ADF factory (optional, idempotent)."""

from __future__ import annotations

import json
import logging
import os
import subprocess

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import Factory, PurviewConfiguration

from _config import ENV_FILE, SharedAdfConfig, _load_dotenv, find_az, get_credential

logger = logging.getLogger(__name__)


def _az_json(args: list[str]) -> list | dict:
    result = subprocess.run(
        [find_az(), *args, "-o", "json"],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        return []
    return json.loads(result.stdout or "[]")


def discover_purview_account(cfg: SharedAdfConfig) -> tuple[str, str] | None:
    """Return (account_name, resource_group) from .env or class defaults."""
    env = _load_dotenv(ENV_FILE)
    # Shared-class Purview lives in rg-rohan-class1 (documented exception).
    default_name = "pviewrohan4hnv7s"
    default_rg = "rg-rohan-class1"
    name = (
        os.environ.get("PURVIEW_ACCOUNT_NAME")
        or env.get("PURVIEW_ACCOUNT_NAME", "")
        or default_name
    ).strip()
    rg = (
        os.environ.get("PURVIEW_RESOURCE_GROUP")
        or env.get("PURVIEW_RESOURCE_GROUP", "")
        or default_rg
    ).strip()
    if name and rg:
        return name, rg
    if name:
        accounts = _az_json(
            [
                "purview",
                "account",
                "list",
                "--subscription",
                cfg.subscription_id,
            ]
        )
        for acct in accounts if isinstance(accounts, list) else []:
            if acct.get("name") == name:
                return name, acct.get("resourceGroup", rg or cfg.resource_group)
    accounts = _az_json(
        [
            "purview",
            "account",
            "list",
            "--subscription",
            cfg.subscription_id,
        ]
    )
    if isinstance(accounts, list) and accounts:
        first = accounts[0]
        return first.get("name", ""), first.get("resourceGroup", cfg.resource_group)
    return None


def link_purview_to_adf(
    cfg: SharedAdfConfig,
    data_factory: str,
    *,
    adf: DataFactoryManagementClient | None = None,
) -> str | None:
    """Register Purview account on ADF factory for automatic lineage (best-effort)."""
    found = discover_purview_account(cfg)
    if not found:
        logger.warning(
            "No Purview account found — skip ADF link. "
            "Set PURVIEW_ACCOUNT_NAME in .env or deploy Purview in subscription."
        )
        return None
    account_name, purview_rg = found
    resource_id = (
        f"/subscriptions/{cfg.subscription_id}/resourceGroups/{purview_rg}"
        f"/providers/Microsoft.Purview/accounts/{account_name}"
    )
    client = adf or DataFactoryManagementClient(get_credential(), cfg.subscription_id)
    factory = client.factories.get(cfg.resource_group, data_factory)
    location = factory.location or cfg.adf_location
    identity = factory.identity
    updated = Factory(
        location=location,
        identity=identity,
        purview_configuration=PurviewConfiguration(purview_resource_id=resource_id),
    )
    client.factories.create_or_update(cfg.resource_group, data_factory, updated)
    logger.info("ADF %s linked to Purview %s (lineage enabled for Copy/Data Flow)", data_factory, account_name)
    logger.info(
        "MANUAL STEP: In classic Purview portal, grant Data Curator on root collection to "
        "ADF managed identity '%s'. New Purview portal does NOT show ADF lineage. "
        "See docs/purview_portal_search_guide.md",
        data_factory,
    )
    return account_name
