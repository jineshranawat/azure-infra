"""Deploy ADF factory global parameters (REST — idempotent merge)."""

from __future__ import annotations

import logging

import requests
from azure.mgmt.datafactory import DataFactoryManagementClient

from _config import SharedAdfConfig, get_credential

logger = logging.getLogger(__name__)

API_VERSION = "2018-06-01"

# Factory-wide defaults — referenced as @pipeline().globalParameters.<name>
LAB_GLOBAL_PARAMETERS: dict[str, dict[str, str]] = {
    "g_lab_environment": {"type": "String", "value": "shared-class1"},
    "g_trainer_tag": {"type": "String", "value": "finledger-adf-lab"},
}


def _global_parameters_url(cfg: SharedAdfConfig, factory: str) -> str:
    return (
        f"https://management.azure.com/subscriptions/{cfg.subscription_id}"
        f"/resourceGroups/{cfg.resource_group}/providers/Microsoft.DataFactory"
        f"/factories/{factory}/globalParameters/default?api-version={API_VERSION}"
    )


def deploy_global_parameters(
    cfg: SharedAdfConfig,
    factory: str,
    *,
    adf: DataFactoryManagementClient | None = None,
) -> None:
    """Create or update factory global parameters (merge with existing)."""
    _ = adf  # reserved — REST used for broad SDK compatibility
    token = get_credential().get_token("https://management.azure.com/.default").token
    url = _global_parameters_url(cfg, factory)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    existing: dict[str, dict[str, str]] = {}
    resp = requests.get(url, headers=headers, timeout=60)
    if resp.status_code == 200:
        existing = resp.json().get("properties", {}) or {}
    elif resp.status_code != 404:
        logger.warning("Global parameters GET %s: %s", resp.status_code, resp.text[:200])

    merged = {**existing, **LAB_GLOBAL_PARAMETERS}
    put = requests.put(url, headers=headers, json={"properties": merged}, timeout=60)
    if put.status_code >= 300:
        raise RuntimeError(
            f"Global parameters deploy failed: {put.status_code} {put.text[:300]}"
        )
    for name in LAB_GLOBAL_PARAMETERS:
        logger.info("Global parameter %s → %s", name, LAB_GLOBAL_PARAMETERS[name]["value"])
