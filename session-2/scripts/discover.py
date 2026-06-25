"""Discover Class-1 + ADF resource names from live resource group (idempotent resume)."""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass
from typing import Any

from _config import SessionConfig, find_az

logger = logging.getLogger(__name__)

_AZ_TIMEOUT_SEC = 90


@dataclass(frozen=True)
class Estate:
    storage_account: str
    data_factory: str
    key_vault: str = ""


def _az_json(args: list[str]) -> Any:
    result = subprocess.run(
        [find_az(), *args, "-o", "json"],
        check=True,
        text=True,
        capture_output=True,
        timeout=_AZ_TIMEOUT_SEC,
    )
    return json.loads(result.stdout)


def discover_estate(cfg: SessionConfig) -> Estate:
    """List resources in RG; return storage + ADF names required for Session 2."""
    resources = _az_json(["resource", "list", "--resource-group", cfg.resource_group])
    storage = ""
    adf = ""
    kv = ""
    for res in resources:
        rtype = res.get("type", "")
        name = res.get("name", "")
        if rtype == "Microsoft.Storage/storageAccounts":
            storage = name
        elif rtype == "Microsoft.DataFactory/factories":
            adf = name
        elif rtype == "Microsoft.KeyVault/vaults":
            kv = name

    if not storage:
        raise SystemExit(
            f"Storage account not found in {cfg.resource_group}. "
            "Run Class-1 first: orchestrate.cmd --class1-only"
        )
    if not adf:
        raise SystemExit(
            f"Data Factory not found in {cfg.resource_group}. "
            "Run full lab first: orchestrate.cmd"
        )
    return Estate(storage_account=storage, data_factory=adf, key_vault=kv)
