"""Discover shared estate resource names from live resource group."""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass

from _config import SharedAdfConfig, find_az

logger = logging.getLogger(__name__)
_AZ_TIMEOUT_SEC = 120


@dataclass(frozen=True)
class SharedEstate:
    storage_account: str
    data_factory: str
    key_vault: str
    databricks_workspace: str
    name_hash: str

    @property
    def dfs_endpoint(self) -> str:
        return f"https://{self.storage_account}.dfs.core.windows.net"


def _az_json(cfg: SharedAdfConfig, args: list[str]):
    result = subprocess.run(
        [find_az(), *args, "-o", "json"],
        check=True,
        text=True,
        capture_output=True,
        timeout=_AZ_TIMEOUT_SEC,
    )
    return json.loads(result.stdout)


def discover_estate(cfg: SharedAdfConfig) -> SharedEstate:
    resources = _az_json(cfg, ["resource", "list", "--resource-group", cfg.resource_group])
    storage = adf = kv = dbw = ""
    for res in resources:
        rtype = res.get("type", "")
        name = res.get("name", "")
        if rtype == "Microsoft.Storage/storageAccounts":
            storage = name
        elif rtype == "Microsoft.DataFactory/factories":
            adf = name
        elif rtype == "Microsoft.KeyVault/vaults":
            kv = name
        elif rtype == "Microsoft.Databricks/workspaces":
            dbw = name

    if not storage or not adf:
        raise SystemExit(
            f"Shared estate incomplete in {cfg.resource_group}. "
            "Run: provision-shared.cmd"
        )

    # Hash is suffix after learner prefix (stshared + hash, adf-shared- + hash)
    name_hash = ""
    if storage.startswith(f"st{cfg.learner}"):
        name_hash = storage[len(f"st{cfg.learner}") :]
    elif adf.startswith(f"adf-{cfg.learner}-"):
        name_hash = adf.split("-")[-1]

    logger.info(
        "Estate: storage=%s adf=%s kv=%s dbw=%s hash=%s",
        storage,
        adf,
        kv or "(none)",
        dbw or "(none)",
        name_hash,
    )
    return SharedEstate(
        storage_account=storage,
        data_factory=adf,
        key_vault=kv,
        databricks_workspace=dbw,
        name_hash=name_hash,
    )
