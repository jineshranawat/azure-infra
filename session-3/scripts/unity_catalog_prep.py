"""Ensure ADLS paths exist for Unity Catalog managed storage (all medallion layers).

Creates bronze|silver|gold/_unity_catalog/ — idempotent.
Orchestrate + notebooks use these paths as MANAGED LOCATION roots.
"""

from __future__ import annotations

import logging

from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient

from _config import get_credential

logger = logging.getLogger(__name__)

UC_ROOT_FOLDER = "_unity_catalog"
MEDALLION_LAYERS = ("bronze", "silver", "gold")


def layer_uc_abfss(storage_account: str, layer: str) -> str:
    return f"abfss://{layer}@{storage_account}.dfs.core.windows.net/{UC_ROOT_FOLDER}"


def ensure_unity_catalog_storage(storage_account: str) -> dict[str, str]:
    """Create _unity_catalog in each medallion container. Returns layer -> abfss path."""
    account_url = f"https://{storage_account}.dfs.core.windows.net"
    service = DataLakeServiceClient(account_url=account_url, credential=get_credential())
    roots: dict[str, str] = {}

    for layer in MEDALLION_LAYERS:
        fs = service.get_file_system_client(layer)
        try:
            fs.create_directory(UC_ROOT_FOLDER)
            logger.info("Created %s/%s for Unity Catalog", layer, UC_ROOT_FOLDER)
        except ResourceExistsError:
            logger.info("%s/%s already present — skip", layer, UC_ROOT_FOLDER)
        roots[layer] = layer_uc_abfss(storage_account, layer)

    for layer, path in roots.items():
        logger.info("  UC %s: %s", layer, path)
    return roots
