"""Watermark control file in bronze/_control (azure.html Day 2 Hour 20 — incremental ingest).

Stores last successful run_id and UTC timestamp as JSON. Check-before-write on re-run.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)

WATERMARK_PATH = "_control/watermark.json"


def read_watermark(storage_account: str) -> dict[str, str] | None:
    """Return parsed watermark JSON or None if not yet created."""
    account_url = f"https://{storage_account}.dfs.core.windows.net"
    credential = DefaultAzureCredential()
    fs = DataLakeServiceClient(account_url=account_url, credential=credential).get_file_system_client(
        "bronze"
    )
    file_client = fs.get_file_client(WATERMARK_PATH)
    try:
        raw = file_client.download_file().readall().decode()
        return json.loads(raw)
    except Exception:
        return None


def write_watermark(storage_account: str, run_id: str, loaded_path: str) -> dict[str, str]:
    """Upsert watermark after successful bronze load — idempotent overwrite."""
    body = {
        "last_run_id": run_id,
        "last_loaded_path": loaded_path,
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "feed": "sample_transactions",
    }
    account_url = f"https://{storage_account}.dfs.core.windows.net"
    credential = DefaultAzureCredential()
    fs = DataLakeServiceClient(account_url=account_url, credential=credential).get_file_system_client(
        "bronze"
    )
    try:
        fs.create_directory("_control")
    except ResourceExistsError:
        pass
    file_client = fs.get_file_client(WATERMARK_PATH)
    data = json.dumps(body, indent=2).encode()
    file_client.upload_data(data, overwrite=True)
    logger.info("Watermark updated: run_id=%s", run_id)
    return body
