"""Upload synthetic feed to ADLS bronze with run-stamped path (azure.html Day 2 / Day 1 Hour 13).

Idempotent: same run_id on re-run overwrites the same paths — no duplicate folders.
Auth: DefaultAzureCredential (learner RBAC from Class-1 orchestrator).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from _config import SessionConfig

logger = logging.getLogger(__name__)

SESSION_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = SESSION_ROOT / "data" / "sample_transactions.csv"


def upload_bronze_feed(
    cfg: SessionConfig,
    storage_account: str,
    *,
    run_id: str | None = None,
) -> dict[str, str]:
    """Copy sample CSV to bronze/incoming and bronze/loaded/{run_id}/ — returns paths used."""
    if not SAMPLE_CSV.is_file():
        raise FileNotFoundError(f"Sample data missing: {SAMPLE_CSV}")

    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    incoming_path = f"incoming/transactions/{run_id}/sample_transactions.csv"

    account_url = f"https://{storage_account}.dfs.core.windows.net"
    credential = DefaultAzureCredential()
    service = DataLakeServiceClient(account_url=account_url, credential=credential)
    fs = service.get_file_system_client("bronze")
    try:
        fs.create_directory("_control")
    except ResourceExistsError:
        pass

    payload = SAMPLE_CSV.read_bytes()
    file_client = fs.get_file_client(incoming_path)
    file_client.upload_data(payload, overwrite=True)
    logger.info("Uploaded bronze/%s (%d bytes)", incoming_path, len(payload))

    return {
        "run_id": run_id,
        "incoming": f"bronze/{incoming_path}",
        "loaded_folder": f"loaded/run={run_id}",
    }
