"""Ensure bronze loaded data exists for Databricks to read (Session 2 output or fresh upload).

Idempotent: overwrites the same run folder on re-run — no duplicate blobs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient

from _config import SessionConfig, get_credential

logger = logging.getLogger(__name__)

SESSION_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = SESSION_ROOT / "data" / "sample_transactions.csv"
MESSY_CSV = SESSION_ROOT / "data" / "transactions_messy.csv"
LAB_RUN_ID = "session3-lab"


def _upload_file(
    fs,
    relative_path: str,
    payload: bytes,
) -> None:
    file_client = fs.get_file_client(relative_path)
    file_client.upload_data(payload, overwrite=True)
    logger.info("Uploaded bronze/%s (%d bytes)", relative_path, len(payload))


def ensure_bronze_for_databricks(
    cfg: SessionConfig,
    storage_account: str,
    *,
    run_id: str | None = None,
) -> dict[str, str]:
    """Upload lab CSVs to bronze paths Databricks notebooks will read."""
    if not SAMPLE_CSV.is_file():
        raise FileNotFoundError(f"Sample data missing: {SAMPLE_CSV}")

    run_id = run_id or LAB_RUN_ID
    loaded_path = f"loaded/run={run_id}/sample_transactions.csv"
    messy_path = f"incoming/transactions/{run_id}/transactions_messy.csv"

    account_url = f"https://{storage_account}.dfs.core.windows.net"
    service = DataLakeServiceClient(account_url=account_url, credential=get_credential())
    fs = service.get_file_system_client("bronze")
    for folder in ("_control", f"loaded/run={run_id}", f"incoming/transactions/{run_id}"):
        try:
            fs.create_directory(folder)
        except ResourceExistsError:
            pass

    _upload_file(fs, loaded_path, SAMPLE_CSV.read_bytes())
    if MESSY_CSV.is_file():
        _upload_file(fs, messy_path, MESSY_CSV.read_bytes())

    control = {
        "last_run_id": run_id,
        "last_loaded_path": f"bronze/{loaded_path}",
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "feed": "sample_transactions",
        "session": "session-3-databricks",
    }
    import json

    control_bytes = (json.dumps(control, indent=2) + "\n").encode("utf-8")
    _upload_file(fs, "_control/watermark.json", control_bytes)

    return {
        "run_id": run_id,
        "bronze_loaded_csv": f"bronze/{loaded_path}",
        "bronze_messy_csv": f"bronze/{messy_path}",
        "loaded_abfss": (
            f"abfss://bronze@{storage_account}.dfs.core.windows.net/{loaded_path}"
        ),
        "messy_abfss": (
            f"abfss://bronze@{storage_account}.dfs.core.windows.net/{messy_path}"
        ),
    }
