"""Complete ADF + Databricks infrastructure — secrets, jobs, manifests (idempotent)."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

from _config import ENV_FILE, SharedAdfConfig, _load_dotenv, find_az, get_datalake_credential, get_storage_account_key
from discover import SharedEstate

logger = logging.getLogger(__name__)

SCOPE = "finledger"
ACCOUNT_SECRET = "storage-account"
KEY_SECRET = "storage-key"
JOB_NAME = "adf-finledger-integration-job"
JOB_NOTEBOOK = "/Shared/shared-adf/nb_adf_job_task"
MANIFEST_BLOB = "adf_databricks_job.json"


def _databricks_cli() -> str:
    cli = shutil.which("databricks")
    if cli:
        return cli
    raise RuntimeError(
        "Databricks CLI not found. Install: pip install databricks-cli\n"
        "Then: databricks configure --token (or set DATABRICKS_TOKEN in .env)"
    )


def _run_cli(host: str, args: list[str], *, token: str | None = None) -> subprocess.CompletedProcess:
    cli = _databricks_cli()
    env = os.environ.copy()
    env["DATABRICKS_HOST"] = host.rstrip("/")
    if token:
        env["DATABRICKS_TOKEN"] = token
    cmd = [cli, *args]
    return subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)


def _resolve_host_token(cfg: SharedAdfConfig, estate: SharedEstate) -> tuple[str, str]:
    env = _load_dotenv(ENV_FILE)
    host = (cfg.databricks_host or env.get("DATABRICKS_HOST", "")).rstrip("/")
    token = cfg.databricks_token or env.get("DATABRICKS_TOKEN", "")
    if not host:
        raise RuntimeError("DATABRICKS_HOST not set in .env")
    if not token:
        raise RuntimeError("DATABRICKS_TOKEN not set in .env")
    return host, token


def _storage_account_key(env: dict[str, str], estate: SharedEstate, cfg: SharedAdfConfig) -> str:
    key = get_storage_account_key(estate.storage_account, cfg.subscription_id)
    if key:
        return key
    key = env.get("STORAGE_ACCOUNT_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "STORAGE_ACCOUNT_KEY missing in .env — required for secret scope setup.\n"
            "Portal: Storage account → Access keys → key1"
        )
    return key


def setup_finledger_secrets(cfg: SharedAdfConfig, estate: SharedEstate) -> str:
    """Create finledger secret scope + storage secrets (idempotent)."""
    host, token = _resolve_host_token(cfg, estate)
    env = _load_dotenv(ENV_FILE)
    storage_account = estate.storage_account
    storage_key = _storage_account_key(env, estate, cfg)
    headers = {"Authorization": f"Bearer {token}"}

    create = requests.post(
        f"{host}/api/2.0/secrets/scopes/create",
        headers=headers,
        json={"scope": SCOPE},
        timeout=60,
    )
    if create.status_code >= 300 and "already exists" not in (create.text or "").lower():
        logger.warning("create-scope: %s %s", create.status_code, create.text[:200])

    for key, value in ((ACCOUNT_SECRET, storage_account), (KEY_SECRET, storage_key)):
        put = requests.post(
            f"{host}/api/2.0/secrets/put",
            headers=headers,
            json={"scope": SCOPE, "key": key, "string_value": value},
            timeout=60,
        )
        if put.status_code >= 300:
            raise RuntimeError(f"secrets put {key} failed: {put.status_code} {put.text[:200]}")

    requests.post(
        f"{host}/api/2.0/secrets/acls/put",
        headers=headers,
        json={"scope": SCOPE, "principal": "users", "permission": "READ"},
        timeout=60,
    )
    logger.info("Secret scope '%s' ready (account=%s)", SCOPE, storage_account)
    return host


def _list_jobs(host: str, token: str) -> list[dict[str, Any]]:
    url = f"{host}/api/2.1/jobs/list"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    resp.raise_for_status()
    return resp.json().get("jobs", [])


def ensure_integration_job(
    cfg: SharedAdfConfig,
    estate: SharedEstate,
    *,
    cluster_id: str | None = None,
) -> int:
    """Create or reuse Databricks job for ADF Job activity (Preview). Returns job_id."""
    host, token = _resolve_host_token(cfg, estate)
    cluster = cluster_id or cfg.databricks_cluster_id
    if not cluster:
        raise RuntimeError("DATABRICKS_CLUSTER_ID not set")

    for job in _list_jobs(host, token):
        settings = job.get("settings", {})
        if settings.get("name") == JOB_NAME:
            job_id = int(job["job_id"])
            logger.info("Databricks job '%s' already exists (id=%s)", JOB_NAME, job_id)
            return job_id

    payload = {
        "name": JOB_NAME,
        "tags": {"course": "shared-adf-lab", "integration": "adf-job-preview"},
        "tasks": [
            {
                "task_key": "integration_task",
                "existing_cluster_id": cluster,
                "notebook_task": {
                    "notebook_path": JOB_NOTEBOOK,
                    "base_parameters": {
                        "run_id": "default",
                        "triggered_by": "databricks-job-deploy",
                        "job_note": "created-by-databricks_infra",
                    },
                },
            }
        ],
    }
    url = f"{host}/api/2.1/jobs/create"
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=60,
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Job create failed: {resp.status_code} {resp.text[:300]}")
    job_id = int(resp.json()["job_id"])
    logger.info("Created Databricks job '%s' id=%s", JOB_NAME, job_id)
    return job_id


def upload_job_manifest(cfg: SharedAdfConfig, estate: SharedEstate, job_id: int) -> None:
    """Write job id manifest to ADLS audit for ADF Lookup activities."""
    from azure.storage.filedatalake import DataLakeServiceClient

    body = json.dumps(
        {
            "job_id": job_id,
            "job_name": JOB_NAME,
            "notebook_path": JOB_NOTEBOOK,
            "integration": "adf-databricks-job-preview",
        },
        indent=2,
    )
    try:
        client = DataLakeServiceClient(
            account_url=f"https://{estate.storage_account}.dfs.core.windows.net",
            credential=get_datalake_credential(estate.storage_account, cfg.subscription_id),
        )
        fs = client.get_file_system_client("audit")
        file_client = fs.get_file_client(MANIFEST_BLOB)
        try:
            file_client.create_file()
        except Exception:
            pass
        file_client.upload_data(body.encode("utf-8"), overwrite=True)
        logger.info("Job manifest at audit/%s (job_id=%s)", MANIFEST_BLOB, job_id)
    except Exception as exc:
        logger.warning(
            "Could not upload job manifest to ADLS (%s). "
            "Set pipeline parameter job_id=%s for pl_db_11 or re-run after az login. "
            "Manifest JSON: %s",
            exc,
            job_id,
            body,
        )


def setup_complete_integration(cfg: SharedAdfConfig, estate: SharedEstate) -> int:
    """Full Databricks integration bootstrap: secrets → job → manifest."""
    setup_finledger_secrets(cfg, estate)
    job_id = ensure_integration_job(cfg, estate)
    upload_job_manifest(cfg, estate, job_id)
    return job_id


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
    from _config import load_config
    from discover import discover_estate

    cfg = load_config()
    estate = discover_estate(cfg)
    job_id = setup_complete_integration(cfg, estate)
    print()
    print("=" * 70)
    print("DATABRICKS COMPLETE INTEGRATION — READY")
    print("=" * 70)
    print(f"  Secret scope     : {SCOPE}")
    print(f"  Databricks job   : {JOB_NAME} (id={job_id})")
    print(f"  ADF manifest     : abfss://audit@.../ {MANIFEST_BLOB}")
    print(f"  Pipelines        : pl_db_11_databricks_job_preview, pl_db_12_end_to_end_integration")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
