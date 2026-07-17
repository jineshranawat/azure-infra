#!/usr/bin/env python3
"""Submit medallion governance Databricks notebook job (idempotent poll)."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

logger = logging.getLogger(__name__)

NOTEBOOK = "/Shared/day9/medallion_governance_e2e"
JOB_NAME = "finledger-medallion-governance"
CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID", "0703-105931-31juyffm")


def _load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    dotenv = REPO_ROOT / ".env"
    if dotenv.is_file():
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    file_env = _load_env()
    host = (os.environ.get("DATABRICKS_HOST") or file_env.get("DATABRICKS_HOST", "")).strip().rstrip("/")
    token = (os.environ.get("DATABRICKS_TOKEN") or file_env.get("DATABRICKS_TOKEN", "")).strip()
    if not host or not token:
        logger.error("Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")
        return 1
    if not host.startswith("http"):
        host = f"https://{host}"

    import requests

    headers = {"Authorization": f"Bearer {token}"}
    jobs = requests.get(f"{host}/api/2.1/jobs/list", headers=headers, params={"name": JOB_NAME}, timeout=60)
    jobs.raise_for_status()
    job_id = None
    for job in jobs.json().get("jobs", []):
        if job.get("settings", {}).get("name") == JOB_NAME:
            job_id = job["job_id"]
            break

    settings = {
        "name": JOB_NAME,
        "tasks": [
            {
                "task_key": "medallion_governance",
                "existing_cluster_id": CLUSTER_ID,
                "notebook_task": {
                    "notebook_path": NOTEBOOK,
                    "base_parameters": {"run_id": "session3-lab"},
                },
                "timeout_seconds": 3600,
            }
        ],
    }
    if job_id:
        resp = requests.post(
            f"{host}/api/2.1/jobs/reset",
            headers=headers,
            json={"job_id": job_id, "new_settings": settings},
            timeout=60,
        )
        logger.info("Updated job %s (%s)", JOB_NAME, job_id)
    else:
        resp = requests.post(
            f"{host}/api/2.1/jobs/create",
            headers=headers,
            json=settings,
            timeout=60,
        )
        job_id = resp.json().get("job_id")
        logger.info("Created job %s (%s)", JOB_NAME, job_id)
    resp.raise_for_status()

    run = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=headers,
        json={"job_id": job_id},
        timeout=60,
    )
    run.raise_for_status()
    run_id = run.json()["run_id"]
    logger.info("Started run %s — polling...", run_id)

    for _ in range(120):
        poll = requests.get(
            f"{host}/api/2.1/jobs/runs/get",
            headers=headers,
            params={"run_id": run_id},
            timeout=60,
        )
        poll.raise_for_status()
        state = poll.json().get("state", {})
        life = state.get("life_cycle_state", "")
        result = state.get("result_state", "")
        if life == "TERMINATED":
            logger.info("Job finished: %s", result or state)
            return 0 if result == "SUCCESS" else 2
        time.sleep(15)
    logger.error("Timed out waiting for job run %s", run_id)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
