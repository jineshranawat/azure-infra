#!/usr/bin/env python3
"""Submit medallion governance Databricks notebook job (idempotent poll).

Auth / host / cluster are self-healing (same pattern as phase 2):
  - Prefer valid PAT; else Azure AD token from az login (opens browser if needed).
  - Prefer shared workspace host from Azure CLI when .env host is wrong/missing.
  - Prefer a live all-purpose cluster; else single-node job cluster.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from deploy_shared_lab import (  # noqa: E402
    _load_env,
    _resolve_host,
    _resolve_token,
)

logger = logging.getLogger(__name__)

NOTEBOOK = "/Shared/day9/medallion_governance_e2e"
JOB_NAME = "finledger-medallion-governance"
DEFAULT_CLUSTER = os.environ.get("DATABRICKS_CLUSTER_ID", "0703-105931-31juyffm")

# Cheap single-node job cluster when no all-purpose cluster exists (guardrail 6).
JOB_CLUSTER_SPEC = {
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 0,
    "spark_conf": {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*]",
    },
    "custom_tags": {"ResourceClass": "SingleNode", "course": "medallion-governance"},
}


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _resolve_cluster(host: str, token: str, configured: str) -> str:
    """Return a live cluster id, or "" to mean 'use job cluster'."""
    import requests

    headers = _headers(token)
    if configured:
        got = requests.get(
            f"{host}/api/2.0/clusters/get",
            headers=headers,
            params={"cluster_id": configured},
            timeout=30,
        )
        if got.status_code < 300:
            return configured
        logger.warning(
            "Cluster %s missing — looking for another all-purpose cluster…", configured
        )
    listing = requests.get(f"{host}/api/2.0/clusters/list", headers=headers, timeout=30)
    if listing.status_code >= 300:
        logger.warning("clusters/list failed (%s) — will use job cluster", listing.status_code)
        return ""
    clusters = [
        c
        for c in listing.json().get("clusters", [])
        if c.get("cluster_source") in ("UI", "API")
    ]
    running = [c for c in clusters if c.get("state") == "RUNNING"]
    pick = (running or clusters)[0] if (running or clusters) else None
    if pick:
        logger.info(
            "Using cluster '%s' (%s)",
            pick.get("cluster_name"),
            pick["cluster_id"],
        )
        return pick["cluster_id"]
    logger.warning("No all-purpose cluster — job will use a single-node job cluster")
    return ""


def _task(cluster: str) -> dict[str, Any]:
    task: dict[str, Any] = {
        "task_key": "medallion_governance",
        "notebook_task": {
            "notebook_path": NOTEBOOK,
            "base_parameters": {"run_id": "session3-lab"},
        },
        "timeout_seconds": 3600,
    }
    if cluster:
        task["existing_cluster_id"] = cluster
    else:
        task["new_cluster"] = JOB_CLUSTER_SPEC
    return task


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    import requests

    file_env = _load_env()
    # Corrects wrong DATABRICKS_HOST in .env to the shared class workspace.
    host = _resolve_host(file_env)
    token, auth_src = _resolve_token(file_env, host)
    logger.info("Databricks host: %s (auth=%s)", host, auth_src)

    headers = _headers(token)
    jobs = requests.get(
        f"{host}/api/2.1/jobs/list",
        headers=headers,
        params={"name": JOB_NAME},
        timeout=60,
    )
    if jobs.status_code in (401, 403):
        logger.error(
            "Databricks rejected jobs/list (%s). Checklist:\n"
            "  1) git pull\n"
            "  2) az login  (same account that can open the shared workspace)\n"
            "  3) Fix .env DATABRICKS_HOST to the shared workspace URL (or remove the line)\n"
            "  4) Or re-run phase 9 with --skip-run to finish deploy without submitting the job:\n"
            "       release-medallion-governance.cmd --skip-run\n"
            "Detail: %s",
            jobs.status_code,
            jobs.text[:300],
        )
        return 1
    jobs.raise_for_status()

    job_id = None
    for job in jobs.json().get("jobs", []):
        if job.get("settings", {}).get("name") == JOB_NAME:
            job_id = job["job_id"]
            break

    cluster = _resolve_cluster(host, token, DEFAULT_CLUSTER)
    settings = {"name": JOB_NAME, "tasks": [_task(cluster)]}

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
        if resp.status_code < 300:
            job_id = resp.json().get("job_id")
        logger.info("Created job %s (%s)", JOB_NAME, job_id)
    if resp.status_code >= 300:
        logger.error("Job create/reset failed: %s %s", resp.status_code, resp.text[:300])
        return 1

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
