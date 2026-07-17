#!/usr/bin/env python3
"""Import + run FinLedger Event processors notebook on shared Databricks."""

from __future__ import annotations

import base64
import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_SRC = REPO_ROOT / "day9" / "notebooks" / "nb_finledger_event_processors.py"
NOTEBOOK_PATH = "/Shared/day9/finledger_event_processors"
JOB_NAME = "finledger-event-processors"
CLUSTER_ID_DEFAULT = "0703-105931-31juyffm"
SHARED_RG = "rg-shared-class1"
WORKSPACE = "dbw-shared-qgr7mj"

logger = logging.getLogger(__name__)


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


def _request(host: str, token: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{host.rstrip('/')}{path}",
        data=data,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Databricks API {method} {path} failed ({e.code}): {e.read().decode()[:500]}") from e


def _resolve_host(file_env: dict[str, str]) -> str:
    host = (os.environ.get("DATABRICKS_HOST") or file_env.get("DATABRICKS_HOST", "")).strip().rstrip("/")
    if host:
        return host if host.startswith("http") else f"https://{host}"
    az = shutil_which_az()
    data = json.loads(
        subprocess.check_output(
            [az, "databricks", "workspace", "show", "-g", SHARED_RG, "-n", WORKSPACE, "-o", "json"],
            text=True,
        )
    )
    raw = data.get("workspaceUrl", "").strip()
    return f"https://{raw}" if raw and not raw.startswith("http") else raw


def shutil_which_az() -> str:
    import shutil

    az = shutil.which("az")
    if not az:
        raise SystemExit("az CLI not found")
    return az


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    file_env = _load_env()
    token = (os.environ.get("DATABRICKS_TOKEN") or file_env.get("DATABRICKS_TOKEN", "")).strip()
    if not token:
        raise SystemExit("Set DATABRICKS_TOKEN in .env")
    host = _resolve_host(file_env)
    cluster_id = (
        os.environ.get("DATABRICKS_CLUSTER_ID")
        or file_env.get("DATABRICKS_CLUSTER_ID")
        or CLUSTER_ID_DEFAULT
    ).strip()

    if not NOTEBOOK_SRC.is_file():
        raise SystemExit(f"Missing notebook: {NOTEBOOK_SRC}")

    # Warm cluster
    logger.info("Ensuring cluster %s is RUNNING...", cluster_id)
    state = _request(host, token, "GET", f"/api/2.0/clusters/get?cluster_id={cluster_id}").get("state", "")
    if state != "RUNNING":
        _request(host, token, "POST", "/api/2.0/clusters/start", {"cluster_id": cluster_id})
        for _ in range(40):
            time.sleep(15)
            state = _request(host, token, "GET", f"/api/2.0/clusters/get?cluster_id={cluster_id}").get("state", "")
            logger.info("  cluster state: %s", state)
            if state == "RUNNING":
                break
        else:
            raise SystemExit("Cluster did not reach RUNNING")

    # Import notebook
    _request(host, token, "POST", "/api/2.0/workspace/mkdirs", {"path": "/Shared/day9"})
    content = base64.b64encode(NOTEBOOK_SRC.read_bytes()).decode()
    _request(
        host,
        token,
        "POST",
        "/api/2.0/workspace/import",
        {
            "path": NOTEBOOK_PATH,
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
            "content": content,
        },
    )
    logger.info("Imported %s", NOTEBOOK_PATH)

    # Upsert job
    jobs = _request(host, token, "GET", f"/api/2.1/jobs/list?name={JOB_NAME}")
    job_id = None
    for job in jobs.get("jobs", []):
        if job.get("settings", {}).get("name") == JOB_NAME:
            job_id = job["job_id"]
            break
    settings = {
        "name": JOB_NAME,
        "tasks": [
            {
                "task_key": "event_processors",
                "existing_cluster_id": cluster_id,
                "notebook_task": {
                    "notebook_path": NOTEBOOK_PATH,
                    "base_parameters": {"run_id": "session3-lab"},
                },
                "timeout_seconds": 3600,
            }
        ],
    }
    if job_id:
        _request(host, token, "POST", "/api/2.1/jobs/reset", {"job_id": job_id, "new_settings": settings})
        logger.info("Updated job %s (%s)", JOB_NAME, job_id)
    else:
        job_id = _request(host, token, "POST", "/api/2.1/jobs/create", settings)["job_id"]
        logger.info("Created job %s (%s)", JOB_NAME, job_id)

    run_id = _request(host, token, "POST", "/api/2.1/jobs/run-now", {"job_id": job_id})["run_id"]
    logger.info("Started run %s — polling...", run_id)

    for _ in range(80):
        poll = _request(host, token, "GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
        state = poll.get("state", {})
        life = state.get("life_cycle_state", "")
        result = state.get("result_state", "")
        if life == "TERMINATED":
            note_url = f"{host}/#workspace{NOTEBOOK_PATH}"
            run_url = f"{host}/#job/{job_id}/run/{run_id}"
            print()
            print("=" * 70)
            print("FINLEDGER EVENT PROCESSORS — RUN COMPLETE")
            print("=" * 70)
            print(f"  Result        : {result}")
            print(f"  Notebook      : {NOTEBOOK_PATH}")
            print(f"  Notebook link : {note_url}")
            print(f"  Job run link  : {run_url}")
            print("=" * 70)
            return 0 if result == "SUCCESS" else 2
        time.sleep(15)

    logger.error("Timed out waiting for run %s", run_id)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
