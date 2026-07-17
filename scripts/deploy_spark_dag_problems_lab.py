#!/usr/bin/env python3
"""Deploy Spark DAG problems lab notebook."""

from __future__ import annotations

import base64
import json
import logging
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD = REPO_ROOT / "day9" / "scripts" / "build_spark_dag_problems_notebook.py"
NOTEBOOK_SRC = REPO_ROOT / "day9" / "notebooks" / "nb_spark_dag_problems_lab.py"
NOTEBOOK_PATH = "/Shared/day9/spark_dag_problems_lab"
SHARED_RG = "rg-shared-class1"
WORKSPACE = "dbw-shared-qgr7mj"


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
        raise SystemExit(f"Databricks API failed ({e.code}): {e.read().decode()[:500]}") from e


def _resolve_host(file_env: dict[str, str]) -> str:
    host = (os.environ.get("DATABRICKS_HOST") or file_env.get("DATABRICKS_HOST", "")).strip().rstrip("/")
    if host:
        return host if host.startswith("http") else f"https://{host}"
    import shutil

    az = shutil.which("az")
    data = json.loads(
        subprocess.check_output(
            [az, "databricks", "workspace", "show", "-g", SHARED_RG, "-n", WORKSPACE, "-o", "json"],
            text=True,
        )
    )
    raw = data.get("workspaceUrl", "").strip()
    return f"https://{raw}" if raw and not raw.startswith("http") else raw


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    file_env = _load_env()
    token = (file_env.get("DATABRICKS_TOKEN") or "").strip()
    if not token:
        raise SystemExit("DATABRICKS_TOKEN required")
    subprocess.run([sys.executable, str(BUILD)], cwd=REPO_ROOT, check=True)
    host = _resolve_host(file_env)
    _request(host, token, "POST", "/api/2.0/workspace/mkdirs", {"path": "/Shared/day9"})
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
            "content": base64.b64encode(NOTEBOOK_SRC.read_bytes()).decode(),
        },
    )
    print(f"Deployed: {host}/#workspace{NOTEBOOK_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
