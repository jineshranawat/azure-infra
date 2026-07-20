#!/usr/bin/env python3
"""Build + import perf Databricks + ADF lab notebook.

Auth: shared phase-2 helpers (PAT → AAD, shared host).
"""

from __future__ import annotations

import base64
import logging
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from deploy_shared_lab import DbxClient, _load_env, _resolve_host, _resolve_token  # noqa: E402

BUILD = REPO_ROOT / "day9" / "scripts" / "build_perf_databricks_adf_notebook.py"
NOTEBOOK_SRC = REPO_ROOT / "day9" / "notebooks" / "nb_perf_databricks_adf_lab.py"
NOTEBOOK_PATH = "/Shared/day9/perf_databricks_adf_lab"

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    file_env = _load_env()
    host = _resolve_host(file_env)
    token, auth_src = _resolve_token(file_env, host)
    logger.info("Databricks host: %s (auth=%s)", host, auth_src)
    subprocess.run([sys.executable, str(BUILD)], cwd=REPO_ROOT, check=True)
    client = DbxClient(host, token)
    client._request("POST", "/api/2.0/workspace/mkdirs", {"path": "/Shared/day9"})
    client._request(
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
    print()
    print("=" * 70)
    print("PERF DATABRICKS + ADF LAB DEPLOYED")
    print("=" * 70)
    print(f"  {host}/#workspace{NOTEBOOK_PATH}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
