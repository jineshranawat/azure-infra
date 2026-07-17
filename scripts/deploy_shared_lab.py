#!/usr/bin/env python3
"""Upload bronze sample data, FinLedger secrets, and master PySpark notebook to shared Databricks."""

from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SHARED_SUB = "a64c0dd2-3a31-4604-bde3-3d40c7d5e8be"
SHARED_RG = "rg-shared-class1"
STORAGE_ACCOUNT = "stsharedqgr7mj"
WORKSPACE = "dbw-shared-qgr7mj"
NOTEBOOK_SRC = REPO_ROOT / "day8" / "notebooks" / "nb_master_pyspark_complete.py"
SAMPLE_CSV = REPO_ROOT / "day8" / "data" / "sample_transactions.csv"
RETURNS_CSV = REPO_ROOT / "shared-adf-lab" / "data" / "returns_raw.csv"
NOTEBOOK_PATH = "/Shared/day7-day8/master_pyspark_complete"
DAY9_NOTEBOOK_DIR = REPO_ROOT / "day9" / "notebooks"
DAY9_BUILD_SCRIPT = REPO_ROOT / "day9" / "scripts" / "build_all_notebooks.py"
DAY9_WORKSPACE = "/Shared/day9"
SCOPE = "finledger"

logger = logging.getLogger(__name__)


class DbxClient:
    def __init__(self, host: str, token: str) -> None:
        self.base = host.rstrip("/")
        self.token = token

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{self.base}{path}",
            data=data,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read().decode()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            payload = e.read().decode()
            raise SystemExit(f"Databricks API {method} {path} failed ({e.code}): {payload}") from e

    def list_scopes(self) -> list[str]:
        data = self._request("GET", "/api/2.0/secrets/scopes/list")
        return [s["name"] for s in data.get("scopes", [])]

    def create_scope(self, name: str) -> None:
        self._request("POST", "/api/2.0/secrets/scopes/create", {"scope": name})

    def put_acl(self) -> None:
        try:
            self._request(
                "POST",
                "/api/2.0/secrets/acls/put",
                {"scope": SCOPE, "principal": "users", "permission": "READ"},
            )
        except SystemExit as exc:
            if "already exists" not in str(exc).lower():
                logger.warning("ACL put: %s", exc)

    def put_secret(self, key: str, value: str) -> None:
        self._request(
            "POST",
            "/api/2.0/secrets/put",
            {"scope": SCOPE, "key": key, "string_value": value},
        )

    def mkdirs(self, path: str) -> None:
        self._request("POST", "/api/2.0/workspace/mkdirs", {"path": path})

    def import_notebook(self, path: str, content: bytes) -> None:
        self._request(
            "POST",
            "/api/2.0/workspace/import",
            {
                "path": path,
                "format": "SOURCE",
                "language": "PYTHON",
                "overwrite": True,
                "content": base64.b64encode(content).decode(),
            },
        )


def _find_az() -> str:
    az = shutil.which("az")
    if az:
        return az
    win = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Microsoft SDKs" / "Azure" / "CLI2" / "wbin" / "az.cmd"
    if win.is_file():
        return str(win)
    raise SystemExit("Azure CLI not found")


def _load_env() -> dict[str, str]:
    env_path = REPO_ROOT / ".env"
    values: dict[str, str] = {}
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            values[k.strip()] = v.strip()
    return values


def _resolve_host(file_env: dict[str, str]) -> str:
    host = (os.environ.get("DATABRICKS_HOST") or file_env.get("DATABRICKS_HOST", "")).strip().rstrip("/")
    if host:
        return host if host.startswith("http") else f"https://{host}"
    az = _find_az()
    data = json.loads(
        subprocess.check_output(
            [az, "databricks", "workspace", "show", "-g", SHARED_RG, "-n", WORKSPACE, "-o", "json"],
            text=True,
        )
    )
    raw = data.get("workspaceUrl", "").strip()
    return f"https://{raw}" if raw and not raw.startswith("http") else raw


def _upload_bronze(file_env: dict[str, str]) -> None:
    az = _find_az()
    key = (os.environ.get("STORAGE_ACCOUNT_KEY") or file_env.get("STORAGE_ACCOUNT_KEY", "")).strip()
    if not key:
        key = subprocess.check_output(
            [az, "storage", "account", "keys", "list", "-g", SHARED_RG, "-n", STORAGE_ACCOUNT, "--query", "[0].value", "-o", "tsv"],
            text=True,
        ).strip()
    dest = "loaded/run=session3-lab/sample_transactions.csv"
    logger.info("Uploading sample CSV to %s/bronze/%s", STORAGE_ACCOUNT, dest)
    subprocess.run(
        [
            az, "storage", "blob", "upload",
            "--account-name", STORAGE_ACCOUNT,
            "--account-key", key,
            "--container-name", "bronze",
            "--name", dest,
            "--file", str(SAMPLE_CSV),
            "--overwrite",
        ],
        check=True,
    )
    if RETURNS_CSV.is_file():
        returns_dest = "incoming/returns/returns_raw.csv"
        logger.info("Uploading returns CSV to %s/bronze/%s", STORAGE_ACCOUNT, returns_dest)
        subprocess.run(
            [
                az, "storage", "blob", "upload",
                "--account-name", STORAGE_ACCOUNT,
                "--account-key", key,
                "--container-name", "bronze",
                "--name", returns_dest,
                "--file", str(RETURNS_CSV),
                "--overwrite",
            ],
            check=True,
        )
    else:
        logger.warning("Returns CSV missing at %s — skip (finledger_event_processors returns processor will skip)", RETURNS_CSV)


def _setup_secrets(client: DbxClient, storage_key: str) -> None:
    scopes = client.list_scopes()
    if SCOPE not in scopes:
        client.create_scope(SCOPE)
        logger.info("Created secret scope '%s'", SCOPE)
    else:
        logger.info("Secret scope '%s' already present", SCOPE)
    client.put_acl()
    client.put_secret("storage-account", STORAGE_ACCOUNT)
    client.put_secret("storage-key", storage_key)
    logger.info("Secrets stored: %s/storage-account, %s/storage-key", SCOPE, SCOPE)


def _build_day9_notebooks() -> None:
    if not DAY9_BUILD_SCRIPT.is_file():
        logger.warning("Day 9 build script missing — skip")
        return
    logger.info("Building Day 9+ notebooks...")
    subprocess.run(
        [sys.executable, str(DAY9_BUILD_SCRIPT)],
        cwd=REPO_ROOT,
        check=True,
    )


def _import_notebooks(client: DbxClient) -> None:
    if not NOTEBOOK_SRC.is_file():
        raise SystemExit("Run: python day8/scripts/build_master_pyspark_notebook.py")
    client.mkdirs("/Shared/day7-day8")
    logger.info("Importing notebook to %s", NOTEBOOK_PATH)
    client.import_notebook(NOTEBOOK_PATH, NOTEBOOK_SRC.read_bytes())
    logger.info("Notebook ready at %s", NOTEBOOK_PATH)

    if not DAY9_NOTEBOOK_DIR.is_dir():
        return
    client.mkdirs(DAY9_WORKSPACE)
    for src in sorted(DAY9_NOTEBOOK_DIR.glob("nb_*.py")):
        ws_path = f"{DAY9_WORKSPACE}/{src.stem.replace('nb_', '')}"
        logger.info("Importing %s -> %s", src.name, ws_path)
        client.import_notebook(ws_path, src.read_bytes())
    logger.info("Day 9+ notebooks under %s", DAY9_WORKSPACE)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
    file_env = _load_env()
    token = (file_env.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_TOKEN", "")).strip()
    storage_key = (file_env.get("STORAGE_ACCOUNT_KEY") or os.environ.get("STORAGE_ACCOUNT_KEY", "")).strip()
    if not token or not storage_key:
        raise SystemExit("DATABRICKS_TOKEN and STORAGE_ACCOUNT_KEY required in .env")
    host = _resolve_host(file_env)
    client = DbxClient(host, token)
    _upload_bronze(file_env)
    _setup_secrets(client, storage_key)
    _build_day9_notebooks()
    _import_notebooks(client)
    logger.info("Deploy complete — open %s#workspace/Shared", host)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
