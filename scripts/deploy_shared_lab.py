#!/usr/bin/env python3
"""Upload bronze sample data, FinLedger secrets, and master PySpark notebook to shared Databricks.

Auth (phase 2 / deploy-shared-lab.cmd):
  1. If DATABRICKS_TOKEN in .env is present and valid → use PAT.
  2. Else (missing or 401/403) → Azure AD token from `az account get-access-token`
     for resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d (students only need az login).
  3. STORAGE_ACCOUNT_KEY and DATABRICKS_HOST are optional — auto-resolved via Azure CLI.
"""

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


# Azure AD resource ID for Azure Databricks REST APIs (Microsoft-documented).
# Used when DATABRICKS_TOKEN is missing or invalid — students only need `az login`.
DATABRICKS_AAD_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


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
            hint = ""
            if e.code in (401, 403):
                hint = (
                    "\n\nToken rejected by Databricks (invalid / expired / no workspace access).\n"
                    "Fix options:\n"
                    "  1) Prefer: stay logged in with `az login` — this script can use an Azure AD token.\n"
                    "  2) Or create a PAT: Workspace → your icon → Settings → Developer → Access tokens\n"
                    "     Put it in repo-root .env as DATABRICKS_TOKEN=dapi... (no quotes).\n"
                    "  3) Confirm DATABRICKS_HOST matches the shared workspace URL.\n"
                )
            raise SystemExit(
                f"Databricks API {method} {path} failed ({e.code}): {payload}{hint}"
            ) from e

    def whoami(self) -> dict:
        """Lightweight auth preflight."""
        try:
            return self._request("GET", "/api/2.0/preview/scim/v2/Me")
        except SystemExit as exc:
            if "failed (401)" in str(exc) or "failed (403)" in str(exc):
                raise
            return {"ok": True, "scopes_probe": self.list_scopes()}

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


def _normalize_secret(value: str) -> str:
    """Strip whitespace and accidental wrapping quotes from .env values."""
    v = (value or "").strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        v = v[1:-1].strip()
    return v


def _aad_databricks_token(az: str) -> str:
    """Get a fresh AAD token; open browser sign-in when Azure CLI login expired."""
    command = [
        az,
        "account",
        "get-access-token",
        "--resource",
        DATABRICKS_AAD_RESOURCE,
        "--query",
        "accessToken",
        "-o",
        "tsv",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        logger.warning("Azure CLI login expired/missing — opening browser for az login…")
        login = subprocess.run([az, "login"], check=False)
        if login.returncode == 0:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
    tok = result.stdout.strip()
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise SystemExit(
            "Could not refresh Azure AD token for Databricks. Complete the browser "
            f"sign-in opened by `az login`, then re-run this phase.\nDetail: {detail}"
        )
    if not tok:
        raise SystemExit("Empty Azure AD token — run `az login` and complete browser sign-in")
    return tok


def _token_preflight(host: str, token: str) -> bool:
    """Return True if token can call the workspace API."""
    client = DbxClient(host, token)
    try:
        client.whoami()
        return True
    except SystemExit as exc:
        msg = str(exc)
        if "failed (401)" in msg or "failed (403)" in msg or "invalid" in msg.lower():
            return False
        # Other errors (network, 5xx) — still try; let later calls surface
        logger.warning("Preflight inconclusive: %s", msg.split("\n", 1)[0][:200])
        return True


def _resolve_token(file_env: dict[str, str], host: str) -> tuple[str, str]:
    """Return (token, source_label). Prefer valid PAT from .env; else Azure AD via az login."""
    az = _find_az()
    pat = _normalize_secret(
        file_env.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_TOKEN", "")
    )
    if pat:
        if _token_preflight(host, pat):
            logger.info("Databricks auth: DATABRICKS_TOKEN from .env (PAT) — OK")
            return pat, "pat"
        logger.warning(
            "DATABRICKS_TOKEN in .env was rejected (invalid/expired). "
            "Falling back to Azure AD token from az login…"
        )
    else:
        logger.info("DATABRICKS_TOKEN not set — using Azure AD token from az login…")

    aad = _aad_databricks_token(az)
    if not _token_preflight(host, aad):
        raise SystemExit(
            "Azure AD token also rejected by Databricks.\n"
            "Checklist:\n"
            f"  1) az account set --subscription {SHARED_SUB}\n"
            "  2) az login (same work account that can open the workspace in browser)\n"
            f"  3) Portal → {WORKSPACE} → Launch Workspace — confirm you can open it\n"
            "  4) Trainer: add your user to the Databricks workspace (Admin → Users)\n"
            "  5) Or put a fresh PAT in .env:\n"
            "       DATABRICKS_TOKEN=dapi...   (no quotes, no spaces)\n"
            f"       DATABRICKS_HOST=https://…  (optional; auto-detected from {WORKSPACE})\n"
        )
    logger.info("Databricks auth: Azure AD token via az login — OK")
    return aad, "aad"


def _storage_key_works(az: str, key: str) -> bool:
    """Cheap probe: can this account key list the bronze container?"""
    result = subprocess.run(
        [
            az, "storage", "container", "exists",
            "--account-name", STORAGE_ACCOUNT,
            "--account-key", key,
            "--name", "bronze",
            "-o", "json",
        ],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _fetch_storage_key_from_cli(az: str) -> str:
    key = subprocess.check_output(
        [
            az,
            "storage",
            "account",
            "keys",
            "list",
            "-g",
            SHARED_RG,
            "-n",
            STORAGE_ACCOUNT,
            "--query",
            "[0].value",
            "-o",
            "tsv",
        ],
        text=True,
    ).strip()
    if not key:
        raise SystemExit(
            f"Could not read storage key for {STORAGE_ACCOUNT}. "
            "Set STORAGE_ACCOUNT_KEY in .env or ensure az has access to the RG."
        )
    return key


def _resolve_storage_key(file_env: dict[str, str]) -> str:
    az = _find_az()
    key = _normalize_secret(
        file_env.get("STORAGE_ACCOUNT_KEY") or os.environ.get("STORAGE_ACCOUNT_KEY", "")
    )
    if key:
        if _storage_key_works(az, key):
            logger.info("Storage auth: STORAGE_ACCOUNT_KEY from .env — OK")
            return key
        logger.warning(
            "STORAGE_ACCOUNT_KEY in .env was rejected (stale/rotated key). "
            "Fetching current key1 from Azure CLI…"
        )
    else:
        logger.info("STORAGE_ACCOUNT_KEY not in .env — fetching key1 from Azure CLI…")
    key = _fetch_storage_key_from_cli(az)
    if not _storage_key_works(az, key):
        raise SystemExit(
            f"Fetched key1 for {STORAGE_ACCOUNT} but the storage API still rejects it.\n"
            "Checklist:\n"
            "  1) az login (account with access to rg-shared-class1)\n"
            f"  2) az account set --subscription {SHARED_SUB}\n"
            "  3) Remove any stale STORAGE_ACCOUNT_KEY line from .env and re-run\n"
        )
    logger.info("Storage auth: fresh key1 via Azure CLI — OK")
    return key


def _resolve_host(file_env: dict[str, str]) -> str:
    """Prefer shared workspace from Azure CLI when .env host is missing or wrong."""
    configured = _normalize_secret(
        os.environ.get("DATABRICKS_HOST") or file_env.get("DATABRICKS_HOST", "")
    ).rstrip("/")
    if configured and not configured.startswith("http"):
        configured = f"https://{configured}"

    az = _find_az()
    try:
        data = json.loads(
            subprocess.check_output(
                [
                    az,
                    "databricks",
                    "workspace",
                    "show",
                    "-g",
                    SHARED_RG,
                    "-n",
                    WORKSPACE,
                    "-o",
                    "json",
                ],
                text=True,
            )
        )
        raw = (data.get("workspaceUrl") or "").strip()
        shared = f"https://{raw}" if raw and not raw.startswith("http") else raw
    except Exception as exc:
        logger.warning("Could not auto-detect shared Databricks host (%s)", exc)
        if configured:
            return configured
        raise SystemExit(
            f"DATABRICKS_HOST missing and could not read workspace {WORKSPACE}."
        ) from exc

    if not shared:
        if configured:
            return configured
        raise SystemExit(f"Empty workspaceUrl for {WORKSPACE}")

    if configured and configured.rstrip("/") != shared.rstrip("/"):
        logger.warning(
            "DATABRICKS_HOST in .env (%s) is NOT shared workspace %s — using %s",
            configured,
            WORKSPACE,
            shared,
        )
        return shared
    return configured or shared


def _upload_bronze(file_env: dict[str, str], storage_key: str) -> None:
    az = _find_az()
    key = storage_key
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
    host = _resolve_host(file_env)
    logger.info("Databricks host: %s", host)
    token, auth_src = _resolve_token(file_env, host)
    storage_key = _resolve_storage_key(file_env)
    client = DbxClient(host, token)
    me = client.whoami()
    user_hint = me.get("userName") or me.get("displayName") or auth_src
    logger.info("Authenticated to Databricks as: %s (via %s)", user_hint, auth_src)
    _upload_bronze(file_env, storage_key)
    _setup_secrets(client, storage_key)
    _build_day9_notebooks()
    _import_notebooks(client)
    logger.info("Deploy complete — open %s#workspace/Shared", host)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
