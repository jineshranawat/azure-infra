"""Shared ADF lab config — LEARNER=shared, eastus RG, westus SQL exception."""

from __future__ import annotations

import os
import platform
import re
import secrets
import shutil
import string
import subprocess
from dataclasses import dataclass
from pathlib import Path

import requests

LAB_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = LAB_ROOT.parent
ENV_FILE = REPO_ROOT / ".env"

SHARED_LEARNER = "shared"
SHARED_RG = "rg-shared-class1"
ADF_LOCATION = "eastus"
SQL_LOCATION = "westus"  # documented exception — SQL only
# Shared all-purpose cluster in dbw-shared-* (same as run-50-problems.cmd).
DEFAULT_DATABRICKS_CLUSTER_ID = "0703-105931-31juyffm"
DATABRICKS_AAD_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
LEARNER_RE = re.compile(r"^[a-z0-9]{2,10}$")


@dataclass(frozen=True)
class SharedAdfConfig:
    subscription_id: str
    learner: str
    owner_email: str
    databricks_host: str
    databricks_token: str
    databricks_cluster_id: str

    @property
    def resource_group(self) -> str:
        return SHARED_RG

    @property
    def adf_location(self) -> str:
        return ADF_LOCATION

    @property
    def sql_location(self) -> str:
        return SQL_LOCATION


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, raw = line.partition("=")
        values[key.strip()] = raw.strip().strip('"').strip("'")
    return values


def get_storage_account_key(
    storage_account: str = "",
    subscription_id: str = "",
) -> str:
    """Resolve storage account key — .env / env var first, then az CLI."""
    file_env = _load_dotenv(ENV_FILE)
    key = (
        os.environ.get("STORAGE_ACCOUNT_KEY") or file_env.get("STORAGE_ACCOUNT_KEY", "")
    ).strip()
    if key:
        return key
    if not storage_account:
        return ""
    sub = subscription_id or os.environ.get("AZURE_SUBSCRIPTION_ID") or file_env.get(
        "AZURE_SUBSCRIPTION_ID", ""
    )
    if not sub:
        return ""
    import subprocess

    az = find_az()
    result = subprocess.run(
        [
            az,
            "storage",
            "account",
            "keys",
            "list",
            "--account-name",
            storage_account,
            "--subscription",
            sub,
            "--query",
            "[0].value",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return result.stdout.strip()
    return ""


def get_datalake_credential(
    storage_account: str = "",
    subscription_id: str = "",
):
    """ADLS uploads: account key (works without Blob RBAC); else az login."""
    key = get_storage_account_key(storage_account, subscription_id)
    if key:
        return key
    return get_credential()


def get_credential():
    from azure.identity import (
        AzureCliCredential,
        ChainedTokenCredential,
        DefaultAzureCredential,
    )

    excludes: dict[str, bool] = {
        "exclude_interactive_browser_credential": True,
        "exclude_visual_studio_code_credential": True,
        "exclude_shared_token_cache_credential": True,
    }
    if platform.system() == "Windows":
        excludes["exclude_powershell_credential"] = True
    try:
        find_az()
        return ChainedTokenCredential(
            AzureCliCredential(),
            DefaultAzureCredential(**excludes),
        )
    except SystemExit:
        return DefaultAzureCredential(**excludes)


def find_az() -> str:
    az = shutil.which("az")
    if az:
        return az
    if platform.system() == "Windows":
        win_az = (
            Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
            / "Microsoft SDKs"
            / "Azure"
            / "CLI2"
            / "wbin"
            / "az.cmd"
        )
        if win_az.is_file():
            return str(win_az)
    raise SystemExit(
        "Azure CLI not found. Run from repo root: orchestrate.cmd --install-cli"
    )


def generate_sql_password(length: int = 20) -> str:
    """Azure SQL password rules: upper, lower, digit, special."""
    alphabet = string.ascii_letters + string.digits + "!@#$%"
    while True:
        pwd = "".join(secrets.choice(alphabet) for _ in range(length))
        if (
            any(c.islower() for c in pwd)
            and any(c.isupper() for c in pwd)
            and any(c.isdigit() for c in pwd)
            and any(c in "!@#$%" for c in pwd)
        ):
            return pwd


def _databricks_token_works(host: str, token: str) -> bool:
    """Return whether a token can authenticate to this Databricks workspace."""
    if not host or not token:
        return False
    try:
        response = requests.get(
            f"{host.rstrip('/')}/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        return response.status_code < 300
    except requests.RequestException:
        # Do not open a browser for transient DNS/network failures. The real API
        # call will report the useful network error later.
        return True


def _azure_ad_databricks_token(*, allow_browser_login: bool = True) -> str:
    """Get a fresh Databricks AAD token, opening az login when the session expired."""
    az = find_az()
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
    if result.returncode != 0 and allow_browser_login:
        print("INFO  Azure CLI login expired/missing - opening browser for az login...")
        login = subprocess.run([az, "login"], check=False)
        if login.returncode == 0:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
    token = result.stdout.strip()
    if result.returncode != 0 or not token:
        detail = (result.stderr or result.stdout).strip()
        raise SystemExit(
            "Could not refresh Azure AD token for Databricks. Run `az login`, "
            f"complete browser sign-in, then re-run this phase.\nDetail: {detail}"
        )
    return token


def resolve_databricks_cluster(host: str, token: str, configured_id: str) -> str:
    """Return a cluster id that actually exists in the workspace.

    The default/configured cluster can be deleted between classes; instead of a
    404 at job-create time, validate it and pick any live all-purpose cluster.
    Returns "" when the workspace has no all-purpose cluster (callers fall back
    to a job cluster).
    """
    base = host.rstrip("/")
    headers = {"Authorization": f"Bearer {token}"}
    try:
        if configured_id:
            got = requests.get(
                f"{base}/api/2.0/clusters/get",
                headers=headers,
                params={"cluster_id": configured_id},
                timeout=30,
            )
            if got.status_code < 300:
                return configured_id
            print(
                f"WARN  Databricks cluster {configured_id} does not exist any more - "
                "looking for another all-purpose cluster..."
            )
        listing = requests.get(
            f"{base}/api/2.0/clusters/list", headers=headers, timeout=30
        )
        clusters = [
            c
            for c in listing.json().get("clusters", [])
            if c.get("cluster_source") in ("UI", "API")
        ]
    except requests.RequestException:
        return configured_id  # network blip — let the real call surface the error
    running = [c for c in clusters if c.get("state") == "RUNNING"]
    pick = (running or clusters)[0] if (running or clusters) else None
    if pick:
        print(
            f"INFO  Using cluster '{pick.get('cluster_name')}' ({pick['cluster_id']}) instead."
        )
        return pick["cluster_id"]
    print("WARN  No all-purpose cluster in workspace - jobs will use a job cluster.")
    return ""


def resolve_databricks_token(host: str, configured_token: str) -> str:
    """Prefer Azure AD from az login; fall back to PAT. Stale PATs are the #1 class failure."""
    try:
        aad_token = _azure_ad_databricks_token()
        if _databricks_token_works(host, aad_token):
            print("INFO  Databricks authentication: fresh Azure AD token is valid.")
            return aad_token
        print("WARN  Azure AD token rejected — trying DATABRICKS_TOKEN from .env...")
    except SystemExit as exc:
        print(f"WARN  Azure AD unavailable ({str(exc).split(chr(10), 1)[0][:120]}) — trying PAT...")

    token = configured_token.strip().strip('"').strip("'")
    if token and _databricks_token_works(host, token):
        print("INFO  Databricks authentication: configured PAT is valid.")
        return token

    raise SystemExit(
        "Could not authenticate to Databricks (Azure AD and PAT both failed). "
        "Run: git pull && az login && re-run the phase. "
        "Use the same account that can open the shared Databricks workspace."
    )


def load_config() -> SharedAdfConfig:
    file_env = _load_dotenv(ENV_FILE)
    subscription_id = (
        os.environ.get("AZURE_SUBSCRIPTION_ID") or file_env.get("AZURE_SUBSCRIPTION_ID", "")
    ).strip()
    learner = (
        os.environ.get("LEARNER") or file_env.get("LEARNER", SHARED_LEARNER)
    ).strip().lower()
    owner_email = (os.environ.get("OWNER_EMAIL") or file_env.get("OWNER_EMAIL", "")).strip()
    databricks_host = (
        os.environ.get("DATABRICKS_HOST") or file_env.get("DATABRICKS_HOST", "")
    ).strip().rstrip("/")
    databricks_token = (
        os.environ.get("DATABRICKS_TOKEN") or file_env.get("DATABRICKS_TOKEN", "")
    ).strip()
    databricks_cluster_id = (
        os.environ.get("DATABRICKS_CLUSTER_ID")
        or file_env.get("DATABRICKS_CLUSTER_ID", DEFAULT_DATABRICKS_CLUSTER_ID)
    ).strip()

    if not subscription_id:
        raise SystemExit(f"AZURE_SUBSCRIPTION_ID required in {ENV_FILE}")
    if not owner_email:
        raise SystemExit(f"OWNER_EMAIL required in {ENV_FILE}")
    if learner != SHARED_LEARNER:
        # This lab only ever targets rg-shared-class1, so a personal LEARNER in
        # .env is harmless — warn and continue instead of blocking the phase.
        print(
            f"WARN  LEARNER={learner!r} in .env — this lab always targets the shared "
            f"estate ({SHARED_RG}); continuing as LEARNER={SHARED_LEARNER}."
        )
        learner = SHARED_LEARNER
    if databricks_host:
        databricks_token = resolve_databricks_token(databricks_host, databricks_token)
        databricks_cluster_id = resolve_databricks_cluster(
            databricks_host, databricks_token, databricks_cluster_id
        )

    return SharedAdfConfig(
        subscription_id=subscription_id,
        learner=learner,
        owner_email=owner_email,
        databricks_host=databricks_host,
        databricks_token=databricks_token,
        databricks_cluster_id=databricks_cluster_id,
    )
