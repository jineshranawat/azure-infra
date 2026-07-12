"""Shared ADF lab config — LEARNER=shared, eastus RG, westus SQL exception."""

from __future__ import annotations

import os
import platform
import re
import secrets
import shutil
import string
from dataclasses import dataclass
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = LAB_ROOT.parent
ENV_FILE = REPO_ROOT / ".env"

SHARED_LEARNER = "shared"
SHARED_RG = "rg-shared-class1"
ADF_LOCATION = "eastus"
SQL_LOCATION = "westus"  # documented exception — SQL only
# Shared all-purpose cluster in dbw-shared-* (same as run-50-problems.cmd).
DEFAULT_DATABRICKS_CLUSTER_ID = "0703-105931-31juyffm"
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
        raise SystemExit(
            f"This lab targets the shared estate only. Set LEARNER={SHARED_LEARNER} in .env"
        )
    if not LEARNER_RE.match(learner):
        raise SystemExit("LEARNER must be 2–10 lowercase alphanumeric characters.")

    return SharedAdfConfig(
        subscription_id=subscription_id,
        learner=learner,
        owner_email=owner_email,
        databricks_host=databricks_host,
        databricks_token=databricks_token,
        databricks_cluster_id=databricks_cluster_id,
    )
