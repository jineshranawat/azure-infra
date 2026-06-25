"""Session 2 shared config — reads parent repo .env (same as Class-1 orchestrator)."""

from __future__ import annotations

import os
import platform
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

SESSION_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = SESSION_ROOT.parent
ENV_FILE = REPO_ROOT / ".env"

ALLOWED_LOCATIONS = frozenset({"uksouth", "ukwest"})
LEARNER_RE = re.compile(r"^[a-z0-9]{2,10}$")


@dataclass(frozen=True)
class SessionConfig:
    subscription_id: str
    learner: str
    owner_email: str
    location: str

    @property
    def resource_group(self) -> str:
        return f"rg-{self.learner}-class1"


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


def get_credential():
    """Prefer Azure CLI (fast after az login); skip slow Windows credential brokers."""
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
    """Resolve Azure CLI on Windows (az.cmd) and POSIX (az on PATH)."""
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


def load_config() -> SessionConfig:
    """Merge .env with environment variables; validate UK region and learner id."""
    file_env = _load_dotenv(ENV_FILE)
    subscription_id = (
        os.environ.get("AZURE_SUBSCRIPTION_ID") or file_env.get("AZURE_SUBSCRIPTION_ID", "")
    ).strip()
    learner = (os.environ.get("LEARNER") or file_env.get("LEARNER", "demo")).strip().lower()
    owner_email = (os.environ.get("OWNER_EMAIL") or file_env.get("OWNER_EMAIL", "")).strip()
    location = (os.environ.get("LOCATION") or file_env.get("LOCATION", "uksouth")).strip().lower()

    if not subscription_id:
        raise SystemExit(f"AZURE_SUBSCRIPTION_ID required in {ENV_FILE}")
    if not owner_email:
        raise SystemExit(f"OWNER_EMAIL required in {ENV_FILE}")
    if location not in ALLOWED_LOCATIONS:
        raise SystemExit(f"LOCATION must be one of {sorted(ALLOWED_LOCATIONS)}")
    if not LEARNER_RE.match(learner):
        raise SystemExit("LEARNER must be 2–10 lowercase alphanumeric characters.")

    return SessionConfig(
        subscription_id=subscription_id,
        learner=learner,
        owner_email=owner_email,
        location=location,
    )
