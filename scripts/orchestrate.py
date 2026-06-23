#!/usr/bin/env python3
"""End-to-end lab orchestration: Class-1 landing zone + platform services + verify.

Windows learners run orchestrate.cmd (this module is the real logic).

First time on a clean machine:
    orchestrate.cmd --install-cli

Full lab (default, safe to re-run — idempotent incremental deploys):
    orchestrate.cmd

Class-1 landing zone only:
    orchestrate.cmd --class1-only

Teardown:
    orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes

Environment variables / .env keys:
    AZURE_SUBSCRIPTION_ID, LEARNER, OWNER_EMAIL, LOCATION (uksouth|ukwest)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import re
import secrets
import shutil
import string
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
VENV_DIR = REPO_ROOT / ".venv"
ENV_FILE = REPO_ROOT / ".env"
ENV_EXAMPLE = REPO_ROOT / ".env.example"
REQUIREMENTS = REPO_ROOT / "requirements.txt"
BICEP_TEMPLATE = REPO_ROOT / "infra" / "main.bicep"
PLATFORM_BICEP = REPO_ROOT / "infra" / "platform-services.bicep"

ALLOWED_LOCATIONS = frozenset({"uksouth", "ukwest"})
LEARNER_RE = re.compile(r"^[a-z0-9]{2,10}$")

MANDATORY_TAGS: dict[str, str] = {
    "env": "training",
    "costcentre": "boe-data-enablement",
    "data-class": "training-synthetic",
    "course": "azure-etl-boe",
    "class": "class-1",
    "auto-teardown": "nightly",
}

logger = logging.getLogger("orchestrate")

# --- Config & paths (guardrail: UK-only locations, deterministic learner id) ---


@dataclass(frozen=True)
class Config:
    subscription_id: str
    learner: str
    owner_email: str
    location: str

    @property
    def resource_group(self) -> str:
        return f"rg-{self.learner}-class1"


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )


def _step(title: str) -> None:
    logger.info("")
    logger.info("==> %s", title)


def _ok(msg: str) -> None:
    logger.info("    OK: %s", msg)


def _warn(msg: str) -> None:
    logger.warning("    WARN: %s", msg)


def _run(
    cmd: list[str],
    *,
    check: bool = True,
    capture: bool = False,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    logger.debug("exec: %s", " ".join(cmd))
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        capture_output=capture,
        env=merged_env,
        cwd=str(cwd or REPO_ROOT),
    )


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


def _merge_config(args: argparse.Namespace) -> Config:
    file_env = _load_dotenv(ENV_FILE)
    subscription_id = (
        args.subscription_id
        or os.environ.get("AZURE_SUBSCRIPTION_ID")
        or file_env.get("AZURE_SUBSCRIPTION_ID", "")
    ).strip()
    learner = (
        args.learner or os.environ.get("LEARNER") or file_env.get("LEARNER", "demo")
    ).strip().lower()
    owner_email = (
        args.owner_email
        or os.environ.get("OWNER_EMAIL")
        or file_env.get("OWNER_EMAIL", "")
    ).strip()
    location = (
        args.location or os.environ.get("LOCATION") or file_env.get("LOCATION", "uksouth")
    ).strip().lower()

    if not subscription_id:
        raise SystemExit(
            "AZURE_SUBSCRIPTION_ID required. Set in .env or pass --subscription-id."
        )
    if not owner_email:
        raise SystemExit("OWNER_EMAIL required. Set in .env or pass --owner-email.")
    if location not in ALLOWED_LOCATIONS:
        raise SystemExit(f"LOCATION must be one of {sorted(ALLOWED_LOCATIONS)}.")
    if not LEARNER_RE.match(learner):
        raise SystemExit("LEARNER must be 2–10 lowercase alphanumeric characters.")

    return Config(
        subscription_id=subscription_id,
        learner=learner,
        owner_email=owner_email,
        location=location,
    )


# --- Bootstrap: .env, venv, Azure CLI (Windows-only auto-install) ---
def _ensure_dotenv() -> None:
    if ENV_FILE.is_file():
        _ok(f".env exists ({ENV_FILE})")
        return
    if not ENV_EXAMPLE.is_file():
        raise SystemExit(f"Missing {ENV_EXAMPLE}")
    shutil.copy(ENV_EXAMPLE, ENV_FILE)
    _ok(f"Created .env from template — edit {ENV_FILE} then re-run")
    raise SystemExit(1)


def _python_for_venv() -> str:
    """Resolve a Python 3.11+ executable (Windows Store python3 stubs are skipped)."""
    if sys.version_info >= (3, 11) and Path(sys.executable).is_file():
        return sys.executable

    if platform.system() == "Windows":
        candidates = ("py", "python", "python3")
    else:
        candidates = ("python3", "python")

    for candidate in candidates:
        path = shutil.which(candidate)
        if not path:
            continue
        try:
            ok = subprocess.run(
                [path, "-c", "import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)"],
                capture_output=True,
                check=False,
            )
            if ok.returncode == 0:
                return path
        except OSError:
            continue
    raise SystemExit("Python 3.11+ not found on PATH.")


def _venv_python() -> Path:
    if platform.system() == "Windows":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def _ensure_venv(*, skip: bool) -> Path:
    if skip:
        if _venv_python().is_file():
            return _venv_python()
        return Path(_python_for_venv())

    py = _python_for_venv()
    version = _run([py, "-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"], capture=True)
    major, minor = map(int, version.stdout.strip().split("."))
    if major < 3 or (major == 3 and minor < 11):
        raise SystemExit(f"Python 3.11+ required; found {version.stdout.strip()}")
    _ok(f"Python {version.stdout.strip()}")

    vpy = _venv_python()
    if vpy.is_file():
        _ok("Virtual environment already present (.venv)")
    else:
        _step("Creating virtual environment (.venv)")
        _run([py, "-m", "venv", str(VENV_DIR)])
        _ok("Virtual environment created")

    _step("Installing Python dependencies (idempotent pip install)")
    _run([str(vpy), "-m", "pip", "install", "--upgrade", "pip", "-q"])
    _run([str(vpy), "-m", "pip", "install", "-r", str(REQUIREMENTS), "-q"])
    _ok("requirements.txt installed")
    return vpy


def _find_az() -> str | None:
    az = shutil.which("az")
    if az:
        return az
    if platform.system() == "Windows":
        win_az = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Microsoft SDKs" / "Azure" / "CLI2" / "wbin" / "az.cmd"
        if win_az.is_file():
            return str(win_az)
    return None


def _install_azure_cli_windows() -> None:
    if not shutil.which("winget"):
        raise SystemExit(
            "winget not found. Install Azure CLI manually:\n"
            "  https://learn.microsoft.com/cli/azure/install-azure-cli-windows"
        )
    _step("Installing Azure CLI (winget)")
    _run(
        [
            "winget", "install", "-e", "--id", "Microsoft.AzureCLI",
            "--accept-source-agreements", "--accept-package-agreements",
        ],
        check=False,
    )


def _ensure_azure_cli(*, install: bool) -> str:
    az = _find_az()
    if az:
        ver = _run([az, "version", "-o", "json"], capture=True)
        cli_ver = json.loads(ver.stdout).get("azure-cli", "unknown")
        _ok(f"Azure CLI {cli_ver}")
        return az

    if not install:
        raise SystemExit(
            "Azure CLI not found. Re-run: orchestrate.cmd --install-cli"
        )

    if platform.system() != "Windows":
        raise SystemExit(
            "This lab is Windows-only. Use a Windows machine and orchestrate.cmd."
        )
    _install_azure_cli_windows()

    az = _find_az()
    if not az:
        raise SystemExit("Azure CLI install finished but 'az' is not on PATH. Open a new shell.")
    return az


# --- Azure login & subscription context ---
def _ensure_az_login(az: str, *, use_device_code: bool) -> None:
    _step("Checking Azure login")
    probe = _run([az, "account", "show", "-o", "json"], check=False, capture=True)
    if probe.returncode == 0:
        acct = json.loads(probe.stdout)
        _ok(f"Signed in as {acct.get('user', {}).get('name', 'unknown')}")
        return

    _warn("Not signed in — starting az login")
    login_cmd = [az, "login"]
    if use_device_code:
        login_cmd.append("--use-device-code")
    _run(login_cmd, check=True)
    _ok("Login complete")


def _set_subscription(az: str, subscription_id: str) -> None:
    _step("Setting active subscription")
    _run([az, "account", "set", "--subscription", subscription_id])
    show = _run([az, "account", "show", "-o", "json"], capture=True)
    acct = json.loads(show.stdout)
    _ok(f"Subscription: {acct.get('name')} ({acct.get('id')})")


def _principal_object_id(az: str) -> str:
    result = _run([az, "ad", "signed-in-user", "show", "--query", "id", "-o", "tsv"], capture=True)
    principal = result.stdout.strip()
    if not principal:
        raise RuntimeError("Could not resolve signed-in user object ID.")
    return principal


def _budget_start_date() -> str:
    today = datetime.now(timezone.utc).date()
    return today.replace(day=1).isoformat()


def _tag_args(cfg: Config) -> list[str]:
    tags = dict(MANDATORY_TAGS)
    tags["owner"] = cfg.owner_email
    args: list[str] = []
    for key, value in tags.items():
        args.extend(["--tags", f"{key}={value}"])
    return args


# --- Class-1 deploy: incremental Bicep + discover partial estate ---
def _resource_group_exists(az: str, name: str) -> bool:
    """Return True when the RG already exists (idempotent re-run)."""
    result = _run(
        [az, "group", "exists", "--name", name, "-o", "tsv"],
        capture=True,
        check=False,
    )
    return result.stdout.strip().lower() == "true"


def _deploy_bicep(az: str, cfg: Config, principal_id: str) -> dict[str, Any]:
    if _resource_group_exists(az, cfg.resource_group):
        _ok(f"Resource group {cfg.resource_group} already present — incremental update")
    else:
        _step(f"Creating resource group {cfg.resource_group}")
    _run(
        [az, "group", "create", "--name", cfg.resource_group, "--location", cfg.location, "--output", "none"]
        + _tag_args(cfg)
    )
    _ok(f"Resource group {cfg.resource_group} ready")

    _step("Deploying Bicep template (incremental — budget, KV, storage, RBAC)")
    deploy = _run(
        [
            az, "deployment", "group", "create",
            "--resource-group", cfg.resource_group,
            "--name", "main",
            "--mode", "Incremental",
            "--template-file", str(BICEP_TEMPLATE),
            "--parameters",
            f"location={cfg.location}",
            f"learner={cfg.learner}",
            f"ownerEmail={cfg.owner_email}",
            f"principalObjectId={principal_id}",
            f"budgetStartDate={_budget_start_date()}",
            "-o", "json",
        ],
        check=False,
        capture=True,
    )

    outputs: dict[str, Any] = {}
    if deploy.returncode == 0:
        body = json.loads(deploy.stdout)
        raw_outputs = body.get("properties", {}).get("outputs", {})
        for key, val in raw_outputs.items():
            outputs[key] = val.get("value") if isinstance(val, dict) else val
        _ok("Bicep deployment succeeded")
        return outputs

    _warn("Bicep deployment reported errors — checking partial estate")
    logger.debug(deploy.stderr or deploy.stdout)
    return _discover_outputs(az, cfg)


def _discover_outputs(az: str, cfg: Config) -> dict[str, Any]:
    """Read resource names from live RG when deployment output is unavailable."""
    resources = json.loads(
        _run(
            [az, "resource", "list", "--resource-group", cfg.resource_group, "-o", "json"],
            capture=True,
        ).stdout
    )
    outputs: dict[str, Any] = {"resourceGroupName": cfg.resource_group, "location": cfg.location}
    for res in resources:
        rtype = res.get("type", "")
        name = res.get("name", "")
        if rtype == "Microsoft.KeyVault/vaults":
            outputs["keyVaultName"] = name
        elif rtype == "Microsoft.Storage/storageAccounts":
            outputs["storageAccountName"] = name
        elif rtype == "Microsoft.DataFactory/factories":
            outputs["dataFactoryName"] = name
        elif rtype == "Microsoft.Databricks/workspaces":
            outputs["databricksWorkspaceName"] = name
        elif rtype == "Microsoft.Purview/accounts":
            outputs["purviewAccountName"] = name
        elif rtype == "Microsoft.Synapse/workspaces":
            outputs["synapseWorkspaceName"] = name
        elif rtype == "Microsoft.Fabric/capacities":
            outputs["fabricCapacityName"] = name
            outputs["fabricCapacityId"] = res.get("id", "")
    return outputs


def _synapse_sql_password() -> str:
    """Azure SQL admin password complexity."""
    special = "!@#$%"
    while True:
        chars = [
            secrets.choice(string.ascii_lowercase),
            secrets.choice(string.ascii_uppercase),
            secrets.choice(string.digits),
            secrets.choice(special),
        ]
        pool = string.ascii_letters + string.digits + special
        chars.extend(secrets.choice(pool) for _ in range(12))
        secrets.SystemRandom().shuffle(chars)
        pwd = "".join(chars)
        if len(pwd) >= 12:
            return pwd


def _register_platform_providers(az: str) -> None:
    _step("Registering Azure resource providers (idempotent)")
    for ns in (
        "Microsoft.Network",
        "Microsoft.Databricks",
        "Microsoft.Synapse",
        "Microsoft.DataFactory",
        "Microsoft.Sql",
        "Microsoft.Fabric",
        "Microsoft.Purview",
        "Microsoft.EventHub",
    ):
        _run([az, "provider", "register", "--namespace", ns], check=False)
    _ok("Provider registration requested")


def _deploy_platforms(az: str, cfg: Config, outputs: dict[str, Any], vpy: Path) -> dict[str, Any]:
    storage_name = outputs.get("storageAccountName")
    if not storage_name:
        outputs = {**outputs, **_discover_outputs(az, cfg)}
        storage_name = outputs.get("storageAccountName")
    if not storage_name:
        raise SystemExit(
            "Class-1 storage account not found. Run main deploy first (orchestrate without --platforms-only)."
        )

    _step("Deploying platform services (incremental — ADF, Synapse, Purview, Fabric, Databricks)")
    _register_platform_providers(az)
    sql_password = _synapse_sql_password()
    admin_json = json.dumps([cfg.owner_email])
    deploy = _run(
        [
            az, "deployment", "group", "create",
            "--resource-group", cfg.resource_group,
            "--name", "platforms",
            "--mode", "Incremental",
            "--template-file", str(PLATFORM_BICEP),
            "--parameters",
            f"location={cfg.location}",
            f"learner={cfg.learner}",
            f"ownerEmail={cfg.owner_email}",
            f"storageAccountName={storage_name}",
            f"synapseSqlPassword={sql_password}",
            "deployPurview=true",
            "deploySynapse=false",
            "deployFabric=true",
            "purviewLocation=eastus",
            "fabricLocation=westeurope",
            f"fabricSkuName=F0",
            f"fabricAdminMembers={admin_json}",
            "-o", "json",
        ],
        check=False,
        capture=True,
    )

    platform_outputs: dict[str, Any] = {}
    if deploy.returncode != 0:
        _warn("Platform deploy failed — retrying without Fabric (subscription Fabric quota may be 0)")
        deploy = _run(
            [
                az, "deployment", "group", "create",
                "--resource-group", cfg.resource_group,
                "--name", "platforms",
                "--mode", "Incremental",
                "--template-file", str(PLATFORM_BICEP),
                "--parameters",
                f"location={cfg.location}",
                f"learner={cfg.learner}",
                f"ownerEmail={cfg.owner_email}",
                f"storageAccountName={storage_name}",
                f"synapseSqlPassword={sql_password}",
                "deployPurview=true",
                "deploySynapse=false",
                "deployFabric=false",
                "purviewLocation=eastus",
                f"fabricAdminMembers={admin_json}",
                "-o", "json",
            ],
            check=False,
            capture=True,
        )

    if deploy.returncode == 0:
        body = json.loads(deploy.stdout)
        for key, val in body.get("properties", {}).get("outputs", {}).items():
            platform_outputs[key] = val.get("value") if isinstance(val, dict) else val
        _ok("Platform services deployment succeeded")
        merged = {**outputs, **platform_outputs}
        _create_fabric_workspace(vpy, cfg, merged)
        return merged

    logger.error(deploy.stderr or deploy.stdout)
    raise SystemExit("Platform services deployment failed — see log above")


def _create_fabric_workspace(vpy: Path, cfg: Config, outputs: dict[str, Any]) -> None:
    capacity_id = outputs.get("fabricCapacityId") or ""
    if not capacity_id or str(capacity_id).startswith("skipped"):
        _warn("Fabric capacity not deployed — skipping workspace creation")
        return

    ws_name = f"ws-{cfg.learner}-class1"
    _step(f"Creating Fabric workspace '{ws_name}'")
    result = _run(
        [
            str(vpy),
            str(REPO_ROOT / "scripts" / "fabric_workspace.py"),
            "--workspace-name", ws_name,
            "--capacity-id", capacity_id,
            "--description", f"Class-1 training workspace for {cfg.owner_email}",
        ],
        check=False,
        capture=True,
    )
    if result.returncode == 0:
        for line in (result.stdout or "").splitlines():
            if "Workspace" in line or "Fabric" in line:
                logger.info("    %s", line.split("—", 1)[-1].strip())
        _ok(f"Fabric workspace '{ws_name}' ready")
    else:
        _warn("Fabric workspace API call failed (capacity exists — create workspace in https://app.fabric.microsoft.com)")
        logger.debug(result.stderr or result.stdout)


def _deploy_python(vpy: Path, cfg: Config, principal_id: str | None) -> dict[str, Any]:
    _step("Deploying via Python SDK (provision.py)")
    cmd = [
        str(vpy),
        str(REPO_ROOT / "scripts" / "provision.py"),
        "--subscription-id", cfg.subscription_id,
        "--learner", cfg.learner,
        "--owner-email", cfg.owner_email,
        "--location", cfg.location,
    ]
    if principal_id:
        cmd.extend(["--principal-id", principal_id])
    _run(cmd)
    return _discover_outputs(_find_az() or "az", cfg)


def _ensure_rbac(az: str, cfg: Config, principal_id: str, outputs: dict[str, Any]) -> None:
    kv_name = outputs.get("keyVaultName")
    st_name = outputs.get("storageAccountName")
    if not kv_name or not st_name:
        _warn("Could not resolve KV/storage names — skipping RBAC fallback")
        return

    sub = cfg.subscription_id
    kv_scope = f"/subscriptions/{sub}/resourceGroups/{cfg.resource_group}/providers/Microsoft.KeyVault/vaults/{kv_name}"
    st_scope = f"/subscriptions/{sub}/resourceGroups/{cfg.resource_group}/providers/Microsoft.Storage/storageAccounts/{st_name}"

    _step("Ensuring data-plane RBAC (idempotent fallback)")
    for role, scope in (
        ("Key Vault Secrets Officer", kv_scope),
        ("Storage Blob Data Contributor", st_scope),
    ):
        existing = _run(
            [
                az, "role", "assignment", "list",
                "--assignee-object-id", principal_id,
                "--scope", scope,
                "--query", "length(@)",
                "-o", "tsv",
            ],
            capture=True,
            check=False,
        )
        if existing.stdout.strip() not in ("", "0"):
            _ok(f"{role} already assigned")
            continue
        _run(
            [
                az, "role", "assignment", "create",
                "--role", role,
                "--assignee-object-id", principal_id,
                "--assignee-principal-type", "User",
                "--scope", scope,
                "-o", "none",
            ],
            check=False,
        )
        _ok(f"{role} assigned on {scope.split('/')[-1]}")


def _verify(vpy: Path, cfg: Config, *, include_platforms: bool = False) -> float:
    _step("Verifying SKUs and month-to-date cost")
    env = os.environ.copy()
    env["AZURE_SUBSCRIPTION_ID"] = cfg.subscription_id
    cmd = [
        str(vpy),
        str(REPO_ROOT / "scripts" / "verify_cost.py"),
        "--resource-group", cfg.resource_group,
    ]
    if include_platforms:
        cmd.append("--include-platforms")
    result = _run(
        cmd,
        env=env,
        capture=True,
        check=False,
    )
    for line in (result.stdout or "").splitlines():
        if "INFO" in line and "__main__" in line:
            logger.info("    %s", line.split("—", 1)[-1].strip())
    if result.returncode != 0:
        logger.error(result.stderr or result.stdout)
        raise SystemExit("verify_cost.py failed — see log above")
    _ok("All resources pass the £0 SKU allow-list")
    for line in (result.stdout or "").splitlines():
        if "Month-to-date" in line:
            return 0.0
    return 0.0


def _compare_platforms(vpy: Path, cfg: Config) -> None:
    _step("Comparing data platform list prices (ADF, Databricks, Purview, SQL...)")
    env = os.environ.copy()
    env["AZURE_SUBSCRIPTION_ID"] = cfg.subscription_id
    env["LEARNER"] = cfg.learner
    env["LOCATION"] = cfg.location
    result = _run(
        [
            str(vpy),
            str(REPO_ROOT / "scripts" / "compare_platform_costs.py"),
            "--subscription-id", cfg.subscription_id,
            "--resource-group", cfg.resource_group,
            "--region", cfg.location,
        ],
        env=env,
        check=False,
    )
    if result.stdout:
        for line in result.stdout.splitlines():
            logger.info("%s", line)
    if result.returncode != 0:
        _warn("Platform cost comparison failed (non-fatal)")


def _print_summary(cfg: Config, outputs: dict[str, Any], *, lab_mode: str) -> None:
    _step("Deployment summary")
    try:
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        from compare_platform_costs import cost_portal_urls

        urls = cost_portal_urls(cfg.subscription_id, cfg.resource_group)
    except ImportError:
        urls = {}

    sub = cfg.subscription_id
    rg = cfg.resource_group
    rg_url = f"https://portal.azure.com/#@/resource/subscriptions/{sub}/resourceGroups/{rg}/overview"

    def _portal_link(rtype_path: str, name: str) -> str:
        if not name or name.startswith("skipped"):
            return "(not deployed)"
        return (
            f"https://portal.azure.com/#@/resource/subscriptions/{sub}"
            f"/resourceGroups/{rg}/providers/{rtype_path}/{name}/overview"
        )

    kv = outputs.get("keyVaultName", "")
    st = outputs.get("storageAccountName", "")
    adf = outputs.get("dataFactoryName", "")
    dbw = outputs.get("databricksWorkspaceName", "")
    purview = outputs.get("purviewAccountName", "")
    synapse = outputs.get("synapseWorkspaceName", "")
    fabric = outputs.get("fabricCapacityName", "")

    lines = [
        f"Lab mode       : {lab_mode}",
        f"Resource group : {rg} ({cfg.location})",
        "",
        "--- Class-1 landing zone ---",
        f"  Key Vault      : {kv or '(see portal)'}",
        f"  Storage (ADLS) : {st or '(see portal)'}",
        f"  Budget         : budget-{cfg.learner}-class1",
        "",
        "--- Platform services ---",
        f"  Data Factory   : {adf or '(see portal)'}",
        f"  Databricks     : {dbw or '(see portal)'}",
        f"  Purview        : {purview or '(see portal)'}",
        f"  Synapse SQL    : {synapse}",
        f"  Fabric capacity: {fabric or 'manual trial — see docs/TRAINER-NOTES.md'}",
        "",
        "--- Portal quick links ---",
        f"  Resource group : {rg_url}",
    ]
    if purview and not str(purview).startswith("skipped"):
        lines.append(
            f"  Purview catalog: https://web.purview.azure.com/resource/subscriptions/{sub}"
            f"/resourceGroups/{rg}/providers/Microsoft.Purview/accounts/{purview}/overview"
        )
    if adf:
        lines.append(f"  Data Factory   : {_portal_link('Microsoft.DataFactory/factories', adf)}")
    if dbw:
        lines.append(f"  Databricks     : {_portal_link('Microsoft.Databricks/workspaces', dbw)}")
    lines.extend([
        f"  Fabric portal  : https://app.fabric.microsoft.com/",
        "",
        "--- Cost Management ---",
        f"  Cost analysis (RG): {urls.get('cost_analysis_resource_group', '')}",
        f"  Budgets           : {urls.get('budgets', 'https://portal.azure.com/#view/Microsoft_Azure_CostManagement/BudgetsBlade')}",
        "",
        "--- Teardown ---",
        f"  orchestrate.cmd teardown --resource-group {rg} --yes",
        "",
        "Trainer notes: docs/TRAINER-NOTES.md",
    ])
    for line in lines:
        logger.info("  %s", line)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Full Azure ETL lab orchestrator: Class-1 + platforms + verify.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--class1-only",
        action="store_true",
        help="Deploy Class-1 landing zone only (no ADF/Purview/Databricks)",
    )
    mode.add_argument(
        "--platforms-only",
        action="store_true",
        help="Deploy platform services only (Class-1 must already exist)",
    )
    parser.add_argument("--subscription-id", help="Azure subscription GUID")
    parser.add_argument("--learner", help="2-10 char learner id (lowercase)")
    parser.add_argument("--owner-email", help="Owner email for tags and budget alerts")
    parser.add_argument("--location", choices=sorted(ALLOWED_LOCATIONS), help="UK region")
    parser.add_argument(
        "--method",
        choices=("bicep", "python", "auto"),
        default="auto",
        help="Class-1 deploy path: Bicep (default) or Python SDK",
    )
    parser.add_argument(
        "--install-cli",
        action="store_true",
        help="Install Azure CLI if missing (winget on Windows)",
    )
    parser.add_argument("--skip-setup", action="store_true", help="Skip venv and pip install")
    parser.add_argument("--skip-verify", action="store_true", help="Skip verify_cost.py")
    parser.add_argument(
        "--skip-compare",
        action="store_true",
        help="Skip platform list-price comparison report",
    )
    parser.add_argument(
        "--use-device-code",
        action="store_true",
        help="Use device-code flow for az login",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _lab_mode_label(args: argparse.Namespace) -> str:
    if args.platforms_only:
        return "platforms-only"
    if args.class1_only:
        return "class1-only"
    return "full-lab"


def _run_platforms(args: argparse.Namespace) -> bool:
    if args.platforms_only:
        return True
    if args.class1_only:
        return False
    return True  # full lab default


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    lab_mode = _lab_mode_label(args)
    run_platforms = _run_platforms(args)
    run_compare = not args.skip_compare and run_platforms

    logger.info("Azure ETL lab orchestrator — mode: %s — repo: %s", lab_mode, REPO_ROOT)

    _ensure_dotenv()
    cfg = _merge_config(args)

    vpy = _ensure_venv(skip=args.skip_setup)
    az = _ensure_azure_cli(install=args.install_cli)
    _ensure_az_login(az, use_device_code=args.use_device_code)
    _set_subscription(az, cfg.subscription_id)

    principal_id = _principal_object_id(az)
    _ok(f"Principal object ID: {principal_id}")

    method = "bicep" if args.method == "auto" else args.method
    outputs: dict[str, Any] = {}

    # Phase 1: Class-1 landing zone (budget, KV, storage, RBAC)
    if not args.platforms_only:
        if method == "bicep":
            outputs = _deploy_bicep(az, cfg, principal_id)
        else:
            outputs = _deploy_python(vpy, cfg, principal_id)
        _ensure_rbac(az, cfg, principal_id, outputs)

    # Phase 2: Platform services (ADF, Purview, Databricks; Synapse/Fabric best-effort)
    if run_platforms:
        if args.platforms_only:
            outputs = _discover_outputs(az, cfg)
        outputs = _deploy_platforms(az, cfg, outputs, vpy)

    # Phase 3: Verify SKUs + MTD cost
    if not args.skip_verify:
        _verify(vpy, cfg, include_platforms=run_platforms)

    # Phase 4: List-price comparison + Cost Explorer links
    if run_compare:
        _compare_platforms(vpy, cfg)

    _print_summary(cfg, outputs, lab_mode=lab_mode)
    logger.info("")
    logger.info("Orchestration complete (%s).", lab_mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
