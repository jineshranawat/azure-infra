"""Provision the shared student estate in eastus (one RG for all learners).

Explicit region exception: eastus on subscription AzureSponsorship-DataTraining.
Idempotent — safe to re-run. Auth: Azure CLI (az login) or DefaultAzureCredential.

Usage:
    provision-shared.cmd
    python scripts/provision_shared.py --subscription-id a64c0dd2-... --owner-email you@example.com
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SHARED_BICEP = REPO_ROOT / "infra" / "shared-eastus.bicep"
DEFAULT_SUBSCRIPTION = "a64c0dd2-3a31-4604-bde3-3d40c7d5e8be"
SHARED_LEARNER = "shared"
SHARED_RG = f"rg-{SHARED_LEARNER}-class1"
SHARED_LOCATION = "eastus"
LEARNER_RE = re.compile(r"^[a-z0-9]{2,10}$")

logger = logging.getLogger(__name__)


def _load_dotenv() -> None:
    """Load repo .env into os.environ (do not overwrite existing env vars)."""
    dotenv = REPO_ROOT / ".env"
    if not dotenv.is_file():
        return
    for line in dotenv.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip()
        if key and key not in os.environ:
            os.environ[key] = val


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )


def _find_az() -> str:
    az = shutil.which("az")
    if az:
        return az
    if sys.platform == "win32":
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
    raise SystemExit("Azure CLI not found. Run: orchestrate.cmd --install-cli")


def _run(az: str, args: list[str], *, capture: bool = False) -> subprocess.CompletedProcess[str]:
    cmd = [az, *args]
    logger.debug("exec: %s", " ".join(cmd))
    return subprocess.run(cmd, text=True, capture_output=capture, check=True)


def _budget_start_date() -> str:
    today = datetime.now(timezone.utc).date()
    return date(today.year, today.month, 1).isoformat()


def _get_principal(az: str) -> tuple[str, str]:
    """Return (object_id, principal_type) for RBAC assignments."""
    account = json.loads(_run(az, ["account", "show", "-o", "json"], capture=True).stdout)
    user = account.get("user", {})
    if user.get("type") == "servicePrincipal":
        sp_id = user.get("name", "")
        if not sp_id:
            raise SystemExit("Could not resolve service principal object ID from az account show.")
        sp = json.loads(
            _run(
                az,
                ["ad", "sp", "show", "--id", sp_id, "-o", "json"],
                capture=True,
            ).stdout
        )
        return sp["id"], "ServicePrincipal"
    result = _run(
        az,
        ["ad", "signed-in-user", "show", "--query", "id", "-o", "tsv"],
        capture=True,
    )
    principal_id = result.stdout.strip()
    if not principal_id:
        raise SystemExit("Could not resolve signed-in user object ID.")
    return principal_id, "User"


def _ensure_subscription(az: str, subscription_id: str) -> None:
    current = json.loads(_run(az, ["account", "show", "-o", "json"], capture=True).stdout)
    if current.get("id") != subscription_id:
        logger.info("Setting subscription to %s", subscription_id)
        _run(az, ["account", "set", "--subscription", subscription_id])


def _ensure_resource_group(az: str, owner_email: str) -> None:
    tags = (
        f"env=training owner={owner_email} costcentre=boe-data-enablement "
        f"data-class=training-synthetic course=azure-etl-boe class=shared-eastus estate=shared"
    )
    exists = _run(
        az,
        ["group", "exists", "--name", SHARED_RG, "-o", "tsv"],
        capture=True,
    ).stdout.strip().lower() == "true"
    if exists:
        logger.info("Resource group %s already present — incremental update", SHARED_RG)
    else:
        logger.info("Creating resource group %s in %s", SHARED_RG, SHARED_LOCATION)
    _run(
        az,
        [
            "group",
            "create",
            "--name",
            SHARED_RG,
            "--location",
            SHARED_LOCATION,
            "--tags",
            tags,
            "--output",
            "none",
        ],
    )


def _what_if(az: str, owner_email: str, principal_id: str, principal_type: str) -> None:
    """ARM what-if = Bicep's answer to `terraform plan` (preview, no changes)."""
    logger.info("what-if preview of shared-eastus.bicep (no changes will be applied)")
    result = subprocess.run(
        [
            az,
            "deployment",
            "group",
            "what-if",
            "--resource-group",
            SHARED_RG,
            "--name",
            "shared-eastus-whatif",
            "--mode",
            "Incremental",
            "--template-file",
            str(SHARED_BICEP),
            "--parameters",
            f"location={SHARED_LOCATION}",
            f"learner={SHARED_LEARNER}",
            f"ownerEmail={owner_email}",
            f"principalObjectId={principal_id}",
            f"principalType={principal_type}",
            f"budgetStartDate={_budget_start_date()}",
            "deployBudget=false",
        ],
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit("what-if failed — see output above.")
    print()
    print("Plan only (ARM what-if) — no changes applied. Re-run without --plan-only to deploy.")


def _deploy(az: str, owner_email: str, principal_id: str, principal_type: str) -> dict:
    logger.info("Deploying shared-eastus.bicep (storage + ADF + Databricks)")
    result = subprocess.run(
        [
            az,
            "deployment",
            "group",
            "create",
            "--resource-group",
            SHARED_RG,
            "--name",
            "shared-eastus",
            "--mode",
            "Incremental",
            "--template-file",
            str(SHARED_BICEP),
            "--parameters",
            f"location={SHARED_LOCATION}",
            f"learner={SHARED_LEARNER}",
            f"ownerEmail={owner_email}",
            f"principalObjectId={principal_id}",
            f"principalType={principal_type}",
            f"budgetStartDate={_budget_start_date()}",
            "deployBudget=false",
            "-o",
            "json",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        logger.error(result.stderr or result.stdout)
        raise SystemExit("Bicep deployment failed — see log above.")
    body = json.loads(result.stdout)
    outputs: dict = {}
    for key, val in body.get("properties", {}).get("outputs", {}).items():
        outputs[key] = val.get("value") if isinstance(val, dict) else val
    return outputs


def _print_summary(subscription_id: str, outputs: dict) -> None:
    rg = outputs.get("resourceGroupName", SHARED_RG)
    sub = subscription_id
    logger.info("")
    logger.info("=== Shared estate deployed (eastus) ===")
    logger.info("  Subscription   : %s", sub)
    logger.info("  Resource group : %s", rg)
    logger.info("  Storage (ADLS) : %s", outputs.get("storageAccountName", ""))
    logger.info("  Key Vault      : %s", outputs.get("keyVaultName", ""))
    logger.info("  Data Factory   : %s", outputs.get("dataFactoryName", ""))
    logger.info("  Databricks     : %s", outputs.get("databricksWorkspaceName", ""))
    logger.info("")
    logger.info("Learners set in .env:")
    logger.info("  AZURE_SUBSCRIPTION_ID=%s", sub)
    logger.info("  LEARNER=%s", SHARED_LEARNER)
    logger.info("  LOCATION=%s", SHARED_LOCATION)
    logger.info("")
    logger.info(
        "  Portal: https://portal.azure.com/#@/resource/subscriptions/%s/resourceGroups/%s/overview",
        sub,
        rg,
    )


def provision(
    subscription_id: str,
    owner_email: str,
    *,
    verbose: bool = False,
    plan_only: bool = False,
) -> dict:
    _configure_logging(verbose)
    if not LEARNER_RE.match(SHARED_LEARNER):
        raise SystemExit(f"SHARED_LEARNER '{SHARED_LEARNER}' must match learner id pattern.")

    az = _find_az()
    _ensure_subscription(az, subscription_id)
    principal_id, principal_type = _get_principal(az)
    logger.info("Principal %s (%s)", principal_id, principal_type)

    if plan_only:
        exists = _run(
            az, ["group", "exists", "--name", SHARED_RG, "-o", "tsv"], capture=True
        ).stdout.strip().lower() == "true"
        if not exists:
            raise SystemExit(
                f"Resource group {SHARED_RG} does not exist yet — what-if needs it.\n"
                "Run without --plan-only for the first deploy."
            )
        _what_if(az, owner_email, principal_id, principal_type)
        return {}

    _ensure_resource_group(az, owner_email)
    outputs = _deploy(az, owner_email, principal_id, principal_type)
    _print_summary(subscription_id, outputs)
    return outputs


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provision shared eastus training estate.")
    parser.add_argument(
        "--subscription-id",
        default=os.environ.get("AZURE_SUBSCRIPTION_ID", DEFAULT_SUBSCRIPTION),
        help=f"Shared subscription (default: {DEFAULT_SUBSCRIPTION}).",
    )
    parser.add_argument(
        "--owner-email",
        default=os.environ.get("OWNER_EMAIL") or os.environ.get("CLASS_OWNER_EMAIL", ""),
        help="Owner email for tags and budget alerts.",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Preview via ARM what-if — Bicep's equivalent of `terraform plan` (no changes).",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    _load_dotenv()
    args = _parse_args(argv)
    if not args.owner_email:
        raise SystemExit(
            "--owner-email or OWNER_EMAIL / CLASS_OWNER_EMAIL required.\n"
            "Fix: set CLASS_OWNER_EMAIL in .env  OR  run:\n"
            "  provision-shared.cmd --owner-email you@example.com\n"
            "  infra-walkthrough.cmd --phase 1"
        )
    try:
        provision(
            args.subscription_id,
            args.owner_email,
            verbose=args.verbose,
            plan_only=args.plan_only,
        )
    except SystemExit:
        raise
    except Exception:
        logger.exception("Provisioning failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
