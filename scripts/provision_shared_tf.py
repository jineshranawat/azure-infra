#!/usr/bin/env python3
"""Provision shared eastus estate with Terraform (twin of provision_shared.py / Bicep).

Same resources as infra/shared-eastus.bicep. Docs: docs/BICEP-TERRAFORM-SHARED-ESTATE.md

Usage:
  provision-shared-tf.cmd
  provision-shared-tf.cmd --plan-only
  provision-shared-tf.cmd --name-hash qgr7mj
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_DIR = REPO_ROOT / "infra" / "terraform" / "shared-eastus"
DEFAULT_SUBSCRIPTION = "a64c0dd2-3a31-4604-bde3-3d40c7d5e8be"
SHARED_RG = "rg-shared-class1"
# Live Bicep estate hash (stsharedqgr7mj → qgr7mj)
DEFAULT_LIVE_HASH = "qgr7mj"

logger = logging.getLogger(__name__)


def _load_dotenv() -> None:
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


def _find_az() -> str:
    az = shutil.which("az")
    if az:
        return az
    raise SystemExit("Azure CLI not found. Run: orchestrate.cmd --install-cli")


def _find_terraform() -> str:
    tf = shutil.which("terraform")
    if tf:
        return tf
    raise SystemExit(
        "Terraform not found on PATH.\n"
        "Install: https://developer.hashicorp.com/terraform/install\n"
        "  winget install Hashicorp.Terraform\n"
        "Then re-run: provision-shared-tf.cmd"
    )


def _run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    check: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    logger.info("RUN: %s", " ".join(cmd))
    return subprocess.run(cmd, cwd=cwd or REPO_ROOT, text=True, check=check, env=env)


def _principal(az: str) -> tuple[str, str]:
    account = json.loads(
        subprocess.check_output([az, "account", "show", "-o", "json"], text=True)
    )
    user = account.get("user", {})
    if user.get("type") == "servicePrincipal":
        sp = json.loads(
            subprocess.check_output(
                [az, "ad", "sp", "show", "--id", user.get("name", ""), "-o", "json"],
                text=True,
            )
        )
        return sp["id"], "ServicePrincipal"
    oid = subprocess.check_output(
        [az, "ad", "signed-in-user", "show", "--query", "id", "-o", "tsv"],
        text=True,
    ).strip()
    if not oid:
        raise SystemExit("Could not resolve signed-in user object ID.")
    return oid, "User"


def _budget_start() -> str:
    today = datetime.now(timezone.utc).date()
    return date(today.year, today.month, 1).isoformat()


def main() -> int:
    _load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Terraform twin of shared-eastus.bicep")
    ap.add_argument("--subscription-id", default=os.environ.get("AZURE_SUBSCRIPTION_ID", DEFAULT_SUBSCRIPTION))
    ap.add_argument(
        "--owner-email",
        default=os.environ.get("OWNER_EMAIL") or os.environ.get("CLASS_OWNER_EMAIL", ""),
    )
    ap.add_argument(
        "--name-hash",
        default=os.environ.get("TF_NAME_HASH", DEFAULT_LIVE_HASH),
        help=f"Match Bicep uniqueString slice (default {DEFAULT_LIVE_HASH} for live estate)",
    )
    ap.add_argument("--plan-only", action="store_true", help="terraform plan only (no apply)")
    ap.add_argument("--auto-approve", action="store_true", help="terraform apply -auto-approve")
    args = ap.parse_args()

    if not args.owner_email:
        raise SystemExit("Set OWNER_EMAIL / CLASS_OWNER_EMAIL in .env or pass --owner-email")

    az = _find_az()
    tf = _find_terraform()
    principal_id, principal_type = _principal(az)

    _run([az, "account", "set", "--subscription", args.subscription_id])

    if not TF_DIR.is_dir():
        raise SystemExit(f"Missing Terraform dir: {TF_DIR}")

    env = os.environ.copy()
    # Avoid interactive Azure login prompts when using az CLI token
    env.setdefault("ARM_USE_CLI", "true")

    _run([tf, "init", "-input=false"], cwd=TF_DIR, env=env)

    var_args = [
        f"-var=subscription_id={args.subscription_id}",
        f"-var=owner_email={args.owner_email}",
        f"-var=principal_object_id={principal_id}",
        f"-var=principal_type={principal_type}",
        f"-var=resource_group_name={SHARED_RG}",
        f"-var=name_hash={args.name_hash}",
        f"-var=budget_start_date={_budget_start()}",
        "-var=deploy_budget=false",
        "-var=location=eastus",
        "-var=learner=shared",
    ]

    if args.plan_only:
        _run([tf, "plan", "-input=false", *var_args], cwd=TF_DIR, env=env)
        print()
        print("Plan only — no changes applied. Re-run without --plan-only to apply.")
        return 0

    apply = [tf, "apply", "-input=false", *var_args]
    if args.auto_approve:
        apply.append("-auto-approve")
    _run(apply, cwd=TF_DIR, env=env)

    out = subprocess.check_output([tf, "output", "-json"], cwd=TF_DIR, text=True)
    outputs = {k: v.get("value") for k, v in json.loads(out).items()}

    print()
    print("=" * 70)
    print("SHARED ESTATE — TERRAFORM APPLY COMPLETE")
    print("=" * 70)
    print(f"  RG            : {outputs.get('resourceGroupName')}")
    print(f"  name_hash     : {outputs.get('nameHash')}  (Bicep twin uses same when set)")
    print(f"  Storage       : {outputs.get('storageAccountName')}")
    print(f"  Key Vault     : {outputs.get('keyVaultName')}")
    print(f"  ADF           : {outputs.get('dataFactoryName')}")
    print(f"  Databricks    : {outputs.get('databricksWorkspaceName')}")
    print(f"  Managed RG    : {outputs.get('databricksManagedResourceGroup')}")
    print()
    print("  Bicep twin   : provision-shared.cmd  → infra/shared-eastus.bicep")
    print("  Compare docs : docs/BICEP-TERRAFORM-SHARED-ESTATE.md")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
