#!/usr/bin/env python3
"""Provision shared eastus estate with Terraform (twin of provision_shared.py / Bicep).

Same resources as infra/shared-eastus.bicep. Docs: docs/BICEP-TERRAFORM-SHARED-ESTATE.md

Usage:
  provision-shared-tf.cmd
  provision-shared-tf.cmd --plan-only
  provision-shared-tf.cmd --name-hash qgr7mj

Idempotency (guardrail 2): before apply, every resource that already exists in
Azure (e.g. created by the Bicep twin) but is missing from terraform.tfstate is
imported automatically — so `--auto-approve` is safe to re-run on the live
estate instead of failing with "already exists ... needs to be imported".
A destroy-gate then aborts if the plan would delete or recreate anything.
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


def _az_json(az: str, args: list[str]) -> object | None:
    """Run az and parse JSON; None when the resource does not exist."""
    proc = subprocess.run(
        [az, *args, "-o", "json"], text=True, capture_output=True
    )
    if proc.returncode != 0:
        return None
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None


def _state_addresses(tf: str, env: dict[str, str]) -> set[str]:
    proc = subprocess.run(
        [tf, "state", "list"], cwd=TF_DIR, text=True, capture_output=True, env=env
    )
    if proc.returncode != 0:  # no state file yet
        return set()
    return {line.strip() for line in proc.stdout.splitlines() if line.strip()}


def _role_assignment_id(az: str, scope: str, role: str, assignee: str) -> str | None:
    # az returns the scope lowercased (/resourcegroups/); rebuild the ID from our
    # proper-case scope + GUID or Terraform sees a scope diff that forces replacement.
    guid = _az_json(
        az,
        ["role", "assignment", "list", "--scope", scope, "--role", role,
         "--assignee", assignee, "--query", "[0].name"],
    )
    if not isinstance(guid, str) or not guid:
        return None
    return f"{scope}/providers/Microsoft.Authorization/roleAssignments/{guid}"


def _import_existing(
    tf: str,
    az: str,
    env: dict[str, str],
    var_args: list[str],
    subscription_id: str,
    name_hash: str,
    principal_id: str,
) -> None:
    """Check-before-create: import Azure resources that exist but are not in state.

    This is what makes re-running apply safe on an estate first built by Bicep.
    Each entry: (terraform address, azure resource id, az-cli existence probe).
    """
    rg_id = f"/subscriptions/{subscription_id}/resourceGroups/{SHARED_RG}"
    st_name = f"stshared{name_hash}"
    kv_name = f"kv-shared-{name_hash}"
    adf_name = f"adf-shared-{name_hash}"
    dbw_name = f"dbw-shared-{name_hash}"
    st_id = f"{rg_id}/providers/Microsoft.Storage/storageAccounts/{st_name}"
    kv_id = f"{rg_id}/providers/Microsoft.KeyVault/vaults/{kv_name}"
    adf_id = f"{rg_id}/providers/Microsoft.DataFactory/factories/{adf_name}"
    dbw_id = f"{rg_id}/providers/Microsoft.Databricks/workspaces/{dbw_name}"

    in_state = _state_addresses(tf, env)

    candidates: list[tuple[str, str]] = []

    def add(address: str, resource_id: str) -> None:
        if address not in in_state:
            candidates.append((address, resource_id))

    if _az_json(az, ["group", "show", "--name", SHARED_RG]) is not None:
        add("azurerm_resource_group.shared", rg_id)
    else:
        logger.info("RG %s not found in Azure — nothing to import (greenfield).", SHARED_RG)
        return

    if _az_json(az, ["resource", "show", "--ids", kv_id]) is not None:
        add("azurerm_key_vault.shared", kv_id)
        ra = _role_assignment_id(az, kv_id, "Key Vault Secrets Officer", principal_id)
        if ra:
            add("azurerm_role_assignment.kv_secrets_officer", ra)

    storage = _az_json(az, ["resource", "show", "--ids", st_id])
    if storage is not None:
        add("azurerm_storage_account.shared", st_id)
        add(
            "azurerm_storage_management_policy.lifecycle",
            f"{st_id}/managementPolicies/default",
        )
        for container in ("bronze", "silver", "gold", "audit"):
            # azurerm 3.x imports containers by data-plane URL
            add(
                f'azurerm_storage_container.medallion["{container}"]',
                f"https://{st_name}.blob.core.windows.net/{container}",
            )
        ra = _role_assignment_id(az, st_id, "Storage Blob Data Contributor", principal_id)
        if ra:
            add("azurerm_role_assignment.storage_blob_contributor", ra)

    adf = _az_json(az, ["resource", "show", "--ids", adf_id])
    if adf is not None:
        add("azurerm_data_factory.shared", adf_id)
        ls_id = f"{adf_id}/linkedservices/AdlsBronzeLinkedService"
        if _az_json(az, ["resource", "show", "--ids", ls_id]) is not None:
            add(
                "azurerm_data_factory_linked_service_data_lake_storage_gen2.bronze",
                ls_id,
            )
        adf_msi = (adf.get("identity") or {}).get("principalId") if isinstance(adf, dict) else None
        if adf_msi and storage is not None:
            ra = _role_assignment_id(az, st_id, "Storage Blob Data Contributor", adf_msi)
            if ra:
                add("azurerm_role_assignment.adf_storage", ra)

    if _az_json(az, ["resource", "show", "--ids", dbw_id]) is not None:
        add("azurerm_databricks_workspace.shared", dbw_id)

    if not candidates:
        logger.info("State already covers every existing resource — no imports needed.")
        return

    logger.info("Importing %d existing resource(s) into Terraform state...", len(candidates))
    for address, resource_id in candidates:
        proc = subprocess.run(
            [tf, "import", "-input=false", *var_args, address, resource_id],
            cwd=TF_DIR, text=True, capture_output=True, env=env,
        )
        if proc.returncode == 0:
            logger.info("  imported: %s", address)
        elif "Resource already managed" in (proc.stderr or ""):
            logger.info("  already present: %s", address)
        else:
            logger.warning("  import failed for %s: %s", address, (proc.stderr or "").strip()[:400])


def _plan_is_destructive(tf: str, env: dict[str, str], var_args: list[str]) -> bool:
    """Save a plan and refuse to continue if anything would be deleted/recreated."""
    plan_file = TF_DIR / "tfplan.autoguard"
    _run(
        [tf, "plan", "-input=false", f"-out={plan_file.name}", *var_args],
        cwd=TF_DIR, env=env,
    )
    show = subprocess.check_output(
        [tf, "show", "-json", plan_file.name], cwd=TF_DIR, text=True, env=env
    )
    plan = json.loads(show)
    destructive = [
        rc["address"]
        for rc in plan.get("resource_changes", [])
        if "delete" in (rc.get("change", {}).get("actions") or [])
    ]
    if destructive:
        print()
        print("ABORTED — the plan would DELETE or RECREATE these resources:")
        for address in destructive:
            print(f"  - {address}")
        print("On the shared class estate this is never done automatically.")
        print("Review with: provision-shared-tf.cmd --plan-only")
        return True
    return False


def main() -> int:
    # Windows consoles often default to cp1252 which cannot print arrows/dashes
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
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

    # Guardrail 2: adopt anything Bicep/portal already built before applying,
    # so re-runs converge instead of failing with "already exists".
    _import_existing(tf, az, env, var_args, args.subscription_id, args.name_hash, principal_id)

    if _plan_is_destructive(tf, env, var_args):
        return 1

    plan_file = TF_DIR / "tfplan.autoguard"
    try:
        if args.auto_approve:
            _run([tf, "apply", "-input=false", plan_file.name], cwd=TF_DIR, env=env)
        else:
            _run([tf, "apply", "-input=false", *var_args], cwd=TF_DIR, env=env)
    finally:
        plan_file.unlink(missing_ok=True)

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
