"""Create FinLedger Databricks secret scope + secrets via CLI (not dbutils.secrets.put).

Notebooks on Shared clusters often cannot call dbutils.secrets.put — this script runs on
your Windows laptop with the Databricks CLI instead.

Requires in repo-root .env (never commit real values):
  DATABRICKS_HOST=https://adb-....azuredatabricks.net   (optional — auto from Azure)
  DATABRICKS_TOKEN=<PAT>                               (or run: databricks auth login)
  STORAGE_ACCOUNT_KEY=<storage key1 from portal>
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from _config import ENV_FILE, SessionConfig, _load_dotenv, find_az

logger = logging.getLogger(__name__)

SCOPE = "finledger"
ACCOUNT_SECRET = "storage-account"
KEY_SECRET = "storage-key"
_AZ_TIMEOUT = 45
_SETUP_STEPS = 11

_verbose_detail = False
_step_counter = 0


def set_verbose_detail(enabled: bool = True) -> None:
    """Enable step-by-step CLI logging (used automatically by --setup-secrets)."""
    global _verbose_detail
    _verbose_detail = enabled


def _step(title: str) -> None:
    global _step_counter
    _step_counter += 1
    logger.info("")
    logger.info("=" * 64)
    logger.info("STEP %d/%d — %s", _step_counter, _SETUP_STEPS, title)
    logger.info("=" * 64)


def _mask_cmd_display(cmd: list[str]) -> str:
    """Show CLI command learners can recognise; never print secret values."""
    parts: list[str] = []
    skip_next = False
    for i, part in enumerate(cmd):
        if skip_next:
            skip_next = False
            parts.append("<hidden — value from .env>")
            continue
        if part == "--string-value":
            parts.append(part)
            skip_next = True
            continue
        parts.append(part)
    return " ".join(parts)


def _log_cli_block(cmd: list[str], result: subprocess.CompletedProcess, *, note: str = "") -> None:
    if not _verbose_detail:
        return
    logger.info("Command : %s", _mask_cmd_display(cmd))
    if note:
        logger.info("Note    : %s", note)
    logger.info("Exit    : %d", result.returncode)
    out = (result.stdout or "").strip()
    err = (result.stderr or "").strip()
    if out:
        logger.info("Output  :\n%s", out)
    elif result.returncode == 0:
        logger.info("Output  : (empty — success)")
    if err:
        # urllib3 deprecation warnings land on stderr — still show them
        logger.info("Stderr  :\n%s", err)


def _find_databricks() -> str:
    dbx = shutil.which("databricks")
    if dbx:
        return dbx
    raise SystemExit(
        "Databricks CLI not found. Re-run: session-3\\orchestrate.cmd --setup-secrets\n"
        "(installs databricks-cli into the repo venv)"
    )


def _venv_databricks() -> str | None:
    """Databricks CLI installed into repo .venv/Scripts by orchestrate."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    candidate = repo_root / ".venv" / "Scripts" / "databricks.exe"
    return str(candidate) if candidate.is_file() else None


def resolve_databricks_cli() -> str:
    venv = _venv_databricks()
    if venv:
        return venv
    return _find_databricks()


def _az_json(args: list[str]) -> dict:
    result = subprocess.run(
        [find_az(), *args, "-o", "json"],
        check=True,
        text=True,
        capture_output=True,
        timeout=_AZ_TIMEOUT,
    )
    return json.loads(result.stdout)


def resolve_databricks_host(cfg: SessionConfig, workspace_name: str) -> str:
    env = {**os.environ, **_load_dotenv(ENV_FILE)}
    host = (env.get("DATABRICKS_HOST") or "").strip().rstrip("/")
    if host:
        if not host.startswith("http"):
            host = f"https://{host}"
        if _verbose_detail:
            logger.info("DATABRICKS_HOST found in .env → %s", host)
        return host

    if _verbose_detail:
        logger.info(
            "DATABRICKS_HOST not in .env — querying Azure CLI:\n"
            "  az databricks workspace show -g %s -n %s",
            cfg.resource_group,
            workspace_name,
        )
    data = _az_json(
        [
            "databricks",
            "workspace",
            "show",
            "--resource-group",
            cfg.resource_group,
            "--name",
            workspace_name,
        ]
    )
    raw = (data.get("workspaceUrl") or "").strip().rstrip("/")
    if not raw:
        raise SystemExit(
            "Could not resolve Databricks host. Add to .env:\n"
            "  DATABRICKS_HOST=https://adb-....azuredatabricks.net"
        )
    if not raw.startswith("http"):
        raw = f"https://{raw}"
    if _verbose_detail:
        logger.info("Azure returned workspace URL → %s", raw)
    return raw


def resolve_storage_key() -> str:
    env = {**os.environ, **_load_dotenv(ENV_FILE)}
    key = (env.get("STORAGE_ACCOUNT_KEY") or "").strip()
    if not key:
        raise SystemExit(
            "STORAGE_ACCOUNT_KEY missing in .env (repo root).\n"
            "Portal -> Storage account -> Security + networking -> Access keys -> key1\n"
            "Add line: STORAGE_ACCOUNT_KEY=...  (never commit .env to git)"
        )
    if _verbose_detail:
        logger.info("STORAGE_ACCOUNT_KEY found in .env (%d characters — not printed)", len(key))
    return key


def _dbx_env(host: str) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABRICKS_HOST"] = host
    file_env = _load_dotenv(ENV_FILE)
    token = (os.environ.get("DATABRICKS_TOKEN") or file_env.get("DATABRICKS_TOKEN", "")).strip()
    if token:
        env["DATABRICKS_TOKEN"] = token
    return env


def _run_dbx(
    host: str,
    args: list[str],
    *,
    input_text: str | None = None,
    note: str = "",
) -> subprocess.CompletedProcess:
    cli = resolve_databricks_cli()
    cmd = [cli, *args]
    if _verbose_detail:
        logger.info("Env     : DATABRICKS_HOST=%s", host)
        logger.info("Env     : DATABRICKS_TOKEN=<set from .env — not printed>")
    result = subprocess.run(
        cmd,
        env=_dbx_env(host),
        input=input_text.encode() if input_text else None,
        text=input_text is None,
        capture_output=True,
        timeout=120,
    )
    _log_cli_block(cmd, result, note=note)
    return result


def _list_scope_acls(host: str, label: str = "") -> subprocess.CompletedProcess:
    note = label or f"Lists who can READ/WRITE scope '{SCOPE}'"
    return _run_dbx(host, ["secrets", "list-acls", "--scope", SCOPE], note=note)


def _list_all_scopes(host: str, label: str = "") -> subprocess.CompletedProcess:
    note = label or "Lists every secret scope in the workspace (Backend = DATABRICKS or AZURE_KEYVAULT)"
    return _run_dbx(host, ["secrets", "list-scopes"], note=note)


def _list_scope_secrets(host: str, label: str = "") -> subprocess.CompletedProcess:
    note = label or f"Lists key names in scope '{SCOPE}' (values are never shown by CLI)"
    return _run_dbx(host, ["secrets", "list", "--scope", SCOPE], note=note)


def _scope_exists(host: str, probe: subprocess.CompletedProcess | None = None) -> bool:
    result = probe if probe is not None else _list_all_scopes(host)
    if result.returncode != 0:
        return False
    return SCOPE in result.stdout


def _ensure_scope(host: str) -> None:
    if _scope_exists(host):
        logger.info("Secret scope '%s' already present — skip create", SCOPE)
        _list_all_scopes(host, label="Confirm finledger scope visible after skip-create")
        _list_scope_secrets(host, label="Keys currently in finledger (before put steps)")
        return
    # New Databricks CLI requires --scope; legacy CLI used a positional name.
    for args in (
        ["secrets", "create-scope", "--scope", SCOPE],
        ["secrets", "create-scope", SCOPE],
    ):
        result = _run_dbx(
            host,
            args,
            note=f"Creates empty secret scope '{SCOPE}' — backend DATABRICKS (not Key Vault)",
        )
        if result.returncode == 0:
            logger.info("Created secret scope '%s'", SCOPE)
            _list_all_scopes(host, label="AFTER create-scope — finledger should appear with Backend DATABRICKS")
            _list_scope_secrets(host, label="AFTER create-scope — scope is empty until put steps")
            return
        err = (result.stderr or result.stdout or "").lower()
        if "already exists" in err or "resource already exists" in err:
            logger.info("Secret scope '%s' already exists", SCOPE)
            _list_all_scopes(host, label="AFTER already-exists — confirm finledger in list-scopes")
            return
        if "missing option '--scope'" in err:
            continue
        raise SystemExit(
            f"Failed to create scope '{SCOPE}':\n{result.stderr or result.stdout}\n"
            "Auth fix: databricks auth login --host " + host
        )
    raise SystemExit(
        f"Failed to create scope '{SCOPE}' (tried --scope and positional forms).\n"
        "Auth fix: databricks auth login --host " + host
    )


def _ensure_read_acl(host: str) -> None:
    result = _run_dbx(
        host,
        ["secrets", "put-acl", "--scope", SCOPE, "--principal", "users", "--permission", "READ"],
        note="Allows all workspace users to READ secrets (not write)",
    )
    if result.returncode == 0:
        logger.info("Granted READ on '%s' to principal 'users'", SCOPE)
    else:
        err = (result.stderr or result.stdout or "").lower()
        if "already exists" in err:
            logger.info("READ ACL for 'users' already on '%s'", SCOPE)
        else:
            logger.warning("put-acl returned: %s", result.stderr or result.stdout)
    _list_scope_acls(host, label="AFTER put-acl — users should have READ on finledger")
    _list_all_scopes(host, label="AFTER put-acl — list-scopes refresh")


def _put_secret(host: str, key: str, value: str) -> None:
    display = value if key == ACCOUNT_SECRET else f"<hidden — {len(value)} chars from .env>"
    # Prefer --string-value (non-interactive). Fall back to stdin for older CLI.
    result = _run_dbx(
        host,
        ["secrets", "put", "--scope", SCOPE, "--key", key, "--string-value", value],
        note=f"Stores {SCOPE}/{key} = {display}",
    )
    if result.returncode == 0:
        logger.info("Stored secret %s/%s", SCOPE, key)
        _list_scope_secrets(host, label=f"AFTER put {key} — confirm key name appears (value hidden)")
        return

    result = _run_dbx(
        host,
        ["secrets", "put", "--scope", SCOPE, "--key", key],
        input_text=value,
        note=f"Fallback: stdin put for {SCOPE}/{key} = {display}",
    )
    if result.returncode == 0:
        logger.info("Stored secret %s/%s (stdin)", SCOPE, key)
        _list_scope_secrets(host, label=f"AFTER put {key} (stdin) — confirm key name appears")
        return

    raise SystemExit(
        f"Failed to store secret {SCOPE}/{key}:\n{result.stderr or result.stdout}\n"
        "Ensure DATABRICKS_TOKEN in .env or run: databricks auth login --host " + host
    )


def _verify_secrets_list(host: str) -> None:
    _list_all_scopes(host, label="FINAL list-scopes — finledger Backend should be DATABRICKS")
    result = _list_scope_secrets(host, label="FINAL list --scope finledger — both keys must appear")
    _list_scope_acls(host, label="FINAL list-acls — who can read finledger")
    if result.returncode != 0:
        raise SystemExit(f"Cannot list secrets: {result.stderr or result.stdout}")
    for name in (ACCOUNT_SECRET, KEY_SECRET):
        if name not in result.stdout:
            raise SystemExit(f"Secret {name} missing after setup")
    logger.info("Verified secrets in scope '%s': %s, %s", SCOPE, ACCOUNT_SECRET, KEY_SECRET)


def _log_prerequisites(cfg: SessionConfig, storage_account: str, workspace_name: str) -> None:
    file_env = _load_dotenv(ENV_FILE)
    has_host = bool((file_env.get("DATABRICKS_HOST") or "").strip())
    has_token = bool((file_env.get("DATABRICKS_TOKEN") or "").strip())
    has_key = bool((file_env.get("STORAGE_ACCOUNT_KEY") or "").strip())
    cli = resolve_databricks_cli()

    logger.info("Prerequisites check (repo-root .env):")
    logger.info("  .env file        : %s", ENV_FILE)
    logger.info("  LEARNER          : %s", cfg.learner)
    logger.info("  Resource group   : %s", cfg.resource_group)
    logger.info("  Storage account  : %s", storage_account)
    logger.info("  Databricks ws    : %s", workspace_name)
    logger.info("  DATABRICKS_HOST  : %s", "set" if has_host else "missing (will use Azure CLI)")
    logger.info("  DATABRICKS_TOKEN : %s", "set" if has_token else "MISSING — add PAT to .env")
    logger.info("  STORAGE_ACCOUNT_KEY : %s", "set" if has_key else "MISSING — paste key1 from portal")
    logger.info("  Databricks CLI   : %s", cli)
    if not has_token:
        raise SystemExit(
            "DATABRICKS_TOKEN missing in .env.\n"
            "Databricks → User settings → Developer → Access tokens → Generate new token"
        )


def _log_keyvault_vs_databricks(key_vault: str = "") -> None:
    """Clarify training lab uses Databricks-backed scope, not Azure Key Vault-backed scope."""
    logger.info("Secret storage model for this lab:")
    logger.info("  Scope name    : %s", SCOPE)
    logger.info("  Backend       : DATABRICKS (Databricks control plane — NOT Azure Key Vault)")
    logger.info("  Key Vault URL : N/A — see list-scopes output column 'KeyVault URL'")
    if key_vault:
        logger.info(
            "  Azure KV in RG: %s — deployed for Class-1 / ADF labs; separate from scope finledger",
            key_vault,
        )
    else:
        logger.info("  Azure KV in RG: (not discovered — optional in shared estate)")
    logger.info("")
    logger.info("  We are NOT integrating finledger with Key Vault in this lab.")
    logger.info("  Flow: .env key1 → CLI put → Databricks scope → dbutils.secrets.get in notebook")
    logger.info("")
    logger.info("  Production upgrade (later): create-scope with --scope-backend-type AZURE_KEYVAULT")
    logger.info("  ADF pattern (Session 2): Key Vault linked service — different path, same idea")


def setup_finledger_secrets(
    cfg: SessionConfig,
    storage_account: str,
    workspace_name: str,
    *,
    verbose: bool = False,
    key_vault: str = "",
) -> str:
    """Idempotent: scope + ACL + storage-account + storage-key. Returns Databricks host."""
    global _step_counter
    _step_counter = 0
    set_verbose_detail(verbose)

    if verbose:
        logger.info("")
        logger.info("#" * 64)
        logger.info("#  FINLEDGER SECRET SETUP — detailed trace")
        logger.info("#  Scope: finledger | Keys: storage-account, storage-key")
        logger.info("#" * 64)

    _step("Check .env prerequisites")
    _log_prerequisites(cfg, storage_account, workspace_name)

    _step("Resolve Databricks workspace URL")
    host = resolve_databricks_host(cfg, workspace_name)
    storage_key = resolve_storage_key()
    logger.info("Target host     : %s", host)
    logger.info("Storage account : %s (will store as secret storage-account)", storage_account)

    _step("Databricks scope vs Azure Key Vault — what this lab uses")
    _log_keyvault_vs_databricks(key_vault)

    _step("Auth test — databricks secrets list-scopes")
    probe = _list_all_scopes(host)
    if probe.returncode != 0:
        raise SystemExit(
            "Databricks CLI auth failed.\n"
            f"  1) Add DATABRICKS_TOKEN to .env (User settings -> Developer -> Access token)\n"
            f"  2) Or run: databricks auth login --host {host}\n"
            f"Error: {probe.stderr or probe.stdout}"
        )
    if SCOPE in probe.stdout:
        logger.info("Scope '%s' already in workspace — will update secrets in place", SCOPE)
    else:
        logger.info("Scope '%s' not found yet — will create in next step", SCOPE)

    _step("List secrets BEFORE changes — databricks secrets list --scope finledger")
    before = _list_scope_secrets(host, label="BEFORE — keys currently in finledger (empty if new scope)")
    if before.returncode != 0 and "does not exist" in (before.stderr or before.stdout or "").lower():
        logger.info("Scope '%s' does not exist yet — expected on first run", SCOPE)
    elif before.returncode == 0 and not (before.stdout or "").strip():
        logger.info("Scope exists but has no secrets yet")

    _step("Create scope — databricks secrets create-scope finledger")
    _ensure_scope(host)

    _step("Grant READ — databricks secrets put-acl + list-acls")
    _ensure_read_acl(host)

    _step("Put storage-account — databricks secrets put + list --scope finledger")
    _put_secret(host, ACCOUNT_SECRET, storage_account)

    _step("Put storage-key — databricks secrets put + list --scope finledger")
    _put_secret(host, KEY_SECRET, storage_key)

    _step("Final verify — list-scopes + list + list-acls (all three list commands)")
    _verify_secrets_list(host)

    _step("Summary — notebook usage")
    logger.info("")
    logger.info("Databricks secrets ready — notebooks load storage-account + storage-key from scope finledger")
    logger.info("  Scope : %s", SCOPE)
    logger.info("  Backend : DATABRICKS (not Azure Key Vault — training lab)")
    logger.info("  Keys  : %s, %s", ACCOUNT_SECRET, KEY_SECRET)
    logger.info("  Notebook test cell:")
    logger.info('    dbutils.secrets.get("finledger", "storage-key")')
    logger.info("  UC ADLS roots (bronze | silver | gold):")
    for layer in ("bronze", "silver", "gold"):
        logger.info(
            "    %s: abfss://%s@%s.dfs.core.windows.net/_unity_catalog",
            layer,
            layer,
            storage_account,
        )
    logger.info("  Open workspace: %s", host)
    return host


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
    from _config import load_config  # noqa: E402
    from discover import discover_estate  # noqa: E402

    set_verbose_detail(True)
    cfg = load_config()
    estate = discover_estate(cfg)
    setup_finledger_secrets(cfg, estate.storage_account, estate.databricks_workspace, verbose=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
