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
        return host

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
    return key


def _dbx_env(host: str) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABRICKS_HOST"] = host
    file_env = _load_dotenv(ENV_FILE)
    token = (os.environ.get("DATABRICKS_TOKEN") or file_env.get("DATABRICKS_TOKEN", "")).strip()
    if token:
        env["DATABRICKS_TOKEN"] = token
    return env


def _run_dbx(host: str, args: list[str], *, input_text: str | None = None) -> subprocess.CompletedProcess:
    cli = resolve_databricks_cli()
    cmd = [cli, *args]
    logger.debug("Running: %s", " ".join(cmd))
    return subprocess.run(
        cmd,
        env=_dbx_env(host),
        input=input_text.encode() if input_text else None,
        text=input_text is None,
        capture_output=True,
        timeout=120,
    )


def _scope_exists(host: str) -> bool:
    result = _run_dbx(host, ["secrets", "list-scopes"])
    if result.returncode != 0:
        return False
    return SCOPE in result.stdout


def _ensure_scope(host: str) -> None:
    if _scope_exists(host):
        logger.info("Secret scope '%s' already present — skip create", SCOPE)
        return
    # New Databricks CLI requires --scope; legacy CLI used a positional name.
    for args in (
        ["secrets", "create-scope", "--scope", SCOPE],
        ["secrets", "create-scope", SCOPE],
    ):
        result = _run_dbx(host, args)
        if result.returncode == 0:
            logger.info("Created secret scope '%s'", SCOPE)
            return
        err = (result.stderr or result.stdout or "").lower()
        if "already exists" in err or "resource already exists" in err:
            logger.info("Secret scope '%s' already exists", SCOPE)
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
    )
    if result.returncode == 0:
        logger.info("Granted READ on '%s' to principal 'users'", SCOPE)
        return
    err = (result.stderr or result.stdout or "").lower()
    if "already exists" in err:
        logger.info("READ ACL for 'users' already on '%s'", SCOPE)
        return
    logger.warning("put-acl returned: %s", result.stderr or result.stdout)


def _put_secret(host: str, key: str, value: str) -> None:
    # Prefer --string-value (non-interactive). Fall back to stdin for older CLI.
    result = _run_dbx(
        host,
        ["secrets", "put", "--scope", SCOPE, "--key", key, "--string-value", value],
    )
    if result.returncode == 0:
        logger.info("Stored secret %s/%s", SCOPE, key)
        return

    result = _run_dbx(
        host,
        ["secrets", "put", "--scope", SCOPE, "--key", key],
        input_text=value,
    )
    if result.returncode == 0:
        logger.info("Stored secret %s/%s (stdin)", SCOPE, key)
        return

    raise SystemExit(
        f"Failed to store secret {SCOPE}/{key}:\n{result.stderr or result.stdout}\n"
        "Ensure DATABRICKS_TOKEN in .env or run: databricks auth login --host " + host
    )


def _verify_secrets_list(host: str) -> None:
    result = _run_dbx(host, ["secrets", "list", "--scope", SCOPE])
    if result.returncode != 0:
        raise SystemExit(f"Cannot list secrets: {result.stderr or result.stdout}")
    for name in (ACCOUNT_SECRET, KEY_SECRET):
        if name not in result.stdout:
            raise SystemExit(f"Secret {name} missing after setup")
    logger.info("Verified secrets in scope '%s': %s, %s", SCOPE, ACCOUNT_SECRET, KEY_SECRET)


def setup_finledger_secrets(cfg: SessionConfig, storage_account: str, workspace_name: str) -> str:
    """Idempotent: scope + ACL + storage-account + storage-key. Returns Databricks host."""
    host = resolve_databricks_host(cfg, workspace_name)
    storage_key = resolve_storage_key()

    logger.info("Databricks host: %s", host)
    logger.info("Storage account: %s", storage_account)

    # Preflight — list-scopes proves auth works
    probe = _run_dbx(host, ["secrets", "list-scopes"])
    if probe.returncode != 0:
        raise SystemExit(
            "Databricks CLI auth failed.\n"
            f"  1) Add DATABRICKS_TOKEN to .env (User settings -> Developer -> Access token)\n"
            f"  2) Or run: databricks auth login --host {host}\n"
            f"Error: {probe.stderr or probe.stdout}"
        )

    _ensure_scope(host)
    _ensure_read_acl(host)
    _put_secret(host, ACCOUNT_SECRET, storage_account)
    _put_secret(host, KEY_SECRET, storage_key)
    _verify_secrets_list(host)

    logger.info("")
    logger.info("Databricks secrets ready — notebooks can use auth_mode=auto")
    logger.info("  Scope : %s", SCOPE)
    logger.info("  Keys  : %s, %s", ACCOUNT_SECRET, KEY_SECRET)
    logger.info("  Open  : %s", host)
    return host


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
    from _config import load_config  # noqa: E402
    from discover import discover_estate  # noqa: E402

    cfg = load_config()
    estate = discover_estate(cfg)
    setup_finledger_secrets(cfg, estate.storage_account, estate.databricks_workspace)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
