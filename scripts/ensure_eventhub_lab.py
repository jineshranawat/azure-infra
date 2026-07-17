#!/usr/bin/env python3
"""Idempotent FinLedger Event Hub for advanced PySpark lab (cheapest Basic SKU).

Creates (if missing):
  RG rg-shared-class1 / eastus (shared estate exception — same as ADF lab)
  Namespace: ehns-shared-finledger
  Hub:       finledger-payments
  Auth rule: lab-sendlisten (Send + Listen)

Puts Databricks secret scope finledger keys:
  eventhub-connection-string
  eventhub-name
  eventhub-namespace

Re-run safe. Cost: Basic namespace, 1 TU — warn if create needed.

Usage:
  python scripts/ensure_eventhub_lab.py
"""

from __future__ import annotations

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
LOCATION = "eastus"  # shared class estate (documented exception vs UK)
EH_NS = "ehns-shared-finledger"
EH_HUB = "finledger-payments"
EH_RULE = "lab-sendlisten"
WORKSPACE = "dbw-shared-qgr7mj"
SCOPE = "finledger"

logger = logging.getLogger(__name__)


def _az() -> str:
    az = shutil.which("az")
    if not az:
        raise SystemExit("az CLI required")
    return az


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def _load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    dotenv = REPO_ROOT / ".env"
    if dotenv.is_file():
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env


def _dbx_request(host: str, token: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{host.rstrip('/')}{path}",
        data=data,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Databricks API failed ({e.code}): {e.read().decode()[:500]}") from e


def _resolve_host(file_env: dict[str, str], az: str) -> str:
    host = (os.environ.get("DATABRICKS_HOST") or file_env.get("DATABRICKS_HOST", "")).strip().rstrip("/")
    if host:
        return host if host.startswith("http") else f"https://{host}"
    data = json.loads(
        _run([az, "databricks", "workspace", "show", "-g", SHARED_RG, "-n", WORKSPACE, "-o", "json"]).stdout
    )
    raw = data.get("workspaceUrl", "").strip()
    return f"https://{raw}" if raw and not raw.startswith("http") else raw


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
    az = _az()
    _run([az, "account", "set", "--subscription", SHARED_SUB])

    # Namespace
    show = _run(
        [az, "eventhubs", "namespace", "show", "-g", SHARED_RG, "-n", EH_NS, "-o", "json"],
        check=False,
    )
    if show.returncode != 0:
        logger.warning("COST: creating Event Hubs namespace %s (Basic / 1 TU) in %s", EH_NS, LOCATION)
        _run(
            [
                az,
                "eventhubs",
                "namespace",
                "create",
                "-g",
                SHARED_RG,
                "-n",
                EH_NS,
                "-l",
                LOCATION,
                "--sku",
                "Basic",
                "--capacity",
                "1",
                "-o",
                "none",
            ]
        )
        logger.info("Created namespace %s", EH_NS)
    else:
        logger.info("Namespace already present: %s", EH_NS)

    # Hub
    hub = _run(
        [az, "eventhubs", "eventhub", "show", "-g", SHARED_RG, "--namespace-name", EH_NS, "-n", EH_HUB, "-o", "json"],
        check=False,
    )
    if hub.returncode != 0:
        _run(
            [
                az,
                "eventhubs",
                "eventhub",
                "create",
                "-g",
                SHARED_RG,
                "--namespace-name",
                EH_NS,
                "-n",
                EH_HUB,
                "--partition-count",
                "2",
                "--cleanup-policy",
                "Delete",
                "--retention-time",
                "24",
                "-o",
                "none",
            ]
        )
        logger.info("Created hub %s", EH_HUB)
    else:
        logger.info("Hub already present: %s", EH_HUB)

    # Auth rule
    rule = _run(
        [
            az,
            "eventhubs",
            "eventhub",
            "authorization-rule",
            "show",
            "-g",
            SHARED_RG,
            "--namespace-name",
            EH_NS,
            "--eventhub-name",
            EH_HUB,
            "-n",
            EH_RULE,
            "-o",
            "json",
        ],
        check=False,
    )
    if rule.returncode != 0:
        _run(
            [
                az,
                "eventhubs",
                "eventhub",
                "authorization-rule",
                "create",
                "-g",
                SHARED_RG,
                "--namespace-name",
                EH_NS,
                "--eventhub-name",
                EH_HUB,
                "-n",
                EH_RULE,
                "--rights",
                "Send",
                "Listen",
                "-o",
                "none",
            ]
        )
        logger.info("Created auth rule %s", EH_RULE)
    else:
        logger.info("Auth rule already present: %s", EH_RULE)

    keys = json.loads(
        _run(
            [
                az,
                "eventhubs",
                "eventhub",
                "authorization-rule",
                "keys",
                "list",
                "-g",
                SHARED_RG,
                "--namespace-name",
                EH_NS,
                "--eventhub-name",
                EH_HUB,
                "--name",
                EH_RULE,
                "-o",
                "json",
            ]
        ).stdout
    )
    conn = (keys.get("primaryConnectionString") or "").strip()
    if not conn:
        raise SystemExit("Empty Event Hub connection string")

    file_env = _load_env()
    token = (os.environ.get("DATABRICKS_TOKEN") or file_env.get("DATABRICKS_TOKEN", "")).strip()
    if not token:
        raise SystemExit("DATABRICKS_TOKEN required to store secrets")
    host = _resolve_host(file_env, az)

    scopes = _dbx_request(host, token, "GET", "/api/2.0/secrets/scopes/list").get("scopes", [])
    if SCOPE not in [s.get("name") for s in scopes]:
        _dbx_request(host, token, "POST", "/api/2.0/secrets/scopes/create", {"scope": SCOPE})
    for key, value in (
        ("eventhub-connection-string", conn),
        ("eventhub-name", EH_HUB),
        ("eventhub-namespace", EH_NS),
    ):
        _dbx_request(
            host,
            token,
            "POST",
            "/api/2.0/secrets/put",
            {"scope": SCOPE, "key": key, "string_value": value},
        )
        logger.info("Secret stored: %s/%s", SCOPE, key)

    print()
    print("=" * 70)
    print("EVENT HUB LAB READY")
    print("=" * 70)
    print(f"  Namespace : {EH_NS}")
    print(f"  Hub       : {EH_HUB}")
    print(f"  Region    : {LOCATION} (shared estate)")
    print(f"  Secrets   : {SCOPE}/eventhub-connection-string, eventhub-name, eventhub-namespace")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
