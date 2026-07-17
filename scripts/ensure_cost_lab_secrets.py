#!/usr/bin/env python3
"""Push Azure SPN + subscription secrets into the `finledger` scope.

Lets the cost-management notebook call Azure Cost Management REST live.
Idempotent: PUT overwrites the same keys every run.

Re-run: python scripts\\ensure_cost_lab_secrets.py
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCOPE = "finledger"

KEY_MAP = {
    "azure-tenant-id": "AZURE_TENANT_ID",
    "azure-client-id": "AZURE_CLIENT_ID",
    "azure-client-secret": "AZURE_CLIENT_SECRET",
    "azure-subscription-id": "AZURE_SUBSCRIPTION_ID",
}


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


def main() -> int:
    file_env = _load_env()
    host = (os.environ.get("DATABRICKS_HOST") or file_env.get("DATABRICKS_HOST", "")).strip().rstrip("/")
    token = (os.environ.get("DATABRICKS_TOKEN") or file_env.get("DATABRICKS_TOKEN", "")).strip()
    if not host or not token:
        raise SystemExit("DATABRICKS_HOST and DATABRICKS_TOKEN required (.env)")
    if not host.startswith("http"):
        host = "https://" + host

    ok = 0
    for secret_key, env_key in KEY_MAP.items():
        value = (os.environ.get(env_key) or file_env.get(env_key, "")).strip()
        if not value:
            print(f"SKIP {secret_key} — {env_key} not in .env")
            continue
        body = json.dumps({"scope": SCOPE, "key": secret_key, "string_value": value}).encode()
        req = urllib.request.Request(
            f"{host}/api/2.0/secrets/put",
            data=body,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60):
                pass
            ok += 1
            print(f"OK   {SCOPE}/{secret_key}")
        except urllib.error.HTTPError as e:
            print(f"FAIL {secret_key}: {e.code} {e.read().decode()[:200]}")

    print(f"\n{ok}/{len(KEY_MAP)} secrets ensured in scope '{SCOPE}'")
    return 0 if ok == len(KEY_MAP) else 1


if __name__ == "__main__":
    raise SystemExit(main())
