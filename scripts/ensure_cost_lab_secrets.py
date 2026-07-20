#!/usr/bin/env python3
"""Push Azure SPN + subscription secrets into the `finledger` scope.

Lets the cost-management notebook call Azure Cost Management REST live.
Idempotent: PUT overwrites the same keys every run.

Auth: same as phase 2 — valid PAT, else Azure AD via az login; shared host auto-corrected.

Re-run: python scripts\\ensure_cost_lab_secrets.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from deploy_shared_lab import _load_env, _resolve_host, _resolve_token  # noqa: E402

SCOPE = "finledger"
KEY_MAP = {
    "azure-tenant-id": "AZURE_TENANT_ID",
    "azure-client-id": "AZURE_CLIENT_ID",
    "azure-client-secret": "AZURE_CLIENT_SECRET",
    "azure-subscription-id": "AZURE_SUBSCRIPTION_ID",
}

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    file_env = _load_env()
    host = _resolve_host(file_env)
    token, auth_src = _resolve_token(file_env, host)
    logger.info("Databricks host: %s (auth=%s)", host, auth_src)

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
