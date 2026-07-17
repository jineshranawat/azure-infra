#!/usr/bin/env python3
"""Grant Unity Catalog access to Databricks system tables (class lab).

Error you fix:
  PERMISSION_DENIED: User does not have BROWSE on Catalog 'system'

Requires a token whose principal is BOTH account admin AND metastore admin
(or already owns / can MANAGE catalog `system`).

Re-run (idempotent):
  python scripts\\grant_system_tables_access.py
  python scripts\\grant_system_tables_access.py --principal "account users"
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SHARED_RG = "rg-shared-class1"
WORKSPACE = "dbw-shared-qgr7mj"

# Schemas commonly present under catalog system (skip quietly if missing)
SYSTEM_SCHEMAS = [
    "access",
    "billing",
    "compute",
    "lakeflow",
    "query",
    "information_schema",
    "storage",
    "sharing",
    "marketplace",
    "mlflow",
    "serving",
    "ai_gateway",
    "data_classification",
    "data_quality_monitoring",
    "replication",
]

logger = logging.getLogger(__name__)


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


def _resolve_host(file_env: dict[str, str]) -> str:
    host = (os.environ.get("DATABRICKS_HOST") or file_env.get("DATABRICKS_HOST", "")).strip().rstrip("/")
    if host:
        return host if host.startswith("http") else f"https://{host}"
    import shutil

    az = shutil.which("az")
    data = json.loads(
        subprocess.check_output(
            [az, "databricks", "workspace", "show", "-g", SHARED_RG, "-n", WORKSPACE, "-o", "json"],
            text=True,
        )
    )
    raw = data.get("workspaceUrl", "").strip()
    return f"https://{raw}" if raw and not raw.startswith("http") else raw


def _request(host: str, token: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict | list | str]:
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
            if not raw:
                return resp.status, {}
            try:
                return resp.status, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = raw
        return e.code, payload


def _sql_statements(host: str, token: str, warehouse_id: str, sql: str) -> tuple[bool, str]:
    code, payload = _request(
        host,
        token,
        "POST",
        "/api/2.0/sql/statements",
        {
            "warehouse_id": warehouse_id,
            "statement": sql,
            "wait_timeout": "50s",
            "on_wait_timeout": "CANCEL",
        },
    )
    if code >= 400:
        return False, str(payload)[:400]
    if isinstance(payload, dict):
        status = (payload.get("status") or {}).get("state", "")
        if status == "SUCCEEDED":
            return True, "ok"
        err = (payload.get("status") or {}).get("error") or payload
        return False, str(err)[:400]
    return False, str(payload)[:400]


def _pick_warehouse(host: str, token: str) -> str | None:
    code, payload = _request(host, token, "GET", "/api/2.0/sql/warehouses")
    if code >= 400 or not isinstance(payload, dict):
        logger.warning("Cannot list SQL warehouses: %s", payload)
        return None
    warehouses = payload.get("warehouses") or []
    if not warehouses:
        logger.warning("No SQL warehouses in workspace — create a small serverless warehouse first")
        return None
    # Prefer running, else first
    for w in warehouses:
        if (w.get("state") or "").upper() == "RUNNING":
            return w["id"]
    return warehouses[0]["id"]


def _grant_sql_bundle(principal: str) -> list[str]:
    p = principal.replace("`", "")
    stmts = [
        f"GRANT USE CATALOG ON CATALOG `system` TO `{p}`",
        f"GRANT BROWSE ON CATALOG `system` TO `{p}`",
    ]
    for sch in SYSTEM_SCHEMAS:
        stmts.append(f"GRANT USE SCHEMA ON SCHEMA `system`.`{sch}` TO `{p}`")
        stmts.append(f"GRANT SELECT ON SCHEMA `system`.`{sch}` TO `{p}`")
    return stmts


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Grant system catalog access for class")
    ap.add_argument(
        "--principal",
        default="account users",
        help='UC principal (default: "account users"). Use your email to grant only yourself.',
    )
    args = ap.parse_args()

    file_env = _load_env()
    token = (os.environ.get("DATABRICKS_TOKEN") or file_env.get("DATABRICKS_TOKEN", "")).strip()
    if not token:
        raise SystemExit("DATABRICKS_TOKEN required in .env or environment")

    host = _resolve_host(file_env)
    logger.info("Host: %s", host)
    logger.info("Principal: %s", args.principal)

    # Who am I?
    code, me = _request(host, token, "GET", "/api/2.0/preview/scim/v2/Me")
    if code < 400 and isinstance(me, dict):
        logger.info("Token user: %s", me.get("userName") or me.get("displayName"))

    warehouse_id = _pick_warehouse(host, token)
    if not warehouse_id:
        print()
        print("FIX (portal):")
        print("  1. SQL → create a tiny Serverless SQL warehouse (2X-Small) and start it")
        print("  2. Re-run: python scripts\\grant_system_tables_access.py")
        print()
        print("OR run these as metastore admin + account admin in a SQL editor:")
        for s in _grant_sql_bundle(args.principal):
            print(f"  {s};")
        return 2

    logger.info("Using warehouse_id=%s", warehouse_id)

    ok_n = 0
    fail_n = 0
    for sql in _grant_sql_bundle(args.principal):
        ok, detail = _sql_statements(host, token, warehouse_id, sql)
        if ok:
            ok_n += 1
            logger.info("OK  %s", sql)
        else:
            fail_n += 1
            logger.warning("FAIL %s → %s", sql, detail)

    # Verify
    verify_sql = "SHOW SCHEMAS IN system"
    vok, vdetail = _sql_statements(host, token, warehouse_id, verify_sql)
    print()
    print("=" * 70)
    print("SYSTEM TABLES ACCESS GRANT")
    print("=" * 70)
    print(f"  grants_ok={ok_n}  grants_fail={fail_n}")
    print(f"  verify SHOW SCHEMAS IN system → {'OK' if vok else 'FAIL: ' + vdetail}")
    if fail_n and not vok:
        print()
        print("  Token principal is probably NOT metastore admin + account admin.")
        print("  Ask the account/metastore admin to run the GRANT statements above.")
        print("  Docs: https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/")
    print("=" * 70)
    return 0 if vok else 1


if __name__ == "__main__":
    raise SystemExit(main())
