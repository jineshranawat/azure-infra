"""Shared Databricks REST helpers for Genie lab (no SDK required)."""

from __future__ import annotations

import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
OUT.mkdir(parents=True, exist_ok=True)
REPO = ROOT.parents[1]
SHARED_HOST = "https://adb-7405613791235979.19.azuredatabricks.net"
WAREHOUSE_NAME_HINT = "Serverless Starter Warehouse"
MAX_TABLES = 30  # Genie Agent hard limit


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    path = REPO / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip().strip('"').strip("'")
            env[k.strip()] = v
    return env


def host_and_token() -> tuple[str, str]:
    env = load_env()
    host = (os.environ.get("DATABRICKS_HOST") or env.get("DATABRICKS_HOST") or SHARED_HOST).rstrip("/")
    if "adb-7405613791235979" not in host:
        host = SHARED_HOST
    token = (os.environ.get("DATABRICKS_TOKEN") or env.get("DATABRICKS_TOKEN") or "").strip()
    if not token:
        raise SystemExit("Set DATABRICKS_TOKEN in .env (or Azure AD via classroom scripts).")
    return host, token


def api(method: str, path: str, body: object | None = None, timeout: int = 120) -> tuple[int, object]:
    host, token = host_and_token()
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        host + path,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8") or "{}"
            return resp.status, json.loads(raw)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        try:
            parsed: object = json.loads(detail)
        except json.JSONDecodeError:
            parsed = detail
        return exc.code, parsed


def must_ok(status: int, payload: object, what: str) -> object:
    if status >= 400:
        raise SystemExit(f"{what} failed HTTP {status}: {payload}")
    return payload


def gen_id() -> str:
    """32-char lowercase hex id (Genie serialized_space requirement)."""
    t = int((datetime.now(timezone.utc) - datetime(1582, 10, 15, tzinfo=timezone.utc)).total_seconds() * 1e7)
    hi = (t & 0xFFFFFFFFFFFF0000) | (1 << 12) | ((t & 0xFFFF) >> 4)
    lo = random.getrandbits(62) | 0x8000000000000000
    return f"{hi:016x}{lo:016x}"


def write_json(name: str, payload: object) -> Path:
    path = OUT / name
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def current_user_home() -> str:
    status, me = api("GET", "/api/2.0/preview/scim/v2/Me")
    must_ok(status, me, "whoami")
    assert isinstance(me, dict)
    uname = me.get("userName") or "unknown"
    path = f"/Users/{uname}"
    st, _ = api("GET", "/api/2.0/workspace/get-status?path=" + urllib.parse.quote(path))
    if st == 200:
        return path
    # Prefer Shared for class demos
    return "/Shared"


def ensure_shared_folder(folder: str = "/Shared/genie-lab") -> str:
    st, _ = api("GET", "/api/2.0/workspace/get-status?path=" + urllib.parse.quote(folder))
    if st == 200:
        return folder
    status, payload = api(
        "POST",
        "/api/2.0/workspace/mkdirs",
        {"path": folder},
    )
    must_ok(status, payload, f"mkdirs {folder}")
    return folder


def list_warehouses() -> list[dict]:
    status, payload = api("GET", "/api/2.0/sql/warehouses")
    must_ok(status, payload, "list warehouses")
    assert isinstance(payload, dict)
    return list(payload.get("warehouses") or [])


def pick_warehouse() -> dict:
    whs = list_warehouses()
    if not whs:
        raise SystemExit("No SQL warehouse found. Create a serverless/pro warehouse first.")
    for w in whs:
        if WAREHOUSE_NAME_HINT.lower() in (w.get("name") or "").lower():
            return w
    return whs[0]


def start_warehouse(warehouse_id: str, wait_sec: int = 180) -> None:
    status, payload = api("POST", f"/api/2.0/sql/warehouses/{warehouse_id}/start", {})
    if status not in (200, 201) and status != 400:
        # 400 often means already starting/running
        must_ok(status, payload, "start warehouse")
    deadline = time.time() + wait_sec
    while time.time() < deadline:
        st, info = api("GET", f"/api/2.0/sql/warehouses/{warehouse_id}")
        must_ok(st, info, "warehouse status")
        assert isinstance(info, dict)
        state = info.get("state")
        print(f"  warehouse state: {state}")
        if state == "RUNNING":
            return
        time.sleep(5)
    raise SystemExit(f"Warehouse {warehouse_id} did not reach RUNNING in {wait_sec}s")


def list_tables(catalog: str, schema: str) -> list[dict]:
    q = urllib.parse.urlencode({"catalog_name": catalog, "schema_name": schema})
    status, payload = api("GET", f"/api/2.1/unity-catalog/tables?{q}")
    must_ok(status, payload, f"tables {catalog}.{schema}")
    assert isinstance(payload, dict)
    return list(payload.get("tables") or [])


def list_schemas(catalog: str) -> list[str]:
    q = urllib.parse.urlencode({"catalog_name": catalog})
    status, payload = api("GET", f"/api/2.1/unity-catalog/schemas?{q}")
    must_ok(status, payload, f"schemas {catalog}")
    assert isinstance(payload, dict)
    return [s["name"] for s in payload.get("schemas") or [] if s.get("name") != "information_schema"]


def inventory_class_catalog(catalog: str = "dbw_shared_qgr7mj") -> list[dict]:
    rows: list[dict] = []
    for schema in list_schemas(catalog):
        for t in list_tables(catalog, schema):
            cols = [c.get("name") for c in (t.get("columns") or []) if c.get("name")]
            rows.append(
                {
                    "identifier": f"{catalog}.{schema}.{t['name']}",
                    "catalog": catalog,
                    "schema": schema,
                    "name": t["name"],
                    "table_type": t.get("table_type"),
                    "columns": cols,
                }
            )
    rows.sort(key=lambda r: r["identifier"])
    return rows
