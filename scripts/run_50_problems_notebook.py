#!/usr/bin/env python3
"""Submit 50_data_engineering_problems notebook to cluster and poll until done."""
from __future__ import annotations

import json
import sys
import time
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CLUSTER_ID = "0703-105931-31juyffm"
NOTEBOOK_PATH = "/Shared/day9/50_data_engineering_problems"


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in (REPO_ROOT / ".env").read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def api(host: str, token: str, path: str, body: dict | None = None, method: str = "POST") -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{host}{path}",
        data=data,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method=method,
    )
    return json.loads(urllib.request.urlopen(req, timeout=120).read().decode())


def main() -> int:
    env = load_env()
    host = env["DATABRICKS_HOST"].rstrip("/")
    token = env["DATABRICKS_TOKEN"]

    st = api(host, token, f"/api/2.0/clusters/get?cluster_id={CLUSTER_ID}", None, "GET")
    if st.get("state") != "RUNNING":
        api(host, token, "/api/2.0/clusters/start", {"cluster_id": CLUSTER_ID})
        for i in range(40):
            st = api(host, token, f"/api/2.0/clusters/get?cluster_id={CLUSTER_ID}", None, "GET")
            print(f"cluster poll {i}: {st.get('state')}")
            if st.get("state") == "RUNNING":
                break
            time.sleep(15)

    submit = api(
        host,
        token,
        "/api/2.0/jobs/runs/submit",
        {
            "run_name": "50-problems-full-run",
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {"notebook_path": NOTEBOOK_PATH, "timeout_seconds": 7200},
        },
    )
    run_id = submit["run_id"]
    print("run_id:", run_id)

    final: dict | None = None
    for i in range(180):
        st = api(host, token, f"/api/2.0/jobs/runs/get?run_id={run_id}", None, "GET")
        life = st["state"]["life_cycle_state"]
        res = st["state"].get("result_state")
        if i % 5 == 0:
            secs = st.get("execution_duration", 0) // 1000
            print(f"poll {i}: {life} / {res} ({secs}s)")
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            final = st
            break
        time.sleep(20)

    out = api(host, token, f"/api/2.0/jobs/runs/get-output?run_id={run_id}", None, "GET")
    print("RESULT:", final["state"].get("result_state") if final else "TIMEOUT")
    if final:
        print("Run page:", final.get("run_page_url", ""))
        print("Execution seconds:", final.get("execution_duration", 0) // 1000)
    err = out.get("error") or ""
    if err:
        print("ERROR:", err[:3000])
    else:
        nb = out.get("notebook_output") or {}
        if nb.get("result"):
            print("Notebook output (truncated):", str(nb.get("result"))[:500])
    return 0 if final and final["state"].get("result_state") == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
