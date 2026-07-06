"""Submit master notebook to a running Databricks cluster and poll until done."""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CLUSTER_ID = "0703-105931-31juyffm"
NOTEBOOK_PATH = "/Shared/day7-day8/master_pyspark_complete"


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
    try:
        return json.loads(urllib.request.urlopen(req, timeout=120).read().decode())
    except urllib.error.HTTPError as ex:
        return {"http_error": ex.code, "body": ex.read().decode()}


def main() -> int:
    env = load_env()
    host = env["DATABRICKS_HOST"].rstrip("/")
    token = env["DATABRICKS_TOKEN"]

    submit = api(
        host,
        token,
        "/api/2.0/jobs/runs/submit",
        {
            "run_name": "agent-full-notebook-run",
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {"notebook_path": NOTEBOOK_PATH},
        },
    )
    print("submit:", json.dumps(submit, indent=2))
    run_id = submit.get("run_id")
    if not run_id:
        return 1

    final: dict | None = None
    for i in range(120):
        st = api(host, token, f"/api/2.0/jobs/runs/get?run_id={run_id}", None, "GET")
        life = st.get("state", {}).get("life_cycle_state")
        result = st.get("state", {}).get("result_state")
        msg = st.get("state", {}).get("state_message", "")
        print(f"poll {i}: {life} / {result} — {msg[:200]}")
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            final = st
            break
        time.sleep(15)

    if not final:
        print("TIMEOUT waiting for run")
        return 1

    print("\n=== RUN STATE ===")
    print(json.dumps(final.get("state"), indent=2))

    out = api(host, token, f"/api/2.0/jobs/runs/get-output?run_id={run_id}", None, "GET")
    print("\n=== NOTEBOOK OUTPUT ===")
    nb = out.get("notebook_output") or {}
    if nb.get("result"):
        print(nb["result"][:8000])
    if nb.get("truncated"):
        print("... (truncated)")
    if out.get("error"):
        print("\n=== ERROR ===")
        print(out["error"][:8000])
    if out.get("metadata"):
        print("\n=== METADATA ===")
        print(json.dumps(out["metadata"], indent=2)[:4000])

    result_state = final.get("state", {}).get("result_state")
    return 0 if result_state == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
