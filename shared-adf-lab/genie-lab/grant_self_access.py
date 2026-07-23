"""Grant USE SCHEMA + SELECT on class schemas to the Genie lab principal (idempotent)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import api, must_ok, pick_warehouse, start_warehouse


def main() -> int:
    st, me = api("GET", "/api/2.0/preview/scim/v2/Me")
    must_ok(st, me, "whoami")
    assert isinstance(me, dict)
    principal = me["userName"]
    print(f"Principal: {principal}")

    wh = pick_warehouse()
    start_warehouse(wh["id"])
    wid = wh["id"]

    schemas = ["advanced_lab", "cost_lab", "demo_problems", "default"]
    statements = [f"GRANT USE CATALOG ON CATALOG dbw_shared_qgr7mj TO `{principal}`"]
    for schema in schemas:
        statements.append(
            f"GRANT USE SCHEMA ON SCHEMA dbw_shared_qgr7mj.`{schema}` TO `{principal}`"
        )
        statements.append(
            f"GRANT SELECT ON SCHEMA dbw_shared_qgr7mj.`{schema}` TO `{principal}`"
        )

    for sql in statements:
        print(f"  {sql}")
        status, body = api(
            "POST",
            "/api/2.0/sql/statements",
            {"warehouse_id": wid, "statement": sql, "wait_timeout": "50s"},
        )
        assert isinstance(body, dict)
        state = (body.get("status") or {}).get("state")
        err = ((body.get("status") or {}).get("error") or {}).get("message")
        print(f"    -> {state}" + (f" | {err[:200]}" if err else ""))

    # Verify
    status, body = api(
        "POST",
        "/api/2.0/sql/statements",
        {
            "warehouse_id": wid,
            "statement": "SELECT COUNT(*) AS c FROM dbw_shared_qgr7mj.advanced_lab.channel_kpi",
            "wait_timeout": "50s",
        },
    )
    assert isinstance(body, dict)
    print("VERIFY", body.get("status"), body.get("result"))
    return 0 if (body.get("status") or {}).get("state") == "SUCCEEDED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
