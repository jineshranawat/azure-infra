"""UC01 — Incident notify (reuses enterprise-notify flow). Every engineer: on-call alerting."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# scenarios/ → enterprise-de/ → shared-adf-lab/ → repo
REPO = Path(__file__).resolve().parents[3]
ED_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ED_ROOT))

from _common import OUT, banner, write_json  # noqa: E402

NOTIFY_CLIENT = REPO / "shared-adf-lab" / "enterprise-notify" / "client"
START = REPO / "shared-adf-lab" / "enterprise-notify" / "start_services.py"


def run() -> dict:
    banner("UC01 Incident notify - fail -> Jira -> attach -> email")
    subprocess.run([sys.executable, str(START)], check=True)
    sys.path.insert(0, str(NOTIFY_CLIENT))
    from run_incident_flow import run_flow  # noqa: E402

    result = run_flow(
        run_id=f"uc01-{__import__('time').time_ns() // 1_000_000}",
        pipeline_name="pl_ntf_05_master_incident",
        force_fail=True,
        jira_base="http://127.0.0.1:18080",
        mail_url="http://127.0.0.1:18081",
        owner_email="training@example.com",
    )
    payload = {
        "uc": "UC01",
        "name": "incident_notify",
        "analogy": "Kitchen fire alarm -> ticket -> evidence photos -> text the manager",
        "enterprise_map": "ADF folder 15-enterprise-notify · enterprise-notify.cmd",
        **result,
    }
    write_json(OUT / "uc01_incident_result.json", payload)
    print(f"  Jira: {payload.get('jira_key')} | Mail sent: {payload.get('mail_ok')}")
    return payload
