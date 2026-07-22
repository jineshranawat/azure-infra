"""UC06 — Run audit trail. Every engineer: prove what ran, when, with which params."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from _common import OUT, banner, utc_now, write_json


def run(prior_results: list[dict] | None = None) -> dict:
    banner("UC06 Run audit trail - immutable run record for ops / compliance")
    prior_results = prior_results or []
    summary = []
    for r in prior_results:
        summary.append(
            {
                "uc": r.get("uc"),
                "name": r.get("name"),
                "status": r.get("status")
                or r.get("overall")
                or ("incident_raised" if r.get("jira_key") else "ok"),
                "jira_key": r.get("jira_key"),
            }
        )

    payload = {
        "uc": "UC06",
        "name": "run_audit_trail",
        "analogy": "Guest book of every dinner service - who cooked what when",
        "enterprise_map": "ADF: logging + audit/ JSON Copy; Purview lineage; Monitor activity outputs",
        "audit_utc": utc_now(),
        "pipeline_analog": "pl_gov / pl_ntf / medallion master",
        "scenarios_executed": summary,
        "engineer_checklist": [
            "run_id present",
            "start/end timestamps",
            "parameters hashed (no secrets)",
            "success/fail per activity",
            "link to ticket if failed",
        ],
    }
    raw = json.dumps(payload, sort_keys=True)
    payload["content_sha256"] = hashlib.sha256(raw.encode()).hexdigest()[:16]
    out = write_json(OUT / "uc06_audit_trail.json", payload)
    print(f"  Audit written: {out}")
    print(f"  Scenarios recorded: {len(summary)} | sha={payload['content_sha256']}")
    return payload
