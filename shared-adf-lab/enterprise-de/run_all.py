"""Run all (or one) enterprise DE playbook scenarios — local end-to-end demos."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scenarios"))

from _common import OUT, banner, write_json  # noqa: E402

SCENARIOS = {
    "1": ("uc01_incident_notify", "UC01 Incident notify (Jira + attach + email)"),
    "2": ("uc02_data_quality_gate", "UC02 Data quality gate"),
    "3": ("uc03_incremental_watermark", "UC03 Incremental watermark"),
    "4": ("uc04_quarantine_reject", "UC04 Quarantine / dead-letter"),
    "5": ("uc05_config_driven_entities", "UC05 Config-driven ForEach"),
    "6": ("uc06_run_audit_trail", "UC06 Run audit trail"),
}


def _run_one(key: str, prior: list[dict]) -> dict:
    mod_name, title = SCENARIOS[key]
    print()
    print("-" * 68)
    print(f">>> {title}")
    print("-" * 68)
    mod = importlib.import_module(mod_name)
    if key == "6":
        return mod.run(prior_results=prior)
    return mod.run()


def main() -> int:
    parser = argparse.ArgumentParser(description="FinLedger enterprise DE playbook")
    parser.add_argument(
        "--uc",
        default="all",
        help="Scenario number 1-6, or 'all' (default)",
    )
    args = parser.parse_args()
    banner("ENTERPRISE DATA ENGINEERING PLAYBOOK - local end-to-end")
    print(f"  Out dir: {OUT}")

    keys = list(SCENARIOS.keys()) if args.uc.lower() == "all" else [str(args.uc)]
    for k in keys:
        if k not in SCENARIOS:
            print(f"Unknown UC '{k}'. Choose: {', '.join(SCENARIOS)} or all")
            return 2

    results: list[dict] = []
    # If running single UC06 without priors, still ok
    for k in keys:
        if k == "6" and args.uc.lower() != "all" and not results:
            # Load prior result JSONs if present
            for p in sorted(OUT.glob("uc0*_*.json")):
                if "uc06" in p.name:
                    continue
                try:
                    results.append(json.loads(p.read_text(encoding="utf-8")))
                except Exception:
                    pass
        r = _run_one(k, results)
        results.append(r)

    summary = {
        "playbook": "enterprise-de",
        "ran": [r.get("uc") for r in results],
        "results": [
            {
                "uc": r.get("uc"),
                "name": r.get("name"),
                "jira_key": r.get("jira_key"),
                "overall": r.get("overall") or r.get("status"),
            }
            for r in results
        ],
    }
    path = write_json(OUT / "playbook_summary.json", summary)
    print()
    banner("PLAYBOOK COMPLETE")
    print(f"  Summary: {path}")
    for item in summary["results"]:
        print(f"  - {item['uc']}: {item['name']} {item.get('jira_key') or item.get('overall') or ''}")
    print()
    print("  Teach HTML: docs\\enterprise-de-playbook.html")
    print("  Jira board: http://127.0.0.1:18080/   (if UC01 ran)")
    print("  Mail inbox: http://127.0.0.1:18081/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
