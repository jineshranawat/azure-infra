"""UC02 — Data quality gate. Every engineer: block bad data before promote."""

from __future__ import annotations

import csv
from pathlib import Path

from _common import OUT, SAMPLE, banner, utc_now, write_json

REQUIRED_COLS = ["txn_id", "channel", "amount", "currency", "event_ts"]
ALLOWED_CHANNELS = {"card", "wire", "fps"}


def _check_file(path: Path) -> dict:
    issues: list[str] = []
    rows = 0
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        cols = reader.fieldnames or []
        missing = [c for c in REQUIRED_COLS if c not in cols]
        if missing:
            issues.append(f"missing columns: {missing}")
        for i, row in enumerate(reader, start=2):
            rows += 1
            if not (row.get("txn_id") or "").strip():
                issues.append(f"L{i}: empty txn_id")
            ch = (row.get("channel") or "").strip()
            if ch and ch not in ALLOWED_CHANNELS:
                issues.append(f"L{i}: bad channel '{ch}'")
            amt = (row.get("amount") or "").strip()
            if not amt:
                issues.append(f"L{i}: empty amount")
            else:
                try:
                    float(amt)
                except ValueError:
                    issues.append(f"L{i}: amount not numeric '{amt}'")
    status = "PASS" if not issues and rows > 0 else "FAIL"
    if rows == 0 and not issues:
        issues.append("zero rows")
        status = "FAIL"
    return {
        "file": path.name,
        "rows": rows,
        "status": status,
        "issues": issues[:20],
        "issue_count": len(issues),
        "checked_utc": utc_now(),
    }


def run() -> dict:
    banner("UC02 Data quality gate - validate before bronze->silver")
    results = []
    for name in ("good_transactions.csv", "bad_transactions.csv"):
        r = _check_file(SAMPLE / name)
        results.append(r)
        print(f"  {name}: {r['status']} rows={r['rows']} issues={r['issue_count']}")
        for iss in r["issues"][:5]:
            print(f"    - {iss}")
    gate = {
        "uc": "UC02",
        "name": "data_quality_gate",
        "analogy": "Health check before plating - reject bad ingredients",
        "enterprise_map": "ADF: Get Metadata + If / Databricks expectations / Great Expectations",
        "files": results,
        "overall": (
            "MIXED"
            if any(r["status"] == "PASS" for r in results) and any(r["status"] == "FAIL" for r in results)
            else ("PASS" if all(r["status"] == "PASS" for r in results) else "FAIL")
        ),
        "promote": [r["file"] for r in results if r["status"] == "PASS"],
        "block": [r["file"] for r in results if r["status"] == "FAIL"],
    }
    out = write_json(OUT / "uc02_dq_gate_result.json", gate)
    print(f"  Result JSON: {out}")
    print(f"  Promote: {gate['promote']} | Block: {gate['block']}")
    return gate
