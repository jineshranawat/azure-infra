"""UC04 — Quarantine / dead-letter. Every engineer: isolate bad files, don't poison the lake."""

from __future__ import annotations

import shutil
from pathlib import Path

from _common import OUT, SAMPLE, banner, utc_now, write_json

# Reuse DQ checks lightly
from uc02_data_quality_gate import _check_file  # type: ignore


def run() -> dict:
    banner("UC04 Quarantine / dead-letter - bad files go to reject/, good to loaded/")
    incoming = OUT / "uc04" / "incoming"
    loaded = OUT / "uc04" / "loaded"
    reject = OUT / "uc04" / "reject"
    for d in (incoming, loaded, reject):
        d.mkdir(parents=True, exist_ok=True)

    # Stage sample files into incoming
    for name in ("good_transactions.csv", "bad_transactions.csv"):
        shutil.copy2(SAMPLE / name, incoming / name)

    moved = {"loaded": [], "reject": []}
    details = []
    for path in sorted(incoming.glob("*.csv")):
        check = _check_file(path)
        dest_dir = loaded if check["status"] == "PASS" else reject
        dest = dest_dir / path.name
        shutil.move(str(path), str(dest))
        bucket = "loaded" if check["status"] == "PASS" else "reject"
        moved[bucket].append(path.name)
        details.append({**check, "landed_in": bucket, "path": str(dest)})
        print(f"  {path.name} -> {bucket}/ ({check['status']})")

    result = {
        "uc": "UC04",
        "name": "quarantine_dead_letter",
        "analogy": "Bad deliveries go to returns desk, not the walk-in fridge",
        "enterprise_map": "ADF: If + Copy to reject/ + Fail/Notify; event trigger on reject/",
        "moved": moved,
        "details": details,
        "checked_utc": utc_now(),
    }
    write_json(OUT / "uc04_quarantine_result.json", result)
    print(f"  loaded={moved['loaded']} reject={moved['reject']}")
    return result
