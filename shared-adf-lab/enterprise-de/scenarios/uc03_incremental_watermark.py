"""UC03 — Incremental watermark. Every engineer: only process new data."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from _common import OUT, SAMPLE, banner, utc_now, write_json


def _parse_ts(value: str) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def run() -> dict:
    banner("UC03 Incremental watermark - copy only rows after last watermark")
    wm_path = SAMPLE / "watermark.json"
    wm = json.loads(wm_path.read_text(encoding="utf-8"))
    last = _parse_ts(wm["last_successful_watermark"])
    print(f"  Watermark: {wm['last_successful_watermark']}")

    src = SAMPLE / "good_transactions.csv"
    selected: list[dict] = []
    with src.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            ts = _parse_ts(row.get("event_ts", ""))
            if ts and last and ts > last:
                selected.append(row)
            elif ts and last is None:
                selected.append(row)

    # Simulate sink + new watermark = max event_ts of selected (or keep old)
    if selected:
        new_wm = max(r["event_ts"] for r in selected)
    else:
        new_wm = wm["last_successful_watermark"]

    delta_path = OUT / "uc03_incremental_delta.csv"
    with delta_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["txn_id", "channel", "amount", "currency", "event_ts"])
        writer.writeheader()
        writer.writerows(selected)

    new_wm_payload = {
        "last_successful_watermark": new_wm,
        "source": "enterprise-de-uc03",
        "updated_utc": utc_now(),
        "rows_copied": len(selected),
    }
    write_json(OUT / "uc03_watermark_new.json", new_wm_payload)

    result = {
        "uc": "UC03",
        "name": "incremental_watermark",
        "analogy": "Only cook new tickets since last dinner service",
        "enterprise_map": "ADF pl_05 / pl_23 Lookup watermark + Copy filter; Databricks MERGE",
        "old_watermark": wm["last_successful_watermark"],
        "new_watermark": new_wm,
        "rows_selected": len(selected),
        "delta_file": str(delta_path),
    }
    write_json(OUT / "uc03_incremental_result.json", result)
    print(f"  Selected {len(selected)} new rows -> {delta_path.name}")
    print(f"  New watermark: {new_wm}")
    return result
