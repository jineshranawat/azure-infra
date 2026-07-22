"""UC05 — Config-driven multi-entity. Every engineer: one pipeline, many tables from config."""

from __future__ import annotations

import json
from pathlib import Path

from _common import OUT, SAMPLE, banner, utc_now, write_json


def run() -> dict:
    banner("UC05 Config-driven entities - ForEach over entities.json")
    cfg = json.loads((SAMPLE / "entities.json").read_text(encoding="utf-8"))
    processed = []
    skipped = []
    for item in cfg:
        entity = item["entity"]
        if not item.get("enabled", True):
            skipped.append(entity)
            print(f"  SKIP  {entity} (enabled=false)")
            continue
        # Simulate work per entity
        artifact = {
            "entity": entity,
            "incoming": item.get("incoming"),
            "status": "SUCCEEDED",
            "rows_stub": 100 if entity == "transactions" else 25,
            "run_utc": utc_now(),
        }
        write_json(OUT / f"uc05_entity_{entity}.json", artifact)
        processed.append(artifact)
        print(f"  RUN   {entity} -> stub rows={artifact['rows_stub']}")

    result = {
        "uc": "UC05",
        "name": "config_driven_foreach",
        "analogy": "One kitchen timetable, many menu items from a config clipboard",
        "enterprise_map": "ADF: Lookup/Get Metadata config + ForEach + Execute Pipeline child",
        "processed": processed,
        "skipped": skipped,
        "config_file": str(SAMPLE / "entities.json"),
    }
    write_json(OUT / "uc05_config_foreach_result.json", result)
    print(f"  Processed {len(processed)} | Skipped {len(skipped)}")
    return result
