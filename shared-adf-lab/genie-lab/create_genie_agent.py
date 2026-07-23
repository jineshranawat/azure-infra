"""Select up to 30 UC tables and create/update FinLedger Genie Agent."""

from __future__ import annotations

import json
from pathlib import Path

from _common import (
    MAX_TABLES,
    OUT,
    api,
    ensure_shared_folder,
    gen_id,
    inventory_class_catalog,
    must_ok,
    pick_warehouse,
    start_warehouse,
    write_json,
)

AGENT_TITLE = "FinLedger Class Genie"
STATE_FILE = OUT / "genie_agent_state.json"


def _priority_score(identifier: str) -> tuple[int, str]:
    """Lower score = higher priority for the 30-table slot."""
    schema = identifier.split(".")[1] if "." in identifier else ""
    name = identifier.split(".")[-1]
    if schema == "advanced_lab":
        return (0, identifier)
    if schema == "cost_lab":
        return (1, identifier)
    # Prefer business-shaped demo tables
    prefer = (
        "channel",
        "dq",
        "scd2",
        "watermark",
        "cdc",
        "incremental",
        "monitoring",
        "lineage",
        "canonical",
        "merged",
        "valid",
        "kpi",
    )
    hit = 0 if any(p in name for p in prefer) else 1
    return (2 + hit, identifier)


def select_tables(all_tables: list[dict], limit: int = MAX_TABLES) -> list[dict]:
    ranked = sorted(all_tables, key=lambda t: _priority_score(t["identifier"]))
    selected = ranked[:limit]
    selected.sort(key=lambda t: t["identifier"])
    return selected


def _build_serialized_space(tables: list[dict]) -> str:
    sample_qs = [
        "How many rows are in advanced_lab.channel_kpi?",
        "What are the distinct channels in channel_kpi?",
        "Show total amount or value by channel if available",
        "List tables related to data quality (dq) metrics",
        "What does cost_lab.chargeback_estimate contain?",
        "Summarize advanced_lab.window_adv with a few aggregate metrics",
        "How many demo_problems tables are in this agent?",
        "Show top rows from advanced_lab.joined_dim",
    ]
    sample_questions = sorted(
        [{"id": gen_id(), "question": [q]} for q in sample_qs],
        key=lambda x: x["id"],
    )

    table_objs = []
    for t in tables:
        entry: dict = {"identifier": t["identifier"]}
        if t.get("columns"):
            # Light column configs for common-looking dims
            configs = []
            for c in t["columns"]:
                cl = c.lower()
                if any(k in cl for k in ("channel", "status", "date", "ts", "amount", "kpi", "name")):
                    cfg: dict = {"column_name": c}
                    if any(k in cl for k in ("channel", "status", "name")):
                        cfg["get_example_values"] = True
                        cfg["build_value_dictionary"] = True
                    configs.append(cfg)
            if configs:
                configs.sort(key=lambda x: x["column_name"])
                entry["column_configs"] = configs
        table_objs.append(entry)
    table_objs.sort(key=lambda x: x["identifier"])

    text = (
        "You are the FinLedger class Genie for Azure Databricks training. "
        "Prefer tables in dbw_shared_qgr7mj.advanced_lab and cost_lab for operational metrics. "
        "demo_problems tables are lab outputs from 50 DE problems — use them when asked about DQ, CDC, SCD2, watermarks. "
        "Never invent rows. If a column does not exist, say so and list available columns. "
        "Currency context is GBP when amounts appear. Round money to 2 decimals. "
        "When asked 'how many tables', answer from the agent's attached data sources only."
    )
    text_instructions = sorted(
        [{"id": gen_id(), "content": [text]}],
        key=lambda x: x["id"],
    )

    # Example SQL — keep simple and valid against known tables
    examples = [
        {
            "id": gen_id(),
            "question": ["How many rows are in advanced_lab.channel_kpi?"],
            "sql": [
                "SELECT COUNT(*) AS row_count\n",
                "FROM dbw_shared_qgr7mj.advanced_lab.channel_kpi\n",
            ],
        },
        {
            "id": gen_id(),
            "question": ["What are the distinct channels in channel_kpi?"],
            "sql": [
                "SELECT DISTINCT channel\n",
                "FROM dbw_shared_qgr7mj.advanced_lab.channel_kpi\n",
                "ORDER BY channel\n",
            ],
        },
        {
            "id": gen_id(),
            "question": ["Show chargeback estimates from cost_lab"],
            "sql": [
                "SELECT *\n",
                "FROM dbw_shared_qgr7mj.cost_lab.chargeback_estimate\n",
                "LIMIT 50\n",
            ],
        },
    ]
    examples = sorted(examples, key=lambda x: x["id"])

    space = {
        "version": 1,
        "config": {"sample_questions": sample_questions},
        "data_sources": {"tables": table_objs},
        "instructions": {
            "text_instructions": text_instructions,
            "example_question_sqls": examples,
        },
    }
    return json.dumps(space, separators=(",", ":"))


def create_or_update_agent() -> dict:
    print("=" * 68)
    print("CREATE / UPDATE FinLedger Genie Agent")
    print("=" * 68)

    inventory = inventory_class_catalog()
    write_json("inventory_all_tables.json", inventory)
    print(f"  Catalog tables found: {len(inventory)}")

    selected = select_tables(inventory)
    write_json("inventory_selected_for_genie.json", selected)
    print(f"  Selected for Genie (max {MAX_TABLES}): {len(selected)}")
    for t in selected:
        print(f"    - {t['identifier']} ({len(t.get('columns') or [])} cols)")

    warehouse = pick_warehouse()
    wid = warehouse["id"]
    print(f"  Warehouse: {warehouse.get('name')} ({wid}) state={warehouse.get('state')}")
    start_warehouse(wid)

    parent = ensure_shared_folder("/Shared/genie-lab")
    serialized = _build_serialized_space(selected)

    body = {
        "title": AGENT_TITLE,
        "description": (
            "Class Genie over dbw_shared_qgr7mj curated tables (advanced_lab, cost_lab, "
            "key demo_problems). Orchestrator-tested via genie-lab.cmd."
        ),
        "warehouse_id": wid,
        "parent_path": parent,
        "serialized_space": serialized,
    }

    space_id = None
    if STATE_FILE.exists():
        try:
            prev = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            space_id = prev.get("space_id")
        except json.JSONDecodeError:
            space_id = None

    if space_id:
        print(f"  Updating existing agent space_id={space_id}")
        status, payload = api("POST", f"/api/2.0/genie/spaces/{space_id}", body)
        if status >= 400:
            print(f"  Update failed ({status}); creating new agent instead…")
            space_id = None
        else:
            result = payload if isinstance(payload, dict) else {}
    if not space_id:
        print("  Creating new Genie Agent…")
        status, payload = api("POST", "/api/2.0/genie/spaces", body)
        must_ok(status, payload, "create Genie Agent")
        assert isinstance(payload, dict)
        result = payload

    space_id = result.get("space_id") or result.get("id")
    if not space_id:
        raise SystemExit(f"No space_id in response: {result}")

    state = {
        "space_id": space_id,
        "title": result.get("title") or AGENT_TITLE,
        "warehouse_id": wid,
        "parent_path": parent,
        "table_count": len(selected),
        "tables": [t["identifier"] for t in selected],
        "inventory_total": len(inventory),
        "ui_url": (
            "https://adb-7405613791235979.19.azuredatabricks.net/genie/rooms/"
            f"{space_id}"
        ),
    }
    write_json("genie_agent_state.json", state)
    print(f"  space_id: {space_id}")
    print(f"  UI: {state['ui_url']}")
    return state


if __name__ == "__main__":
    create_or_update_agent()
