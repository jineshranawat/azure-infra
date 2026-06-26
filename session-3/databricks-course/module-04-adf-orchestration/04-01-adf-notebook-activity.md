# 04-01 · ADF Databricks notebook activity

> Module 4 · Time budget: 30 min · Source: [session-2 module 04-01](../../../session-2/adf-course/module-04-external-compute/04-01-databricks-notebook-activity.md)

## What you'll build

ADF pipeline **`pl_databricks_finledger`** calling notebook **`nb_04_end_to_end`** with lake paths as parameters.

## Why wire ADF to Databricks?

| ADF alone | With Databricks activity |
|-----------|-------------------------|
| Schedule file copies | Schedule **transformations** |
| One monitoring pane | Same Monitor blade shows notebook success |
| Pass `run_id` from watermark | End-to-end lineage Session 2 → 3 |

## Part A — UI (ADF Studio)

### A1 — Databricks linked service

1. ADF Studio → **Manage** → **Linked services** → **+ New**.
2. Search **Azure Databricks** → Continue.
3. **Name:** `ls_databricks_finledger`.
4. **Account selection:** From Azure subscription → pick `dbw-<learner>-...`.
5. **Select cluster:** New job cluster → smallest node → terminate 30 min idle.
6. **Test connection** → **Publish all**.

### A2 — Import notebook to Databricks

Import `session-3/notebooks/nb_04_end_to_end.py` to workspace path `/nb_finledger_end_to_end`.

### A3 — Pipeline

1. **Author** → **+ Pipeline** → `pl_databricks_finledger`.
2. Drag **Databricks Notebook** activity → name `Transform_bronze_to_gold`.
3. **Azure Databricks** tab:
   - Linked service: `ls_databricks_finledger`
   - Notebook path: `/nb_finledger_end_to_end`
4. **Base parameters** (from `orchestrate.cmd` output):

| Parameter | Example value |
|-----------|---------------|
| `bronze_path` | `abfss://bronze@st....dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv` |
| `silver_path` | `abfss://silver@st....dfs.core.windows.net/transactions` |
| `gold_path` | `abfss://gold@st....dfs.core.windows.net/daily_channel_summary` |
| `run_id` | `session3-lab` |

5. **Debug** or **Trigger now** → Monitor 15–40 min (cluster cold start).

## Part B — JSON reference

```json
{
  "name": "pl_databricks_finledger",
  "properties": {
    "activities": [
      {
        "name": "Transform_bronze_to_gold",
        "type": "DatabricksNotebook",
        "typeProperties": {
          "notebookPath": "/nb_finledger_end_to_end",
          "baseParameters": {
            "bronze_path": "abfss://bronze@stYOUR.dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv",
            "silver_path": "abfss://silver@stYOUR.dfs.core.windows.net/transactions",
            "gold_path": "abfss://gold@stYOUR.dfs.core.windows.net/daily_channel_summary",
            "run_id": "session3-lab"
          }
        },
        "linkedServiceName": {
          "referenceName": "ls_databricks_finledger",
          "type": "LinkedServiceReference"
        }
      }
    ]
  }
}
```

## Verify

| Check | Expected |
|-------|----------|
| ADF activity | Succeeded |
| gold/_delta_log | Updated timestamp |
| Notebook output | JSON with row counts |

## Cost & tear-down

Terminate job cluster. Optional: delete linked service after lab.

## Next

Session 4 — Purview estate-wide governance
