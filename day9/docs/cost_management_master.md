# Cost Management Master — end-to-end stack cost lab

**Notebook:** https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/cost_management_master

## What & why

One executable notebook that teaches **cost management for the entire data engineering stack** in `rg-shared-class1`: Databricks (DBUs, clusters, warehouses, jobs), ADF, ADLS, Event Hub, Key Vault, Purview, SQL DB — from beginner to PhD (unit economics, forecasting, chargeback).

**Master analogy:** the stack is a house; every service is a utility meter. You cannot save what you cannot see.

## Design rules

- **Every code cell is 1–4 lines** and executable.
- **Everything is logged** — findings go to `cost_lab.run_log` (Delta) at the end.
- **Soft-runs** — `system.billing` cells skip cleanly if UC grants are missing (see [system_tables_master.md](system_tables_master.md) §fix).
- **Live data** — three sources:
  1. Databricks REST API (clusters, warehouses, jobs) via the notebook's own context token
  2. `system.billing.usage` / `list_prices` (soft)
  3. **Azure Cost Management REST** via SPN secrets in the `finledger` scope (`azure-tenant-id`, `azure-client-id`, `azure-client-secret`, `azure-subscription-id`)

## Levels

| Level | Topic | Save-money move |
|-------|-------|-----------------|
| 0 | Setup: logger + soft runner | — |
| 1 | Clusters: DBU×VM, auto-terminate, spot | auto-stop, job clusters, spot |
| 2 | SQL warehouses + jobs | auto_stop_mins, right-size |
| 3 | `system.billing` — DBUs → $ per day/SKU/job/cluster | attack the top SKU/job |
| 4 | Storage/Delta: OPTIMIZE, VACUUM, history | compaction + lifecycle tiers |
| 5 | ADF / Event Hub / Key Vault / Purview / SQL | tier + batching table |
| 6 | **Live Azure bill** — MTD per service + budgets | see the real invoice |
| 7 | Tagging + chargeback attribution | cluster policies enforce tags |
| 8 | PhD: forecast, unit economics, FinOps loop | $/run, $/GB, $/query |

## Deploy / re-run (idempotent)

```cmd
python scripts\deploy_cost_management_lab.py
```

Also pushes the SPN secrets (`scripts\ensure_cost_lab_secrets.py`) so Level 6 works live.

**Student laptop HTML dashboard (no Databricks):** from repo root run `cost-dashboard.cmd --open`  
→ see [cost_dashboard.md](cost_dashboard.md) (RG + subscription + ADF pipeline $ estimate).

Four re-run scenarios: fresh deploy creates the notebook; re-run after success overwrites in place; re-run after partial failure resumes (import is atomic); re-run after workspace notebook deletion recreates it.

## Region / cost note

Shared lab is **eastus** (documented exception to UK-only rule — pre-existing shared estate). All services on cheapest viable tier (Event Hub Basic, SQL Basic, Standard LRS storage). The notebook itself is the cost gate — budgets + MTD queries warn on spend.

## Pair with

- [system_tables_master.md](system_tables_master.md) — the grants fix for `system.billing`
- [perf_databricks_adf_lab.md](perf_databricks_adf_lab.md) — make the expensive job faster
