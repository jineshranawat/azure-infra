# Cost Management Master — end-to-end stack cost lab (6–10 hours)

**Notebook:** https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/cost_management_master

**One-file analogy guide:** [finledger_cost_and_databricks_master_guide.md](finledger_cost_and_databricks_master_guide.md)

## What & why

One executable Databricks notebook that teaches **cost management for the entire data engineering stack** (Databricks, ADF, ADLS, Event Hub, Key Vault, Purview, SQL) from beginner to PhD — sized for **~6–10 classroom hours**.

**Master analogy:** the stack is a house; every service is a utility meter. You cannot save what you cannot see. Databricks DBUs are the taxi meter; Azure Cost Management is the city utility bill — **two invoices**, never confuse them.

## Design rules

- **Every code cell is 1–4 lines** and executable.
- **Widgets at the top** — change `resource_group`, `adf_name`, `lookback_days` without editing cells.
- **Existing L0–L8 command labels preserved**; many **NEW** cells added (L0.4, L1.5–L1.8, L2.4–L2.6, L3.5–L3.8, L4.4–L4.5, L5.3–L5.5, L6.7–L6.9, L6b.6–L6b.7, Level 6c Monitor, L7.3–L7.4, L8.4–L8.5, Levels 9–10, W.3).
- **Everything is logged** — findings go to `cost_lab.run_log` (Delta) at the end.
- **Soft-runs** — `system.billing` cells skip cleanly if UC grants are missing (see [system_tables_master.md](system_tables_master.md) §fix).
- **Live data** — Databricks REST, `system.billing` (soft), Azure Cost Management REST (SPN secrets), Azure Monitor metrics, ADF pipeline runs.

## Widgets (run L0.0 first)

| Widget | Default | Purpose |
|--------|---------|---------|
| `resource_group` | `rg-shared-class1` | All Cost Management / ADF / ARM URLs use this |
| `adf_name` | `adf-shared-qgr7mj` | Factory for pipeline $ attribution |
| `lookback_days` | `14` | ADF runs + Monitor metrics window |

## Levels & suggested hours

| Level | Topic | ~Hours | Save-money move |
|-------|-------|--------|-----------------|
| 0 | Setup: widgets + logger + soft runner | 0.5 | — |
| 1 | Clusters: DBU×VM, auto-terminate, spot, pools, events | 1–1.5 | auto-stop, job clusters, spot |
| 2 | SQL warehouses + jobs + run durations | 0.75–1 | auto_stop_mins, job_cluster |
| 3 | `system.billing` — DBUs → $ per day/SKU/job/cluster/warehouse | 1–1.5 | attack top SKU/job |
| 4 | Storage/Delta: OPTIMIZE, VACUUM, inventory | 0.75 | compaction + lifecycle |
| 5 | ADF / EH / KV / Purview / SQL stories | 0.75–1 | tier + batching |
| 6 | Live Azure bill — MTD service + budgets + sub + portal + ARM | 1.5–2 | see real invoice |
| 6b | Per-resource + ADF pipeline $ estimate + fail tax | 1 | rank pipelines |
| 6c | Azure Monitor metrics (smart meters) | 0.75–1 | rightsizing evidence |
| 7 | Tagging + ARM tags + Cost by TagKey | 0.5–0.75 | chargeback |
| 8 | PhD: forecast, unit economics, chart, $/ADF-min | 1 | FinOps loop |
| 9 | NFR × cost drills + decision card | 0.75–1 | fix fails before scale |
| 10 | Challenges + chargeback DataFrame | 1–1.5 | practice |
| Wrap | Checklist + persist log + summary | 0.25 | weekly ritual |

## Deploy / re-run (idempotent)

```cmd
python day9\scripts\build_cost_management_notebook.py
python scripts\deploy_cost_management_lab.py
```

Also pushes the SPN secrets (`scripts\ensure_cost_lab_secrets.py`) so Level 6 works live.

**Student laptop HTML dashboard (no Databricks):** from repo root run `.\cost-dashboard.cmd --open`  
→ see [cost_dashboard.md](cost_dashboard.md) · code map [cost_dashboard_code_overview.md](cost_dashboard_code_overview.md).

Four re-run scenarios: fresh deploy creates the notebook; re-run after success overwrites in place; re-run after partial failure resumes (import is atomic); re-run after workspace notebook deletion recreates it.

## Region / cost note

Shared lab is **eastus** (documented exception to UK-only rule — pre-existing shared estate). All services on cheapest viable tier (Event Hub Basic, SQL Basic, Standard LRS storage). The notebook itself is the cost gate — budgets + MTD queries warn on spend.

## Citations (use when presenting)

1. [Cost Management Query — Usage](https://learn.microsoft.com/en-us/rest/api/cost-management/query/usage)  
2. [Databricks system tables — billing](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing)  
3. [Grant access to system tables](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/#grant-access-to-system-tables)  
4. [ADF pricing concepts](https://learn.microsoft.com/en-us/azure/data-factory/pricing-concepts)  
5. [Azure Monitor Metrics API](https://learn.microsoft.com/en-us/rest/api/monitor/metrics/list)  
6. [FinOps Framework](https://www.finops.org/framework/)  

## Pair with

- [finledger_cost_and_databricks_master_guide.md](finledger_cost_and_databricks_master_guide.md) — kitchen analogies + full citations  
- [system_tables_master.md](system_tables_master.md) — grants fix for `system.billing`  
- [perf_databricks_adf_lab.md](perf_databricks_adf_lab.md) — make the expensive job faster  
