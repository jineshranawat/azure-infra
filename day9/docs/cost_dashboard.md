# Cost Dashboard — student machine + Azure Portal + Databricks

## What & why

Students need to **see money** for the whole shared estate (`rg-shared-class1`) without
being Cost Management experts. This lab gives three views of the same truth:

| View | Entry | Best for |
|------|-------|----------|
| **Windows tool** | `cost-dashboard.cmd` | Every student laptop |
| **HTML / CSV** | `docs/cost-dashboard-out/` | Share / Excel / project |
| **Databricks notebook** | `/Shared/day9/cost_management_master` (Level 6–6b) | Live in class |
| **Azure Portal** | Cost Analysis deep links printed by the tool | Official Microsoft UI |

### Analogy

The **utility company invoice** (subscription) has pages for each house (resource group),
each room (resource), and each day. ADF is one room — the **pipelines** are appliances;
Azure only meters the room, so we estimate appliance cost by how long each ran.

### Important truth about “minute detail”

Azure Cost Management **does not bill per minute**. Granularity is **daily**.
What *is* minute-level:

- ADF **pipeline run duration** (`durationInMs` → minutes)
- Databricks job / cluster runtime

What is **estimated** for ADF:

```text
pipeline_$ ≈ ADF_factory_MTD × (pipeline_duration_ms ÷ sum_all_pipeline_ms)
```

## A. Student machine — one command

```cmd
cd <repo-root>
cost-dashboard.cmd
cost-dashboard.cmd --open
cost-dashboard.cmd --days 30 --open
```

**Prerequisites (clean Windows laptop):**

1. Python 3.10+ on PATH **or** repo `.venv` (from `orchestrate.cmd`)
2. `az login` **or** `.env` with `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_SUBSCRIPTION_ID`

The `.cmd` auto-`pip install`s Azure SDK packages — learners do **not** touch execution policy.

### What it prints / writes

| Output | Contents |
|--------|----------|
| Console | RG by service, by resource, daily; subscription top services; ADF $ estimate; recent runs |
| `docs/cost-dashboard-out/index.html` | Full dashboard (open in browser) |
| `*.csv` | Same tables for Excel |
| `summary.json` | Machine-readable totals |
| Portal links | Cost Analysis (RG + sub), Budgets, ADF Monitor |

### Four re-run scenarios

| Scenario | Safe? |
|----------|-------|
| Fresh machine, first run | Yes — installs deps, creates `docs/cost-dashboard-out/` |
| Re-run after success | Yes — overwrites HTML/CSV in place |
| Re-run after partial fail (e.g. cost API 429) | Yes — retry; empty tables if lag |
| After deleting output folder | Yes — recreates folder |

## B. Azure Portal dashboard (official UI)

1. Open **Cost Analysis** for the RG (link printed by the tool, or):
   - Portal → `rg-shared-class1` → **Cost analysis**
2. Group by: **Service name** → then **Resource** → then set granularity **Daily**
3. Scope switch to **Subscription** for the whole bill
4. **Budgets** blade — confirm alert thresholds
5. **ADF Studio → Monitor → Pipeline runs** — duration / fail rate (minute-level ops)

## C. Databricks notebook

```cmd
python scripts\deploy_cost_management_lab.py
```

Open: `/Shared/day9/cost_management_master`  
Levels **6** (live Azure bill) and **6b** (resource drill-down + **each ADF pipeline estimated $**).

## D. Shared estate defaults

| Setting | Value |
|---------|-------|
| RG | `rg-shared-class1` |
| ADF | `adf-shared-qgr7mj` |
| Subscription | from `.env` / `az account show` |
| Region note | Shared lab is **eastus** (documented exception to UK-only rule) |

## Save money (same checklist)

1. Auto-terminate Databricks clusters  
2. SQL warehouse auto-stop  
3. Job clusters for schedules  
4. ADF: fewer/bigger Copies; no always-on SHIR  
5. Event Hub Basic 1 TU  
6. ADLS lifecycle → Cool  
7. Budget alerts 50/80/100%  

## Related

- **[cost_dashboard_code_overview.md](cost_dashboard_code_overview.md) — detailed code walkthrough**: every scenario, every Azure API, all Monitor metrics, NFR mapping, 429 handling, and what to read in `scripts/cost_dashboard.py`
- [cost_management_master.md](cost_management_master.md) — full FinOps teaching notebook  
- [system_tables_master.md](system_tables_master.md) — `system.billing` grants  
