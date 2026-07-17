# FinLedger Cost & Databricks — One Master Guide (read this first)

**Audience:** anyone who has never seen Databricks or Azure Cost Management.  
**Promise:** if you only open **one** markdown file, this is it. Every idea uses a plain analogy first, then points to the exact lab notebook / script / official docs.

**Created:** 2026-07-17 (cost + system tables + Monitor metrics work)  
**Shared estate:** `rg-shared-class1` · ADF `adf-shared-qgr7mj` · secret scope `finledger`

---

## 0. Open these three things and you are done for today

| What | How to open |
|------|-------------|
| **This guide** (you are here) | `day9/docs/finledger_cost_and_databricks_master_guide.md` |
| **Databricks cost notebook** | Workspace → `/Shared/day9/cost_management_master` (**~6–10 h**; widgets for `resource_group` / `adf_name` / `lookback_days`) |
| **Laptop HTML cost dashboard** | From repo root: `.\cost-dashboard.cmd --open` |

Companion deep-dives (only if you want more detail later):

| Topic | File |
|-------|------|
| Cost notebook levels | [cost_management_master.md](cost_management_master.md) |
| Cost dashboard how-to | [cost_dashboard.md](cost_dashboard.md) |
| Cost dashboard **code** line-by-line | [cost_dashboard_code_overview.md](cost_dashboard_code_overview.md) |
| System tables + UC grants | [system_tables_master.md](system_tables_master.md) |
| Perf / NFR (ADF ↔ Spark) | [perf_databricks_adf_lab.md](perf_databricks_adf_lab.md) |
| Day-9 index of all notebooks | [../README.md](../README.md) |

---

## 1. The master analogy — one restaurant kitchen for the whole stack

Imagine FinLedger is a **bank restaurant** that must cook the same dinner every night (medallion bronze → silver → gold).

| Kitchen role | Azure / Databricks piece | What the bill looks like |
|--------------|--------------------------|--------------------------|
| Manager with a timetable | **ADF** (Data Factory) | “Open kitchen at 2am, copy food, then call chefs” — billed per activity / IR hour |
| Kitchen + cooks | **Databricks / Spark** | Transform data — billed in **DBUs** × VM hours |
| Walk-in fridge | **ADLS Gen2** (storage) | Pay for how much food you store + how often you open the door (transactions) |
| Delivery scooter | **Event Hub** | Messages in/out — throttling = scooter queue full |
| Key cupboard | **Key Vault** | Every time a cook asks for a password — API hits |
| Health inspector ledger | **Purview** | Catalog / lineage — governance cost |
| Back-office ledger | **Azure SQL** | Small DTU database for watermarks / metadata |
| Security cameras | **Spark UI + Azure Monitor** | See which stove is slow (performance NFR) |
| Gas / electricity bill | **Azure Cost Management + `system.billing`** | Money burned even when food is fine |

**Late dinner checklist (always in this order):**

1. Was the **manager** late? → ADF Monitor (pipeline duration)  
2. Was a **stove** stuck? → Spark UI / DAG lab  
3. Is the **gas bill** crazy? → Cost dashboard + `system.billing`  
4. Did we serve **wrong food**? → row counts / MERGE correctness (never cut cost by skipping this)

That order is the architect rule: **Correctness → SLA → Cost**.

---

## 2. What we built today (inventory with “open here”)

### 2.1 Databricks notebooks (in workspace under `/Shared/day9/`)

| Notebook | One-line purpose | Deploy / re-run |
|----------|------------------|-----------------|
| `cost_management_master` | End-to-end cost for whole stack; 1–4 line cells; beginner → FinOps | `python scripts\deploy_cost_management_lab.py` |
| `system_tables_master` | Every major `system.*` schema with soft-skip + synthetic samples | `python scripts\deploy_system_tables_lab.py` |
| `perf_databricks_adf_lab` | Performance + NFR debug; ADF duration ↔ Spark stages | `python scripts\deploy_perf_databricks_adf_lab.py` |
| `spark_dag_problems_lab` | Skew / shuffle / spill problems as graphs → fix | `python scripts\deploy_spark_dag_problems_lab.py` |

### 2.2 Student laptop tool (no Databricks needed)

| File | Purpose |
|------|---------|
| `cost-dashboard.cmd` | One Windows entry point |
| `scripts/cost_dashboard.py` | Calls Cost Management + ADF runs + Azure Monitor metrics |
| `docs/cost-dashboard-out/index.html` | Generated dashboard (gitignored output) |

### 2.3 Helpers

| File | Purpose |
|------|---------|
| `scripts/ensure_cost_lab_secrets.py` | Puts Azure SPN into Databricks scope `finledger` |
| `scripts/grant_system_tables_access.py` | Tries UC GRANTs on catalog `system` (needs Account + Metastore Admin) |
| `day9/scripts/build_cost_management_notebook.py` | Regenerates the cost notebook source |

---

## 3. Two bills, not one — Databricks vs Azure

**Analogy:** the restaurant has **two invoices**.

| Invoice | Who sends it | What it covers | Where we read it |
|---------|--------------|----------------|------------------|
| **Databricks invoice** | Databricks (DBUs) | Cluster / warehouse / jobs compute | `system.billing.usage`, account console |
| **Azure invoice** | Microsoft Cost Management | VMs under Databricks, ADF, Storage, EH, KV, SQL, Purview | Cost Management Query API / Portal Cost Analysis |

**Important teaching point:** Azure Cost Management does **not** break Databricks into “this SQL query cost $0.03”. For that you need Databricks system tables / billing. The laptop dashboard answers “what did the **resource group** cost?” The notebook Level 3 answers “what did **DBUs** cost?”

Official references:

- [Azure Cost Management — Query API](https://learn.microsoft.com/en-us/rest/api/cost-management/query/usage)  
- [Databricks system tables — billing](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing)  
- [Databricks system tables — overview & grants](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/)  
- [Monitor Azure Data Factory metrics](https://learn.microsoft.com/en-us/azure/data-factory/monitor-metrics-alerts)

---

## 4. Cost dashboard — what each table means (easy English)

Run: `.\cost-dashboard.cmd --open` from `D:\azure` (PowerShell needs the `.\`).

| HTML table | Analogy | Truth level |
|------------|---------|-------------|
| 1. RG by Azure **service** | Which utility meter (Storage vs ADF vs SQL) | Actual $ (MTD) |
| 2. RG by **resource** | Which fridge / which factory name | Actual $ |
| 3. RG **daily** | Day-by-day gas bill | Actual $ (daily grain only) |
| 4. Subscription by service | Whole house + neighbours on same power company | Actual $ |
| 5. ADF **pipeline $ estimate** | Split the factory bill by how long each recipe ran | **Estimate** (see §5) |
| 6. ADF **pipeline runs** | Every dinner: start, end, minutes, Succeeded/Failed | Duration truth (ms) |
| 7. Azure **Monitor metrics** | Smart meters: how hard each appliance worked | Performance / reliability NFRs |
| 8. RG **inventory** | List of appliances you own | Existence |

Live sample from this estate (2026-07-17 run, 7-day lookback):

- **77** ADF pipeline runs attributed (e.g. governance master ~14 min share)  
- **17** Monitor metrics (ADF 69 succeeded / 8 failed; Key Vault ~73 ms avg latency; Event Hub 0 throttles; storage ~10.7 MB used)  
- Cost $ tables may show **$0** for 24–48h on sponsorship subscriptions — that is lag, not a broken tool

---

## 5. Why ADF “pipeline cost” is an estimate (and how we calculate it)

**Analogy:** the power company bills the **kitchen building**, not each recipe.  
If pasta cooked for 30 minutes and soup for 10, we *estimate* pasta used 75% of the kitchen’s ADF bill.

```text
pipeline_$ ≈ factory_MTD_$ × (pipeline_duration_ms ÷ all_pipelines_duration_ms)
```

Code: `scripts/cost_dashboard.py` → `_attribute_adf_cost`.

**Limits to teach in class:**

- A short Copy with many DIUs can cost more than a long Wait — duration ≠ DIU.  
- For exact per-run consumption: ADF Studio → Monitor → run → **Consumption**.  
- Official: [Understanding Azure Data Factory pricing](https://learn.microsoft.com/en-us/azure/data-factory/pricing-concepts)

---

## 6. Azure Monitor metrics we pull (performance NFRs)

**Analogy:** the gas bill says how much you paid; the smart meter says how hard the oven worked.

| Resource | Metrics | NFR | Save-money / ops move |
|----------|---------|-----|------------------------|
| ADF factory | PipelineSucceeded/FailedRuns, ActivityFailedRuns, TriggerSucceededRuns | Reliability | Fix failures before “optimising” cost |
| Storage account | UsedCapacity, Transactions, Ingress, Egress | Cost driver + throughput | Lifecycle Cool/Archive; fewer tiny files |
| Event Hub | Incoming/OutgoingMessages, ThrottledRequests | Scalability | Stay on Basic 1 TU while throttles = 0 |
| Key Vault | ServiceApiHit, ServiceApiLatency | Perf | Cache secrets — don’t call KV per row |
| SQL DB | dtu_consumption_percent, storage_percent, connection_successful | Rightsizing | Stay on Basic if DTU % ≈ 0 |

Code map: `METRIC_MAP` + `_monitor_metrics` in `scripts/cost_dashboard.py`.  
API: [Azure Monitor Metrics REST](https://learn.microsoft.com/en-us/rest/api/monitor/metrics/list)

**Databricks is missing on purpose:** the workspace resource does not expose useful Azure Monitor compute metrics for DBU teaching — use `system.billing` / Spark UI instead.

---

## 7. NFR dictionary (novice question → architect question → where to look)

| NFR | Novice asks | Architect asks | Tool / notebook |
|-----|-------------|----------------|-----------------|
| **Performance** | How long for one run? | p95 vs SLA | ADF duration, Spark UI, perf lab |
| **Scalability** | If data ×10? | Linear or cliff? | Partitions, skew, EH throttles, DAG lab |
| **Cost** | Are we wasting money? | $ per successful pipeline | Cost dashboard + cost notebook |
| **Reliability** | Random failures? | Error budget, retries hiding bugs? | FailedRuns metrics, ADF status |
| **Observability** | Can a stranger debug at 2am? | Shared `run_id`, logs, dashboards | Notebook logger + `summary.json` |
| **Correctness** | Are totals right? | Idempotent MERGE? | Medallion labs / reconciliations |

---

## 8. System tables — the Databricks “manager’s logbook”

**Analogy:** Unity Catalog `system` is the restaurant’s locked office logbook. Waiters (learners) cannot read it until the **owner** gives keys.

| Schema (examples) | What it tells you |
|-------------------|-------------------|
| `system.billing` | DBUs, SKU, usage → $ |
| `system.compute` | Clusters / warehouses timelines |
| `system.lakeflow` | Jobs / pipelines |
| `system.query` | Query history |
| `system.access` | Audit / lineage (modern) |
| `system.information_schema` | SQL metadata (different beast) |

Notebook: `/Shared/day9/system_tables_master` (soft-skips with synthetic samples if denied).

### 8.1 Why “I am RG Owner” is not enough

| Role you may have | Unlocks `system` catalog? |
|-------------------|---------------------------|
| Azure subscription / RG **Owner** | **No** — Azure RBAC ≠ Databricks account roles |
| Databricks **workspace admin** | **No** — clusters/notebooks only |
| Databricks **Account Admin** | Required to assign Metastore Admin |
| Databricks **Metastore Admin** | Required to `GRANT` on catalog `system` |

**Bootstrap (Microsoft rule):** an Entra **Global Administrator** signs in once to [Databricks account console](https://accounts.azuredatabricks.net), enables Account Admin for the trainer, who then becomes Metastore Admin and runs GRANTs.

Official:

- [Establish your first account admin](https://learn.microsoft.com/en-us/azure/databricks/admin/admin-concepts#--establish-your-first-account-admin)  
- [Grant access to system tables](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/#grant-access-to-system-tables)

SQL bundle (trainer / metastore admin) — also in [system_tables_master.md](system_tables_master.md):

```sql
GRANT USE CATALOG ON CATALOG system TO `account users`;
GRANT BROWSE ON CATALOG system TO `account users`;
GRANT USE SCHEMA ON SCHEMA system.billing TO `account users`;
GRANT SELECT ON SCHEMA system.billing TO `account users`;
-- repeat USE SCHEMA + SELECT for access, compute, lakeflow, query, …
```

---

## 9. Cost management notebook — levels as a story

Open `/Shared/day9/cost_management_master`. Each code cell is **1–4 lines**.

| Level | Kitchen story | Save-money move |
|-------|---------------|-----------------|
| 0 | Put a logbook on the wall | Soft runner never crashes class |
| 1 | List ovens (clusters), auto-off timers | Auto-terminate, job clusters, spot |
| 2 | Dining room (SQL warehouses) + shift schedule (jobs) | `auto_stop_mins`, right-size |
| 3 | Read DBU gas meter (`system.billing`) | Attack top SKU / top job |
| 4 | Fridge cleanup (OPTIMIZE / VACUUM / tiers) | Compact + Cool storage |
| 5 | Other appliances (ADF, EH, KV, Purview, SQL) | Cheapest viable SKU + batching |
| 6 | Live Azure bill + budgets | Real invoice by service |
| 6b | Drill: every resource + each ADF pipeline $ share | Same estimate as laptop dashboard |
| 7 | Name tags on every appliance (chargeback) | Cluster policies enforce tags |
| 8 | PhD: forecast, $/run, FinOps loop | Unit economics |

Secrets for Level 6 live Azure calls: `python scripts\ensure_cost_lab_secrets.py`  
→ keys `azure-tenant-id`, `azure-client-id`, `azure-client-secret`, `azure-subscription-id` in scope `finledger`.

---

## 10. How money is actually saved (cheat sheet)

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Cluster always “Running” overnight | No / high auto-terminate | Set 10–20 min auto-stop; prefer **job** clusters |
| Huge shuffle / spill | Skew, wrong join | DAG lab + broadcast / rebalance |
| Storage bill grows | Tiny files, hot tier forever | OPTIMIZE + lifecycle to Cool |
| KV latency / hit spikes | Secret call inside row loop | Cache once per job |
| EH throttles | Too few TUs | Scale TU **only** when throttles > 0 |
| SQL DTU always ~0% | Over-provisioned | Stay Basic |
| ADF many FailedRuns | Paid retries | Fix pipeline before FinOps polish |
| Cost API 429 in classroom | Rate limit | Dashboard retries 8→13→20→33s; sleep between queries |

---

## 11. Scenarios you will see (and what they mean)

| What you see | Meaning | What to do |
|--------------|---------|------------|
| PowerShell: `cost-dashboard.cmd` not recognized | PS does not run cwd scripts without `.\` | Run `.\cost-dashboard.cmd --open` |
| `WARNING Cost API 429 — sleep 8s…` | Cost Management throttle | Wait — auto-retry works |
| Cost tables empty, HTTP 200 | Billing lag (esp. sponsorship) | Re-run tomorrow; durations + metrics still live |
| ADF $ = 0.0 but runs listed | Factory MTD not landed yet | Duration share still ranks pipelines |
| `PERMISSION_DENIED` / no `BROWSE` on `system` | UC grants missing | Global Admin → Account Admin → Metastore Admin → GRANTs |
| Storage metrics HTTP 400 (old runs) | Capacity metrics need hourly grain | Fixed: tool uses `PT1H` for storage |

Deep scenario catalog with code pointers: [cost_dashboard_code_overview.md](cost_dashboard_code_overview.md) §D.

---

## 12. Citations & official references (what we built today stands on)

### 12.1 Cost & FinOps

1. Microsoft Learn — [Azure Cost Management + Billing documentation](https://learn.microsoft.com/en-us/azure/cost-management-billing/)  
2. Microsoft Learn — [Cost Management Query — Usage](https://learn.microsoft.com/en-us/rest/api/cost-management/query/usage) (API used by `_cost_query`)  
3. Microsoft Learn — [Group and filter options in Cost Analysis](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/group-filter) (ServiceName / ResourceId / Daily)  
4. Microsoft Learn — [Create and manage Azure budgets](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-acm-create-budgets)  
5. FinOps Foundation — [FinOps Framework](https://www.finops.org/framework/) (Inform → Optimize → Operate — Level 8 of the notebook)

### 12.2 Databricks billing & system tables

6. Microsoft Learn — [Monitor costs with system tables](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing)  
7. Microsoft Learn — [System tables overview](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/)  
8. Microsoft Learn — [Grant access to system tables](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/#grant-access-to-system-tables)  
9. Microsoft Learn — [Establish first account admin](https://learn.microsoft.com/en-us/azure/databricks/admin/admin-concepts#--establish-your-first-account-admin)  
10. Databricks — [Compute pricing / DBUs concepts](https://www.databricks.com/product/pricing) (DBU × SKU story in Levels 1–3)

### 12.3 ADF, Monitor, stack services

11. Microsoft Learn — [Data Factory pricing concepts](https://learn.microsoft.com/en-us/azure/data-factory/pricing-concepts)  
12. Microsoft Learn — [Monitor Data Factory](https://learn.microsoft.com/en-us/azure/data-factory/monitor-visually) (pipeline runs / duration)  
13. Microsoft Learn — [ADF metrics and alerts](https://learn.microsoft.com/en-us/azure/data-factory/monitor-metrics-alerts)  
14. Microsoft Learn — [Azure Monitor REST — Metrics List](https://learn.microsoft.com/en-us/rest/api/monitor/metrics/list)  
15. Microsoft Learn — [Storage account metrics](https://learn.microsoft.com/en-us/azure/storage/common/storage-metrics)  
16. Microsoft Learn — [Event Hubs metrics](https://learn.microsoft.com/en-us/azure/event-hubs/monitor-event-hubs-data-reference)  
17. Microsoft Learn — [Key Vault monitoring](https://learn.microsoft.com/en-us/azure/key-vault/general/monitor-key-vault)  
18. Microsoft Learn — [SQL Database metrics](https://learn.microsoft.com/en-us/azure/azure-sql/database/monitor-tune-overview)

### 12.4 Auth / SDK (what the Python tool uses)

19. Microsoft Learn — [Azure Identity client library](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme) (`ClientSecretCredential` / `DefaultAzureCredential`)  
20. PyPI / docs — `azure-mgmt-costmanagement`, `azure-mgmt-datafactory`, `azure-mgmt-resource`

### 12.5 Repo artefacts produced today (primary citations inside this course)

| Artefact | Path |
|----------|------|
| Cost dashboard engine | `scripts/cost_dashboard.py` |
| Windows launcher | `cost-dashboard.cmd` |
| Cost notebook builder | `day9/scripts/build_cost_management_notebook.py` |
| Cost notebook source | `day9/notebooks/nb_cost_management_master.py` |
| Deploy cost lab | `scripts/deploy_cost_management_lab.py` |
| SPN → secret scope | `scripts/ensure_cost_lab_secrets.py` |
| System tables notebook | `day9/notebooks/nb_system_tables_master.py` |
| Grant helper | `scripts/grant_system_tables_access.py` |
| Code overview (functions) | `day9/docs/cost_dashboard_code_overview.md` |

---

## 13. One-command cheat sheet

```cmd
REM From repo root D:\azure (PowerShell: use .\ before .cmd)

REM 1) Laptop dashboard + browser
.\cost-dashboard.cmd --open

REM 2) Push SPN secrets into Databricks (for notebook Level 6)
python scripts\ensure_cost_lab_secrets.py

REM 3) Deploy / refresh cost notebook
python scripts\deploy_cost_management_lab.py

REM 4) Deploy system tables teaching notebook
python scripts\deploy_system_tables_lab.py
```

Workspace paths after deploy:

- `/Shared/day9/cost_management_master`  
- `/Shared/day9/system_tables_master`  
- `/Shared/day9/perf_databricks_adf_lab`

---

## 14. Re-run scenarios (idempotent)

| Scenario | Cost dashboard | Notebooks |
|----------|----------------|-----------|
| Fresh machine | Installs packages, writes HTML/CSV | Deploy creates workspace path |
| After full success | Overwrites same output folder | Import overwrites notebook in place |
| After partial / 429 failure | Re-run; retries resume cleanly | Soft cells skip / continue |
| After teardown / delete notebook | Recreate with deploy script | Same deploy command |

---

## 15. Region & cost guardrail note

Shared class estate lives in **eastus** (documented exception to the repo’s UK-primary rule — pre-existing shared RG). SKUs stay on cheapest viable tiers (Event Hub Basic, SQL Basic, Standard LRS). The dashboard and cost notebook **are** the spend visibility gate — they warn; they do not silently invent spend.

---

## 16. Suggested 90-minute classroom path

1. **10 min** — Read §1 kitchen analogy together.  
2. **15 min** — Run `.\cost-dashboard.cmd --open`; walk tables 1→8.  
3. **15 min** — Open ADF Monitor; correlate one slow pipeline with table 6 duration.  
4. **20 min** — Run `cost_management_master` Levels 0–2 (clusters / warehouses).  
5. **15 min** — Attempt Level 3 `system.billing`; if SKIP, teach §8 grants story.  
6. **15 min** — Level 6 / 6b live Azure bill + “how we estimate pipeline $”.  
7. **Homework** — Pick one save-money row from §10 and apply it; re-run dashboard tomorrow for $ lag.

---

*End of master guide. Next file if you outgrow this: [cost_dashboard_code_overview.md](cost_dashboard_code_overview.md) for every function, then the live notebooks for hands-on cells.*
