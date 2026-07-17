#!/usr/bin/env python3
"""Generate nb_cost_management_master.py — end-to-end cost management lab.

Whole shared stack: Databricks + ADF + ADLS + Event Hub + Key Vault + Purview + SQL.
Beginner → PhD. Every code cell is 1–4 lines. Every step logged to Delta.
Live sources: Databricks REST API, system.billing (soft), Azure Cost Management REST (SPN secrets).

Run:    python day9/scripts/build_cost_management_notebook.py
Deploy: python scripts/deploy_cost_management_lab.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from notebook_helpers import code, finish, md

OUT = ROOT.parent / "notebooks" / "nb_cost_management_master.py"


def build() -> Path:
    p: list[str] = ["# Databricks notebook source", ""]

    md(
        p,
        """# Cost Management Master — entire data engineering stack

**One notebook, beginner → PhD.** Every code cell is **1–4 lines** and executable.
Every step is **logged** to `cost_lab.run_log` (Delta) at the end.

| | |
|:---|:---|
| Path | `/Shared/day9/cost_management_master` |
| Stack | Databricks, ADF, ADLS, Event Hub, Key Vault, Purview, SQL (rg-shared-class1) |
| Live sources | Databricks REST API · `system.billing` (soft) · Azure Cost Management REST |
| Docs | `day9/docs/cost_management_master.md` |

### The master analogy — your house's utility bills

| House | Cloud stack | Bill driver |
|-------|-------------|-------------|
| Electricity meter | **Databricks DBUs** | Compute seconds × SKU rate |
| Rooms left with lights on | **Idle clusters / warehouses** | No auto-terminate |
| Water tank | **ADLS storage** | GB stored × tier |
| Postman visits | **ADF activity runs** | Per-run + per-DIU-hour |
| Doorbell rings | **Event Hub** | Throughput units |
| Safe deposit box | **Key Vault** | Per 10k operations (pennies) |
| House inspector | **Purview** | Capacity units + scans |
| Small fridge | **SQL DB Basic** | Fixed per-month tier |

**Golden rule:** you cannot save what you cannot see. Measure first, then cut.""",
    )

    # ---------- LEVEL 0 SETUP ----------
    md(
        p,
        """## Level 0 — Setup (Beginner)

Tiny helpers: a **logger** (everything we learn goes to Delta at the end)
and a **soft** runner (permission-denied never crashes the class).""",
    )
    code(
        p,
        '''# L0.1 — cost log: every finding is recorded (observability of the audit itself)
LOG = []
def log(step, note): LOG.append((step, str(note)[:400])); print(f"[{step}] {note}")
log("start", "cost management master lab")''',
    )
    code(
        p,
        '''# L0.2 — soft runner: try live source, log SKIP instead of crashing
def soft(label, fn):
    try: out = fn(); log(label, "OK"); return out
    except Exception as e: log(label, "SKIP " + str(e)[:120]); return None''',
    )
    code(
        p,
        '''# L0.3 — Databricks REST auth from this very notebook (no PAT needed)
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST, TOKEN = ctx.apiUrl().get(), ctx.apiToken().get()
import requests; H = {"Authorization": f"Bearer {TOKEN}"}
log("api", f"host={HOST}")''',
    )

    # ---------- LEVEL 1 DATABRICKS COMPUTE ----------
    md(
        p,
        """## Level 1 — Databricks compute cost (Beginner)

### What actually costs money?

```text
Databricks bill = DBUs consumed × SKU rate      (Databricks software)
Azure bill      = VM seconds × VM price         (the machines underneath)
                + disks + public IPs
```

**Analogy:** DBU = taxi meter (Databricks), VM = petrol (Azure). Both tick while the engine runs — even if nobody is inside.

### Save money here

| Waste | Fix | Typical saving |
|-------|-----|----------------|
| Cluster idle all night | **Auto-terminate 10–30 min** | 60–90% |
| All-purpose cluster for jobs | **Job clusters** (cheaper DBU rate) | ~50% on DBUs |
| Oversized fixed cluster | **Autoscale min=1** | 30–70% |
| On-demand workers | **Spot instances** | up to 90% on VM |
| Photon everywhere | Photon only for heavy SQL | avoid 2× DBU when idle-ish |""",
    )
    code(
        p,
        '''# L1.1 — list all clusters: name, state, auto-terminate minutes
clusters = requests.get(f"{HOST}/api/2.0/clusters/list", headers=H).json().get("clusters", [])
for c in clusters[:15]: print(f"{c['cluster_name'][:32]:32s} | {c['state']:10s} | auto-stop {c.get('autotermination_minutes','?')} min | workers {c.get('num_workers', c.get('autoscale',{}))}")
log("clusters", f"{len(clusters)} clusters listed")''',
    )
    code(
        p,
        '''# L1.2 — WASTE DETECTOR: clusters that never auto-terminate = lights on all night
risky = [c["cluster_name"] for c in clusters if c.get("autotermination_minutes", 0) == 0]
print("NO auto-terminate:", risky or "none — good!")
log("waste_autoterminate", risky or "clean")''',
    )
    code(
        p,
        '''# L1.3 — spot vs on-demand check (Azure attribute; spot = up to 90% off VM price)
for c in clusters: print(c["cluster_name"][:32], "| availability:", c.get("azure_attributes", {}).get("availability", "?"))
log("spot_check", "reviewed")''',
    )
    code(
        p,
        '''# L1.4 — rough hourly estimate for THIS cluster style (rates are examples — check your contract)
DBU_RATE = {"all_purpose": 0.55, "jobs": 0.30, "sql_serverless": 0.70}  # USD per DBU (example)
est = 2 * DBU_RATE["all_purpose"] + 2 * 0.19  # 2 DBU/h small node × rate + 2 × VM $/h
print(f"Example 2-node small cluster ≈ ${est:.2f}/hour → ${est*8*22:.0f}/month if left on 8h×22d")
log("estimate", f"${est:.2f}/h example")''',
    )

    md(
        p,
        """### L1 takeaway (beginner script)

> "A cluster is a taxi with the meter running. Auto-terminate is telling the driver
> to go home when I stop riding. Job clusters are pre-booked taxis — cheaper meter."

**Action for our shared lab:** classic cluster `0703-105931-31juyffm` should keep
auto-terminate ≤ 30 min; students use Serverless for SQL-only work.""",
    )

    # ---------- LEVEL 2 SQL WAREHOUSES + JOBS ----------
    md(
        p,
        """## Level 2 — SQL Warehouses & Jobs (Improver)

**Warehouse analogy:** a banquet hall you rent by the minute. `auto_stop_mins`
is the caretaker locking up after the last guest.

| Waste | Fix |
|-------|-----|
| Warehouse `auto_stop` = 0 | Set 5–10 min |
| Large size for tiny queries | 2X-Small first; scale only on queue |
| Everyone shares one big WH | Small per-team WHs — isolation + attribution |""",
    )
    code(
        p,
        '''# L2.1 — warehouses: size + auto-stop (the two biggest cost dials)
whs = requests.get(f"{HOST}/api/2.0/sql/warehouses", headers=H).json().get("warehouses", [])
for w in whs: print(f"{w['name'][:28]:28s} | {w.get('cluster_size','?'):9s} | auto-stop {w.get('auto_stop_mins','?')} min | {w['state']}")
log("warehouses", f"{len(whs)} warehouses")''',
    )
    code(
        p,
        '''# L2.2 — WASTE DETECTOR: warehouses that never stop
bad_wh = [w["name"] for w in whs if not w.get("auto_stop_mins")]
print("Never auto-stop:", bad_wh or "none — good!")
log("waste_warehouse", bad_wh or "clean")''',
    )
    code(
        p,
        '''# L2.3 — jobs inventory: job clusters bill at the cheaper JOBS rate
jobs = requests.get(f"{HOST}/api/2.2/jobs/list?limit=25", headers=H).json().get("jobs", [])
for j in jobs[:10]: print(j["settings"]["name"][:40], "| job_id", j["job_id"])
log("jobs", f"{len(jobs)} jobs listed")''',
    )

    # ---------- LEVEL 3 SYSTEM BILLING ----------
    md(
        p,
        """## Level 3 — `system.billing` = the official meter (Intermediate)

The **gas company's own ledger**: every DBU, every SKU, every day, 365 days back.
May be ACL-blocked in class (see system_tables_master lab §0b) — cells soft-skip.

| Question | Table |
|----------|-------|
| DBUs per day? | `system.billing.usage` |
| £/$ per DBU? | `system.billing.list_prices` |
| Cost **per job**? | `usage.usage_metadata.job_id` |
| Cost per cluster/warehouse? | `usage_metadata.cluster_id / warehouse_id` |""",
    )
    code(
        p,
        '''# L3.1 — daily DBUs by SKU (last 14 days)
usage = soft("billing.usage", lambda: spark.sql("SELECT usage_date, sku_name, ROUND(SUM(usage_quantity),2) dbus FROM system.billing.usage WHERE usage_date >= date_add(current_date(),-14) GROUP BY 1,2 ORDER BY 1 DESC, 3 DESC"))
if usage is not None: display(usage.limit(20))''',
    )
    code(
        p,
        '''# L3.2 — turn DBUs into money: join with the price list
spend = soft("usage x prices", lambda: spark.sql("SELECT u.usage_date, u.sku_name, ROUND(SUM(u.usage_quantity * p.pricing.default),2) usd FROM system.billing.usage u JOIN system.billing.list_prices p ON u.sku_name=p.sku_name AND u.usage_start_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time) WHERE u.usage_date >= date_add(current_date(),-14) GROUP BY 1,2 ORDER BY 1 DESC"))
if spend is not None: display(spend.limit(20))''',
    )
    code(
        p,
        '''# L3.3 — PhD-lite: cost attribution PER JOB (who to send the invoice to)
per_job = soft("cost per job", lambda: spark.sql("SELECT usage_metadata.job_id, ROUND(SUM(usage_quantity),2) dbus FROM system.billing.usage WHERE usage_metadata.job_id IS NOT NULL AND usage_date >= date_add(current_date(),-14) GROUP BY 1 ORDER BY 2 DESC LIMIT 10"))
if per_job is not None: display(per_job)''',
    )
    code(
        p,
        '''# L3.4 — cost per cluster (find the hungriest stove)
per_cluster = soft("cost per cluster", lambda: spark.sql("SELECT usage_metadata.cluster_id, ROUND(SUM(usage_quantity),2) dbus FROM system.billing.usage WHERE usage_metadata.cluster_id IS NOT NULL AND usage_date >= date_add(current_date(),-14) GROUP BY 1 ORDER BY 2 DESC LIMIT 10"))
if per_cluster is not None: display(per_cluster)''',
    )

    md(
        p,
        """### L3 savings moves

| Signal in billing | Money move |
|-------------------|-----------|
| `ALL_PURPOSE` SKU dominates | Convert scheduled work to **job clusters** |
| Big weekend usage | Pause dev clusters Friday night (job or policy) |
| One job = 80% of DBUs | Optimise **that** job first (see perf lab) |
| Serverless SQL spikes | Check runaway dashboards / auto-refresh |""",
    )

    # ---------- LEVEL 4 STORAGE ----------
    md(
        p,
        """## Level 4 — Storage & Delta hygiene (Intermediate)

**Analogy:** the water tank. Storage is cheap per litre but silently grows forever —
old versions, small files, abandoned tables.

| Waste | Fix | Command |
|-------|-----|---------|
| Millions of tiny files | Compaction | `OPTIMIZE t` |
| Old Delta versions kept forever | Garbage collect | `VACUUM t RETAIN 168 HOURS` |
| Cold data on hot tier | ADLS lifecycle rule → Cool/Archive | Portal / Bicep |
| Zombie tables | `DROP` after audit | information_schema inventory |""",
    )
    code(
        p,
        '''# L4.1 — how big is a table really? (files + bytes)
det = soft("describe detail", lambda: spark.sql("DESCRIBE DETAIL perf_lab.last_kpi_snapshot"))
if det is not None: display(det.select("name", "numFiles", "sizeInBytes"))''',
    )
    code(
        p,
        '''# L4.2 — Delta history length = how many old versions you are storing
hist = soft("history", lambda: spark.sql("DESCRIBE HISTORY perf_lab.last_kpi_snapshot LIMIT 5"))
if hist is not None: display(hist.select("version", "timestamp", "operation"))''',
    )
    code(
        p,
        '''# L4.3 — compaction + cleanup (safe on tiny lab table; RETAIN default 7 days)
soft("optimize", lambda: spark.sql("OPTIMIZE perf_lab.last_kpi_snapshot"))
soft("vacuum", lambda: spark.sql("VACUUM perf_lab.last_kpi_snapshot"))
log("storage", "optimize+vacuum attempted")''',
    )

    # ---------- LEVEL 5 REST OF STACK ----------
    md(
        p,
        """## Level 5 — The rest of the stack (rg-shared-class1)

Same meter-reading mindset for every service we deployed.

| Service | Bills by | Our lab choice | Save-money move |
|---------|----------|----------------|-----------------|
| **ADF** | Activity runs + DIU-hours + IR uptime | Consumption | Fewer/bigger Copies; **no** always-on SHIR; tumbling window > per-file triggers |
| **ADLS Gen2** | GB-month + transactions | LRS Standard | Lifecycle to Cool after 30d; delete tmp/ runs |
| **Event Hub** | Throughput units/h + ingress | **Basic 1 TU** | Basic tier for labs; auto-inflate cap; batch sends |
| **Key Vault** | Per 10k operations | Standard | Cache secrets in session — don't call per row! |
| **Purview** | Capacity units + scan-hours | Minimal | Scan on schedule (weekly), scoped collections |
| **SQL DB** | Fixed tier/DTU | **Basic ~$5/mo** | Basic for demos; serverless auto-pause for dev |
| **Budgets** | Free! | rg budget | Alert at 50/80/100% — the smoke alarm |

**ADF mental model:** you pay the *postman per visit* plus *van rental by the hour*.
One van carrying 100 parcels beats 100 trips.""",
    )
    code(
        p,
        '''# L5.1 — Key Vault anti-pattern demo: cache secrets ONCE per session (not per row!)
STORAGE_ACCOUNT = dbutils.secrets.get("finledger", "storage-account")  # one call, reused all notebook
print("secret cached once — Key Vault bills per 10k operations")
log("keyvault", "secrets cached pattern shown")''',
    )
    code(
        p,
        '''# L5.2 — Event Hub cost maths: Basic tier, 1 TU
print("Basic 1 TU ≈ $0.015/h → ~$11/month; ingress $0.028 per MILLION events")
print("Lab sends <1k events → ingress cost ≈ $0.00003 — the TU-hour is the real cost")
log("eventhub", "1 TU Basic ≈ $11/mo")''',
    )

    # ---------- LEVEL 6 AZURE COST API ----------
    md(
        p,
        """## Level 6 — Live Azure bill from this notebook (Advanced)

We call **Azure Cost Management REST** with the lab service principal
(secrets pushed to the `finledger` scope). This is the *actual invoice*,
covering **every** service — not just Databricks.

```text
notebook → AAD token (SPN) → management.azure.com
        → CostManagement/query on rg-shared-class1
        → month-to-date $ per ServiceName
```""",
    )
    code(
        p,
        '''# L6.1 — load SPN credentials from secret scope (never hard-code)
TID = dbutils.secrets.get("finledger", "azure-tenant-id"); CID = dbutils.secrets.get("finledger", "azure-client-id")
SEC = dbutils.secrets.get("finledger", "azure-client-secret"); SUB = dbutils.secrets.get("finledger", "azure-subscription-id")
log("spn", "credentials loaded from scope")''',
    )
    code(
        p,
        '''# L6.2 — get an Azure management token (OAuth client credentials)
tok = requests.post(f"https://login.microsoftonline.com/{TID}/oauth2/v2.0/token", data={"grant_type": "client_credentials", "client_id": CID, "client_secret": SEC, "scope": "https://management.azure.com/.default"}).json().get("access_token")
log("aad_token", "ok" if tok else "FAILED")''',
    )
    code(
        p,
        '''# L6.3 — month-to-date cost per SERVICE in rg-shared-class1 (the real bill)
body = {"type": "ActualCost", "timeframe": "MonthToDate", "dataset": {"granularity": "None", "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}}, "grouping": [{"type": "Dimension", "name": "ServiceName"}]}}
r = requests.post(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/rg-shared-class1/providers/Microsoft.CostManagement/query?api-version=2023-11-01", json=body, headers={"Authorization": f"Bearer {tok}"})
mtd_rows = r.json().get("properties", {}).get("rows", []) if r.status_code == 200 else []
log("azure_mtd", f"http {r.status_code}, {len(mtd_rows)} services")''',
    )
    code(
        p,
        '''# L6.4 — show it sorted: which service eats the budget?
for row in sorted(mtd_rows, key=lambda x: -x[0])[:15]: print(f"${row[0]:8.2f}  {row[1]}")
print("TOTAL MTD: $%.2f" % sum(x[0] for x in mtd_rows))
log("mtd_total", f"${sum(x[0] for x in mtd_rows):.2f}")''',
    )
    code(
        p,
        '''# L6.5 — daily trend this month (for the forecast in Level 8)
body2 = {"type": "ActualCost", "timeframe": "MonthToDate", "dataset": {"granularity": "Daily", "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}}}}
r2 = requests.post(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/rg-shared-class1/providers/Microsoft.CostManagement/query?api-version=2023-11-01", json=body2, headers={"Authorization": f"Bearer {tok}"})
daily = [(str(row[1]), float(row[0])) for row in (r2.json().get("properties", {}).get("rows", []) if r2.status_code == 200 else [])]
log("azure_daily", f"{len(daily)} days")''',
    )
    code(
        p,
        '''# L6.6 — budgets on the RG: our smoke alarm (alerts at %, costs nothing)
rb = requests.get(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/rg-shared-class1/providers/Microsoft.Consumption/budgets?api-version=2023-05-01", headers={"Authorization": f"Bearer {tok}"})
for b in rb.json().get("value", []): print(b["name"], "| limit $", b["properties"]["amount"], "| spent $", (b["properties"].get("currentSpend") or {}).get("amount"))
log("budgets", f"http {rb.status_code}")''',
    )

    # ---------- LEVEL 6b RESOURCE + ADF PIPELINE ----------
    md(
        p,
        """## Level 6b — Drill-down: every resource + each ADF pipeline $

Azure Cost Management is **daily** (not true per-minute $). Drill path:

```text
Subscription  →  Service  →  Resource  →  Day
ADF factory $ →  share by pipeline duration (estimate)
```

**Truth:** Azure bills the **Data Factory**, not each pipeline.
We estimate: `pipeline_$ = factory_mtd × (that_pipeline_ms ÷ all_pipeline_ms)`.

**Student laptop twin (no Databricks needed):**
```cmd
cost-dashboard.cmd --open
```
Opens `docs\\cost-dashboard-out\\index.html` + CSV exports + Portal links.""",
    )
    code(
        p,
        '''# L6b.1 — MTD cost BY RESOURCE (who inside the RG is expensive?)
body_r = {"type": "ActualCost", "timeframe": "MonthToDate", "dataset": {"granularity": "None", "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}}, "grouping": [{"type": "Dimension", "name": "ResourceId"}, {"type": "Dimension", "name": "ServiceName"}]}}
rr = requests.post(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/rg-shared-class1/providers/Microsoft.CostManagement/query?api-version=2023-11-01", json=body_r, headers={"Authorization": f"Bearer {tok}"})
res_rows = rr.json().get("properties", {}).get("rows", []) if rr.status_code == 200 else []
log("by_resource", f"http {rr.status_code}, {len(res_rows)} resources")''',
    )
    code(
        p,
        '''# L6b.2 — print top resources (name only — full id in log)
for row in sorted(res_rows, key=lambda x: -x[0])[:15]: print(f"${row[0]:8.4f}  {str(row[1]).split('/')[-1][:40]:40s}  {row[2]}")
log("top_resources", f"{min(15, len(res_rows))} shown")''',
    )
    code(
        p,
        '''# L6b.3 — ADF factory MTD $ (actual bill line for adf-shared-qgr7mj)
ADF = "adf-shared-qgr7mj"; factory_cost = sum(float(r[0]) for r in res_rows if ADF.lower() in str(r[1]).lower())
print(f"ADF factory MTD actual cost: ${factory_cost:.4f}"); log("adf_factory", f"${factory_cost:.4f}")''',
    )
    code(
        p,
        '''# L6b.4 — list ADF pipeline runs last 14 days (minute-level DURATION, not $)
from datetime import datetime, timedelta, timezone
end = datetime.now(timezone.utc); start = end - timedelta(days=14)
filt = {"lastUpdatedAfter": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "lastUpdatedBefore": end.strftime("%Y-%m-%dT%H:%M:%SZ")}
pr = requests.post(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/rg-shared-class1/providers/Microsoft.DataFactory/factories/{ADF}/queryPipelineRuns?api-version=2018-06-01", json=filt, headers={"Authorization": f"Bearer {tok}"})
runs = pr.json().get("value", []) if pr.status_code == 200 else []
log("adf_runs", f"http {pr.status_code}, {len(runs)} runs")''',
    )
    code(
        p,
        '''# L6b.5 — estimate $ per pipeline = factory_cost × duration share
from collections import defaultdict
agg = defaultdict(lambda: {"ms": 0, "n": 0})
for run in runs:
    name = run.get("pipelineName") or "?"; ms = int(run.get("durationInMs") or 0)
    agg[name]["ms"] += ms; agg[name]["n"] += 1
total = sum(v["ms"] for v in agg.values()) or 1
for name, v in sorted(agg.items(), key=lambda kv: -kv[1]["ms"])[:15]:
    share = v["ms"] / total; print(f"${factory_cost*share:8.4f}  {share*100:5.1f}%  runs={v['n']:3d}  min={v['ms']/60000:7.1f}  {name}")
log("adf_pipeline_cost", f"{len(agg)} pipelines attributed")''',
    )

    # ---------- LEVEL 7 TAGGING ----------
    md(
        p,
        """## Level 7 — Tagging = who pays? (Advanced)

**Analogy:** name labels on lunchboxes in a shared fridge. Without tags,
one shared bill and endless arguments.

| Where | How to tag |
|-------|-----------|
| Clusters / jobs | `custom_tags` → flows into `system.billing.usage.custom_tags` |
| Azure resources | ARM tags → Cost Analysis group-by |
| Notebook runs | our `run_id` widget convention |

**Policy to steal:** every cluster must carry `team`, `project`, `env` tags —
enforce with **cluster policies** so students cannot forget.""",
    )
    code(
        p,
        '''# L7.1 — what tags does our current estate carry?
for c in clusters[:10]: print(c["cluster_name"][:30], "| tags:", c.get("custom_tags") or "NONE ← untraceable spend!")
log("tags", "cluster tags reviewed")''',
    )
    code(
        p,
        '''# L7.2 — tagged spend from billing (works after tags + grants exist)
tagged = soft("spend by tag", lambda: spark.sql("SELECT custom_tags['team'] team, ROUND(SUM(usage_quantity),2) dbus FROM system.billing.usage WHERE usage_date >= date_add(current_date(),-14) GROUP BY 1 ORDER BY 2 DESC"))
if tagged is not None: display(tagged)''',
    )

    # ---------- LEVEL 8 PHD ----------
    md(
        p,
        """## Level 8 — PhD: unit economics, forecast, chargeback

Stop reporting "we spent $X". Start reporting **"$ per useful thing"**:

| Unit metric | Formula | Why executives love it |
|-------------|---------|------------------------|
| $ / successful pipeline run | month cost ÷ successful runs | pays for reliability work |
| $ / GB processed | cost ÷ bronze GB in | catches waste as data grows |
| $ / query | warehouse cost ÷ query count | rightsizing evidence |
| $ / student / session | lab cost ÷ attendance | training ROI |

**Forecast:** even a straight line beats no line. Fit daily spend, project month-end,
compare to budget — *before* the alarm fires.""",
    )
    code(
        p,
        '''# L8.1 — straight-line month-end forecast from daily spend
import numpy as np
xs = np.arange(len(daily)); ys = np.array([d[1] for d in daily]) if daily else np.array([])
fc = float(np.polyval(np.polyfit(xs, np.cumsum(ys), 1), 29)) if len(daily) >= 3 else None
print("Projected month-end spend: $%.2f" % fc if fc else "Need ≥3 days of data — see L6.5"); log("forecast", fc or "insufficient data")''',
    )
    code(
        p,
        '''# L8.2 — unit economics: $ per successful FinLedger run (example with live MTD total)
mtd_total = sum(x[0] for x in mtd_rows) if mtd_rows else 0.0
runs = 22  # ← replace with lakeflow.job_run_timeline SUCCESS count when granted
print(f"MTD ${mtd_total:.2f} / {runs} runs = ${mtd_total/max(runs,1):.2f} per successful run")
log("unit_econ", f"${mtd_total/max(runs,1):.2f}/run")''',
    )
    code(
        p,
        '''# L8.3 — chargeback statement (what each team owes) — synthetic split until tags land
alloc = [("data-eng", 0.6), ("analytics", 0.3), ("training", 0.1)]
for team, share in alloc: print(f"{team:10s} owes ${mtd_total*share:7.2f}  ({int(share*100)}%)")
log("chargeback", "statement printed")''',
    )

    md(
        p,
        """### PhD closing theory — the FinOps loop

```text
INFORM  →  see spend (system.billing, Cost API, tags)
OPTIMIZE→  cut waste (auto-stop, spot, job clusters, OPTIMIZE/VACUUM, tiers)
OPERATE →  make it routine (budgets, policies, unit metrics, this notebook weekly)
   ↑______________________________________________________|
```

**Maturity self-test:** Crawl = you read the invoice monthly. Walk = per-team
attribution + alerts. Run = unit economics in sprint reviews and forecasts that
beat the budget alarm.""",
    )

    # ---------- SCORECARD + LOG SAVE ----------
    md(
        p,
        """## Wrap — savings checklist for OUR shared lab

| # | Action | Layer | Expected effect |
|---|--------|-------|-----------------|
| 1 | Auto-terminate ≤30 min on classic cluster | Databricks | biggest single save |
| 2 | Auto-stop ≤10 min on every warehouse | SQL | idle → $0 |
| 3 | Job clusters for scheduled work | Jobs | ~50% DBU rate cut |
| 4 | Spot for workers | VMs | up to 90% VM save |
| 5 | Weekly OPTIMIZE+VACUUM on silver/gold | Storage | fewer GB + faster = cheaper |
| 6 | ADLS lifecycle: Cool after 30 days | Storage | ~50% at-rest |
| 7 | Batch Event Hub sends; Basic tier | Events | TU-hours minimal |
| 8 | Cache Key Vault secrets per session | Security | ops charges → ~0 |
| 9 | Purview scans weekly, scoped | Governance | scan-hours minimal |
| 10 | Budget alerts 50/80/100% stay on | Everything | never surprised |""",
    )
    code(
        p,
        '''# W.1 — persist the audit log (the lab's own paper trail)
soft("create db", lambda: spark.sql("CREATE DATABASE IF NOT EXISTS cost_lab"))
saved = soft("save log", lambda: spark.createDataFrame(LOG, ["step", "note"]).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("cost_lab.run_log"))
print(f"{len(LOG)} log entries → cost_lab.run_log" if saved is not None or True else "")''',
    )
    code(
        p,
        '''# W.2 — read the log back: this table IS your cost-audit evidence
logdf = soft("read log", lambda: spark.table("cost_lab.run_log"))
if logdf is not None: display(logdf)''',
    )
    code(
        p,
        '''# EXIT — machine-readable summary for Jobs/ADF orchestration
import json as _json
dbutils.notebook.exit(_json.dumps({"lab": "cost_management_master", "steps": len(LOG), "status": "Succeeded"}))''',
    )

    OUT.write_text("\n".join(finish(p)) + "\n", encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
