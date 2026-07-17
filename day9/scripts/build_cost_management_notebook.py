#!/usr/bin/env python3
"""Generate nb_cost_management_master.py — end-to-end cost management lab (6–10 h).

Whole shared stack: Databricks + ADF + ADLS + Event Hub + Key Vault + Purview + SQL.
Beginner → PhD. Every code cell is 1–4 lines. Every step logged to Delta.
Live sources: Databricks REST API, system.billing (soft), Azure Cost Management REST,
Azure Monitor metrics, ADF pipeline runs.

Widgets at top: resource_group, adf_name, lookback_days (change RG without editing cells).
Existing L0–L8 cell labels are PRESERVED; this build ADDS more cells around them.

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

    # =====================================================================
    # TITLE + 6–10 HOUR AGENDA
    # =====================================================================
    md(
        p,
        """# Cost Management Master — entire data engineering stack

**One notebook · beginner → PhD · ~6–10 classroom hours.**  
Every code cell is **1–4 lines** and executable. Every step is **logged** to `cost_lab.run_log`.

| | |
|:---|:---|
| Path | `/Shared/day9/cost_management_master` |
| Stack | Databricks, ADF, ADLS, Event Hub, Key Vault, Purview, SQL |
| Live sources | Databricks REST · `system.billing` (soft) · Azure Cost Management · Monitor metrics · ADF runs |
| Docs | `day9/docs/cost_management_master.md` · master guide `finledger_cost_and_databricks_master_guide.md` |
| Twin on laptop | `cost-dashboard.cmd --open` (same RG / ADF numbers) |

### How to use the widgets (do this FIRST)

At the top of the notebook bar you will see widgets after you run **L0.0**:

| Widget | Default | Change when… |
|--------|---------|--------------|
| `resource_group` | `rg-shared-class1` | You deploy your own RG |
| `adf_name` | `adf-shared-qgr7mj` | Your factory name differs |
| `lookback_days` | `14` | You want a longer/shorter ADF/metrics window |

**Do not edit cells** to change the RG — change the widget and re-run from L0.0.

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

**Golden rule:** you cannot save what you cannot see. Measure first, then cut.

### Two invoices (never confuse them)

| Invoice | Who bills you | Analogy | Where this notebook reads it |
|---------|---------------|---------|------------------------------|
| **Databricks** | DBUs × SKU | Taxi meter (software) | Level 3 `system.billing` |
| **Azure** | VMs, ADF, storage, EH, KV, SQL… | Petrol + utilities | Level 6 Cost Management API |

### Suggested timetable (6–10 hours)

| Block | Hours | Levels | Goal |
|-------|-------|--------|------|
| A | 0–1.5 | 0–1 | Setup widgets + cluster waste detectors |
| B | 1.5–3 | 2–3 | Warehouses, jobs, `system.billing` $ |
| C | 3–4.5 | 4–5 | Storage hygiene + whole-stack save moves |
| D | 4.5–7 | 6–6c | Live Azure bill, per-resource, ADF $, Monitor |
| E | 7–9 | 7–9 | Tags, FinOps PhD, NFR×cost drills |
| F | 9–10 | 10 + Wrap | Challenges, scorecard, persist log |

**Official references (keep open in a second tab):**  
[Azure Cost Management Query API](https://learn.microsoft.com/en-us/rest/api/cost-management/query/usage) ·  
[Databricks system tables — billing](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing) ·  
[ADF pricing concepts](https://learn.microsoft.com/en-us/azure/data-factory/pricing-concepts) ·  
[FinOps Framework](https://www.finops.org/framework/)""",
    )

    # ---------- LEVEL 0 SETUP ----------
    md(
        p,
        """## Level 0 — Setup (Beginner) · ~30–45 min

### Novice story
Before you open any meter, you put a **logbook** on the wall (every finding written down)
and a **soft fuse** (if one meter is locked, the lights elsewhere still work — class never dies).

### Architect note
Observability of the *audit itself* matters: `cost_lab.run_log` is evidence you can show
a FinOps review. Soft-skip is how you teach under partial UC grants.""",
    )
    code(
        p,
        '''# L0.0 — WIDGETS: change resource group / ADF / lookback HERE (then Run All below)
dbutils.widgets.text("resource_group", "rg-shared-class1", "Azure resource group")
dbutils.widgets.text("adf_name", "adf-shared-qgr7mj", "ADF factory name")
dbutils.widgets.text("lookback_days", "14", "Lookback days (ADF + metrics)")
RG = dbutils.widgets.get("resource_group").strip() or "rg-shared-class1"
ADF = dbutils.widgets.get("adf_name").strip() or "adf-shared-qgr7mj"
LOOKBACK = int(dbutils.widgets.get("lookback_days") or "14")
print(f"RG={RG} | ADF={ADF} | lookback={LOOKBACK}d")''',
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
    code(
        p,
        '''# L0.4 — remember targets (RG/ADF from widgets — change at top, do not edit later cells)
print("Will query Cost Management on resource group:", RG)
print("ADF factory name:", ADF, "| lookback days:", LOOKBACK)
log("scope", f"rg={RG} adf={ADF} lookback={LOOKBACK}")''',
    )
    md(
        p,
        """### Checkpoint 0
You should see three widgets above and a log line `[start]…`.  
If widgets are missing, re-run **L0.0** alone, then continue.""",
    )

    # ---------- LEVEL 1 DATABRICKS COMPUTE ----------
    md(
        p,
        """## Level 1 — Databricks compute cost (Beginner) · ~60–90 min

### What actually costs money?

```text
Databricks bill = DBUs consumed × SKU rate      (Databricks software)
Azure bill      = VM seconds × VM price         (the machines underneath)
                + disks + public IPs
```

**Analogy:** DBU = taxi meter (Databricks), VM = petrol (Azure). Both tick while the engine runs — even if nobody is inside.

### Kitchen detail
- **All-purpose cluster** = open restaurant kitchen (always ready, expensive meter).  
- **Job cluster** = catering truck that appears only when an order arrives (cheaper DBU rate).  
- **Auto-terminate** = “driver, go home when I stop riding.”  
- **Spot workers** = discount petrol that can run out — fine for retryable batch.

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
    code(
        p,
        '''# L1.5 — NEW: node type + runtime (right-size evidence)
for c in clusters[:10]:
    print(f"{c['cluster_name'][:28]:28s} | node={c.get('node_type_id','?'):18s} | spark={str(c.get('spark_version','?'))[:22]}")
log("node_types", "reviewed")''',
    )
    code(
        p,
        '''# L1.6 — NEW: idle detector — RUNNING clusters with auto-stop > 60 min (soft waste)
idle_risk = [c["cluster_name"] for c in clusters if c.get("state") == "RUNNING" and (c.get("autotermination_minutes") or 999) > 60]
print("RUNNING + long auto-stop:", idle_risk or "none — good!")
log("idle_risk", idle_risk or "clean")''',
    )
    code(
        p,
        '''# L1.7 — NEW: list instance pools (shared warm VMs — good OR expensive if oversized)
pools = requests.get(f"{HOST}/api/2.0/instance-pools/list", headers=H).json().get("instance_pools", [])
for x in pools[:10]: print(x.get("instance_pool_name"), "| idle=", x.get("stats", {}).get("idle_count"), "| used=", x.get("stats", {}).get("used_count"))
log("pools", f"{len(pools)} pools")''',
    )
    code(
        p,
        '''# L1.8 — NEW: cluster events for first cluster (start/terminate = meter on/off)
cid = clusters[0]["cluster_id"] if clusters else None
ev = requests.post(f"{HOST}/api/2.0/clusters/events", headers={**H, "Content-Type": "application/json"}, json={"cluster_id": cid, "limit": 5}).json() if cid else {}
for e in (ev.get("events") or [])[:5]: print(e.get("type"), e.get("timestamp"))
log("cluster_events", f"{len(ev.get('events') or [])} events")''',
    )
    md(
        p,
        """### L1 takeaway (beginner script)

> "A cluster is a taxi with the meter running. Auto-terminate is telling the driver
> to go home when I stop riding. Job clusters are pre-booked taxis — cheaper meter."

**Action for our shared lab:** classic cluster should keep auto-terminate ≤ 30 min;
students use Serverless for SQL-only work.

### Discussion (10 min)
1. Which cluster in L1.1 has the longest auto-stop?  
2. Would you put Spot on a 9am demo cluster? Why / why not?  
3. Convert L1.4 numbers to *your* contract rates if the trainer provides them.""",
    )

    # ---------- LEVEL 2 SQL WAREHOUSES + JOBS ----------
    md(
        p,
        """## Level 2 — SQL Warehouses & Jobs (Improver) · ~45–60 min

**Warehouse analogy:** a banquet hall you rent by the minute. `auto_stop_mins`
is the caretaker locking up after the last guest.

| Waste | Fix |
|-------|-----|
| Warehouse `auto_stop` = 0 | Set 5–10 min |
| Large size for tiny queries | 2X-Small first; scale only on queue |
| Everyone shares one big WH | Small per-team WHs — isolation + attribution |
| Interactive notebook on AP cluster for BI | Prefer SQL warehouse / serverless for dashboards |""",
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
    code(
        p,
        '''# L2.4 — NEW: for each job, is it a job_cluster or existing_cluster? (cost dial)
for j in jobs[:8]:
    s = j.get("settings", {}); tc = (s.get("tasks") or [{}])[0]
    kind = "job_cluster" if "job_cluster_key" in tc or s.get("job_clusters") else ("existing" if tc.get("existing_cluster_id") else "?")
    print(f"{s.get('name','?')[:36]:36s} | {kind}")
log("job_compute_kind", "reviewed")''',
    )
    code(
        p,
        '''# L2.5 — NEW: recent runs of first job (duration = money clock)
jid = jobs[0]["job_id"] if jobs else None
runs_j = requests.get(f"{HOST}/api/2.1/jobs/runs/list?job_id={jid}&limit=5", headers=H).json().get("runs", []) if jid else []
for r in runs_j: print(r.get("state", {}).get("result_state"), "ms=", r.get("run_duration"), "start=", r.get("start_time"))
log("job_runs", f"{len(runs_j)} runs")''',
    )
    code(
        p,
        '''# L2.6 — NEW: warehouse detail — enable_serverless_compute / channel
wid = whs[0]["id"] if whs else None
wd = requests.get(f"{HOST}/api/2.0/sql/warehouses/{wid}", headers=H).json() if wid else {}
print({k: wd.get(k) for k in ("name", "cluster_size", "auto_stop_mins", "enable_serverless_compute", "state")})
log("warehouse_detail", wd.get("name"))''',
    )
    md(
        p,
        """### L2 takeaway
> Banquet hall with no caretaker = bill forever.  
> Scheduled catering truck (job cluster) beats keeping the restaurant open for one delivery.""",
    )

    # ---------- LEVEL 3 SYSTEM BILLING ----------
    md(
        p,
        """## Level 3 — `system.billing` = the official meter (Intermediate) · ~60–90 min

The **gas company's own ledger**: every DBU, every SKU, every day, 365 days back.
May be ACL-blocked in class (see system_tables_master lab §0b) — cells soft-skip.

| Question | Table |
|----------|-------|
| DBUs per day? | `system.billing.usage` |
| £/$ per DBU? | `system.billing.list_prices` |
| Cost **per job**? | `usage.usage_metadata.job_id` |
| Cost per cluster/warehouse? | `usage_metadata.cluster_id / warehouse_id` |

**Analogy:** Azure Cost Analysis is the *city utility bill*. `system.billing` is the
*taxi company's own trip log* — only Databricks can tell you which job burned DBUs.

If you see `PERMISSION_DENIED` / no `BROWSE` on catalog `system`: that is **not** a
notebook bug. Account Admin + Metastore Admin must GRANT (see master guide §8).""",
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
    code(
        p,
        '''# L3.5 — NEW: peek price list (what does 1 DBU of each SKU cost?)
prices = soft("list_prices", lambda: spark.sql("SELECT sku_name, pricing.default AS usd_per_unit, usage_unit FROM system.billing.list_prices WHERE price_end_time IS NULL ORDER BY sku_name LIMIT 30"))
if prices is not None: display(prices)''',
    )
    code(
        p,
        '''# L3.6 — NEW: warehouse DBU burn (SQL warehouse attribution)
per_wh = soft("cost per warehouse", lambda: spark.sql("SELECT usage_metadata.warehouse_id, ROUND(SUM(usage_quantity),2) dbus FROM system.billing.usage WHERE usage_metadata.warehouse_id IS NOT NULL AND usage_date >= date_add(current_date(),-14) GROUP BY 1 ORDER BY 2 DESC LIMIT 10"))
if per_wh is not None: display(per_wh)''',
    )
    code(
        p,
        '''# L3.7 — NEW: ALL_PURPOSE vs JOBS sku share (are we paying the expensive meter?)
sku_share = soft("sku share", lambda: spark.sql("SELECT CASE WHEN sku_name LIKE '%ALL_PURPOSE%' THEN 'ALL_PURPOSE' WHEN sku_name LIKE '%JOBS%' THEN 'JOBS' WHEN sku_name LIKE '%SQL%' THEN 'SQL' ELSE 'OTHER' END family, ROUND(SUM(usage_quantity),2) dbus FROM system.billing.usage WHERE usage_date >= date_add(current_date(),-14) GROUP BY 1 ORDER BY 2 DESC"))
if sku_share is not None: display(sku_share)''',
    )
    code(
        p,
        '''# L3.8 — NEW: synthetic teaching sample if billing was SKIP (class never blocked)
if usage is None:
    sample = spark.createDataFrame([( "2026-07-15","STANDARD_ALL_PURPOSE_COMPUTE",12.5),( "2026-07-15","STANDARD_JOBS_COMPUTE",4.0)], ["usage_date","sku_name","dbus"])
    print("SYNTHETIC sample — replace with live rows after UC grants"); display(sample)
    log("synthetic_billing", "shown")''',
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
        """## Level 4 — Storage & Delta hygiene (Intermediate) · ~45 min

**Analogy:** the water tank. Storage is cheap per litre but silently grows forever —
old versions, small files, abandoned tables.

| Waste | Fix | Command |
|-------|-----|---------|
| Millions of tiny files | Compaction | `OPTIMIZE t` |
| Old Delta versions kept forever | Garbage collect | `VACUUM t RETAIN 168 HOURS` |
| Cold data on hot tier | ADLS lifecycle rule → Cool/Archive | Portal / Bicep |
| Zombie tables | `DROP` after audit | information_schema inventory |

**Tiny-files story:** 10,000 shot glasses instead of one bottle — each open costs a
transaction; Spark lists forever; you pay storage *and* compute.""",
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
    code(
        p,
        '''# L4.4 — NEW: list databases (where could zombie tables hide?)
dbs = soft("show databases", lambda: spark.sql("SHOW DATABASES"))
if dbs is not None: display(dbs.limit(30))''',
    )
    code(
        p,
        '''# L4.5 — NEW: table inventory in cost_lab / perf_lab if present
tabs = soft("show tables cost_lab", lambda: spark.sql("SHOW TABLES IN cost_lab"))
if tabs is not None: display(tabs)
tabs2 = soft("show tables perf_lab", lambda: spark.sql("SHOW TABLES IN perf_lab"))
if tabs2 is not None: display(tabs2)''',
    )
    md(
        p,
        """### Portal homework (10 min)
Azure Portal → storage account in your RG → **Lifecycle management** →  
rule idea: move `bronze/tmp/` to Cool after 30 days. That is the “send winter coats to attic” move.""",
    )

    # ---------- LEVEL 5 REST OF STACK ----------
    md(
        p,
        """## Level 5 — The rest of the stack (uses widget `resource_group`) · ~45–60 min

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
    code(
        p,
        '''# L5.3 — NEW: ADF pricing story in one print (official concepts, teaching numbers)
print("ADF bills: pipeline orchestration + activity runs + DIU-hours (Data Flow) + IR hours")
print("Save: pack many files per Copy; avoid Always-On SHIR; prefer tumbling window over per-blob storm")
print("Docs: https://learn.microsoft.com/en-us/azure/data-factory/pricing-concepts")
log("adf_pricing", "concepts printed")''',
    )
    code(
        p,
        '''# L5.4 — NEW: SQL Basic vs DTU waste story
print("SQL Basic ≈ fixed few $/mo — if dtu_consumption_percent ≈ 0, do NOT scale up")
print("Scaling up unused DTU is buying a bigger fridge for empty shelves")
log("sql_rightsizing", "stay Basic while DTU%~0")''',
    )
    code(
        p,
        '''# L5.5 — NEW: Purview / governance cost reminder
print("Purview: pay for capacity + scan hours — weekly scoped scan beats continuous wide scan")
print("FinLedger pattern: governance pipelines already batch discovery (see pl_gov_*)")
log("purview", "scan schedule reminder")''',
    )

    # ---------- LEVEL 6 AZURE COST API ----------
    md(
        p,
        """## Level 6 — Live Azure bill from this notebook (Advanced) · ~90–120 min

We call **Azure Cost Management REST** with the lab service principal
(secrets pushed to the `finledger` scope). This is the *actual invoice*,
covering **every** service — not just Databricks.

```text
notebook → AAD token (SPN) → management.azure.com
        → CostManagement/query on <resource_group widget>
        → month-to-date $ per ServiceName
```

**Classroom tip:** Cost API rate-limits (HTTP 429). If you see throttling, wait and
re-run the cell — same as `cost-dashboard.cmd` backoff. Data can lag **24–48h**
(especially sponsorship). Empty $ with HTTP 200 ≠ broken auth.

**Change RG?** Edit widget `resource_group` at top — URLs below use `RG`.""",
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
        '''# L6.3 — month-to-date cost per SERVICE in resource group (the real bill) — uses widget RG
body = {"type": "ActualCost", "timeframe": "MonthToDate", "dataset": {"granularity": "None", "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}}, "grouping": [{"type": "Dimension", "name": "ServiceName"}]}}
r = requests.post(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.CostManagement/query?api-version=2023-11-01", json=body, headers={"Authorization": f"Bearer {tok}"})
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
r2 = requests.post(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.CostManagement/query?api-version=2023-11-01", json=body2, headers={"Authorization": f"Bearer {tok}"})
daily = [(str(row[1]), float(row[0])) for row in (r2.json().get("properties", {}).get("rows", []) if r2.status_code == 200 else [])]
log("azure_daily", f"{len(daily)} days")''',
    )
    code(
        p,
        '''# L6.6 — budgets on the RG: our smoke alarm (alerts at %, costs nothing)
rb = requests.get(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.Consumption/budgets?api-version=2023-05-01", headers={"Authorization": f"Bearer {tok}"})
for b in rb.json().get("value", []): print(b["name"], "| limit $", b["properties"]["amount"], "| spent $", (b["properties"].get("currentSpend") or {}).get("amount"))
log("budgets", f"http {rb.status_code}")''',
    )
    code(
        p,
        '''# L6.7 — NEW: subscription-level MTD by service (whole house, not just this RG)
import time; time.sleep(3)  # polite gap — Cost API 429s in classrooms
body_s = {"type": "ActualCost", "timeframe": "MonthToDate", "dataset": {"granularity": "None", "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}}, "grouping": [{"type": "Dimension", "name": "ServiceName"}]}}
rs = requests.post(f"https://management.azure.com/subscriptions/{SUB}/providers/Microsoft.CostManagement/query?api-version=2023-11-01", json=body_s, headers={"Authorization": f"Bearer {tok}"})
sub_rows = rs.json().get("properties", {}).get("rows", []) if rs.status_code == 200 else []
for row in sorted(sub_rows, key=lambda x: -x[0])[:12]: print(f"${row[0]:8.2f}  {row[1]}")
log("sub_mtd", f"http {rs.status_code}, {len(sub_rows)} services")''',
    )
    code(
        p,
        '''# L6.8 — NEW: Portal deep links (verify the same numbers in Azure UI)
print("Cost Analysis RG:", f"https://portal.azure.com/#@/resource/subscriptions/{SUB}/resourceGroups/{RG}/costanalysis")
print("Budgets:", "https://portal.azure.com/#view/Microsoft_Azure_CostManagement/Menu/~/budgets")
print("ADF Monitor:", f"https://adf.azure.com/en/monitoring/pipelineruns?factory=/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.DataFactory/factories/{ADF}")
log("portal_links", "printed")''',
    )
    code(
        p,
        '''# L6.9 — NEW: list ARM resources in the RG (inventory before metrics)
time.sleep(2)
arm = requests.get(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/resources?api-version=2021-04-01", headers={"Authorization": f"Bearer {tok}"})
resources = arm.json().get("value", []) if arm.status_code == 200 else []
for x in resources[:20]: print(f"{x.get('type','?'):48s}  {x.get('name')}")
log("arm_resources", f"http {arm.status_code}, {len(resources)} resources")''',
    )

    # ---------- LEVEL 6b RESOURCE + ADF PIPELINE ----------
    md(
        p,
        """## Level 6b — Drill-down: every resource + each ADF pipeline $ · ~60 min

Azure Cost Management is **daily** (not true per-minute $). Drill path:

```text
Subscription  →  Service  →  Resource  →  Day
ADF factory $ →  share by pipeline duration (estimate)
```

**Truth:** Azure bills the **Data Factory**, not each pipeline.
We estimate: `pipeline_$ = factory_mtd × (that_pipeline_ms ÷ all_pipeline_ms)`.

**Analogy:** power company bills the kitchen building; we split the bill by how long
each recipe kept the ovens on. Honest classroom estimate — not DIU accounting.

**Student laptop twin (no Databricks needed):**
```cmd
.\\cost-dashboard.cmd --open
```
Opens `docs\\cost-dashboard-out\\index.html` + CSV exports + Portal links.""",
    )
    code(
        p,
        '''# L6b.1 — MTD cost BY RESOURCE (who inside the RG is expensive?)
time.sleep(3)
body_r = {"type": "ActualCost", "timeframe": "MonthToDate", "dataset": {"granularity": "None", "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}}, "grouping": [{"type": "Dimension", "name": "ResourceId"}, {"type": "Dimension", "name": "ServiceName"}]}}
rr = requests.post(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.CostManagement/query?api-version=2023-11-01", json=body_r, headers={"Authorization": f"Bearer {tok}"})
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
        '''# L6b.3 — ADF factory MTD $ (actual bill line for widget adf_name)
factory_cost = sum(float(r[0]) for r in res_rows if ADF.lower() in str(r[1]).lower())
print(f"ADF factory MTD actual cost: ${factory_cost:.4f}"); log("adf_factory", f"${factory_cost:.4f}")''',
    )
    code(
        p,
        '''# L6b.4 — list ADF pipeline runs last LOOKBACK days (minute-level DURATION, not $)
from datetime import datetime, timedelta, timezone
end = datetime.now(timezone.utc); start = end - timedelta(days=LOOKBACK)
filt = {"lastUpdatedAfter": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "lastUpdatedBefore": end.strftime("%Y-%m-%dT%H:%M:%SZ")}
pr = requests.post(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.DataFactory/factories/{ADF}/queryPipelineRuns?api-version=2018-06-01", json=filt, headers={"Authorization": f"Bearer {tok}"})
runs = pr.json().get("value", []) if pr.status_code == 200 else []
log("adf_runs", f"http {pr.status_code}, {len(runs)} runs")''',
    )
    code(
        p,
        '''# L6b.5 — estimate $ per pipeline = factory_cost × duration share
from collections import defaultdict
agg = defaultdict(lambda: {"ms": 0, "n": 0, "fail": 0})
for run in runs:
    name = run.get("pipelineName") or "?"; ms = int(run.get("durationInMs") or 0)
    agg[name]["ms"] += ms; agg[name]["n"] += 1
    if (run.get("status") or "").lower() in ("failed", "cancelled"): agg[name]["fail"] += 1
total = sum(v["ms"] for v in agg.values()) or 1
for name, v in sorted(agg.items(), key=lambda kv: -kv[1]["ms"])[:15]:
    share = v["ms"] / total; print(f"${factory_cost*share:8.4f}  {share*100:5.1f}%  runs={v['n']:3d}  fail={v['fail']}  min={v['ms']/60000:7.1f}  {name}")
log("adf_pipeline_cost", f"{len(agg)} pipelines attributed")''',
    )
    code(
        p,
        '''# L6b.6 — NEW: print last 12 individual runs (status + minutes) — NFR reliability view
for run in runs[:12]:
    ms = int(run.get("durationInMs") or 0)
    print(f"{(run.get('status') or '?'):10s}  {ms/60000:6.2f} min  {run.get('pipelineName')}")
log("adf_run_sample", f"{min(12, len(runs))} printed")''',
    )
    code(
        p,
        '''# L6b.7 — NEW: failed-run tax — duration spent on Failed/Cancelled (paid with no value)
fail_ms = sum(int(r.get("durationInMs") or 0) for r in runs if (r.get("status") or "").lower() in ("failed", "cancelled"))
ok_ms = sum(int(r.get("durationInMs") or 0) for r in runs if (r.get("status") or "").lower() == "succeeded")
print(f"Succeeded min={ok_ms/60000:.1f} | Failed/Cancelled min={fail_ms/60000:.1f} | fail share of time={fail_ms/max(fail_ms+ok_ms,1)*100:.1f}%")
log("fail_tax", f"fail_min={fail_ms/60000:.1f}")''',
    )

    # ---------- LEVEL 6c MONITOR METRICS ----------
    md(
        p,
        """## Level 6c — Azure Monitor metrics = smart meters (Advanced) · ~45–60 min

**Analogy:** the gas bill says what you paid; the **smart meter** says how hard each
appliance worked. That is the performance / reliability NFR side of FinOps.

| Resource type | Metrics we pull | NFR |
|---------------|-----------------|-----|
| ADF | PipelineSucceeded/FailedRuns | Reliability |
| Storage | UsedCapacity, Transactions | Cost driver |
| Event Hub | Incoming/Outgoing, ThrottledRequests | Scalability |
| Key Vault | ServiceApiHit, Latency | Perf |
| SQL DB | dtu_consumption_percent, storage_percent | Rightsizing |

Same idea as laptop `cost-dashboard.py` `METRIC_MAP` — now inside Databricks.""",
    )
    code(
        p,
        '''# L6c.1 — helper: pull one metric timespan for a resource id
from datetime import datetime, timedelta, timezone
def metric(rid, names, agg="Total"):
    end = datetime.now(timezone.utc); start = end - timedelta(days=LOOKBACK)
    ts = f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    url = f"https://management.azure.com{rid}/providers/microsoft.insights/metrics?api-version=2018-01-01&metricnames={names}&timespan={ts}&interval=P1D&aggregation={agg}"
    m = requests.get(url, headers={"Authorization": f"Bearer {tok}"})
    return m.json() if m.status_code == 200 else {"error": m.status_code, "body": m.text[:160]}''',
    )
    code(
        p,
        '''# L6c.2 — find ADF resource id from inventory, then PipelineSucceededRuns / FailedRuns
adf_id = next((x["id"] for x in resources if x.get("type") == "Microsoft.DataFactory/factories" and x.get("name") == ADF), None)
print("ADF id:", adf_id)
mj = metric(adf_id, "PipelineSucceededRuns,PipelineFailedRuns") if adf_id else {"error": "no adf"}
for v in mj.get("value", []):
    pts = [p.get("total") for ts in v.get("timeseries", []) for p in ts.get("data", []) if p.get("total") is not None]
    print(v["name"]["value"], "sum=", sum(pts) if pts else None)
log("metrics_adf", "ok" if "value" in mj else mj.get("error"))''',
    )
    code(
        p,
        '''# L6c.3 — storage UsedCapacity (hourly grain required — ask PT1H)
st = next((x for x in resources if x.get("type") == "Microsoft.Storage/storageAccounts"), None)
if st:
    end = datetime.now(timezone.utc); start = end - timedelta(days=LOOKBACK)
    ts = f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    url = f"https://management.azure.com{st['id']}/providers/microsoft.insights/metrics?api-version=2018-01-01&metricnames=UsedCapacity&timespan={ts}&interval=PT1H&aggregation=Average"
    ms = requests.get(url, headers={"Authorization": f"Bearer {tok}"}).json()
    pts = [p.get("average") for v in ms.get("value", []) for t in v.get("timeseries", []) for p in t.get("data", []) if p.get("average") is not None]
    print(st["name"], "UsedCapacity avg bytes≈", round(sum(pts)/len(pts), 0) if pts else None)
log("metrics_storage", st["name"] if st else "none")''',
    )
    code(
        p,
        '''# L6c.4 — Event Hub throttles (0 = do not buy more TUs)
eh = next((x for x in resources if x.get("type") == "Microsoft.EventHub/namespaces"), None)
me = metric(eh["id"], "ThrottledRequests,IncomingMessages,OutgoingMessages") if eh else {}
for v in me.get("value", []):
    pts = [p.get("total") for ts in v.get("timeseries", []) for p in ts.get("data", []) if p.get("total") is not None]
    print(v["name"]["value"], "sum=", sum(pts) if pts else None)
log("metrics_eh", eh["name"] if eh else "none")''',
    )
    code(
        p,
        '''# L6c.5 — Key Vault hits + latency (cache secrets if hits explode)
kv = next((x for x in resources if x.get("type") == "Microsoft.KeyVault/vaults"), None)
mk = metric(kv["id"], "ServiceApiHit", "Total") if kv else {}
mk2 = metric(kv["id"], "ServiceApiLatency", "Average") if kv else {}
for label, mjx, key in (("hits", mk, "total"), ("latency", mk2, "average")):
    pts = [p.get(key) for v in mjx.get("value", []) for t in v.get("timeseries", []) for p in t.get("data", []) if p.get(key) is not None]
    print(label, (sum(pts) if key == "total" else (sum(pts)/len(pts) if pts else None)))
log("metrics_kv", kv["name"] if kv else "none")''',
    )

    # ---------- LEVEL 7 TAGGING ----------
    md(
        p,
        """## Level 7 — Tagging = who pays? (Advanced) · ~30–45 min

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
    code(
        p,
        '''# L7.3 — NEW: Azure resource tags from ARM inventory
for x in resources[:12]:
    print(f"{x.get('name','?')[:28]:28s} | tags={x.get('tags') or '{}'}")
log("arm_tags", "reviewed")''',
    )
    code(
        p,
        '''# L7.4 — NEW: Cost Management group-by TagKey if you use costCenter (may be empty)
time.sleep(3)
body_t = {"type": "ActualCost", "timeframe": "MonthToDate", "dataset": {"granularity": "None", "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}}, "grouping": [{"type": "TagKey", "name": "costCenter"}]}}
rt = requests.post(f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.CostManagement/query?api-version=2023-11-01", json=body_t, headers={"Authorization": f"Bearer {tok}"})
print("http", rt.status_code, "rows", len(rt.json().get("properties", {}).get("rows", [])) if rt.status_code == 200 else rt.text[:120])
log("cost_by_tag", rt.status_code)''',
    )

    # ---------- LEVEL 8 PHD ----------
    md(
        p,
        """## Level 8 — PhD: unit economics, forecast, chargeback · ~60 min

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
ok_runs = sum(1 for r in runs if (r.get("status") or "").lower() == "succeeded") or 22
print(f"MTD ${mtd_total:.2f} / {ok_runs} successful ADF runs = ${mtd_total/max(ok_runs,1):.4f} per successful run")
log("unit_econ", f"${mtd_total/max(ok_runs,1):.4f}/run")''',
    )
    code(
        p,
        '''# L8.3 — chargeback statement (what each team owes) — synthetic split until tags land
alloc = [("data-eng", 0.6), ("analytics", 0.3), ("training", 0.1)]
for team, share in alloc: print(f"{team:10s} owes ${mtd_total*share:7.2f}  ({int(share*100)}%)")
log("chargeback", "statement printed")''',
    )
    code(
        p,
        '''# L8.4 — NEW: matplotlib daily burn chart (empty data → teaching stub)
import matplotlib.pyplot as plt
days_x = [d[0][5:10] for d in daily] or ["d1", "d2", "d3"]
vals_y = [d[1] for d in daily] or [0.01, 0.02, 0.015]
plt.figure(figsize=(8, 3)); plt.bar(days_x, vals_y); plt.title(f"Daily RG spend — {RG}"); plt.ylabel("USD"); plt.xticks(rotation=45); plt.tight_layout(); plt.show()
log("chart", f"{len(daily)} days plotted")''',
    )
    code(
        p,
        '''# L8.5 — NEW: $ per ADF minute (unit metric from duration share world)
all_min = sum(int(r.get("durationInMs") or 0) for r in runs) / 60000.0 or 1
print(f"Factory MTD ${factory_cost:.4f} / {all_min:.1f} pipeline-minutes ≈ ${factory_cost/all_min:.6f} per ADF minute")
log("per_adf_min", factory_cost / all_min)''',
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
beat the budget alarm.

Cite: [FinOps Framework](https://www.finops.org/framework/)""",
    )

    # ---------- LEVEL 9 NFR × COST ----------
    md(
        p,
        """## Level 9 — NFR × cost drills (Architect) · ~45–60 min

**Rule:** never cut cost by skipping correctness. Use this matrix in reviews.

| NFR | Cost trap | Better move |
|-----|-----------|-------------|
| Performance | Bigger cluster “to go faster” without measuring | Measure stage time first (perf lab) |
| Reliability | Endless ADF retries on a broken sink | Fix root cause — failed minutes still bill |
| Scalability | Always-on max workers | Autoscale + spot; watch EH throttles |
| Observability | No tags / no run_id | Free; enables chargeback |
| Correctness | Skip MERGE reconciliation to “save DBUs” | False economy — bad data is most expensive |

### Architect one-liner
Optimising Spark while ADF Copy dominates wall-clock is polishing the stove while
the postman is stuck in traffic.""",
    )
    code(
        p,
        '''# L9.1 — correlate: top duration pipeline vs fail tax (from L6b)
top = max(agg.items(), key=lambda kv: kv[1]["ms"]) if agg else ("?", {"ms": 0, "fail": 0, "n": 0})
print(f"Longest pipeline: {top[0]} | min={top[1]['ms']/60000:.1f} | fails={top[1]['fail']} / runs={top[1]['n']}")
print("If fails>0: fix reliability BEFORE buying bigger DIUs / clusters")
log("nfr_top", top[0])''',
    )
    code(
        p,
        '''# L9.2 — decision card printer (fill blanks in discussion)
print("DECISION CARD")
print("1. Biggest Azure service MTD:", (sorted(mtd_rows, key=lambda x: -x[0])[0][1] if mtd_rows else "no cost data yet"))
print("2. Biggest ADF duration share:", top[0])
print("3. Clusters without auto-stop:", risky or "none")
print("4. Next save-money move this week: ________________")
log("decision_card", "printed")''',
    )
    code(
        p,
        '''# L9.3 — link to sister labs (do NOT re-implement here)
print("Perf / NFR lab: /Shared/day9/perf_databricks_adf_lab")
print("System tables:  /Shared/day9/system_tables_master")
print("Spark DAG lab:  /Shared/day9/spark_dag_problems_lab")
print("Laptop twin:    cost-dashboard.cmd --open")
log("sister_labs", "printed")''',
    )

    # ---------- LEVEL 10 CHALLENGES ----------
    md(
        p,
        """## Level 10 — Hands-on challenges (read-mostly) · ~60–90 min

Work in pairs. Prefer **new cells you add below** — do not break L0–L9.
Change **widgets** if you use another RG.

| # | Challenge | Hint |
|---|-----------|------|
| C1 | Change `lookback_days` to 7, re-run L6b.4–L6b.7, compare fail tax | Widget only |
| C2 | Identify the top 3 ADF pipelines by duration share | L6b.5 |
| C3 | Find any RUNNING cluster with auto-stop > 30 | L1.6 |
| C4 | If `system.billing` works: which SKU family dominates? | L3.7 |
| C5 | From Monitor: is Event Hub throttling? | L6c.4 |
| C6 | Write a 5-line proposal: one save worth doing this week | L9.2 card |
| C7 | Run laptop `cost-dashboard.cmd --open` and match ADF run count | Twin tool |
| C8 | Portal Cost Analysis: same RG — screenshot vs L6.4 | L6.8 link |

### Stretch (PhD)
Build a tiny chargeback table from ADF duration shares × factory_cost and
`display()` it as a Spark DataFrame named `cost_lab.chargeback_estimate`.""",
    )
    code(
        p,
        '''# L10.1 — starter: duration-share chargeback DataFrame (safe even if $0)
rows_cb = [{"pipeline": n, "share_pct": round(v["ms"] / total * 100, 2), "est_usd": round(factory_cost * v["ms"] / total, 4), "runs": v["n"]} for n, v in agg.items()] if agg else []
df_cb = spark.createDataFrame(rows_cb) if rows_cb else spark.createDataFrame([("none", 0.0, 0.0, 0)], ["pipeline", "share_pct", "est_usd", "runs"])
display(df_cb.orderBy(df_cb.est_usd.desc())); log("challenge_df", f"{df_cb.count()} rows")''',
    )
    code(
        p,
        '''# L10.2 — optional persist of chargeback estimate
soft("save chargeback", lambda: df_cb.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("cost_lab.chargeback_estimate"))''',
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
| 10 | Budget alerts 50/80/100% stay on | Everything | never surprised |

### What to cite when you present this lab
1. [Cost Management Query — Usage](https://learn.microsoft.com/en-us/rest/api/cost-management/query/usage)  
2. [Databricks system tables — billing](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing)  
3. [ADF pricing concepts](https://learn.microsoft.com/en-us/azure/data-factory/pricing-concepts)  
4. [Azure Monitor Metrics API](https://learn.microsoft.com/en-us/rest/api/monitor/metrics/list)  
5. [FinOps Framework](https://www.finops.org/framework/)  
6. Repo: `day9/docs/finledger_cost_and_databricks_master_guide.md`""",
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
        '''# W.3 — NEW: session summary for the trainer whiteboard
print("=== SESSION SUMMARY ===")
print(f"RG={RG} ADF={ADF} lookback={LOOKBACK}d")
print(f"clusters={len(clusters)} warehouses={len(whs)} jobs={len(jobs)}")
print(f"azure services with cost rows={len(mtd_rows)} adf_runs={len(runs)} arm_resources={len(resources)}")
print(f"log_steps={len(LOG)} forecast={fc}")
log("summary", f"rg={RG} steps={len(LOG)}")''',
    )
    code(
        p,
        '''# EXIT — machine-readable summary for Jobs/ADF orchestration
import json as _json
dbutils.notebook.exit(_json.dumps({"lab": "cost_management_master", "rg": RG, "adf": ADF, "steps": len(LOG), "status": "Succeeded"}))''',
    )

    OUT.write_text("\n".join(finish(p)) + "\n", encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
