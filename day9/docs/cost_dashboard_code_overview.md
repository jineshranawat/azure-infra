# Cost Dashboard — Detailed Code Overview (scripts/cost_dashboard.py)

Read this top-to-bottom and you will understand **every scenario, every Azure API,
every metric, and every NFR** the tool covers — and exactly **where in the code**
each one lives.

**Run it:** `cost-dashboard.cmd --open` (repo root) · Quick guide: [cost_dashboard.md](cost_dashboard.md)

---

## A. WHAT & WHY — the concept before the code

### The one-paragraph story

Azure gives you four different "truth sources" about money and performance.
This tool calls **all four** with one command and merges them into one HTML page:

| # | Truth source | Question it answers | Azure API |
|---|--------------|---------------------|-----------|
| 1 | **Cost Management Query** | "How many $ did X cost?" | `Microsoft.CostManagement/query` |
| 2 | **ADF run history** | "Which pipeline ran, how long, did it fail?" | `factories/{f}/queryPipelineRuns` |
| 3 | **Azure Monitor metrics** | "How hard did each resource work?" | `microsoft.insights/metrics` |
| 4 | **ARM resource list** | "What exists in the RG at all?" | `resourceGroups/{rg}/resources` |

**Analogy — the household audit:** the *bank statement* (Cost Management) tells you
what you paid; the *postman's logbook* (ADF runs) tells you every delivery and how long
it took; the *smart-meter readings* (Monitor metrics) show which appliance worked hard;
the *inventory list* (ARM) confirms what appliances you even own.
Only together do they explain *why* the bill looks like it does.

### Why cost alone is not enough (NFR view)

A $0.50 bill with 8 failed pipelines is **worse** than a $2 bill with zero failures —
you paid for retries and got nothing. That is why the dashboard pairs **cost** (NFR:
cost efficiency) with **metrics** (NFRs: performance, reliability, scalability).

---

## B. NFR → data → code map

Every non-functional requirement, its evidence source, and the exact function:

| NFR | Evidence | Function in code | Output artifact |
|-----|----------|------------------|-----------------|
| **Cost efficiency** | MTD $ by service / resource / day | `_cost_query` | `rg_by_service.csv`, `rg_by_resource.csv`, `rg_daily.csv` |
| **Cost attribution** | $ per ADF pipeline (duration share) | `_attribute_adf_cost` | `adf_pipeline_cost_estimate.csv` |
| **Performance** | pipeline `duration_min`, KV `ServiceApiLatency` | `_adf_pipeline_runs`, `_monitor_metrics` | `adf_pipeline_runs.csv`, `monitor_metrics.csv` |
| **Reliability** | `PipelineFailedRuns`, `succeeded/failed` counts | `_monitor_metrics`, `_attribute_adf_cost` | HTML tables 5–7 |
| **Scalability** | EH `ThrottledRequests`, SQL `dtu_consumption_percent` | `_monitor_metrics` | `monitor_metrics.csv` |
| **Observability** | everything logged + `summary.json` | `main` | `summary.json` |
| **Resilience of the tool itself** | 429 retry with backoff, soft-fails | `_cost_query` retry loop | console WARNINGs |
| **Security** | SPN from `.env`/az login, never hard-coded | `_credential` | — |

---

## C. Function-by-function walkthrough

Open `scripts/cost_dashboard.py` side-by-side. Order follows the file.

### C.1 Configuration & constants

```34:37:scripts/cost_dashboard.py
REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "docs" / "cost-dashboard-out"
DEFAULT_RG = "rg-shared-class1"
DEFAULT_ADF = "adf-shared-qgr7mj"
```

WHAT: shared-lab defaults so students type nothing.
WHY: overridable via `--resource-group` / `--adf-name` for their own RGs.

### C.2 `_load_env` — .env parsing

WHAT: reads `KEY=VALUE` lines from repo `.env`, merges OS environment on top.
WHY: same `.env` the whole repo uses (single source of truth) — no duplicate config.
SCENARIO: no `.env` at all → returns just OS env → tool falls back to `az login`.

### C.3 `_credential` — who am I?

WHAT: if `.env` has `AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET`
→ `ClientSecretCredential` (service principal). Otherwise `DefaultAzureCredential`
(picks up `az login`, VS Code, managed identity).
WHY: **works on all student machines** — SPN for the shared lab, az-login for personal subs.
SCENARIO TABLE:

| Student machine state | What happens |
|----------------------|--------------|
| Cloned repo with `.env` | SPN auth, zero interaction |
| No `.env`, `az login` done | CLI token used |
| Neither | Clear `SystemExit` telling them to `az login` |

### C.4 `_cost_query` — the heart (Cost Management Query API)

WHAT: one generic function for **every** cost question. Parameters:

| Parameter | Meaning | Used for |
|-----------|---------|----------|
| `scope` | RG or subscription ARM path | tables 1–4 |
| `timeframe` | `MonthToDate` or `Custom\|from\|to` | MTD vs lookback |
| `granularity` | `None` (total) or `Daily` | table 3 daily trend |
| `group_by` | `ServiceName`, `ResourceId` | drill-down level |
| `filter_resource_id` | restrict to one resource | ADF factory $ |
| `retries` | 429 backoff attempts | resilience |

HOW the retry works (the WARNINGs you saw in the terminal):

```text
attempt 1 → 429 → sleep 8s
attempt 2 → 429 → sleep 13s   (8 × 1.6)
attempt 3 → 429 → sleep 20s
attempt 4 → 429 → sleep 33s
attempt 5 → success or give up EMPTY (never crash)
```

WHY 429 happens: Cost Management allows only a handful of queries/minute per tenant
**per client type** — a classroom of students hammering the same API hits it fast.
That is also why `main()` sleeps 3s between the five queries.

KNOWN PITFALL (fixed): custom time windows are passed as `Custom|from|to` with a
**pipe** separator — ISO timestamps contain `:` so splitting on `:` produced the
`TO date: 1/1/0001 is before FROM date` BadRequest you saw. The values are parsed
with `datetime.fromisoformat` before being sent.

SCENARIO TABLE:

| Symptom | Cause | Tool behaviour |
|---------|-------|----------------|
| `429 Too many requests` | tenant/classroom rate limit | exponential backoff ×5 |
| Empty rows, HTTP 200 | cost data lags 24–48 h (sponsorship subs even more) | prints "(no rows …)" note |
| `BadRequest Invalid query` | malformed window | fixed; falls back MTD |
| `401/403` | SPN lacks Cost Management Reader | logged, table empty |

### C.5 `_days_window` — custom lookback

WHAT: builds `Custom|2026-07-11T00:00:00Z|2026-07-17T23:59:59Z` for `--days N`.
WHY: pipe separator (see pitfall above).

### C.6 `_list_rg_resources` — ARM inventory

WHAT: `ResourceManagementClient.resources.list_by_resource_group`.
WHY: (a) the inventory table, (b) the **input list for metrics** — we only query
metrics for resource types we know (see C.9 `METRIC_MAP`).

### C.7 `_adf_pipeline_runs` — minute-level run history

WHAT: `pipeline_runs.query_by_factory` with a `RunFilterParameters` window.
Returns pipeline name, status, start/end, **duration_ms** (this is your true
minute-level detail — Azure billing is daily, but *durations* are millisecond-exact).

KNOWN PITFALL (fixed): the SDK returns a `PipelineRunsQueryResponse` object —
you must read `.value` and follow `.continuation_token` pages; iterating the
response directly raises `'PipelineRunsQueryResponse' object is not iterable`.

### C.8 `_attribute_adf_cost` — $ per pipeline (the estimate)

WHAT: groups runs by pipeline; each pipeline's share of **total run duration**
multiplies the factory's actual MTD cost:

```text
pipeline_$ ≈ factory_MTD_$ × (pipeline_duration_ms ÷ Σ all_pipeline_duration_ms)
```

WHY an estimate: Azure meters the **Data Factory** (activity runs, DIU-hours,
IR-hours) — it never invoices per pipeline. Duration share is the most honest
classroom proxy. The HTML labels it *estimate* explicitly.

LIMITS (teach these):
- A short Copy with 32 DIUs can cost more than a long Wait — duration ≠ DIU.
- Debug runs bill like triggered runs.
- For exact per-activity billing maths, open ADF Monitor → run → **Consumption** button.

### C.9 `METRIC_MAP` + `_monitor_metrics` — Azure Monitor (performance NFRs)

WHAT: per resource type, the metric names + aggregation we pull via plain REST
(`microsoft.insights/metrics`, no extra SDK):

| Resource type | Metrics | NFR they prove |
|---------------|---------|----------------|
| `Microsoft.DataFactory/factories` | `PipelineSucceededRuns`, `PipelineFailedRuns`, `ActivityFailedRuns`, `TriggerSucceededRuns` | reliability |
| `Microsoft.Storage/storageAccounts` | `UsedCapacity` (avg), `Transactions`, `Ingress`, `Egress` | cost driver + throughput |
| `Microsoft.EventHub/namespaces` | `IncomingMessages`, `OutgoingMessages`, `ThrottledRequests` | scalability (throttle = TU ceiling) |
| `Microsoft.KeyVault/vaults` | `ServiceApiHit`, `ServiceApiLatency` (avg) | perf + the "cache secrets" lesson |
| `Microsoft.Sql/servers/databases` | `dtu_consumption_percent` (avg), `storage_percent` (avg), `connection_successful` | rightsizing evidence |

HOW: token from the same credential (`credential.get_token(...)`), one GET per
resource, `interval=P1D` (`PT1H` for storage accounts — their capacity metrics
reject daily grain, the 400 you may see logged), then **Total** metrics are summed
and **Average** metrics averaged over the window.

WHY Databricks is absent: the workspace resource exposes no Azure Monitor metrics —
Databricks-side truth lives in `system.billing` / Spark UI (see the notebook labs).

READING THE RESULTS (live sample from our estate):

| Signal seen | Interpretation | Money/NFR action |
|-------------|----------------|------------------|
| `PipelineFailedRuns = 8` | paid runs that delivered nothing | fix before optimising |
| `ServiceApiHit = 53` | healthy; thousands/day = code calling KV per row | cache secrets |
| `dtu_consumption_percent ≈ 0` | Basic tier is already oversized — good | stay on Basic |
| `ThrottledRequests = 0` | 1 TU suffices | don't buy TUs |
| `UsedCapacity` rising | lake growing | lifecycle to Cool |

### C.10 `_portal_links` — official UI deep links

WHAT: Cost Analysis (RG + subscription), Budgets blade, ADF Monitor.
WHY: the tool is a teaching mirror — the portal is where architects verify.

### C.11 Rendering & exports

- `_write_csv` — one CSV per table (Excel-friendly).
- `_table_html` / `_render_html` — static HTML, zero JS dependencies, summary
  cards (RG MTD, sub MTD, ADF factory $, run count) + 8 tables.
- `_print_table` — console mirror so it works over SSH/classroom projector.

### C.12 `main` — orchestration order

```text
env → credential → subscription
→ 5 cost queries (3s apart, each with 429 backoff)
→ ADF runs → per-pipeline attribution
→ ARM inventory → Monitor metrics
→ console tables → CSVs → index.html → summary.json → optional browser open
```

Every stage is soft: one failed source never blanks the others.

### C.13 Bootstrap block (`if __name__ == "__main__"`)

WHAT: try imports; on `ImportError` pip-install `azure-identity`,
`azure-mgmt-costmanagement`, `azure-mgmt-datafactory`, `azure-mgmt-resource`.
WHY guardrail §1: a clean student laptop with only Python must succeed with
**one command** — `cost-dashboard.cmd` (which also prefers the repo `.venv`).

---

## D. End-to-end scenario catalog

| # | Scenario | What you'll see | Why | Fix / action |
|---|----------|-----------------|-----|--------------|
| 1 | Fresh laptop, only Python | packages auto-install, dashboard builds | bootstrap block | none |
| 2 | `cost-dashboard.cmd` "not recognized" in PowerShell | CommandNotFound | PS doesn't run cwd scripts | type `.\cost-dashboard.cmd` |
| 3 | 429 storm | WARNING sleeps 8/13/20/33s | classroom rate limit | wait — auto-retries |
| 4 | All $ tables empty, HTTP 200 | "(no rows …)" | cost lag 24–48h on sponsorship | re-run tomorrow; durations/metrics still live |
| 5 | ADF table full, $ = 0.0 | factory MTD $0 | same lag | duration share still ranks pipelines |
| 6 | Storage metrics 400 | WARNING for `stshared…` | capacity metrics need PT1H | handled — storage uses PT1H |
| 7 | SPN 403 on Cost API | cost tables empty, metrics fine | SPN lacks Cost Reader role | grant *Cost Management Reader* at sub |
| 8 | Own RG instead of shared | — | defaults | `--resource-group rg-you --adf-name adf-you` |
| 9 | Longer window | — | default 14d | `--days 30` |
| 10 | Automation / CI | parse `summary.json` | machine-readable exit | wire into a Job/pipeline |

---

## E. Where the same truths live elsewhere (cross-reference)

| Question | This tool | Databricks notebook | Azure Portal |
|----------|-----------|---------------------|--------------|
| $ by service/resource/day | tables 1–3 | `cost_management_master` L6/L6b | Cost Analysis |
| $ per ADF pipeline | table 5 | L6b.5 | Monitor → run → Consumption |
| DBU spend / per job | — (no Databricks metrics in Azure) | L3 (`system.billing`) | Databricks account console |
| Perf metrics | table 7 | — | each resource → Metrics blade |
| Budgets | link | L6.6 | Budgets blade |

Notebook: `/Shared/day9/cost_management_master` · System tables grants:
[system_tables_master.md](system_tables_master.md) §fix.

---

## F. Extending the tool (exercises)

1. **New metric:** add a `(name, aggregation)` tuple to `METRIC_MAP` — nothing else.
2. **New resource type:** add key to `METRIC_MAP` (find names: portal → resource →
   Metrics → metric dropdown).
3. **Per-activity ADF drill:** call `activity_runs.query_by_pipeline_run` per run id
   inside `_adf_pipeline_runs` (mind API rate limits).
4. **Trend chart:** feed `rg_daily.csv` into the matplotlib cell of the notebook.
5. **Budget gate:** read `summary.json` in CI, fail the build if `rg_mtd_total`
   exceeds a threshold — that is `verify_cost.py`'s job today (SKU allow-list + MTD).

---

## G. Guardrail compliance

| Guardrail | How this tool satisfies it |
|-----------|---------------------------|
| §1 one entry point, clean Windows | `cost-dashboard.cmd` bootstraps deps, no policy edits |
| §2 idempotent re-run | read-only APIs; output folder overwritten in place |
| §3 teach-first docs | this file + [cost_dashboard.md](cost_dashboard.md) |
| §4 docs-as-code | this file committed with the code |
| §6 cost guardrails | the tool **is** the cost gate; region exception (eastus shared lab) documented |
| §7 V&V | verified live: 77 runs attributed, 13 metrics collected, HTML+CSV written |
