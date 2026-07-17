# Day 9+ — Lakehouse write layer (Sessions 9–15)

**After** the Day 8 master notebook (`/Shared/day7-day8/master_pyspark_complete`), these **seven separate notebooks** cover persisting data to the lake, Delta, time travel, MERGE, gold, and orchestration.

Aligned to [docs/data-engineering-course.html](../docs/data-engineering-course.html) sessions 9–15.

> **Start here for Cost + Databricks (one file):**  
> **[docs/finledger_cost_and_databricks_master_guide.md](docs/finledger_cost_and_databricks_master_guide.md)** — kitchen analogies, every NFR, system tables grants, laptop dashboard, citations to Microsoft Learn / FinOps, and the exact open/run commands.

## A. What & why (before code)

Also builds **`file_formats_and_secrets`** — CSV/JSON/Parquet/Delta + secret scope steps.

| Session | Notebook | What you learn |
|---------|----------|----------------|
| **0** | `file_formats_and_secrets` | Secret scope setup + file format guide (start here if new) |
| **9** | `session09_silver_quarantine` | Cast, split good/quarantine, **write** to `silver` + `audit` |
| **10** | `session10_delta_basics` | Why Delta, `_delta_log`, write modes, read back |
| **11** | `session11_time_travel` | `DESCRIBE HISTORY`, `versionAsOf`, `RESTORE` |
| **12** | `session12_merge_upsert` | `MERGE` upsert, idempotent re-run |
| **13** | `session13_medallion_e2e` | `run_medallion(run_date)` bronze → gold |
| **14** | `session14_gold_reporting` | Daily rollup, pivot, partitioned gold |
| **15** | `session15_sql_jobs_adf` | SQL on Delta, Unity Catalog, Jobs, ADF (watch/demo) |
| **50** | `50_data_engineering_problems` | **All 50 problems → solutions** + **Part K capstone** (medallion + SQL metadata + Purview) |
| **Gov** | `medallion_governance_e2e` | Standalone medallion + Purview manifest + SQL export |
| **Zach** | `finledger_event_processors` | **v3 ABFSS-only** Event → gold pivot (same Cell 0 as 50-problems; **no DBFS**; use classic cluster) |
| **Teach** | `dbx_teach_all_topics` | **50 teach topics** — hard scenarios explained simply with analogies + commented code (trainer walkthrough) |
| **Adv** | `advanced_pyspark_master` | **200+ advanced PySpark/Delta APIs** + system tables + cost charts + dashboards |
| **EH** | `eventhub_finledger_lab` | **Event Hub step-by-step** — send dummy pulses → receive → Spark process (beginner-friendly) |
| **Perf** | `perf_databricks_adf_lab` | **Performance / NFR debug** — system tables, Spark explain, ADF Monitor correlation |
| **DAG** | `spark_dag_problems_lab` | **Spark DAG issues → graph → fix** (skew, shuffle, spill, broadcast, …) |
| **Sys** | `system_tables_master` | **Complete system tables lab** — every major `system.*` schema step-by-step with analogies |
| **Cost** | `cost_management_master` | **End-to-end cost management** — whole stack, 1–4-line cells, beginner→PhD, live Azure bill |

**Governance guide:** [docs/medallion_governance_cicd.md](docs/medallion_governance_cicd.md) — CI/CD via `release-medallion-governance.cmd` from repo root.  
**Event processors (Zach pattern):** [docs/finledger_event_processors.md](docs/finledger_event_processors.md) · **Trainer transcript:** [docs/finledger_event_processors_transcript.md](docs/finledger_event_processors_transcript.md)  
**Spark DAG problems lab:** [docs/spark_dag_problems_lab.md](docs/spark_dag_problems_lab.md) · `/Shared/day9/spark_dag_problems_lab`  
**Perf + ADF NFR lab:** [docs/perf_databricks_adf_lab.md](docs/perf_databricks_adf_lab.md) · `/Shared/day9/perf_databricks_adf_lab`  
**System tables master:** [docs/system_tables_master.md](docs/system_tables_master.md) · `/Shared/day9/system_tables_master` · deploy `python scripts/deploy_system_tables_lab.py`  
**Cost + Databricks master guide (open this first):** [docs/finledger_cost_and_databricks_master_guide.md](docs/finledger_cost_and_databricks_master_guide.md)  
**Cost management master:** [docs/cost_management_master.md](docs/cost_management_master.md) · `/Shared/day9/cost_management_master` · deploy `python scripts/deploy_cost_management_lab.py`  
**Cost dashboard (any student PC):** [docs/cost_dashboard.md](docs/cost_dashboard.md) · run `.\cost-dashboard.cmd --open` from repo root → `docs/cost-dashboard-out/index.html` · **code walkthrough:** [docs/cost_dashboard_code_overview.md](docs/cost_dashboard_code_overview.md)  
**Event Hub lab (step-by-step):** [docs/eventhub_finledger_lab.md](docs/eventhub_finledger_lab.md) · `/Shared/day9/eventhub_finledger_lab` · setup `python scripts/ensure_eventhub_lab.py`

**Trainer primer (read before Problem 01):** [docs/connectivity_and_key_classes_primer.md](docs/connectivity_and_key_classes_primer.md) — `abfss://`, `DataLakeServiceClient`, three-method auth chain, `StringIO` → pandas → Spark. This content is also embedded in the 50-problems notebook after the bronze auth cell.

Each notebook uses **small cells**: WHAT → WHY → Mermaid diagram → HOW → ANALOGY → short code.

**Watch-only Sunday (4 h):** Sessions **9–12**.  
**Full track (~10 h):** Sessions **9–15**.

## B. Quick start

```cmd
cd day9
orchestrate.cmd
orchestrate.cmd --deploy
```

From repo root:

```cmd
deploy-shared-lab.cmd
```

## C. Databricks paths

| Workspace path |
|----------------|
| `/Shared/day9/session09_silver_quarantine` |
| `/Shared/day9/session10_delta_basics` |
| … through `session15_sql_jobs_adf` |
| `/Shared/day9/50_data_engineering_problems` |
| `/Shared/day9/finledger_event_processors` |
| `/Shared/day9/dbx_teach_all_topics` |

**Prerequisite:** Secret scope `finledger`, bronze CSV at `bronze/loaded/run=session3-lab/`.  
**Cluster:** Single-user attached cluster (same as Day 8).

## D. Re-run scenarios

| Scenario | Safe? |
|----------|-------|
| Fresh — nothing written yet | Yes — run 9 → 10 → … in order |
| Re-run after full success | Yes — `overwrite` / MERGE idempotent |
| Re-run one session only | Yes — each notebook bootstraps bronze read |
| After RESTORE in Session 11 | Re-run 12+ if needed |

**Re-run command:** `deploy-shared-lab.cmd` then Run all on the notebook in Databricks.

## E. Code map

| File | Purpose |
|------|---------|
| `scripts/build_all_notebooks.py` | Generates all 7 `.py` notebooks + file_formats + 50_problems + **teach-all** |
| `scripts/build_dbx_teach_all_notebook.py` | Generates `nb_dbx_teach_all_topics.py` (50 analogy topics) |
| `scripts/dbx_topics_teach.py` | Topic data: theory + analogy + code |
| `scripts/notebook_helpers.py` | Small-cell builders |
| `scripts/session_diagrams.py` | Mermaid flows for sessions 9–15 |
| `scripts/bronze_auth_cell.py` | Cell 0 auth (shared estate) |
