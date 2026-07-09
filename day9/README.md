# Day 9+ — Lakehouse write layer (Sessions 9–15)

**After** the Day 8 master notebook (`/Shared/day7-day8/master_pyspark_complete`), these **seven separate notebooks** cover persisting data to the lake, Delta, time travel, MERGE, gold, and orchestration.

Aligned to [docs/data-engineering-course.html](../docs/data-engineering-course.html) sessions 9–15.

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
| **50** | `50_data_engineering_problems` | **All 50 problems → solutions** with UC tables in `main.demo_problems` |

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
| `scripts/build_all_notebooks.py` | Generates all 7 `.py` notebooks |
| `scripts/notebook_helpers.py` | Small-cell builders |
| `scripts/session_diagrams.py` | Mermaid flows for sessions 9–15 |
| `scripts/bronze_auth_cell.py` | Cell 0 auth (shared estate) |
