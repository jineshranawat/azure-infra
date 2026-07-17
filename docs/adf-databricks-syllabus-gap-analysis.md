# Syllabus gap analysis — ADF & Databricks (vs `docs/data-engineering-course.html`)

**Reference syllabus:** [docs/data-engineering-course.html](../data-engineering-course.html)  
*(Note: there is no `databricks-batch-course.html` in the repo — this maps the 60-hour FinLedger course.)*  
**Excludes:** Purview deep-dive (Sessions 24–25) — covered separately in `shared-adf-lab/docs/purview_portal_search_guide.md`.

---

## A. Executive summary — what to teach next

**Added (Zach Wilson / Event processors):** `/Shared/day9/finledger_event_processors` + [day9/docs/finledger_event_processors.md](../day9/docs/finledger_event_processors.md) · transcript [finledger_event_processors_transcript.md](../day9/docs/finledger_event_processors_transcript.md)  
— Event schema → processors → runner → daily aggregates → pivot ([EcZachly / personal-health-analytics](https://github.com/EcZachly/personal-health-analytics)). Covers **data modeling + multi-source processing** between Sessions 14–19.

You already have **far more ADF pipelines than the syllabus lists** (~70+ deployed). The gap is not “more Copy activities” — it is **session-shaped teaching paths** that match Sessions **15–19, 22, 27–30**:

| Priority | Session | Gap | Recommended add |
|----------|---------|-----|-----------------|
| **P1** | 16 | Azure SQL JDBC from **Databricks** | Notebook `nb_session16_sql_jdbc` + ADF `pl_db_15_sql_gold_publish` |
| **P1** | 15 | Databricks **SQL** (`%sql`) on Delta | Notebook `nb_session15_databricks_sql` (split from combined session15); **or** run `finledger_event_processors` §6 SQL check |
| **Done** | 14–19 | Cross-source Event model (Zach pattern) | `nb_finledger_event_processors` — teach after medallion, before / with ADF Jobs |
| **P1** | 18 | **Multi-task Job** (bronze→silver→gold) | Job definition + `pl_db_16_trigger_databricks_job` |
| **P1** | 30 | **Capstone new feed** | `pl_30_capstone_new_feed` + notebook `nb_capstone_returns_feed` |
| **P2** | 17 | Unity Catalog **GRANT** lab | Notebook `nb_session17_unity_catalog_grants` |
| **P2** | 19 | ADF **schedule trigger** (nightly) | `tr_schedule_nightly_medallion` (Stopped by default) |
| **P2** | 27 | Data quality **gate** before gold | `pl_27_quality_gate_metadata` (If + Get Metadata row count) |
| **P2** | 28 | **Morning-check** script | `scripts/morning_check.py` (ADF runs + Databricks jobs) |
| **P3** | 22 | **pytest + chispa** | `day9/tests/test_dedupe.py` + notebook pointer |
| **P3** | 29 | SQL secret + **RESTORE** chain | Add `sql-password` to scope + `pl_29_restore_demo` |

---

## B. Syllabus session → what exists today

### Phase B — Databricks core (Sessions 9–14) ✅ Mostly done

| Session | Syllabus topic | Lab asset | Status |
|---------|----------------|-----------|--------|
| 9 | Silver + quarantine | `day9/nb_session09_silver_quarantine` | ✅ |
| 10 | Delta basics | `nb_session10_delta_basics` | ✅ |
| 11 | Time travel | `nb_session11_time_travel` | ✅ |
| 12 | MERGE upsert | `nb_session12_merge_upsert` | ✅ |
| 13 | Medallion E2E | `nb_session13_medallion_e2e` | ✅ |
| 14 | Gold reporting | `nb_session14_gold_reporting` | ✅ |

**ADF mirror (optional teaching):** `pl_gov_06` runs medallion via Data Flows — good for Session 13–14 **orchestration** angle.

---

### Phase C — Serve, SQL & ADF (Sessions 15–19) ⚠️ Gaps

| Session | Syllabus topic | Databricks | ADF | Gap |
|---------|----------------|------------|-----|-----|
| **15** | Databricks SQL (`%sql`) | Combined in `nb_session15_sql_jobs_adf` (demo) | — | **Split:** dedicated `%sql` notebook with `CREATE TABLE … LOCATION` + `SELECT` on gold Delta |
| **16** | Azure SQL + Python JDBC | ❌ No JDBC read/write notebook | `pl_08_sql_to_lake`, `pl_09_lake_to_sql` | **Databricks side missing** — syllabus shows `spark.read.jdbc` / `write.jdbc` from notebook |
| **17** | Unity Catalog grants | Partial in 50-problems (29–33) | — | **Dedicated UC lab:** `CREATE CATALOG`, `GRANT SELECT`, lineage tab |
| **18** | Databricks Jobs | Single-task job in `databricks_infra.py`; failed `finledger-medallion-governance` job | `pl_db_11` Job Preview | **Multi-task job** bronze→silver→gold not codified |
| **19** | ADF orchestration | Notebooks wired | **70+ pipelines** — see mapping below | Syllabus still says “10 pipelines” — update teaching **path**, not count |

#### Session 19 — recommended ADF teaching path (2-hour class)

Already deployed — use this order:

```text
1. pl_01_bronze_copy              — Copy + parameters
2. pl_02_get_metadata             — discovery
3. pl_07_databricks_notebook      — Databricks activity
4. pl_10_parent_chain             — Execute Pipeline
5. pl_12_sql_metadata_orchestrate — SQL-driven orchestration (Session 16 tie-in)
6. pl_gov_06_master_medallion_governance — capstone medallion (Session 13–14)
```

Expression/trigger extras (if time):

```text
pl_ex_11_debug_print_variables    — debug
pl_trig_01_tumbling_window_bronze   — schedule concept (start trigger in Studio)
```

---

### Phase D — 50 Problems (Sessions 20–23) ✅ Done

| Session | Topic | Asset | Status |
|---------|-------|-------|--------|
| 20–21 | 50 problems | `/Shared/day9/50_data_engineering_problems` + Part K capstone | ✅ |
| 22 | pytest + performance | Problems 5, 11, 45 in notebook | ⚠️ **No `pytest`/`chispa` test file in repo** |
| 23 | Cost control | Problems 26–28 + `orchestrate.cmd --warm-cluster-only` | ✅ partial |

---

### Phase E — Govern, cost, operate (Sessions 26–29) ⚠️ Gaps (excl. Purview)

| Session | Topic | Exists | Missing |
|---------|-------|--------|---------|
| 26 | FinOps cost report | `scripts/verify_cost.py` at repo root | Wire into `day9` README + 15-min demo |
| 27 | Data quality as code | Problem 08, `pl_20_validate_incoming` | **ADF quality gate** before gold write |
| 28 | Morning-check observability | Problem 50 e2e table | **`morning_check.py`** — ADF Monitor + Databricks SDK |
| 29 | RESTORE + secrets | Session 11, Problem 49 | **`sql-password` in secret scope** for JDBC demo |

---

### Phase F — Capstone (Session 30) ⚠️ Partial

| Syllabus requirement | Status |
|---------------------|--------|
| New CSV feed | `returns_raw.csv` exists — `pl_25`/`pl_26` use it in ADF only |
| Parametrised notebook bronze→gold | `nb_session13` / medallion helpers — not a **new feed** story |
| Quality check + test | Problem 08 pattern — not wired to capstone feed |
| UC register gold | Partial in 50-problems |
| **Orchestrate via ADF** | `pl_gov_06`, `pl_db_14` — not returns feed end-to-end |
| Time travel rollback demo | Session 11 — not in capstone chain |

**Recommended capstone package:**

```text
ADF:  pl_30_capstone_returns_feed  (Copy returns → Execute nb_capstone → SQL publish)
DBx:  nb_capstone_returns_feed     (bronze returns → silver → gold + assert_quality)
```

---

## C. ADF — what you have vs what syllabus still needs

### ✅ Already covered (teach, don’t rebuild)

| ADF concept | Pipeline(s) | Folder |
|-------------|-------------|--------|
| Copy | `pl_01`, `pl_03`, `pl_06` | 01-core-copy |
| Get Metadata | `pl_02`, `pl_gov_05` | 02-discovery / governance |
| If / Switch | `pl_03`, `pl_17` | core / advanced |
| ForEach | `pl_04`, `pl_db_07` | iteration |
| Lookup watermark | `pl_05`, `pl_23`, `pl_db_08` | control |
| Execute Pipeline | `pl_10`, `pl_gov_06` | orchestration |
| Databricks Notebook | `pl_07`, `pl_db_*` | integration |
| Databricks Job (Preview) | `pl_db_11`, `pl_db_13` | integration |
| SQL cross-region | `pl_08`, `pl_09`, `pl_12` | 05-integration-sql |
| Mapping Data Flow | `pl_13`, `pl_14`, `pl_gov_02/03` | 06-dataflows |
| Incremental / CDC | `pl_23`, `pl_24` | 08-incremental-cdc |
| Triggers | `tr_tumbling_*`, `tr_storage_*` | 12-triggers |
| Expressions | `pl_ex_01`–`pl_ex_13` | 11-expression-builder |
| Governance medallion | `pl_gov_01`–`pl_gov_06` | 13-governance-purview |
| SQL metadata-driven | `pl_12` | 05-integration-sql |

### ❌ Missing ADF teaching modules (add these)

| # | Module | Pipeline name | Teaches |
|---|--------|---------------|---------|
| 1 | **Schedule trigger (cron)** | `tr_schedule_nightly_bronze` + `pl_01` | Session 19 — production scheduling |
| 2 | **Quality gate** | `pl_27_quality_gate_copy` | Session 27 — If row count &lt; threshold → Fail |
| 3 | **Capstone returns feed** | `pl_30_capstone_returns_feed` | Session 30 — new feed end-to-end |
| 4 | **Databricks Job orchestration** | `pl_db_16_medallion_job_chain` | Session 18 — Job not notebook cluster |
| 5 | **SQL JDBC publish from ADF** | `pl_db_15_sql_gold_publish` | Session 16 — after Databricks writes gold, ADF copies to SQL |
| 6 | **Alert on failure** | `pl_28_webhook_on_failure` | Session 28 — Web activity / log append (no real Teams needed) |
| 7 | **Global parameters** | already `g_lab_environment` | Session 19 — point to `adf_global_parameters.py` in class |

**Folder suggestion:** `14-capstone-session30` for `pl_30` and `pl_db_15`–`pl_db_16`.

---

## D. Databricks — what you have vs what syllabus still needs

### ✅ Already covered

| Topic | Asset |
|-------|-------|
| Bronze auth + ADLS | `bronze_auth_cell.py`, all session notebooks |
| Medallion | `nb_session13`, `nb_medallion_governance_e2e` |
| 50 problems | `50_data_engineering_problems` + Part K |
| Secrets scope | `finledger` via `deploy-shared-lab.cmd` |
| Jobs (single task) | `databricks_infra.py`, `run_medallion_governance_job.py` |
| ADF params → widgets | `nb_adf_params_echo`, `nb_medallion_governance_master` (fixed widgets) |
| Delta MERGE / time travel | Sessions 11–12, Problems 15/49 |

### ❌ Missing Databricks teaching modules (add these)

| # | Notebook | Session | Teaches |
|---|----------|---------|---------|
| 1 | `nb_session15_databricks_sql` | 15 | `%sql` on external Delta; SQL vs PySpark equivalence |
| 2 | `nb_session16_sql_jdbc` | 16 | `spark.read.jdbc` / `write.jdbc` to `finledger` |
| 3 | `nb_session17_unity_catalog_grants` | 17 | `CREATE CATALOG`, `GRANT`, lineage tab |
| 4 | `nb_session18_jobs_multitask` | 18 | 3-task job: bronze → silver → gold notebooks |
| 5 | `nb_session27_quality_gate` | 27 | `assert_quality()` fail-fast before gold |
| 6 | `nb_session28_morning_check` | 28 | SDK: list last 5 job runs + print status |
| 7 | `nb_capstone_returns_feed` | 30 | New returns CSV → medallion → UC gold |
| 8 | `day9/tests/test_dedupe.py` | 22 | pytest + chispa on Problem 01 logic |

---

## E. Suggested 2-hour lesson plans (ADF + Databricks combined)

### Session 15 — Databricks SQL (no ADF required)

1. Run `nb_session15_databricks_sql` — register silver Delta, query with `%sql`
2. Compare same query in PySpark cell
3. Optional: open SQL Warehouse in UI (awareness only)

### Session 16 — Azure SQL + orchestration

1. **Databricks:** `nb_session16_sql_jdbc` — read `dbo.dim_channel`, write gold snapshot
2. **ADF:** `pl_09_lake_to_sql` — lake → SQL from orchestrator side
3. **ADF:** `pl_12_sql_metadata_orchestrate` — metadata table drives run/skip

### Session 18 — Jobs vs ADF

1. Create **multi-task Job** in UI (or `nb_session18_jobs_multitask` deploy script)
2. **ADF:** `pl_db_16_medallion_job_chain` triggers Job instead of notebook
3. Compare: Job = transform scheduling; ADF = cross-service orchestration

### Session 30 — Capstone

```text
Trainer script (90 min):
  1. Drop returns CSV to bronze/incoming/returns/
  2. Run nb_capstone_returns_feed (Databricks)
  3. Trigger pl_30_capstone_returns_feed (ADF)
  4. Monitor → SQL dbo.gold_channel_daily row count
  5. RESTORE TABLE demo (Session 29 callback)
```

---

## F. What NOT to add (syllabus says skip or awareness only)

| Topic | Reason |
|-------|--------|
| Structured Streaming / Auto Loader lab | Syllabus: 5-min awareness at Session 30 only |
| Synapse / Fabric pipelines | Not in 60-hour ADF/Databricks track |
| Second Purview account in shared RG | Cost; use `pviewrohan4hnv7s` + classic portal |
| More Copy-only pipelines | Already 10+ Copy variants — teach paths instead |

---

## G. Quick reference — deploy commands per session block

```cmd
REM Sessions 9–14 (Databricks)
deploy-shared-lab.cmd

REM Sessions 15–19 + governance (ADF + Databricks notebooks)
deploy-shared-lab.cmd
deploy-shared-adf-lab.cmd

REM Session 20–21 (50 problems)
run-50-problems.cmd

REM Session 30 capstone (after pl_30 built)
.\orchestrate.cmd --run-pipeline pl_30_capstone_returns_feed
```

---

## H. Priority build order (for repo work)

If you want the smallest set of **new code** that unlocks the most syllabus:

1. `nb_session16_sql_jdbc` + secret `sql-password` in scope  
2. `nb_session15_databricks_sql` (split from session15)  
3. `pl_30_capstone_returns_feed` + `nb_capstone_returns_feed`  
4. `tr_schedule_nightly_bronze` (Stopped)  
5. `pl_db_16_medallion_job_chain` + multi-task job JSON  
6. `scripts/morning_check.py`  
7. `day9/tests/test_dedupe.py`  

---

*Aligned with `docs/data-engineering-course.html` Sessions 15–30 and `shared-adf-lab` deploy state (July 2026).*
