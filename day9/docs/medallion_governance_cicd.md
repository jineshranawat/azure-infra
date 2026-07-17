# Medallion + SQL metadata + Purview — Databricks & CI/CD

**Goal:** Run full **data engineering** on Databricks (medallion bronze → silver → gold), export a **SQL metadata registry** for ADF/Azure SQL, and publish a **Purview governance manifest** that includes **all 50 problem outputs**.

---

## A. Components

| Component | What | Where |
|-----------|------|--------|
| **50 problems + capstone** | Each `prob*` table + Part K medallion/Purview | `nb_50_data_engineering_problems` |
| **Standalone E2E** | Medallion-only notebook | `nb_medallion_governance_e2e` |
| **ADF Databricks** | ADF triggers medallion notebook | `pl_db_14_databricks_medallion_governance` |
| **SQL metadata** | `de_table_registry`, `de_pipeline_run_log` | Azure SQL westus |
| **Purview manifest** | Glossary, classifications, lineage, 50 assets | `audit/governance/*.json` |
| **CI/CD release** | Build → deploy → job | `release-medallion-governance.cmd` |

---

## B. Medallion paths (shared estate)

```text
bronze/loaded/run=session3-lab/sample_transactions.csv
    → silver/transactions/run=session3-lab  (Delta)
    → gold/daily_channel_summary           (Delta)
audit/quarantine/run=session3-lab          (bad rows)
audit/governance/                          (manifest + SQL export)
```

---

## C. Purview concepts (all covered)

- **Data Map** — manifest `data_assets[]` lists bronze/silver/gold + every `prob*` table  
- **Lineage** — `LINEAGE_EDGES` accumulated in Problems 32+ capstone  
- **Glossary** — `Medallion.Bronze/Silver/Gold`, `FinLedger.Transaction`  
- **Classifications** — `FinLedger.Internal` on demo + medallion assets  
- **Ownership** — `owner: data-platform` in asset records  
- **Scan** — Get Metadata equivalent via `build_problem_inventory()`  
- **ADF link** — `shared-adf-lab` `purview_link.py` + `pl_gov_06`

---

## D. One-command release (CI/CD)

```cmd
cd d:\azure
release-medallion-governance.cmd
```

**Steps:**
1. `build_all_notebooks.py` — regenerates 50-problems notebook with Part K capstone  
2. `deploy-shared-lab.cmd` — bronze upload, secrets, import notebooks to `/Shared/day9/`  
3. `shared-adf-lab\orchestrate.cmd --skip-sql` — ADF governance pipelines + `pl_db_14`  
4. `run_medallion_governance_job.py` — Databricks job `finledger-medallion-governance`

**Flags:**
- `--skip-run` — deploy only, no job  
- `--run-only` — submit job only  
- `--skip-build` / `--skip-deploy` — partial release  

---

## E. Run in Databricks Studio

| Notebook | Path |
|----------|------|
| 50 problems + capstone | `/Shared/day9/50_data_engineering_problems` |
| Medallion E2E | `/Shared/day9/medallion_governance_e2e` |

After capstone, verify artifacts:

```text
abfss://audit@{storage}.dfs.core.windows.net/governance/databricks_governance_manifest.json
abfss://audit@{storage}.dfs.core.windows.net/governance/de_table_registry.csv
```

---

## F. SQL metadata tables (Azure SQL)

| Table | Purpose |
|-------|---------|
| `dbo.de_table_registry` | One row per problem asset + medallion path |
| `dbo.de_pipeline_run_log` | Bronze/silver/gold row counts per run |
| `dbo.adf_job_metadata` | `job-03` = medallion governance Databricks job |

Seeded by `shared-adf-lab` deploy (`seed_de_governance_tables`).

---

## G. Code map

| File | Role |
|------|------|
| `day9/scripts/governance_medallion_helpers.py` | Helper cells injected into 50-problems notebook |
| `day9/scripts/problems_50_data.py` | Problems 32, 33, 48, 50 use governance helpers |
| `day9/scripts/build_50_problems_notebook.py` | Injects helpers + Part K capstone |
| `scripts/release_medallion_governance.py` | CI/CD orchestration |
| `scripts/run_medallion_governance_job.py` | Databricks job submit |
| `shared-adf-lab/notebooks/nb_medallion_governance_master.py` | ADF `pl_db_14` notebook |

---

## H. Re-run scenarios

| Scenario | Command |
|----------|---------|
| Fresh machine | `release-medallion-governance.cmd` |
| After success | Same — idempotent deploy + overwrite manifests |
| Notebook only | `run-50-problems.cmd` or Studio Run all |
| ADF path | `orchestrate.cmd --run-pipeline pl_db_14_databricks_medallion_governance` |

---

*Aligned with `governance_medallion_helpers.py` and `release-medallion-governance.cmd`.*
