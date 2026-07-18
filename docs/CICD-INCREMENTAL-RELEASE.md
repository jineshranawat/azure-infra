# CI/CD — Check-in and incremental release (ADF + Databricks)

> **Classroom projector (preferred):** open **[infra-cicd-walkthrough.html](infra-cicd-walkthrough.html)** — full release table + ADF/Databricks steps + citations.  
> This Markdown is the editable source twin.

**Who this is for:** every developer / student who changes ADF pipelines or Databricks notebooks and needs a **safe, repeatable release** without rebuilding Azure from scratch.

**One hub command:**

```cmd
cd D:\azure
.\release.cmd
.\release.cmd databricks
.\release.cmd adf
.\release.cmd medallion-deploy
```

**Deep infra walkthrough:** [INFRA-WALKTHROUGH-20H.md](INFRA-WALKTHROUGH-20H.md) · **Medallion-only detail:** [../day9/docs/medallion_governance_cicd.md](../day9/docs/medallion_governance_cicd.md)

---

## 0. Analogy first (30 seconds)

| Idea | Analogy |
|------|---------|
| **Git check-in** | Save the new recipe in the shared cookbook (repo) |
| **Release** | Put updated recipe books on the kitchen shelf + update the manager’s checklist |
| **Incremental** | Renovate one room — do **not** demolish the restaurant |
| **ADF release** | Reprint the manager’s timetable (pipelines) |
| **Databricks release** | Replace recipe books under `/Shared/day9/` (overwrite) |
| **Full medallion release** | Rebuild books → restock shelf → update timetable → optionally cook one dinner |

**Golden rule:** check in to `main` first, then run the **smallest** release that matches what you changed.

---

## A. What changed? → which release?

| You changed… | Check-in paths (examples) | Release command | Touches |
|--------------|---------------------------|-----------------|--------|
| Day 9 notebook builder / notebook | `day9/scripts/build_*.py`, `day9/notebooks/*.py` | `release.cmd databricks` | Databricks only |
| Day 8 / shared lab deploy assets | `scripts/deploy_shared_lab.py`, day8 notebooks | `release.cmd databricks` | Databricks + bronze/secrets if deploy-shared-lab runs |
| ADF pipeline JSON / Python generators | `shared-adf-lab/**` | `release.cmd adf` | ADF (`--skip-sql`) |
| SQL schema / SQL Bicep / first-time SQL | `infra/shared-sql-westus.bicep`, SQL scripts | `release.cmd adf-full` | ADF + SQL |
| Medallion + governance (notebook **and** ADF gov) | day9 + `shared-adf-lab` governance | `release.cmd medallion-deploy` | Build + DBX + ADF, **no** job |
| Same + run the job now | as above | `release.cmd medallion` | Above + Databricks job (**costs DBUs**) |
| Bicep / RG / new Azure resource | `infra/shared-eastus.bicep` | `.\infra-walkthrough.cmd --phase 1` | Infra (rare) |

---

## B. Standard check-in (every change)

### B.1 Before you code
1. `git pull` on `main` (trunk-based — no long-lived branches for class).  
2. Confirm you are on the **shared** estate story (`rg-shared-class1`), not a personal RG mix-up.

### B.2 Make the change
- Prefer editing **builders / source** (e.g. `day9/scripts/build_*.py`, ADF lab scripts), then generate artifacts.  
- Update docs in the **same** change (docs-as-code).

### B.3 Commit + push (both remotes)

```cmd
git status
git add -A
git commit -m "Clear why + which guardrail"
git push origin main
git push class main
```

**Never commit:** `.env`, keys, `docs/cost-dashboard-out/`, personal secrets.

### B.4 Release (pick from §A)

```cmd
.\release.cmd medallion-deploy
```

### B.5 Verify (§E)

```cmd
.\release.cmd verify
```

Then open ADF Studio / Databricks Workspace and confirm.

---

## C. Databricks incremental release — step by step

### C.1 What “incremental” means here
Databricks workspace import uses **overwrite** on the same path (e.g. `/Shared/day9/cost_management_master`).  
Re-release replaces the notebook; it does **not** create `cost_management_master (1)`.

### C.2 Student / developer steps

| Step | Action | Command / place |
|-----:|--------|-----------------|
| 1 | Edit builder or notebook source | `day9/scripts/…` or `day9/notebooks/…` |
| 2 | (If builder) regenerate | `python day9\scripts\build_all_notebooks.py` *or* leave to release |
| 3 | Check-in | `git commit` + `git push` |
| 4 | Release to workspace | `.\release.cmd databricks` |
| 5 | Verify | Databricks → `/Shared/day9/` → open notebook → Run all (attach cluster) |

Under the hood, `release.cmd databricks` runs:

```text
day9\orchestrate.cmd --deploy
deploy-shared-lab.cmd          → scripts\deploy_shared_lab.py
```

### C.3 Cost / system / perf notebooks (same pattern)

| Lab | Deploy helper |
|-----|----------------|
| Cost management | `python scripts\deploy_cost_management_lab.py` |
| System tables | `python scripts\deploy_system_tables_lab.py` |
| Perf ADF | `python scripts\deploy_perf_databricks_adf_lab.py` |
| Spark DAG | `python scripts\deploy_spark_dag_problems_lab.py` |
| Event Hub | `python scripts\deploy_eventhub_finledger_lab.py` |

After check-in: run the matching `deploy_*_lab.py` (or include them in your medallion/databricks release habit).

### C.4 Secrets (if Level 6 Azure Cost needs SPN)

```cmd
python scripts\ensure_cost_lab_secrets.py
```

Idempotent PUT into scope `finledger`.

---

## D. ADF incremental release — step by step

### D.1 What “incremental” means here
`shared-adf-lab\orchestrate.cmd` redeploys linked services and pipelines **in place** on factory `adf-shared-qgr7mj`.  
Default classroom release uses **`--skip-sql`** so Azure SQL is not rebuilt every time.

### D.2 Student / developer steps

| Step | Action | Command / place |
|-----:|--------|-----------------|
| 1 | Edit pipeline definition / generator | `shared-adf-lab/scripts/…`, pipeline JSON/code |
| 2 | Update docs if behaviour changed | `shared-adf-lab/README.md` or `docs/…` |
| 3 | Check-in | `git commit` + `git push` |
| 4 | Release ADF only | `.\release.cmd adf` |
| 5 | Verify | ADF Studio → Author → pipeline → Debug / Trigger now |
| 6 | Optional smoke | `shared-adf-lab\orchestrate.cmd --run-pipeline pl_01_bronze_copy` |

Under the hood:

```text
shared-adf-lab\orchestrate.cmd --skip-sql
  → scripts\run_shared_adf.py --skip-sql
```

### D.3 When to use `adf-full` (not every commit)

Use `.\release.cmd adf-full` only if you changed:

- SQL database / firewall / login  
- `infra/shared-sql-westus.bicep`  
- First-time SQL bootstrap on a new subscription  

### D.4 ADF ↔ Databricks integration (rare refresh)

```cmd
shared-adf-lab\orchestrate.cmd --setup-databricks-integration
```

Or infra walkthrough: `.\infra-walkthrough.cmd --phase 4`

---

## E. Full medallion CI/CD (notebook + ADF governance)

### E.1 Pipeline diagram

```text
git push main
    │
    ▼
release.cmd medallion-deploy          (classroom default)
    │
    ├─1─ build_all_notebooks.py       regenerate day9 sources
    ├─2─ deploy-shared-lab.cmd        overwrite /Shared/day9 + bronze/secrets
    └─3─ shared-adf --skip-sql        update ADF governance pipelines

optional:
release.cmd medallion                 (+ run_medallion_governance_job.py)
```

Equivalent classic command:

```cmd
.\release-medallion-governance.cmd --skip-run
.\release-medallion-governance.cmd
```

### E.2 Flags (memorise)

| Flag | Meaning |
|------|---------|
| `--skip-build` | Do not regenerate notebooks |
| `--skip-deploy` | Do not push to Databricks / ADF |
| `--skip-run` | Deploy only — **preferred in class** |
| `--run-only` | Only submit the job |

### E.3 Verify checklist

**Databricks**
- [ ] `/Shared/day9/medallion_governance_e2e` (or 50-problems) updated timestamp  
- [ ] Job `finledger-medallion-governance` exists (if full medallion)  
- [ ] Optional: one Succeeded run  

**ADF**
- [ ] `pl_gov_06_master_medallion_governance` (or `pl_db_14_…`) present  
- [ ] Linked service to Databricks still healthy  
- [ ] Monitor shows a test trigger if you ran one  

**Lake**
- [ ] `audit/governance/` manifest / registry artifacts after a successful run  

---

## F. Classroom “Definition of Done” for a PR / check-in

A change is **done** only when:

1. Code + docs committed on `main` and pushed to **origin** and **class**.  
2. Correct `release.cmd …` completed without error.  
3. §E verify boxes checked (or trainer signed off).  
4. Re-run of the **same** release command is safe (idempotent).  

---

## G. Four re-run scenarios (exam question)

| Scenario | Command again? | Result |
|----------|----------------|--------|
| Fresh after clone | `release.cmd medallion-deploy` | Creates/updates in place |
| After successful release | Same command | Overwrite / already present — OK |
| After failed half-deploy | Same command | Resumes; Incremental / overwrite |
| After teardown of shared RG | **No** — restore infra first (`infra-walkthrough --phase 1`) | Release alone cannot recreate RG |

---

## H. Anti-patterns (do not teach these)

| Bad habit | Why | Do instead |
|-----------|-----|------------|
| Edit notebook only in Databricks UI | Lost on next deploy | Edit in git → release |
| Change ADF only in portal | Drift from repo | Edit repo → `release.cmd adf` |
| Full `provision-shared` for every notebook typo | Slow / noisy | `release.cmd databricks` |
| `teardown` on `rg-shared-class1` | Destroys class estate | Never |
| Commit `.env` | Secrets leak | Keep local; use Key Vault / scope |

---

## I. Quick reference card (print for desks)

```text
CHECK-IN:   git add → commit → push origin + class
DBX only:   release.cmd databricks
ADF only:   release.cmd adf
Both safe:  release.cmd medallion-deploy
Both+job:   release.cmd medallion
Verify:     release.cmd verify
Infra rare: infra-walkthrough.cmd --phase 1
Guide:      docs\CICD-INCREMENTAL-RELEASE.md
```

---

## J. File map

| Concern | Path |
|---------|------|
| Release hub | `release.cmd` |
| Medallion driver | `scripts/release_medallion_governance.py` |
| Medallion launcher | `release-medallion-governance.cmd` |
| Databricks lab deploy | `scripts/deploy_shared_lab.py` / `deploy-shared-lab.cmd` |
| ADF lab deploy | `shared-adf-lab/scripts/run_shared_adf.py` |
| Day 9 build | `day9/scripts/build_all_notebooks.py` |
| This guide | `docs/CICD-INCREMENTAL-RELEASE.md` |

---

## K. Citations

1. [ARM Incremental mode](https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/deployment-modes)  
2. [Azure Data Factory CI/CD](https://learn.microsoft.com/en-us/azure/data-factory/continuous-integration-delivery)  
3. [Databricks Repos / workspace deployment concepts](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/ci-cd/)  
4. Repo medallion CICDdoc: [medallion_governance_cicd.md](../day9/docs/medallion_governance_cicd.md)

---

*Start: `.\release.cmd` then pick the smallest action for your change.*
