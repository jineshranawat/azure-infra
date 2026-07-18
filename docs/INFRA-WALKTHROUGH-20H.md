# FinLedger Infrastructure Walkthrough — 20+ hours (student + trainer)

**Open this file for tomorrow’s infra session.**  
**One command that links every child script:** `infra-walkthrough.cmd` (repo root)

```cmd
cd D:\azure
.\infra-walkthrough.cmd --list
.\infra-walkthrough.cmd --phase 0
.\infra-walkthrough.cmd --phase 1
```

| Role | Start here |
|------|------------|
| **Student** | §0 analogy → §A timetable → follow trainer phase by phase |
| **Trainer** | §B phase map → run `--phase N` → portal verify in §E |
| **DevOps / incremental** | §D — how releases update without rebuilding from zero |

**Companion guides:** [TRAINER-NOTES.md](TRAINER-NOTES.md) · [GOVERNANCE-DEPLOY.md](GOVERNANCE-DEPLOY.md) · [../COVERAGE-MAP.md](../COVERAGE-MAP.md) · [../day9/docs/finledger_cost_and_databricks_master_guide.md](../day9/docs/finledger_cost_and_databricks_master_guide.md)

---

## 0. The master analogy — building one restaurant for the whole class

Imagine FinLedger is a **bank restaurant** that must cook the same dinner every night (bronze → silver → gold).

| Kitchen / city | Azure piece | Script that builds it |
|----------------|-------------|------------------------|
| **Land plot + building permit** | Resource Group `rg-shared-class1` | `provision-shared.cmd` |
| **Blueprint** | Bicep (`infra/shared-eastus.bicep`) | same |
| **Walk-in fridge** | ADLS Gen2 (`stshared…`) | same |
| **Key cupboard** | Key Vault | same |
| **Manager with timetable** | Azure Data Factory | `deploy-shared-adf-lab.cmd` |
| **Kitchen + chefs** | Databricks | `provision-shared` + `deploy-shared-lab` |
| **Back-office ledger** | Azure SQL `finledger` (westus) | ADF lab / `shared-sql-westus.bicep` |
| **Health inspector** | Purview (governance scans) | medallion release |
| **Gas / electricity meters** | Cost Management + Monitor | `cost-dashboard.cmd` |
| **Recipe books on the shelf** | Notebooks under `/Shared/day9/` | `deploy-shared-lab` / `day9\orchestrate.cmd --deploy` |
| **CI/CD conveyor** | Release script (build → deploy → run) | `release-medallion-governance.cmd` |

**Golden rules for students**

1. **Measure before you cut** — re-run is safe; teardown of the shared RG is not a classroom toy.  
2. **Two estates exist** — do not mix them casually (see §0b).  
3. **Incremental = update in place** — like renovating one room, not demolishing the house.

### 0b. Two estates (do not mix)

| Estate | Who uses it | Region | Entry command | RG |
|--------|-------------|--------|---------------|-----|
| **Shared class** (default for Day 8/9 + ADF) | Whole cohort | `eastus` (+ SQL `westus`) | `provision-shared.cmd` … | `rg-shared-class1` |
| **Per-learner Class-1** | Personal sandbox | `uksouth` / `ukwest` | `orchestrate.cmd` | `rg-<learner>-class1` |

Tomorrow’s session focuses on the **shared** estate. Phase 12 of the walkthrough is the *optional* personal path only.

---

## A. 20+ hour timetable (what to teach when)

| Block | Hours | Theme | Run | Read / show |
|------:|-------|-------|-----|-------------|
| **A0** | 0–1 | Why infra-as-code + guardrails | `.\infra-walkthrough.cmd --list` | This file §0–§1; `.cursorrules` |
| **A1** | 1–3 | Shared Bicep house frame | `--phase 1` | `infra/shared-eastus.bicep` in editor + Portal RG |
| **A2** | 3–4 | Secrets, bronze, notebooks | `--phase 2` | Portal Storage + Databricks `/Shared` |
| **A3** | 4–7 | ADF as code (manager) | `--phase 3` | `shared-adf-lab/README.md` |
| **A4** | 7–8.5 | ADF ↔ Databricks | `--phase 4` | ADF Linked Service + Job |
| **A5** | 8.5–10.5 | Day 6–7 Python + lake read | `--phase 5` then `6` | day6/day7 guides |
| **A6** | 10.5–12.5 | Day 8 PySpark | `--phase 7` | Day 8 master notebook |
| **A7** | 12.5–15.5 | Day 9 lakehouse write | `--phase 8` | `day9/README.md` |
| **A8** | 15.5–18 | **DevOps incremental release** | `--phase 9` | §D below + medallion CICDdoc |
| **A9** | 18–19.5 | Cost & ops | `--phase 10` | cost master guide |
| **A10** | 19.5–21+ | Stretch / optional | `--phase 11` | EH, system tables, perf, 50 problems |
| **A11** | +2 optional | Personal Class-1 contrast | `--phase 12` (careful) | `README.md` Class-1 |

**Classroom tip:** run `--list` on the projector, then one `--phase N` at a time. Pause for portal verification (§E) after phases 1, 3, 8, 9.

---

## B. Phase map — every child script linked

Master entry: **`infra-walkthrough.cmd`**

| Phase | Child command(s) | What appears in Azure / Databricks |
|------:|------------------|-------------------------------------|
| **0** | venv + `az` check | Local toolbox ready |
| **1** | `provision-shared.cmd` → `scripts/provision_shared.py` → `infra/shared-eastus.bicep` | RG, KV, Storage, ADF, Databricks |
| **2** | `deploy-shared-lab.cmd` → `scripts/deploy_shared_lab.py` | Bronze CSV, scope `finledger`, notebooks |
| **3** | `deploy-shared-adf-lab.cmd` → `shared-adf-lab/orchestrate.cmd` | Pipelines + SQL westus |
| **4** | `shared-adf-lab\orchestrate.cmd --setup-databricks-integration` | ADF can start Databricks jobs |
| **5** | `day6\orchestrate.cmd` | Local Python DE lab |
| **6** | `day7\orchestrate.cmd` | Storage / lake read path |
| **7** | `day8\orchestrate.cmd` | PySpark transform lab |
| **8** | `day9\orchestrate.cmd --deploy` | `/Shared/day9/*` refreshed |
| **9** | `release-medallion-governance.cmd` → `scripts/release_medallion_governance.py` | Build → deploy → ADF governance → job |
| **10** | `cost-dashboard.cmd` + cost notebook deploy | HTML bill + `/Shared/day9/cost_management_master` |
| **11** | `ensure_eventhub_lab.py`, `deploy_*_lab.py`, … | Optional EH / system / perf / DAG |
| **12** | `orchestrate.cmd` | **Different** personal RG (uksouth) |

### B.1 What we have created so far (shared estate inventory)

Typical live names (hash may vary if re-provisioned):

| Resource | Example name | Role |
|----------|--------------|------|
| Resource group | `rg-shared-class1` | The land plot |
| Storage | `stsharedqgr7mj` | Fridge (bronze/silver/gold paths) |
| Key Vault | `kv-shared-qgr7mj` | Key cupboard |
| Data Factory | `adf-shared-qgr7mj` | Manager |
| Databricks | `dbw-shared-qgr7mj` | Kitchen |
| SQL | `sql-shared-…` / DB `finledger` | Back-office ledger |
| Secret scope | `finledger` | Passwords chefs may use |
| Event Hub (optional) | `ehns-shared-finledger` | Delivery scooter |
| Notebooks | `/Shared/day7-day8/…`, `/Shared/day9/…` | Recipe books |

---

## C. Concept cards (teach before each phase)

### C.1 Resource Group
**Analogy:** one fenced plot. Everything for the class restaurant sits inside so you can see cost and delete (carefully) as one unit.

### C.2 Bicep
**Analogy:** the architect’s blueprint. Same file → same house every time.  
**Why Incremental mode:** Azure updates what changed; it does not build a second house next door.

```text
az deployment group create --mode Incremental ...
```

### C.3 Key Vault + secret scope
**Analogy:** cupboard with locks. Code never stores passwords in notebooks.  
Databricks `finledger` scope mirrors selected secrets for Spark.

### C.4 ADLS Gen2 medallion folders
**Analogy:** fridge shelves — raw (bronze), cleaned (silver), plated (gold), rejects (audit).

### C.5 ADF
**Analogy:** manager with a timetable — Copy, Notebook, Wait, ForEach. Pays per visit + van hours (DIU / IR).

### C.6 Databricks
**Analogy:** kitchen. DBUs = taxi meter (software). Azure VMs = petrol underneath.

### C.7 Azure SQL Basic
**Analogy:** small office ledger for watermarks / metadata — fixed cheap monthly tier.

### C.8 Purview / governance
**Analogy:** health inspector’s catalog — what food exists and where it came from (lineage).

### C.9 Idempotency
**Analogy:** pressing “save” twice on a document — still one document.  
Re-run `provision-shared.cmd` or `deploy-shared-lab.cmd` after success → same end state.

---

## D. DevOps — how we release **incremental** changes

This is the heart of tomorrow’s “how do we change production safely?” story.

### D.1 Three layers of “release”

```text
1. INFRA (rarely)     Bicep Incremental     provision-shared.cmd
2. LAB ASSETS (often) Notebooks + secrets   deploy-shared-lab.cmd / day9 --deploy
3. PIPELINE (often)   ADF JSON + optional job run
                      release-medallion-governance.cmd
```

### D.2 Medallion governance release pipeline (phase 9)

Command:

```cmd
.\release-medallion-governance.cmd
.\release-medallion-governance.cmd --skip-run
.\release-medallion-governance.cmd --run-only
```

What the Python driver does (in order):

| Step | Action | Incremental behaviour |
|------|--------|----------------------|
| 1 | `day9/scripts/build_all_notebooks.py` | Regenerates `.py` sources from builders |
| 2 | `deploy-shared-lab.cmd` | Imports notebooks with **overwrite**; bronze blob overwrite |
| 3 | ADF path with `--skip-sql` | Updates pipelines/linked services; **does not** recreate SQL every time |
| 4 | `run_medallion_governance_job.py` | Triggers Databricks job if not `--skip-run` |

**Analogy:** the conveyor belt reprints recipes (build), puts new books on the shelf (deploy), updates the manager’s checklist (ADF), then optionally cooks one dinner (job run).

### D.3 Flags students must memorise

| Flag | Meaning |
|------|---------|
| `--skip-build` | Use existing notebook sources |
| `--skip-deploy` | Do not re-import / re-push ADF |
| `--skip-run` | Deploy only — run job later in Studio |
| `--run-only` | Job only (assets already deployed) |
| `--skip-sql` | ADF lab without touching SQL server |

### D.4 Safe re-run scenarios (exam them)

| Scenario | Safe? | What happens |
|----------|-------|--------------|
| Fresh machine, nothing deployed | Yes | Phase 0→1 creates estate |
| Re-run after full success | Yes | Incremental / overwrite / “already present” |
| Re-run after partial failure | Yes | Resume; failed step retries |
| After notebook-only change | Yes | Phase 8 or 9 with `--skip-sql` |
| Teardown shared RG mid-class | **NO** | Never. Teardown is for personal `rg-<learner>-class1` only via `orchestrate.cmd teardown` |

### D.5 Git → class delivery (trunk-based)

```text
edit builders / bicep / docs
  → git commit on main
  → git push origin + class
  → trainer runs infra-walkthrough --phase 8 or 9
  → students refresh Databricks / ADF
```

The **git repo is the source of truth**. Students do not “just change it locally” for class assets.

### D.6 What is *not* incremental (call out)

- Changing `uniqueString` inputs / RG name → **new** resource names (breaks hard-coded lab names).  
- Deleting the RG → start over with phase 1.  
- Switching subscription without updating `.env` → auth / empty deploys.

---

## E. Portal verification checklist (after each major phase)

### After phase 1 — house frame
- [ ] Portal → Resource groups → `rg-shared-class1`  
- [ ] See Storage, Key Vault, Data Factory, Databricks  
- [ ] Open Storage → containers / filesystem paths for bronze-style folders  

### After phase 2 — furniture
- [ ] Databricks → Workspace → `/Shared/day7-day8` and `/Shared/day9`  
- [ ] Secret scopes → `finledger` exists  

### After phase 3–4 — manager + chefs linked
- [ ] ADF Studio → pipelines list (`pl_01…`, `pl_gov_…`)  
- [ ] Linked services include Databricks / storage / SQL  
- [ ] Monitor → can open pipeline runs  

### After phase 8–9 — recipes + release
- [ ] `/Shared/day9/medallion_governance_e2e` (or related) present  
- [ ] Job `finledger-medallion-governance` (or lab job name) visible  
- [ ] Optional: one successful job run  

### After phase 10 — meters
- [ ] Local browser opens `docs/cost-dashboard-out/index.html`  
- [ ] Or notebook `/Shared/day9/cost_management_master`  

---

## F. Step-by-step student worksheet (print / share)

1. Clone / open repo `D:\azure`.  
2. Run `.\infra-walkthrough.cmd --list` — screenshot the phase map.  
3. With trainer, run `--phase 0` — confirm Python + `az`.  
4. Watch `--phase 1` — while it runs, open `infra/shared-eastus.bicep` and highlight resources.  
5. Portal verify §E phase 1.  
6. `--phase 2` → open Databricks `/Shared`.  
7. `--phase 3` → ADF Studio tour (Copy vs Notebook activity).  
8. `--phase 4` → draw on whiteboard: ADF → Job → Notebook.  
9. Days 6–8 phases — focus on *concepts*, not every line of Python.  
10. `--phase 8` — list Day 9 notebooks; open one silver → gold story.  
11. **Teach §D** on projector, then `--phase 9 --skip-run` (or full release).  
12. Make a tiny doc typo fix → commit story → re-run phase 8/9 → “incremental”.  
13. `--phase 10` — cost dashboard; discuss two invoices (Azure vs DBUs).  
14. Stretch `--phase 11` only if time / budget allows.  
15. Closing quiz: name three incremental mechanisms (Bicep Incremental, notebook overwrite, ADF `--skip-sql`).

---

## G. File map — “now go to the code”

| Concern | File |
|---------|------|
| Master linker | `infra-walkthrough.cmd` |
| Shared Bicep | `infra/shared-eastus.bicep` |
| Shared provisioner | `scripts/provision_shared.py` |
| Lab asset deploy | `scripts/deploy_shared_lab.py` |
| ADF lab orchestrator | `shared-adf-lab/scripts/run_shared_adf.py` |
| SQL westus Bicep | `infra/shared-sql-westus.bicep` |
| Medallion release | `scripts/release_medallion_governance.py` |
| Per-learner orchestrator | `scripts/orchestrate.py` + `infra/main.bicep` |
| Cost dashboard | `scripts/cost_dashboard.py` |
| Day 9 builders | `day9/scripts/build_*.py` |

---

## H. Danger board (say this out loud)

| Action | OK in class? |
|--------|----------------|
| Re-run `provision-shared.cmd` | Yes |
| Re-run `deploy-shared-lab.cmd` | Yes |
| `release-medallion-governance.cmd --skip-run` | Yes |
| Warm cluster for 2 hours | Costly — trainer only |
| `orchestrate.cmd teardown --resource-group rg-shared-class1` | **NEVER** |
| Teardown personal `rg-<learner>-class1` at course end | Yes, with confirmation |

---

## I. Closing analogies (whiteboard)

```text
Bicep           = blueprint
Incremental     = renovate, don't rebuild next door
deploy-shared   = put furniture / recipe books on shelves
ADF             = manager timetable
Databricks      = kitchen (DBU meter)
release-*.cmd   = CI/CD conveyor (build → deploy → optional cook)
cost-dashboard  = utility bill + smart meters
git main        = single source of truth
```

**Architect one-liner:** Infra creates the building; DevOps releases change the recipes and checklists without knocking walls down.

---

## J. Citations / further reading

1. [Azure Resource Manager deployment modes (Incremental)](https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/deployment-modes)  
2. [Bicep documentation](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/)  
3. [Azure Data Factory](https://learn.microsoft.com/en-us/azure/data-factory/)  
4. [Azure Databricks administration](https://learn.microsoft.com/en-us/azure/databricks/admin/)  
5. Repo: [GOVERNANCE-DEPLOY.md](GOVERNANCE-DEPLOY.md) · [day9/docs/medallion_governance_cicd.md](../day9/docs/medallion_governance_cicd.md)

---

*End of 20+ hour infra walkthrough. Start command: `.\infra-walkthrough.cmd --list`*
