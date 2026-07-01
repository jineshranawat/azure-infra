# Session 3 — Databricks: Lakehouse & Transformation

**Duration:** 2 hours practical | **Source:** [azure.html](../azure.html) Day 3 (Hours 25–32, compressed)

**Prerequisite:** Class-1 + Session 2 bronze data (`orchestrate.cmd` at repo root and `session-2\orchestrate.cmd`).

**Navigation:** [COVERAGE-MAP.md](../COVERAGE-MAP.md) steps 13–16 | **Theory + graphs:** [UI-OVERVIEW.md](UI-OVERVIEW.md) | **Classroom (start here):** [SESSION3-STUDENT-GUIDE.md](SESSION3-STUDENT-GUIDE.md) | **UI clicks:** [MANUAL-LAB.md](MANUAL-LAB.md) | **All links:** [LINK-MAP.md](LINK-MAP.md) | **Course:** [databricks-course/README.md](databricks-course/README.md)

---

## A. What & why (read first)

### Why Databricks? (the decision rule)

**Session 2 (ADF)** moved raw files into bronze and copied them to a loaded zone. That is **orchestration and file movement** — the right tool for schedules, retries, and connecting dozens of sources.

**Session 3 (Databricks)** answers: *"Now that raw data is in the lake, how do we turn it into trustworthy reporting tables?"*

| Need | ADF alone | Databricks |
|------|-----------|------------|
| Copy CSV bronze → loaded | ✅ Copy activity | Overkill |
| Cast types, filter bad rows, dedupe | Limited (Mapping Data Flows) | ✅ PySpark DataFrames |
| ACID updates, MERGE, time travel | ❌ | ✅ Delta Lake |
| Complex Python / ML scoring | Notebook activity only | ✅ Native notebooks & jobs |
| Medallion bronze → silver → gold | Orchestrates external compute | ✅ Spark at scale |

**Analogy:** ADF is the **conveyor belt** that delivers crates to the warehouse. Databricks is the **factory floor** where crates are opened, cleaned, sorted, and packed into labelled pallets (silver/gold Delta tables).

**FinLedger today:** Read `bronze/loaded/run=session3-lab/sample_transactions.csv` → cleanse → write `silver/transactions` (Delta) → aggregate → write `gold/daily_channel_summary` (Delta). TXN-10003 (£50k pending wire) must surface in gold as a high-value pending transaction.

### Azure Databricks workspace

**What:** Managed Spark platform with notebooks, jobs, and (on Premium) Unity Catalog.  
**Why:** Transform lake files at scale without managing your own Hadoop cluster.  
**Code:** `infra/platform-services.bicep` deploys `dbw-<learner>-<hash>` (Premium SKU, no cluster until you start one).  
**Cost guardrail:** Smallest job cluster, auto-terminate 30 min, terminate after lab.

### abfss:// paths

**What:** URI scheme for ADLS Gen2 from Spark (`abfss://container@account.dfs.core.windows.net/path`).  
**Why:** No legacy DBFS mount required; works with managed identity / user credentials.  
**Code:** `scripts/discover.py` → `abfss_path()`; notebooks use widget `bronze_path`.

### PySpark & lazy evaluation

**What:** Distributed DataFrame API; transformations build a plan, **actions** execute.  
**Why:** Process millions of rows across a cluster; only pay for compute while actions run.  
**Code:** `notebooks/nb_02_bronze_to_silver.py`.

### Delta Lake (silver & gold)

**What:** Parquet files plus a transaction log (`_delta_log/`) for ACID writes.  
**Why:** Re-run the same notebook without corrupting tables; auditors can time-travel.  
**Code:** `.write.format("delta").mode("overwrite").save(silver_path)`.

### Medallion architecture

| Layer | Container | FinLedger content | Mutable? |
|-------|-----------|-------------------|----------|
| Bronze | `bronze` | Raw CSV as landed | Append-only |
| Silver | `silver` | Cleansed, typed transactions (Delta) | MERGE / overwrite in lab |
| Gold | `gold` | Daily channel summary (Delta) | Business aggregates |

### ADF ↔ Databricks (preview)

ADF **orchestrates** when the Databricks notebook runs and passes `bronze_path`, `run_id`. Databricks **executes** Spark. See [databricks-course/module-04-adf-orchestration](databricks-course/module-04-adf-orchestration/04-01-adf-notebook-activity.md).

---

## B. How to run (Windows)

**Students:** follow [SESSION3-STUDENT-GUIDE.md](SESSION3-STUDENT-GUIDE.md) in class (Blocks 0–5).

**Normal classroom:** trainer runs `orchestrate.cmd` before class; students **work in Databricks UI** — see guide **START HERE**.

```text
cd session-3
orchestrate.cmd
```

Optional:

```text
orchestrate.cmd --setup-secrets   # one-time: save storage key to Databricks (see §H)
orchestrate.cmd --verify-storage  # after notebooks: check Delta in silver/gold
```

**First run:** uses repo-root `.venv` (same as Session 2).

**Re-run (idempotent):** same command — overwrites same bronze paths, skips existing RBAC.

**Manual UI steps:** [MANUAL-LAB.md](MANUAL-LAB.md) — workspace tour, cluster, notebooks, storage verify.

---

## C. Two-hour schedule

| Time | Block | Students do | Where |
|------|-------|-------------|-------|
| 0:00–0:20 | **1 — Why Databricks** | Mental model + open workspace | [MANUAL-LAB §A–B](MANUAL-LAB.md#lab-a) |
| 0:20–0:50 | **2 — Read bronze** | Run `nb_01_read_bronze` | Databricks notebook |
| 0:50–1:20 | **3 — Silver Delta** | Run `nb_02_bronze_to_silver` | Databricks + Storage silver |
| 1:20–1:45 | **4 — Gold** | Run `nb_03_silver_to_gold` | Databricks + Storage gold |
| 1:45–2:00 | **5 — Checkpoint** | End-to-end + cost | [MANUAL-LAB §I](MANUAL-LAB.md#lab-i) |

Trainer detail: [GUIDE.md](GUIDE.md)

---

## D. Deliverables checklist

- [ ] `orchestrate.cmd` exits 0 and prints `abfss://` paths
- [ ] Bronze CSV readable in notebook (`count` = 5+ rows)
- [ ] `silver/transactions/_delta_log/` exists in Storage
- [ ] `gold/daily_channel_summary/_delta_log/` exists
- [ ] TXN-10003 visible as pending high-value in notebook output
- [ ] Cluster terminated after lab (cost)

---

## E. Files

| Path | Purpose |
|------|---------|
| `orchestrate.cmd` | Session 3 entry point |
| `scripts/run_session3.py` | Phase orchestrator |
| `scripts/databricks_secrets.py` | **CLI secret setup** (`--setup-secrets`) |
| `scripts/storage_access.py` | Prints storage-access options after orchestrate |
| `scripts/bronze_prep.py` | Upload bronze CSV for Databricks |
| `scripts/databricks_rbac.py` | Storage RBAC for access connector |
| `scripts/storage_verify.py` | Verify Delta outputs |
| `notebooks/_storage_auth.py` | Shared auth (`%run` — auto loads secrets) |
| `notebooks/nb_00_setup_credentials.py` | **One-time** save account + key to secrets |
| [SECRET-SCOPE-SETUP.md](SECRET-SCOPE-SETUP.md) | **Detailed** secret scope + CLI + notebook steps |
| `notebooks/nb_00_unity_catalog_storage.py` | Optional Unity Catalog (access connector) |
| `notebooks/nb_01_read_bronze.py` | Read from ADLS |
| `notebooks/nb_02_bronze_to_silver.py` | Cleanse + Delta silver |
| `notebooks/nb_03_silver_to_gold.py` | Gold aggregates |
| `notebooks/nb_04_end_to_end.py` | Single notebook for ADF |
| `LINK-MAP.md` | Master link index (fix 404s) |
| `UI-OVERVIEW.md` | **Theory + mermaid graphs** |
| `SESSION3-STUDENT-GUIDE.md` | **Classroom handout** |
| `MANUAL-LAB.md` | UI click-by-click |
| `databricks-course/` | Extended modules |

---

## F. Re-run scenarios

| Scenario | Safe? | Notes |
|----------|-------|-------|
| Fresh machine, nothing in lake | ✅ | `orchestrate.cmd` uploads bronze |
| Re-run after full success | ✅ | Overwrites same `session3-lab` paths |
| Re-run after partial / failed notebook | ✅ | Re-run notebook with `overwrite` mode |
| Re-run after teardown | ✅ | Re-deploy Class-1 first |

**Re-run command:** `cd session-3` → `orchestrate.cmd`

---

## G. Failures & workarounds

| Symptom | Fix |
|---------|-----|
| `ImportError: ResourceManagementClient` or `azure.mgmt.resource` | `cd ..` then delete `.venv` and re-run `session-3\orchestrate.cmd` (recreates venv), or `git pull` for latest fix |
| `Databricks workspace not found` | Repo root `orchestrate.cmd` (full deploy) |
| `Permission denied` on `abfss://` / **USER_ISOLATION** | Run `orchestrate.cmd --setup-secrets` once (§H) **or** Single-user cluster **or** `nb_00_unity_catalog_storage` — [MANUAL-LAB §E](MANUAL-LAB.md#lab-e) |
| `'SecretsHandler' object has no attribute 'put'` | Normal on Shared clusters — use `orchestrate.cmd --setup-secrets` on your PC, not `nb_00` |
| Cluster won't start (quota) | Use smallest node; MPN quota 0 → portal walkthrough + code review only |
| `stYOURLEARNERHASH` in notebook | Replace with your storage account from `orchestrate.cmd` output |
| Delta not in silver/gold | Run notebooks 02 and 03; then `orchestrate.cmd --verify-storage` |

---

## H. Storage access & secrets (what we built and why)

### The problem

Databricks notebooks on a **Shared** cluster must read FinLedger files from ADLS (`abfss://bronze@…`). Azure enforces **user isolation** — Spark cannot use a storage account key pasted in a cell the way a Single-user cluster sometimes allows.

We also **cannot** save secrets from inside the notebook on many workspaces:

```text
'SecretsHandler' object has no attribute 'put'
```

So `nb_00_setup_credentials` is a fallback only. The **supported path** is the Windows orchestrator.

### The solution (one command on your laptop)

```text
# Repo-root .env (never commit) — trainer or learner fills once:
#   STORAGE_ACCOUNT_KEY = Portal → Storage → Access keys → key1
#   DATABRICKS_TOKEN    = Databricks → User settings → Access token
#   DATABRICKS_HOST     = optional (orchestrate auto-detects)

cd session-3
orchestrate.cmd --setup-secrets
```

**What this does (idempotent — safe to re-run):**

| Step | Tool | Result |
|------|------|--------|
| 1 | Databricks CLI | Creates secret scope `finledger` |
| 2 | Databricks CLI | Grants READ to workspace group `users` |
| 3 | Databricks CLI | Stores `storage-account` and `storage-key` |
| 4 | Notebooks | `%run ./_storage_auth` loads secrets when `auth_mode=auto` |

**Analogy:** The orchestrator is the **HR department** filing your building pass once. Every notebook (`nb_01`–`nb_04`) just swipes the pass at the door — you do not re-type the key each time.

### In the notebook

Each lab notebook starts with:

```python
%run ./_storage_auth
account = finledger_configure_storage(auth_mode="auto")
```

- `auto` — try reading bronze with existing auth; if that fails, load `finledger` secrets and configure Spark.
- `none` — Single-user cluster only; skip secrets (trainer demo).

**Student widgets:** leave `storage_account` **empty** after `--setup-secrets`; leave `auth_mode` at `auto`.

### Detailed walkthrough

[SECRET-SCOPE-SETUP.md](SECRET-SCOPE-SETUP.md) — screenshots-level steps, trainer vs learner roles, key rotation.

---

*Guardrails: UK regions only, cheapest cluster SKU, budget from Class-1, idempotent orchestrator, docs-as-code.*
