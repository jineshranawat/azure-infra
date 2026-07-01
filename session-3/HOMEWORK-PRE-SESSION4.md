# FinLedger UK — Homework before next class

**Your role:** Data engineer at FinLedger UK (UK South).  
**What you did in class:** Session 2 — ADF copy activity landed transactions in bronze. Session 3 — Databricks turned bronze into silver and gold Delta tables.  
**What you do at home:** Prove you understand the flow, finish the **case study**, and wire **ADF → Databricks** so one pipeline click runs the whole medallion path.

**Time budget:** 2–3 hours · **Region:** UK South only · **Cost:** Terminate Databricks clusters when done.

Replace `<learner>` with your `.env` value (e.g. `jinesh`).

---

## A. Prerequisites — must be true before you start

Tick every box. If any fail, fix it first (commands below).

| # | Prerequisite | How to verify | Fix if missing |
|---|--------------|---------------|----------------|
| 1 | Repo cloned; `.env` has `LEARNER`, `AZURE_SUBSCRIPTION_ID`, `LOCATION=uksouth` | Open `.env` in Notepad | [README §1](../../README.md#1-how-to-run-on-windows-step-by-step) |
| 2 | Session 1 landing zone deployed | Portal → `rg-<learner>-class1` exists | Repo root: `orchestrate.cmd` |
| 3 | Session 2 ADF bronze pipeline | ADF Studio → `pl_bronze_copy` **Succeeded** in Monitor | `cd session-2` → `orchestrate.cmd` → optional `--run-pipeline` |
| 4 | Bronze file in loaded zone | Storage → **bronze** → `loaded/run=session3-lab/sample_transactions.csv` (5 rows) | `cd session-3` → `orchestrate.cmd` |
| 5 | Databricks workspace in RG | Portal → `dbw-<learner>-…` → **Launch workspace** opens | Session 1 `orchestrate.cmd` at repo root |
| 6 | Silver + gold Delta (from class or homework) | Storage → **silver** `transactions/_delta_log/` and **gold** `daily_channel_summary/_delta_log/` | Run notebooks `nb_01`–`nb_03` in class guide |

**Quick re-run commands (idempotent — safe to run twice):**

```text
cd d:\azure
orchestrate.cmd

cd session-2
orchestrate.cmd

cd session-3
orchestrate.cmd
```

---

## B. Pre-class reading (30 minutes — do not skip)

Read in this order. You are not graded on speed; you are graded on being able to **explain** each piece in class.

| Order | Document | What you should be able to say in one sentence |
|-------|----------|-----------------------------------------------|
| 1 | [session-2/adf-course/CASE-STUDY.md](../session-2/adf-course/CASE-STUDY.md) | What FinLedger is and what bronze / silver / gold mean |
| 2 | [UI-OVERVIEW.md](UI-OVERVIEW.md) §1–2 | Why ADF and Databricks are different tools |
| 3 | [UI-OVERVIEW.md](UI-OVERVIEW.md) §5–6 | What “lazy evaluation” and `abfss://` mean |
| 4 | [SESSION3-STUDENT-GUIDE.md](SESSION3-STUDENT-GUIDE.md) “What done looks like” | Where TXN-10003 lives and why fraud ops care |

**Case study facts to memorise:**

| Item | Value | Why it matters |
|------|-------|----------------|
| Company | FinLedger UK — retailer + small business banking | All labs use the same story |
| Today’s file | `sample_transactions.csv` — 5 rows | Same schema in Session 2 and 3 |
| Fraud row | **TXN-10003** — £50,000, wire, **pending** | Must appear in silver and gold `pending_count` |
| Bronze path | `bronze/loaded/run=session3-lab/` | Immutable raw evidence |
| Silver table | `silver/transactions` (Delta) | Typed `amount_gbp`, bad rows quarantined |
| Gold table | `gold/daily_channel_summary` (Delta) | One row per date × channel for MI dashboards |

---

## C. Case study — “Monday morning audit” (your homework scenario)

**Story:** You arrive Monday morning. FinLedger ops asks three questions before stand-up:

1. Did Friday’s transaction file still land in bronze?  
2. Are silver and gold tables up to date?  
3. Is the £50k pending wire (TXN-10003) visible for fraud review?

**Your deliverable:** A short email (5–8 sentences) to ops answering all three, with **portal paths** and **screenshot names** listed in section F.

You prove this by doing the Azure steps in section D and E below — not by writing code from scratch.

---

## D. Homework Part 1 — Verify the lake (Azure Portal, ~45 min)

### D1 — Open your resource group

1. Go to [portal.azure.com](https://portal.azure.com).  
2. Search **`rg-<learner>-class1`** → open the resource group.  
3. Write down (you will need these later):

| Item | Your value |
|------|------------|
| Storage account | `st<learner>…` |
| Data Factory | `adf-<learner>-…` |
| Databricks workspace | `dbw-<learner>-…` |

### D2 — Verify bronze (Session 2 proof)

1. Storage account → **Containers** → **bronze**.  
2. Navigate: `incoming/transactions/` — note your `<run_id>` folder name.  
3. Navigate: `loaded/run=<run_id>/` or `loaded/run=session3-lab/` — open `sample_transactions.csv` → **Preview**.  
4. **Verify:** exactly **5 rows**; row TXN-10003 shows `50000.00` and `pending`.  
5. Open `bronze/_control/watermark.json` → **Edit** (read only) — confirm JSON has `last_run_id` or similar.

**Checkpoint:** Screenshot named `hw-bronze-preview.png`.

### D3 — Verify ADF copy pipeline (Session 2 proof)

1. Resource group → **Data factory** → **Open Azure Data Factory Studio**.  
2. **Author** → open pipeline **`pl_bronze_copy`**.  
3. Click the **Copy** activity — note source dataset (`ds_bronze_incoming_csv`) and sink (`ds_bronze_loaded_csv`).  
4. **Monitor** → **Pipeline runs** → find a run with status **Succeeded**.  
5. Open the run → note **Data read** and **Data written** (both &gt; 0).

**Checkpoint:** Screenshot `hw-adf-monitor-succeeded.png`.

**Explain (write in your notes):** What is the difference between *linked service*, *dataset*, and *copy activity*? Use FinLedger names.

### D4 — Verify silver and gold Delta (Session 3 proof)

1. Storage account → **Containers** → **silver** → folder `transactions/`.  
2. Confirm subfolder **`_delta_log`** exists (proves Delta, not plain CSV).  
3. **Containers** → **gold** → `daily_channel_summary/_delta_log/`.  
4. If `_delta_log` is missing, complete Part 2 (notebooks) before continuing.

**Checkpoint:** Screenshot `hw-delta-log-silver.png`.

---

## E. Homework Part 2 — Databricks recap (~60 min)

Only if you did not finish in class, or silver/gold are empty.

### E1 — Prep paths

```text
cd session-3
orchestrate.cmd
```

Copy the printed lines into Notepad:

- `Bronze read: abfss://bronze@st….dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv`
- Silver / gold `abfss://` paths

### E2 — Databricks workspace

1. Portal → **Azure Databricks** → **Launch workspace**.  
2. **Compute** → create cluster **`finledger-homework`** (smallest node, **Terminate after 30 minutes idle**).  
3. **Workspace** → import from repo:
   - `session-3/notebooks/nb_01_read_bronze.py`
   - `session-3/notebooks/nb_02_bronze_to_silver.py`
   - `session-3/notebooks/nb_03_silver_to_gold.py`

### E3 — Run notebooks in order

| Notebook | Widget / input | Pass criteria |
|----------|----------------|---------------|
| `nb_01` | Paste `bronze_path` from orchestrate output | Row count = **5** |
| `nb_02` | Silver path from orchestrate | Write succeeds; quarantine if messy rows |
| `nb_03` | Gold path from orchestrate | Aggregates by `value_date` + `channel` |

4. In `nb_03` (or fraud filter cell): confirm **TXN-10003** appears in pending / high-value logic.  
5. **Compute** → **Terminate** cluster when finished.

**Optional verify script:**

```text
cd session-3
orchestrate.cmd --verify-storage
```

**Checkpoint:** Screenshot `hw-notebook-gold-output.png`.

---

## F. Homework Part 3 — Capstone: ADF orchestrates Databricks (~60 min)

**Goal:** One ADF pipeline triggers notebook `nb_04_end_to_end` so ops can re-run bronze → silver → gold from ADF Monitor (same pattern as production).

**Guide:** [databricks-course/module-04-adf-orchestration/04-01-adf-notebook-activity.md](databricks-course/module-04-adf-orchestration/04-01-adf-notebook-activity.md)

### F1 — Import end-to-end notebook

1. Databricks → **Workspace** → import `session-3/notebooks/nb_04_end_to_end.py`.  
2. Save as **`/nb_finledger_end_to_end`** (path matters for ADF).  
3. Open notebook → set `storage_account = "st<yourhash>"` once if widgets are empty (line near top).  
4. **Do not** run manually yet — ADF will pass `bronze_path`, `silver_path`, `gold_path`, `run_id`.

### F2 — Create Databricks linked service in ADF

1. ADF Studio → **Manage** → **Linked services** → **+ New**.  
2. Search **Azure Databricks** → Continue.  
3. **Name:** `ls_databricks_finledger`.  
4. **Account selection:** From Azure subscription → select `dbw-<learner>-…`.  
5. **Select cluster:** **New job cluster** → smallest node (e.g. Standard_DS3_v2) → **Terminate after 30 minutes** idle.  
6. **Test connection** → **Create** → **Publish all**.

### F3 — Create pipeline with Databricks Notebook activity

1. **Author** → **+** → **Pipeline** → name **`pl_databricks_finledger`**.  
2. **Activities** → **Databricks** → **Databricks Notebook** → name **`Transform_bronze_to_gold`**.  
3. **Azure Databricks** tab:
   - Linked service: `ls_databricks_finledger`
   - Notebook path: `/nb_finledger_end_to_end`
4. **Base parameters** (use YOUR paths from `session-3\orchestrate.cmd`):

| Parameter | Your value (example) |
|-----------|----------------------|
| `bronze_path` | `abfss://bronze@st<learner>….dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv` |
| `silver_path` | `abfss://silver@st<learner>….dfs.core.windows.net/transactions` |
| `gold_path` | `abfss://gold@st<learner>….dfs.core.windows.net/daily_channel_summary` |
| `quarantine_path` | `abfss://silver@st<learner>….dfs.core.windows.net/quarantine/run=session3-lab` |
| `run_id` | `session3-lab` |

5. **Validate** → **Publish all** → **Trigger now**.  
6. **Monitor** → wait **15–40 min** (first cluster cold start is normal).  
7. Activity status must be **Succeeded**.

### F4 — Verify end-to-end

| Check | Where | Expected |
|-------|-------|----------|
| ADF pipeline | Monitor → `pl_databricks_finledger` | **Succeeded** |
| Gold Delta | Storage → gold → `daily_channel_summary/_delta_log` | Timestamp after your run |
| Row counts | Databricks run output / notebook | Bronze 5; gold has channel summaries |
| Fraud row | Notebook output or re-open `nb_03` filter | TXN-10003 pending £50k |

**Checkpoint:** Screenshot `hw-adf-databricks-succeeded.png`.

### F5 — Cost tear-down

1. Databricks → **Compute** → terminate any **running** clusters.  
2. Do **not** delete the workspace or storage — we reuse them next class.

---

## G. What to bring to next class

Submit (trainer will specify Teams / share folder):

| # | Deliverable |
|---|-------------|
| 1 | Completed checklist (section H) |
| 2 | Five screenshots: `hw-bronze-preview`, `hw-adf-monitor-succeeded`, `hw-delta-log-silver`, `hw-notebook-gold-output` (or skip if done in class), `hw-adf-databricks-succeeded` |
| 3 | Your “Monday morning audit” email (section C) — 5–8 sentences |
| 4 | One paragraph: *When should FinLedger use ADF Copy vs Databricks transform?* |

---

## H. Submission checklist

**Prerequisites**

- [ ] `rg-<learner>-class1` exists in UK South  
- [ ] `pl_bronze_copy` has at least one **Succeeded** run  
- [ ] `bronze/loaded/run=session3-lab/sample_transactions.csv` has 5 rows  

**Session 3 lakehouse**

- [ ] `silver/transactions/_delta_log/` exists  
- [ ] `gold/daily_channel_summary/_delta_log/` exists  
- [ ] TXN-10003 explained in my audit email  

**Capstone (ADF → Databricks)**

- [ ] Linked service `ls_databricks_finledger` published  
- [ ] Pipeline `pl_databricks_finledger` **Succeeded** in Monitor  
- [ ] All Databricks clusters **Terminated**  

**Concepts (be ready to answer aloud)**

- [ ] Linked service vs dataset vs pipeline vs activity  
- [ ] Bronze vs silver vs gold — one example path each  
- [ ] What `_delta_log` proves  
- [ ] Why we terminate clusters after labs  

---

## I. Troubleshooting

| Problem | Try |
|---------|-----|
| ADF pipeline failed | Re-run `session-2\orchestrate.cmd`; check ADF managed identity has *Storage Blob Data Contributor* on storage |
| Databricks activity failed | ADF linked service → test connection; notebook path exactly `/nb_finledger_end_to_end` |
| `abfss` path error | Re-run `session-3\orchestrate.cmd` and copy paths again — no typos in account name |
| Cluster quota / MPN limit | Complete portal steps F2–F3 as **screenshot walkthrough**; bring blockers to next class |
| Preview empty in Storage | Re-run `session-3\orchestrate.cmd` |

More detail: [session-3/MANUAL-LAB.md §K](MANUAL-LAB.md#lab-k) · [session-2/MANUAL-LAB.md §K](../session-2/MANUAL-LAB.md#k-troubleshooting-portal)

---

## J. What we cover next class

- Review your ADF → Databricks pipeline and audit email  
- Mapping data flows vs PySpark (when to use which)  
- Introduction to governance / Purview lineage (Session 4 preview)  

**Repo index:** [COVERAGE-MAP.md](../COVERAGE-MAP.md) · **Trainer:** [GUIDE.md](GUIDE.md)

---

*FinLedger homework — after Session 2 (ADF copy) + Session 3 (Databricks). Last aligned with repo session-3 notebooks and module 04-01.*
