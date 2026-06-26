# 01-01 · Workspace UI tour (click by click)

> Module 1 · Time budget: 30 min · Prereqs: Databricks workspace from Class-1

## What you'll build

A running **finledger-lab** cluster and four imported notebooks under your Workspace folder.

## Part A — Launch workspace

1. Portal → `rg-<learner>-class1` → **Azure Databricks** resource.
2. Click **Launch workspace**.
3. Sign in with Microsoft account (same as `az login`).

## Part B — UI panes (why each exists)

### B1 — Workspace (left nav, folder icon)

**What:** File tree for notebooks, libraries, repos.  
**Why:** Production teams commit notebooks to Git; lab imports from `session-3/notebooks/`.

**Do:** Expand **Users** → your email → note empty folder for imports.

### B2 — Compute (cluster icon)

**What:** All-purpose clusters, job clusters, SQL warehouses.  
**Why:** Spark code runs on cluster workers — **DBU billing** applies until cluster stops.

**Do:** Create cluster per [MANUAL-LAB lab-c](../../MANUAL-LAB.md#lab-c).

### B3 — Workflow / Jobs

**What:** Scheduled notebook and job runs.  
**Why:** Production replaces manual **Run all** with midnight jobs (Hour 30).

### B4 — Data / Catalog

**What:** Unity Catalog table browser (`catalog.schema.table`).  
**Why:** Governance layer — Session 3 lab writes to **external** Delta paths; Session 4+ registers tables here.

## Part C — Notebook mechanics

| Control | Location | Purpose |
|---------|----------|---------|
| Attach cluster | Top bar dropdown | Bind Spark session |
| Run cell | Shift+Enter | Execute one cell |
| Run all | Run menu | Full notebook |
| Widgets | Top of notebook or **Run** → **Add widget** | Parameters |
| display() | Python cell | Rich table (better than `show()`) |
| dbutils.fs.ls | Python | List DBFS or abfss paths |

## Part D — Import lab notebooks

1. Workspace → **Import** → select `nb_01_read_bronze.py` from repo.
2. Repeat for nb_02, nb_03, nb_04.
3. Edit `storage_account = "stYOURLEARNERHASH"` in each notebook.

## Verify

| Check | Expected |
|-------|----------|
| Cluster | Running, auto-terminate 30 min |
| Notebooks | 4 files imported |
| Attach | nb_01 shows cluster attached |

## Next

[02-01 · Read bronze, transform silver](../module-02-pyspark/02-01-read-bronze-transform-silver.md)
