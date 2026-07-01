# Day 6 — Manual lab (step-by-step)

**Session 6:** Python for data engineers · 2 hours · FinLedger UK

**Prerequisites:** `.env` at repo root · Sessions 1–5 · Databricks workspace deployed

---

## Lab A — Local Python on your laptop {#lab-a}

**Time:** 20 minutes · **Where:** Command Prompt (Windows)

### A1. Run the orchestrator

```text
cd d:\azure\day6
orchestrate.cmd
```

**Expected output (abridged):**

```text
--- Block A: local CSV read (pathlib + csv) ---
  TXN-10001: raw='1250.50' -> cleaned=1250.5
  ...
Channel totals (GBP):
  wire      51,250.50
  ...
Messy feed: 8 rows, 1 would be quarantined

--- Block B: unit tests (transforms.py) ---
...
OK

DAY 6 — DATABRICKS NOTEBOOKS
```

### A2. Re-run tests only (optional)

```text
cd d:\azure\day6
..\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

### A3. Trace the code

Open in your editor:

1. `scripts/read_local.py` — `Path`, `csv.DictReader`
2. `scripts/transforms.py` — `clean_amount`
3. `scripts/finledger_config.py` — `RunConfig`

**Verify:** change `clean_amount("oops")` mentally — should return `0.0`.

---

## Lab B — Databricks: Python basics {#lab-b}

**Time:** 30 minutes · **Where:** Azure Databricks workspace

### B1. Open workspace

1. [portal.azure.com](https://portal.azure.com) → Resource group `rg-<learner>-class1`
2. Open **Azure Databricks** workspace → **Launch workspace**
3. **Compute** → start or attach cluster (smallest SKU, auto-terminate 30 min)

### B2. Import notebook

1. **Workspace** → your folder (e.g. `/Users/<you>/finledger/day6`)
2. **⋮** → **Import**
3. Select `day6/notebooks/nb_01_python_basics.py` from your cloned repo
4. Repeat is not needed — one file per import

### B3. Set widgets

| Widget | Value |
|--------|-------|
| `storage_account` | `st<learner><hash>` from repo root `orchestrate.cmd` output |
| `run_date` | `session3-lab` (or your bronze run folder) |

### B4. Run all cells

| Cell section | Pass criteria |
|--------------|---------------|
| Variables / dicts | Prints channel list and TXN-10003 |
| `clean_amount` | `50000.0 0.0 0.0` |
| `RunConfig` | Prints `abfss://bronze@...` path |
| Quarantine preview | TXN-20003 shows `QUARANTINE` |

### B5. Terminate?

Not yet — continue to Lab C on same cluster.

---

## Lab C — pandas vs PySpark {#lab-c}

**Time:** 30 minutes · **Where:** Same Databricks workspace

### C1. Import notebook

Import `day6/notebooks/nb_02_pandas_vs_spark.py` into the same folder.

### C2. Set widgets

| Widget | Value |
|--------|-------|
| `storage_account` | Same as Lab B |
| `bronze_path` | Leave empty OR paste full path from `session-3\orchestrate.cmd` |
| `run_id` | `session3-lab` |

**Bronze path example:**

```text
abfss://bronze@stjineshfqdcgg.dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv
```

### C3. Run all cells

| Step | Pass criteria |
|------|---------------|
| pandas read | 5 rows displayed |
| Spark read | Schema printed, 5 rows |
| Both transforms | `amount_clean` column populated |
| Count compare | `pandas rows: 5 | Spark rows: 5` |
| TXN-10003 filter | £50,000 pending wire |

### C4. Discussion (2 min)

Ask yourself: *"Why would we not use pandas for the full 10-year transaction history?"*

Answer: driver memory — Spark distributes across the cluster.

---

## Lab D — Cost & checklist {#lab-d}

**Time:** 10 minutes

### D1. Terminate cluster

**Compute** → your cluster → **Terminate**

### D2. Checklist

- [ ] `day6\orchestrate.cmd` — exit code 0
- [ ] Unit tests — all OK
- [ ] Notebook 01 — `RunConfig` path correct
- [ ] Notebook 02 — row counts match
- [ ] TXN-10003 visible
- [ ] Cluster terminated

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `AZURE_SUBSCRIPTION_ID required` | Edit repo root `.env` |
| Notebook cannot read `abfss://` | IAM → Storage Blob Data Contributor on your user |
| Bronze file missing | `cd session-3` → `orchestrate.cmd` |
| pandas `FileNotFoundError` | Check `bronze_path` widget spelling |
| Tests fail after edit | Restore `transforms.py` from git |

---

*Trainer notes: [GUIDE.md](GUIDE.md) · Course: [data-engineering-course.html](../docs/data-engineering-course.html)*
