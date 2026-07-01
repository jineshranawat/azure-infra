# Day 6 — Python for data engineers

**Duration:** 2 hours | **Course:** [docs/data-engineering-course.html](../docs/data-engineering-course.html) Session 6

**Prerequisite:** Sessions 1–5 complete (infra, storage, ADF tour, Databricks overview, bronze→silver→gold notebooks).

**Navigation:** [SESSION6-STUDENT-GUIDE.md](SESSION6-STUDENT-GUIDE.md) (classroom) · [MANUAL-LAB.md](MANUAL-LAB.md) (step-by-step) · [GUIDE.md](GUIDE.md) (trainer)

---

## A. What & why (read first)

### Why Python before more PySpark?

Sessions 3–5 showed **what** Databricks notebooks do. Session 6 teaches **how to read and write the small Python patterns** inside every notebook:

| Pattern | Where it appears later |
|---------|------------------------|
| `def clean_amount(...)` | Silver cleanse (Session 9), MERGE prep (Session 12) |
| `@dataclass RunConfig` | Widgets, Jobs, ADF parameters (Sessions 18–19) |
| `logging` | Production pipelines — warnings survive after the cell runs |
| `pathlib` | Building lake paths without typo-prone string concat |
| pandas vs Spark read | Quick local tests vs distributed lake reads |

**Analogy:** PySpark is the **factory floor**. Python functions and config are the **tools in your belt** — you use them on the floor and in unit tests on your laptop.

### Pure functions vs Spark transforms

**What:** A function like `clean_amount` runs on one value at a time — no cluster needed.  
**Why:** You can **unit test** it in seconds (`day6/tests/test_transforms.py`) before running a 10-minute Spark job.  
**Code:** `scripts/transforms.py` — same logic copied into `notebooks/nb_01_python_basics.py`.

### `RunConfig` dataclass

**What:** A small frozen object holding `run_date`, `learner`, `storage_account`.  
**Why:** One place to build `abfss://` paths — notebooks, scripts, and ADF parameters all read the same shape.  
**Code:** `scripts/finledger_config.py`.

### Logging vs `print`

**What:** `logging.getLogger("finledger").warning(...)` writes structured messages.  
**Why:** In a long notebook run, warnings about bad amounts are searchable in cluster logs — `print` scrolls away.  
**Code:** See `transforms.py` and notebook cell 2.

### pandas vs PySpark for CSV

**What:** pandas loads the whole file to the driver; Spark builds a lazy distributed plan.  
**Why:** pandas for **small samples and tests** on a laptop; Spark for **lake scale** from Session 7 onward.  
**Code:** `notebooks/nb_02_pandas_vs_spark.py`.

---

## B. How to run (Windows)

```text
cd day6
orchestrate.cmd
```

**What it does:**

1. Reads `data/sample_transactions.csv` locally (`pathlib` + `csv`)
2. Runs `clean_amount` and channel totals
3. Runs unit tests on `transforms.py`
4. Prints Databricks notebook import steps

**Re-run command:** `cd day6` → `orchestrate.cmd` (idempotent — same output every time)

**Databricks UI:** Import `notebooks/nb_01_python_basics.py` and `nb_02_pandas_vs_spark.py` — see [MANUAL-LAB.md](MANUAL-LAB.md).

---

## C. Two-hour schedule

| Time | Block | Students do | Where |
|------|-------|-------------|-------|
| 0:00–0:25 | **1 — Theory: Python for DE** | Functions, config, logging mental model | Trainer + [SESSION6-STUDENT-GUIDE](SESSION6-STUDENT-GUIDE.md) |
| 0:25–0:45 | **2 — Local run** | `orchestrate.cmd` on laptop | Command Prompt |
| 0:45–1:15 | **3 — Notebook 01** | Python basics in Databricks | `nb_01_python_basics` |
| 1:15–1:45 | **4 — Notebook 02** | pandas vs Spark same CSV | `nb_02_pandas_vs_spark` |
| 1:45–2:00 | **5 — Checkpoint** | Tests pass + fraud row visible | Checklist below |

---

## D. Deliverables checklist

- [ ] `orchestrate.cmd` exits 0; unit tests pass
- [ ] `clean_amount("oops")` returns `0.0` with a warning in logs
- [ ] `RunConfig` prints a valid `abfss://` bronze path
- [ ] Notebook 02: pandas row count == Spark row count
- [ ] TXN-10003 visible as £50,000 pending wire
- [ ] Cluster terminated after lab (cost)

---

## E. Files

| Path | Purpose |
|------|---------|
| `orchestrate.cmd` | Day 6 entry point |
| `scripts/run_day6.py` | Local demos + tests + notebook guide |
| `scripts/transforms.py` | `clean_amount`, channel totals (unit-testable) |
| `scripts/finledger_config.py` | `RunConfig` dataclass |
| `scripts/read_local.py` | `pathlib` + csv read on laptop |
| `tests/test_transforms.py` | Unit tests (no Spark required) |
| `notebooks/nb_01_python_basics.py` | Databricks — Python foundations |
| `notebooks/nb_02_pandas_vs_spark.py` | Databricks — two ways to read CSV |
| `data/sample_transactions.csv` | Clean FinLedger sample (5 rows) |
| `data/transactions_messy.csv` | Messy feed with `INVALID` amount |

---

## F. Re-run scenarios

| Scenario | Safe? | Notes |
|----------|-------|-------|
| Fresh machine | ✅ | `orchestrate.cmd` creates venv via repo root if missing |
| Re-run after full success | ✅ | Same test output |
| Re-run after partial notebook | ✅ | Re-run failed cells only |
| Re-run after teardown | ✅ | Re-deploy Class-1 first for Databricks notebooks |

---

## G. Failures & workarounds

| Symptom | Fix |
|---------|-----|
| `.env` missing keys | Copy from `.env.example` at repo root |
| Unit tests fail | Check `transforms.py` — `clean_amount` must return `0.0` for garbage |
| `Permission denied` on `abfss://` in notebook | Storage Blob Data Contributor on your user (Class-1) |
| pandas cannot read abfss | Fill `storage_account` widget; cluster needs storage access |
| `stYOURLEARNERHASH` | Use storage account from repo root `orchestrate.cmd` output |

---

*Guardrails: UK regions only, no new Azure resources in Day 6, idempotent orchestrator, docs-as-code.*
