# Day 6 — Student lab guide

**Session 6: Python for data engineers** — the Python patterns every FinLedger notebook reuses.

| | |
|---|---|
| **Duration** | 2 hours |
| **Course page** | [docs/data-engineering-course.html](../docs/data-engineering-course.html) |
| **Prerequisite** | Sessions 1–5 + **Session 3** `orchestrate.cmd --setup-secrets` (scope `finledger`) |
| **Replace** | `<learner>` = your id from `.env` |

---

## START HERE

### Your goal today

*"I can read Python transform code, run unit tests on my laptop, and read the same CSV with pandas and PySpark in Databricks — and I know why `clean_amount` quarantines bad rows."*

### FinLedger business hook

| Row | Why it matters |
|-----|----------------|
| TXN-10003 | £50,000 pending wire — fraud monitoring |
| TXN-20003 | `INVALID` amount — must become `0.0` and quarantine in silver |

---

## Block 1 — Theory (25 min)

Listen + note these four ideas (trainer explains):

1. **Functions** encapsulate rules (`clean_amount`) — fix once, use in tests and Spark.
2. **`dataclass`** holds run settings (`run_date`, storage account) — same object for scripts and notebooks.
3. **`logging`** keeps warnings in cluster logs; `print` is for quick demos only.
4. **pandas vs Spark** — small data on driver vs distributed lake reads.

---

## Block 2 — Local run on your laptop (20 min)

### Step 0 — Session 3 secrets (if not done yet)

```text
cd d:\azure\session-3
orchestrate.cmd --setup-secrets
```

This creates scope `finledger` with `storage-account` and `storage-key` — **Day 6 notebooks use the same secrets as Session 3**.

### Step 1 — Open Command Prompt

```text
cd d:\azure\day6
orchestrate.cmd
```

### Step 2 — Read the output

You should see:

- Channel totals for wire / card / fps
- `Messy feed: 8 rows, 1 would be quarantined` (TXN-20003)
- `OK` on all unit tests
- Databricks notebook import instructions

### Step 3 — Open the code (read, do not memorise)

| File | What to notice |
|------|----------------|
| `scripts/transforms.py` | `clean_amount` — try/except, logging |
| `scripts/finledger_config.py` | `RunConfig` + `abfss_bronze()` |
| `tests/test_transforms.py` | Proves `clean_amount("oops") == 0.0` |

**Checkpoint:** all unit tests passed.

---

## Block 3 — Databricks notebook 01 (30 min)

Follow [MANUAL-LAB.md § Lab B](MANUAL-LAB.md#lab-b).

| Step | Action |
|------|--------|
| 0 | Confirm `session-3\orchestrate.cmd --setup-secrets` ran |
| 1 | Import `notebooks/nb_01_python_basics.py` |
| 2 | Cell 1 prints `Secrets OK — scope=finledger account=st…` |
| 3 | Run all cells |
| 4 | Confirm `clean_amount("oops")` prints `0.0` |
| 5 | Confirm `RunConfig` prints your `abfss://` bronze path |

---

## Block 4 — Databricks notebook 02 (30 min)

Follow [MANUAL-LAB.md § Lab C](MANUAL-LAB.md#lab-c).

| Step | Action |
|------|--------|
| 1 | Import `notebooks/nb_02_pandas_vs_spark.py` |
| 2 | Leave `bronze_path` empty (path built from secret `storage-account`) |
| 3 | Run all cells |
| 4 | Confirm `pandas rows == Spark rows` |
| 5 | Filter TXN-10003 — £50,000 pending |

---

## Block 5 — Wrap-up (15 min)

| # | Done? | Proof |
|---|-------|-------|
| 1 | Local tests pass | `orchestrate.cmd` exit 0 |
| 2 | `clean_amount` understood | Explain TXN-20003 to partner |
| 3 | pandas vs Spark counts match | Notebook 02 assert cell |
| 4 | Cost | Terminate cluster |

---

## Homework (optional, 30 min)

1. Add one test to `tests/test_transforms.py` for `is_valid_txn_id("TXN-abc")` → `False`.
2. Re-run `orchestrate.cmd` and confirm tests still pass.
3. Skim Session 7 in the course page — storage concepts + PySpark read (next class).

---

*Next session: Day 7 — Storage concepts + read the lake with PySpark.*
