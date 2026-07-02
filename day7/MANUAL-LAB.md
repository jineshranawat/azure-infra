# Day 7 — Manual lab (step-by-step)

**Session 7:** Storage + read lake · 2 hours

---

## Lab A — Create venv first (10 min) {#lab-a}

```text
cd d:\azure
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r day7\requirements.txt
```

This installs `pandas`, `polars`, and `pyspark` into the repo-only environment.

---

## Lab B — Run Day 7 script (10 min) {#lab-b}

```text
cd d:\azure\day7
orchestrate.cmd
```

**Verify:**

- `channels:` printed
- pandas/Polars concept lines printed
- `rows:` from local Spark (or skip message if no Java)
- Unit tests `OK`

**Trace code:**

1. `scripts/python_essentials.py`
2. `scripts/pandas_polars_concepts.py`
3. `scripts/local_spark_read.py`
4. `scripts/path_builder.py`

---

## Lab C — Install Java (if needed) {#lab-c}

```text
java -version
```

If missing: install Temurin 11+ from adoptium.net, reopen Command Prompt, re-run `orchestrate.cmd`.

---

## Lab D — Databricks notebooks {#lab-d}

1. Secrets: `session-3\orchestrate.cmd --setup-secrets`
2. Import `day7/notebooks/*.py` (four files)
3. Attach cluster → run all cells top to bottom per notebook

| Notebook | Key output |
|----------|------------|
| nb_01 | `clean_amount` demo |
| nb_02 | `abfss://bronze@...` path |
| nb_03 | **12-section PySpark foundation** — lazy vs action, F.col |
| nb_04 | bronze display + UC table count |

After nb_03, read [../day8/PYSPARK-REFERENCE.md](../day8/PYSPARK-REFERENCE.md) sections 1–6 before Day 8.

---

## Lab E — Portal cross-check {#lab-e}

1. Portal → Storage account `st<learner>...` → Containers → **bronze**
2. Navigate `loaded/run=session3-lab/sample_transactions.csv`
3. Path matches notebook `bronze_path`

---

## End checklist {#lab-f}

- [ ] Local orchestrate succeeded
- [ ] All four notebooks green
- [ ] Can explain pandas vs Polars vs Spark
- [ ] Read [PYSPARK-REFERENCE](../day8/PYSPARK-REFERENCE.md) sections 1–6
- [ ] Can explain lazy vs action
