# Trainer transcript — FinLedger Event processors (Zach Wilson pattern)

**Notebook:** `/Shared/day9/finledger_event_processors` (**v4** — ABFSS-only; **classic cluster required**)  
**Source:** `day9/notebooks/nb_finledger_event_processors.py` (built by `build_finledger_event_processors_notebook.py`)  
**Duration:** 45–60 minutes  
**Prereq:** Bronze + returns on ADLS (`deploy-shared-lab.cmd`); **NOT Serverless** — attach classic `0703-105931-31juyffm`  
**Inspired by:** [EcZachly](https://github.com/eczachly) / [personal-health-analytics](https://github.com/EcZachly/personal-health-analytics)

> **Your screenshot error is Serverless.** Top-right still shows Serverless → ADLS `mkdirs` / `abfss` fail with `SparkKeyProviderException`. **v4** removed those mkdirs from setup, but lake writes still need classic. Detach Serverless → classic → Run all.

---

## Opening (3 min)

> “You already finished the 50 problems and ADF pipelines. This notebook is **not** another activity type.
> It’s the **engineering contract** Zach Wilson teaches: one Event schema, one processor per source, one runner that builds a timeline, then daily aggregates and a pivot for analysts.
> We adapt Fitbit/LinkedIn → **FinLedger transactions + returns**.”

Write on board:

```text
payments CSV  ─┐
               ├─► Event schema ─► silver timeline ─► daily ─► pivot (gold)
returns CSV   ─┘
```

---

## Cell walkthrough (say this, then Run)

### Section 0 — Roadmap (2 min)

> “Zach’s roadmap is: metrics first → model → Python processors → quality → Spark/Delta → Jobs/ADF.
> Tools are last. Outcomes first: cash and refunds by day.”

**Run:** markdown cells (no code).

---

### Auth + paths (3 min)

> “Same `finledger` secret scope as Sessions 9–15. Widget `run_id` defaults to `session3-lab` so Studio Run-all works; ADF can override it.”

**Do:** Run auth cell. Confirm printed paths.

---

### Section 1 — Event schema (5 min)

> “In Zach’s repo, Event is a `namedtuple`: source, metric_name, timestamp, metric_value, content.
> Fitbit put steps in that shape; LinkedIn put comments in the **same** shape.
> We keep those five fields for FinLedger. Payment amount and refund amount become different `metric_name`s — not different tables.”

**Do:** Run Event / StructType cell. Point at `Event._fields`.

**Ask class:** “Why not leave txn columns as-is forever?”  
**Answer:** New sources (returns, cards, FPS) shouldn’t invent new pipelines — they invent a **processor**.

---

### Section 2 — Processors (8 min)

> “One function per source — just like Zach’s `processors/` folder.
> `txn_processor` only knows payments. `returns_processor` only knows refunds. Neither unions anything.
> The registry `PROCESSORS` is the same idea as his dict of LinkedIn / Fitbit / Spotify.”

**Do:** Run processor cell. Show keys: `transactions_payments`, `transactions_counts`, `returns`.

---

### Section 3 — Runner / timeline (10 min)

> “`run_everything()` is Zach’s runner: call every processor, union to one timeline, quality filter, write silver Delta.
> This is the opposite of spaghetti notebooks that hard-code joins between every source.”

**Do:** Run runner. Show `display(events.limit(20))`.  
**Call out:** rows from `transactions` and `returns` in the same schema.

---

### Section 4 — Daily aggregates (5 min)

> “Same as `do_daily_aggregates` in personal-health-analytics: group by source, metric, date, sum values.
> Analysts rarely want raw events — they want day grain.”

**Do:** Run daily aggregates. Open a few rows in the grid.

---

### Section 5 — Pivot + net_cash (8 min)

> “Zach writes `pivoted.csv` for Tableau. We write Delta `zach_pivoted` and derive `net_cash_gbp` = payments − refunds.
> That’s the business outcome — not a Spark feature.”

**Do:** Run pivot. Point at `net_cash_gbp`.

---

### Section 6 — SQL bridge (3 min)

> “Session 15 mindset: same table, `%sql` / Spark SQL. Analysts live here.”

**Do:** Run SQL cell. Scroll the table.

---

### Section 7 — Quality + exit JSON (3 min)

> “`assert_quality` fails the job if gold is empty — Session 27 style.
> `dbutils.notebook.exit(JSON)` is what ADF / Jobs read for success proof.”

**Do:** Run quality gate. Confirm notebook exit value in cell output.

---

### Section 8 — ADF / Jobs hook (2 min)

> “Wire this notebook later as a Databricks Notebook activity with `run_id`.
> Medallion labs (`session13`, `pl_gov_06`) teach **layers**. This notebook teaches **cross-source Event modeling**. You need both.”

---

## Closing checklist (for learners)

- [ ] Can name the five Event fields  
- [ ] Can explain why each source has its own processor  
- [ ] Found both payments and refunds on the silver timeline  
- [ ] Saw `net_cash_gbp` on the pivot  
- [ ] Know this is orchestratable via Job / ADF  

---

## Demo script — one-liners if short on time (20 min)

1. Run all.  
2. Show Event schema.  
3. Show silver timeline (two sources).  
4. Show pivot + net_cash.  
5. Exit: “Contract first, Spark second.”

---

## Failure tips

| Symptom | Fix |
|---------|-----|
| Widget `run_id` error | Ensure first cells include `dbutils.widgets.text(...)` (already in notebook) |
| Returns processor SKIP | Re-run `deploy-shared-lab.cmd` (uploads `returns_raw.csv`) |
| Secret / ADLS 403 | Check `finledger` scope keys; warm **classic** cluster (not Serverless) |
| `Invalid configuration ... fs.azure.account.key` | Pull **v2** (same Cell 0 as 50-problems: RBAC → key → unset → SDK) or switch classic |
| `DELTA_MISSING_TRANSACTION_LOG` on DBFS | Pull **v3** (no DBFS writes; path collision fixed) |
| Still writing `dbfs:/FileStore/...` | You are on an **old** notebook or Serverless fallback — refresh **v3** + classic |
| 50-problems works, this fails | Should not for bronze — for **writes** use classic (ABFSS required in v3) |
| Empty pivot | Confirm bronze sample under `loaded/run=session3-lab/` |
| Only DBFS / DEMO_BASE paths | OK for class when ABFSS write blocked; still Delta + Event logic |

**v2 note:** Notebook is generated by `day9/scripts/build_finledger_event_processors_notebook.py` and reuses `bronze_auth_cell.py` + `demo_table_helpers.py` from the 50-problems lab.

**Verify:**

```cmd
python scripts\run_finledger_event_processors.py
```

---

## Links after deploy

| Item | Path |
|------|------|
| Workspace notebook | `/Shared/day9/finledger_event_processors` |
| Theory doc | `day9/docs/finledger_event_processors.md` |
| This transcript | `day9/docs/finledger_event_processors_transcript.md` |
| Builder | `day9/scripts/build_finledger_event_processors_notebook.py` |

---

*End of transcript.*
