# Day 6 — Trainer guide

**Session 6:** Python for data engineers · 2 hours  
**Course ref:** [docs/data-engineering-course.html](../docs/data-engineering-course.html) — Session 6

---

## Before class (15 min)

1. Confirm students completed Sessions 1–5.
2. Run `cd day6 && orchestrate.cmd` on demo machine — tests must pass.
3. Confirm Session 3 bronze exists: `session-3\orchestrate.cmd` if needed.
4. Import both notebooks to trainer workspace folder.

---

## Session arc

| Block | Min | Trainer does | Students do |
|-------|-----|--------------|-------------|
| Theory | 25 | Whiteboard: function / config / logging / pandas vs Spark | Take notes |
| Local | 20 | Walk through `transforms.py` on screen | Run `orchestrate.cmd` |
| nb_01 | 30 | Live demo first 2 cells, then let them run | Import + run all |
| nb_02 | 30 | Emphasise count assertion cell | Import + run all |
| Close | 15 | Homework + preview Session 7 (storage + read) | Checklist + terminate cluster |

---

## Key teaching points

1. **`clean_amount` is the pattern** — pure function, unit tested locally, same rule in Spark `withColumn` later.
2. **`RunConfig` threads through the course** — today widgets, later Jobs and ADF `@pipeline().parameters.run_date`.
3. **Do not apologise for Python** — this is data engineering craft, not "programming class".
4. **pandas is not the enemy** — it's the right tool for small samples; Spark is for the lake.

---

## Demo script — local block

```text
cd day6
orchestrate.cmd
```

Point at:

- `TXN-20003` quarantine line
- `OK` on tests
- printed notebook paths

---

## Common student mistakes

| Mistake | Correction |
|---------|------------|
| Editing notebook `clean_amount` but not `transforms.py` | "Source of truth is `scripts/transforms.py` — notebook mirrors it" |
| Wrong storage account in widget | Copy from root `orchestrate.cmd` output, not portal guess |
| Leaving cluster running | 15 min before end: "Terminate now" |
| Confusing `print` and `logging` | Show cluster driver logs where `WARNING` appears |

---

## Verification

```text
cd day6
orchestrate.cmd
..\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Both must exit 0.

---

## Homework

Optional: add one unit test — see [SESSION6-STUDENT-GUIDE.md](SESSION6-STUDENT-GUIDE.md).

---

## Next session bridge

Session 7 adds **storage theory** (HNS, medallion, `abfss://`) then reads bronze with PySpark only — builds directly on `RunConfig` and `clean_amount` from today.

---

*Guardrails: no new Azure spend in Day 6 orchestrator; UK region; idempotent re-run.*
