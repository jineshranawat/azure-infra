# Day 8 — Trainer guide

**Session 8 · 2 hours**

| Block | Time | Activity |
|-------|------|----------|
| 0 | 15 min | Learners read PYSPARK-REFERENCE §1–6 |
| 1 | 10 min | Whiteboard: lazy pipeline → single action |
| 2 | 20 min | Local `day8\orchestrate.cmd` (venv path) |
| 3 | 15 min | Databricks **nb_00** foundation |
| 4 | 50 min | Notebooks nb_01 → nb_04 |
| 5 | 10 min | Q&A + Session 9 bridge |

**Teaching order:** nb_00 is mandatory — do not skip to transforms.

**Key messages:**

1. venv → pandas/Polars → PySpark (same CSV, three scales)
2. Never `.collect()` on production-sized data
3. Bronze strings → cast in silver
4. One pipeline, one write at the end

**Common issues:**

| Symptom | Fix |
|---------|-----|
| Cast nulls | `filter(amount > 0)` + mention quarantine in S9 |
| Join empty | `lookup` demo is in-memory — always 3 rows |
| Window ties | `row_number` vs `rank` — see PYSPARK-REFERENCE §9 |
| No Java locally | `--skip-spark`; cloud notebooks still work |

**Files:** `PYSPARK-REFERENCE.md`, `SESSION8-STUDENT-GUIDE.md`, `MANUAL-LAB.md`
