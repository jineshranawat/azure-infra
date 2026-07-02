# Day 7 — Trainer guide

**Session 7 · 2 hours**

| Block | Time | Activity |
|-------|------|----------|
| 0 | 10 min | Recap Session 3 bronze path |
| 1 | 20 min | Theory: ADLS, medallion, abfss, lazy DF |
| 2 | 25 min | Learners run `day7\orchestrate.cmd` |
| 3 | 55 min | Databricks notebooks 01–04 |
| 4 | 10 min | Checkpoint Q&A |

**Demo tip:** Draw `abfss://bronze@account/...` on whiteboard before opening Storage Explorer.

**Common issues:**

| Symptom | Fix |
|---------|-----|
| `JAVA not found` | adoptium.net, then re-run |
| Secrets error | `session-3\orchestrate.cmd --setup-secrets` |
| Bronze path 404 | Re-run Session 3 nb_01 or check `run_id` widget |

**Files:** `SESSION7-STUDENT-GUIDE.md`, `MANUAL-LAB.md`
