# Day 8 — Manual lab (step-by-step)

**Session 8:** PySpark transforms · 2 hours · [PYSPARK-REFERENCE.md](PYSPARK-REFERENCE.md)

---

## Lab A — venv + packages (10 min) {#lab-a}

```text
cd d:\azure
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r day8\requirements.txt
```

Installs `pandas`, `polars`, `pyspark` into repo `.venv`.

---

## Lab B — Run Day 8 script (15 min) {#lab-b}

```text
cd d:\azure\day8
orchestrate.cmd
```

**Verify output blocks:**

1. pandas rows + columns  
2. polars rows + columns  
3. `PYSPARK FOUNDATION` theory lines  
4. `select + filter`, `withColumn`, `groupBy` demos  
5. Unit tests `OK`

---

## Lab C — Read full PySpark reference (15 min) {#lab-c}

Open [PYSPARK-REFERENCE.md](PYSPARK-REFERENCE.md) — read sections 1–12.

---

## Lab D — Databricks notebooks {#lab-d}

1. Secrets: `session-3\orchestrate.cmd --setup-secrets`
2. Import **five** notebooks (nb_00 first)
3. Run nb_00 → nb_04 in order

| Notebook | Verify |
|----------|--------|
| nb_00 | SQL `GROUP BY channel` works |
| nb_01 | `posted rows:` count |
| nb_02 | schema shows `amount: double` |
| nb_03 | channel totals table |
| nb_04 | `rank_in_channel` column |

---

## Lab E — FinLedger story {#lab-e}

| Row | Transform |
|-----|-----------|
| TXN-10003 | Large wire — window rank |
| INVALID amount | null after cast — filtered in nb_02 |

---

## End checklist {#lab-f}

- [ ] PYSPARK-REFERENCE read
- [ ] nb_00 foundation complete
- [ ] All eight transforms practised
- [ ] Can explain lazy vs action
- [ ] Ready for Session 9 silver write
