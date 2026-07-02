# Day 8 — Student lab guide

**Session 8: PySpark transformations**

| | |
|---|---|
| **Duration** | 2 hours |
| **Prerequisite** | Day 7 complete (especially nb_03 + nb_04) |
| **Full theory** | [PYSPARK-REFERENCE.md](PYSPARK-REFERENCE.md) |

---

## Your goal today

*"I understand PySpark end-to-end — venv, pandas/Polars recap, lazy DataFrames, and all eight transforms — with short cells I am not afraid of."*

---

## Block 0 — Read theory (15 min)

Open and read: **[PYSPARK-REFERENCE.md](PYSPARK-REFERENCE.md)** sections 1–6 before any code.

---

## Block 1 — Local setup (20 min)

### Step 1 — venv first (one time)

```text
cd d:\azure
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r day8\requirements.txt
```

### Step 2 — Run script

```text
cd d:\azure\day8
orchestrate.cmd
```

You should see:

1. pandas row count on sample CSV  
2. Polars row count  
3. PySpark foundation bullet points  
4. Local transform demos (if Java installed)  
5. Tests `OK`

No Java? `orchestrate.cmd --skip-spark`

---

## Block 2 — Databricks (75 min)

### Import order (important)

0. **`nb_00_pyspark_foundation.py`** — read every markdown cell  
1. `nb_01_select_filter.py`  
2. `nb_02_withcolumn_cast.py`  
3. `nb_03_groupby_agg.py`  
4. `nb_04_join_window.py`  

**Rule:** markdown first → 3–4 line code cell → check output.

---

## PySpark cheat sheet

| Lazy | Action |
|------|--------|
| select, filter, withColumn, join, groupBy | count, show, display, write |

```
select → filter → withColumn → dropDuplicates → join → groupBy → orderBy → window
```

---

## Checkpoint

- [ ] venv created and `orchestrate.cmd` succeeded
- [ ] I can explain lazy vs action without notes
- [ ] nb_00 foundation notebook complete
- [ ] `amount_gbp` cast to `double` in nb_02
- [ ] Window rank in nb_04
- [ ] Ready for Session 9 (silver quarantine)
