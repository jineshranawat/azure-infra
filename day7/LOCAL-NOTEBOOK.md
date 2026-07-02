# Day 7 — Run local `.ipynb` notebooks (any Windows PC)

Use this when a learner opens `test.ipynb` in Cursor and the cell **hangs** or shows **Select Kernel**.

---

## One command (parent or child)

```text
cd d:\azure\day7
orchestrate.cmd --setup-notebook
```

This:

1. Creates repo `.venv` if missing  
2. Installs `jupyter`, `ipykernel`, pandas, polars, pyspark  
3. Registers kernel **FinLedger (.venv)**  
4. Runs `test.ipynb` headless to prove it works  

---

## In Cursor (after setup)

1. **Open folder** `d:\azure` (repo root — not only `day7`)  
2. Install extensions if prompted: **Python** + **Jupyter**  
3. Open `day7/notebooks/test.ipynb`  
4. Top-right: click **Select Kernel** → **FinLedger (.venv)**  
5. Run the first code cell (▶)

Expected:

```text
channels: ['wire', 'card', 'fps']
first: wire
```

---

## If a cell still hangs

| Step | Action |
|------|--------|
| 1 | Stop cell (■) |
| 2 | `Ctrl+Shift+P` → **Notebook: Select Notebook Kernel** → **FinLedger (.venv)** |
| 3 | If missing: re-run `orchestrate.cmd --setup-notebook` |
| 4 | Reload window: `Ctrl+Shift+P` → **Developer: Reload Window** |

---

## Databricks notebooks vs local

| File | Where it runs |
|------|----------------|
| `nb_01_*.py` … `nb_04_*.py` | **Databricks** (import as `.py` notebook) |
| `test.ipynb` | **Your PC** in Cursor (practice only) |

Local practice without Jupyter UI: `orchestrate.cmd` (no notebook needed).

**Google Colab / Jupyter online:** see [COLAB-JUPYTER-GUIDE.md](COLAB-JUPYTER-GUIDE.md) and upload `notebooks/online_step_by_step.ipynb`.
