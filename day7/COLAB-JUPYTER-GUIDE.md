# Day 7 — Google Colab or Jupyter (online) step by step

**Rule:** one idea per cell, **2–3 lines of code only**. Run each cell before the next.

**Notebook file to upload:** `day7/notebooks/online_step_by_step.ipynb` (**220 cells** — run in order)

---

## Google Colab (free, in the browser)

### Before you start

1. Open [https://colab.research.google.com](https://colab.research.google.com)
2. **File → Upload notebook** → choose `online_step_by_step.ipynb`
3. Or: **File → New notebook** and copy cells from the guide below

### Steps (Colab)

| Step | What to do |
|------|------------|
| **1** | Run the first code cell (`!pip install ...`) — wait until it finishes |
| **2** | Run each cell top to bottom (▶ or Shift+Enter) |
| **3** | Read the grey **markdown** cell above each code cell first |
| **4** | Do **not** skip ahead — Spark needs the install cell first |

**Colab tip:** No venv needed — Colab installs packages into the session for you.

**Colab limit:** You cannot read Azure `abfss://` paths without cloud login. This notebook uses **sample FinLedger data built in the notebook** (same shape as Session 3).

---

## Jupyter online (Kaggle / Binder / university server)

| Step | What to do |
|------|------------|
| **1** | Upload `online_step_by_step.ipynb` |
| **2** | Select Python 3 kernel |
| **3** | Run cell 1 (`pip install`) if packages missing |
| **4** | Run all other cells in order |

---

## Jupyter on your PC (Cursor / VS Code)

| Step | What to do |
|------|------------|
| **1** | `cd day7` → `orchestrate.cmd --setup-notebook` |
| **2** | Open `online_step_by_step.ipynb` |
| **3** | Kernel → **FinLedger (.venv)** |
| **4** | Run cells in order |

See also: [LOCAL-NOTEBOOK.md](LOCAL-NOTEBOOK.md)

---

## Copy-paste cells (if you build the notebook by hand)

Use these in order. Each block = **one new cell**.

### Step 1 — Install (Colab / fresh Jupyter only)

```python
!pip install -q pandas polars pyspark
print("packages OK")
```

### Step 2 — Python list

```python
channels = ["wire", "card", "fps"]
print(channels[0])
```

### Step 3 — Python dict

```python
txn = {"transaction_id": "TXN-10003", "amount_gbp": "50000.00"}
print(txn["transaction_id"])
```

### Step 4 — Function

```python
def clean_amount(v):
    try: return float(v)
    except: return 0.0
print(clean_amount("50000"), clean_amount("oops"))
```

### Step 5 — pandas DataFrame

```python
import pandas as pd
rows = [{"channel": "wire", "amount_gbp": "1250.50"}, {"channel": "card", "amount_gbp": "89.99"}]
pdf = pd.DataFrame(rows)
print(pdf)
```

### Step 6 — Polars DataFrame

```python
import polars as pl
plf = pl.DataFrame(rows)
print(plf)
```

### Step 7 — Start Spark

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("day7").master("local[*]").getOrCreate()
print(spark.version)
```

### Step 8 — Spark DataFrame (sample data)

```python
df = spark.createDataFrame([
    ("TXN-10001", "wire", "1250.50", "posted"),
    ("TXN-10003", "wire", "50000.00", "pending"),
], ["transaction_id", "channel", "amount_gbp", "status"])
df.show()
```

### Step 9 — Schema (action)

```python
df.printSchema()
print("rows:", df.count())
```

### Step 10 — select (lazy)

```python
slim = df.select("transaction_id", "channel", "status")
slim.show()
```

### Step 11 — filter (lazy + action)

```python
posted = slim.filter("status = 'posted'")
print("posted:", posted.count())
```

### Step 12 — withColumn + cast

```python
from pyspark.sql import functions as F
typed = df.withColumn("amount", F.col("amount_gbp").cast("double"))
typed.select("transaction_id", "amount").show()
```

### Step 13 — groupBy

```python
by_ch = typed.groupBy("channel").agg(F.sum("amount").alias("total"))
by_ch.show()
```

### Step 14 — Stop Spark (end of session)

```python
spark.stop()
print("done")
```

---

## What you learn (order)

```text
pip install → Python basics → pandas → Polars → SparkSession → DataFrame → select → filter → cast → groupBy
```

---

## Databricks vs Colab vs your PC

| Where | File | Azure lake `abfss://` |
|-------|------|------------------------|
| **Google Colab** | `online_step_by_step.ipynb` (220 cells) | No (sample data in notebook) |
| **Your PC Cursor** | `online_step_by_step.ipynb` | No (use Databricks nb_04 for lake) |
| **Databricks** | `nb_01` … `nb_04` (.py) | Yes |

After Colab/Jupyter practice, run **Databricks** notebooks for real bronze paths.
