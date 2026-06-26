# 02-01 · Read bronze, transform to silver

> Module 2 · Time budget: 50 min · Source: [azure.html](../../../azure.html) Day 3 Hour 26

## What you'll build

PySpark pipeline reading `abfss://bronze@.../loaded/run=session3-lab/sample_transactions.csv` and writing **silver/transactions** as Delta.

## Why Databricks for this step

ADF copy activities do not:

- Cast `amount_gbp` to double safely
- Quarantine rows where cast fails
- Write ACID Delta with schema enforcement

Spark does all three in one notebook — testable, version-controlled code.

## Part A — Read from storage (UI + code)

### A1 — Get path from orchestrator

```text
cd session-3
orchestrate.cmd
```

Copy printed line: `Bronze read: abfss://bronze@st....dfs.core.windows.net/loaded/run=session3-lab/sample_transactions.csv`

### A2 — Notebook 01 (read only)

Open `nb_01_read_bronze` → paste path into **bronze_path** widget → **Run all**.

```python
bronze_df = spark.read.option("header", True).option("inferSchema", True).csv(bronze_path)
bronze_df.count()  # action — reads storage
```

**Lazy evaluation:** `spark.read` builds a plan; `count()` executes it.

### A3 — UI concepts while running

| UI element | What you see | Why |
|------------|--------------|-----|
| Cell `*` | Asterisk spinning | Cell running |
| Cell `✓` | Checkmark | Success |
| Output area | `Bronze rows: 5` | Action result |
| display() | Sortable grid | Explore without SQL |
| Spark UI link | Jobs / stages | Performance tuning (Hour 31) |

## Part B — Silver transforms (notebook 02)

### B1 — Transformations

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

cleaned = (
    raw.withColumn("amount_gbp", F.col("amount_gbp").cast(DoubleType()))
    .withColumn("value_date", F.to_date(F.col("value_date")))
    .withColumn("is_high_value", F.col("amount_gbp") >= 1000)
)
valid = cleaned.filter(F.col("amount_gbp").isNotNull())
quarantine = cleaned.filter(F.col("amount_gbp").isNull())
```

**Why quarantine?** FinLedger compliance — bad data must not silently enter silver.

### B2 — Write Delta

```python
valid.write.format("delta").mode("overwrite").save(silver_path)
```

### B3 — Verify in portal

Storage → **silver** container → `transactions/_delta_log/`

## Part C — Complete notebook code reference

Full code: [`notebooks/nb_02_bronze_to_silver.py`](../../notebooks/nb_02_bronze_to_silver.py)

## Verify

| Check | Expected |
|-------|----------|
| Bronze count | 5+ |
| Quarantine | 0–1 (1 if messy feed) |
| Silver Delta | `_delta_log` folder exists |
| TXN-10003 | In valid set, is_high_value=true |

## Next

[03-01 · Delta medallion](../module-03-delta/03-01-delta-medallion.md)
