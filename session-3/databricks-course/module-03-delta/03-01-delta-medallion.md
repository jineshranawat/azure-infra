# 03-01 · Delta Lake & medallion end-to-end

> Module 3 · Time budget: 50 min · Source: [azure.html](../../../azure.html) Day 3 Hours 27–28

## What you'll build

Gold table `daily_channel_summary` as Delta in the **gold** container — completing bronze → silver → gold.

## Why Delta on a data lake?

Plain parquet/Csv on ADLS has no transactions:

- Half-written files if a job fails mid-write
- No MERGE for upserts
- No time travel for auditors

**Delta Lake** adds a `_delta_log` JSON transaction log. Writes are atomic.

## Medallion recap

| Layer | FinLedger | Format | Rule |
|-------|-----------|--------|------|
| Bronze | Raw CSV landing | CSV | Append-only, never edit |
| Silver | `transactions` | Delta | Cleansed, typed, deduped |
| Gold | `daily_channel_summary` | Delta | Business aggregates |

## Part A — Gold notebook (UI)

1. Open `nb_03_silver_to_gold`.
2. Attach cluster → **Run all**.

```python
gold_df = (
    silver_df.groupBy("value_date", "channel")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount_gbp").alias("total_amount_gbp"),
        F.sum(F.when(F.col("is_high_value"), 1).otherwise(0)).alias("high_value_count"),
        F.sum(F.when(F.col("status") == "pending", 1).otherwise(0)).alias("pending_count"),
    )
)
gold_df.write.format("delta").mode("overwrite").save(gold_path)
```

## Part B — Time travel (optional cell)

After silver write, in a new cell:

```python
spark.read.format("delta").option("versionAsOf", 0).load(silver_path).count()
```

**Why:** Auditors ask "what did we know on Tuesday?"

## Part C — End-to-end notebook

`nb_04_end_to_end.py` chains bronze → silver → gold and returns JSON via `dbutils.notebook.exit()` for ADF to read.

## Part D — Verify

```text
orchestrate.cmd --verify-storage
```

| Check | Expected |
|-------|----------|
| silver/transactions/_delta_log | Present |
| gold/daily_channel_summary/_delta_log | Present |
| pending_count in gold | ≥ 1 (TXN-10003) |

## Cost

Terminate cluster after lab. Delta files in storage cost pennies (Hot tier).

## Next

[04-01 · ADF notebook activity](../module-04-adf-orchestration/04-01-adf-notebook-activity.md)
