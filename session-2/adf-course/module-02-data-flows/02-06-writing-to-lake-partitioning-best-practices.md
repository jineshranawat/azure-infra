# 02-06 · Writing to the lake (partitioning)

> Module 2 · Time budget: 40 min · Source: [Best practices writing to lake](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-data-flow-write-to-lake)
> Prereqs: [02-05 · Power Query](02-05-power-query-wrangling-data-flow.md)

## What you'll build

Update **`df_clean_transactions`** sink: **Partition by** `value_date` (or `year`, `month`, `day` derived columns) → `silver/transactions/partitioned/`. Optimise file size: **Partition option** → **Single partition per partition value** for small FinLedger volumes.

## Why this matters

Unpartitioned lakes scan all history for one day's report. **Hive-style partitioning** (`value_date=2026-06-01/`) lets Synapse/Power BI prune files. Over-partitioning tiny data creates small-file problem — FinLedger daily CSV fits **partition by date** only.

## Part A — UI (click by click)

1. Open `df_clean_transactions`.
2. Before sink, **Derived Column**: `partition_date` = `toString(value_date)` or split `year`, `month`, `day` from `value_date`.
3. **Sink** → **Partition options** → **Partition by key(s)** → select `value_date` (or hierarchy).
4. **Folder path** pattern: `silver/transactions/partitioned/{value_date}/` via **Name as column value in path**.
5. **File pattern:** `part-#####.parquet`.
6. **Optimize** tab (if shown): **Compact** on — for production volumes.
7. **Debug** → verify folder structure in data preview path.
8. **Execute** pipeline → portal storage shows:
   - `silver/transactions/partitioned/value_date=2026-06-01/`
   - `value_date=2026-06-02/` etc.
9. **Stop debug** / confirm no orphan clusters.

## Part B — JSON (sink partition)

```json
{
  "name": "PartitionedSink",
  "type": "ParquetSink",
  "partitionBy": ["value_date"],
  "formatSettings": {
    "type": "ParquetWriteSettings"
  },
  "storeSettings": {
    "type": "AzureBlobFSWriteSettings"
  }
}
```

## Part D — Verify

| Check | Expected |
|---|---|
| Multiple folders | One per distinct `value_date` in data |
| Row sum | Still **10** posted rows total |
| Small-file guidance | Not hundreds of empty partitions |

## Best practices (FinLedger)

- Partition by query filter columns (`value_date`, not `transaction_id`).
- Use Delta for upsert entities; partitioned Parquet for append-only facts.
- Keep bronze immutable; silver partitioned; gold aggregated.

## Cost & tear-down

Module 2 end — **delete debug sessions**; review Cost Management for data flow vCore spend.

## Module 2 recap

Silver layer: cleansed, validated, Delta, returns summary, partitioned transactions.

## Next

[03-01 · Pipeline activities catalogue](../module-03-control-flow-orchestration/03-01-pipeline-activities-catalogue.md)
