# 02-04 · Delta Lake transformations

> Module 2 · Time budget: 35 min · Source: [Delta Lake in mapping data flows](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-data-flow-delta-lake)
> Prereqs: [02-03 · Expressions](02-03-expressions-schema-drift-derived-columns.md)

## What you'll build

Extend silver path: data flow **`df_transactions_delta`** sinks to **Delta** format at `silver/transactions/delta/` with **Upsert** on `transaction_id`. Second run with updated row merges — `_delta_log` folder appears.

## Why this matters

Parquet append-only lakes duplicate keys on reprocessing. **Delta Lake** adds ACID transactions, merge/upsert, time travel — FinLedger silver standard for mutable entities.

## Part A — UI (click by click)

1. Open `df_clean_transactions` (or new `df_transactions_delta`).
2. **Sink** → **Dataset** → **Format:** **Delta lake** (or sink type Delta).
3. **Folder:** `silver/transactions/delta/`.
4. **Table action:** **Upsert** (or **Merge**).
5. **Key columns:** `transaction_id`.
6. **Allow insert**, **Allow update** checked.
7. **Debug** / **Execute** via `pl_silver_transactions` (update to delta sink).
8. Storage → `silver/transactions/delta/_delta_log/` → JSON log files exist.
9. Re-run pipeline with same file → row count stable (upsert not duplicate).

## Part B — JSON (sink settings)

```json
{
  "name": "DeltaSink",
  "type": "DeltaLakeSink",
  "storeSettings": {
    "type": "AzureBlobFSWriteSettings"
  },
  "formatSettings": {
    "type": "DeltaLakeWriteSettings",
    "mergeSchema": true
  },
  "upsertable": true,
  "keys": ["transaction_id"]
}
```

## Part D — Verify

| Check | Expected |
|---|---|
| `_delta_log` | Present |
| Upsert | No duplicate `transaction_id` on re-run |

## Cost & tear-down

Delta compute same as data flow vCores.

## Next

[02-05 · Power Query wrangling](02-05-power-query-wrangling-data-flow.md)
