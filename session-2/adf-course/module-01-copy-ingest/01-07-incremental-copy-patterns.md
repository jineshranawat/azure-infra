# 01-07 · Incremental copy (watermark)

> Module 1 · Time budget: 30 min · Source: [Incremental copy overview](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-overview)
> Prereqs: [01-06 · S3](01-06-amazon-s3-to-adls-gen2.md), [`incremental/customers_batch*.csv`](../data/module-01-copy-ingest/incremental/)

## What you'll build

Pipeline **`pl_incremental_customers`** with **Lookup** (read watermark) → **Copy** (delta filter) → **Set variable** / second **Copy** to update `bronze/_control/watermark.json`. Batch1 (**5 rows**) then batch2 (**3 new rows**) — only 3 copied on second run.

## Why this matters

FinLedger customer signups arrive daily; full reload wastes money. **High-watermark** pattern stores `last_signup_date` in JSON control file.

## Part A — UI (click by click)

### A0 — Upload batches

1. Upload `customers_batch1.csv` → `bronze/incoming/customers/incremental/`.
2. Create initial `bronze/_control/watermark.json`:

```json
{"customers":{"last_signup_date":"2025-06-01"}}
```

### A1 — Watermark dataset

3. Dataset `ds_watermark_json` on `ls_adls_main` → `bronze/_control/watermark.json`, format **Json**.

### A2 — Pipeline activities

4. **Lookup** `Lookup_watermark` → source `ds_watermark_json` → output `watermark_row`.
5. **Copy** `Copy_customers_delta` → source query/filter: `signup_date > @{activity('Lookup_watermark').output.firstRow.last_signup_date}` (use **Source** tab → **Query** for delimited: filter in **Mapping** or use **Azure SQL** pattern; for CSV lab use **Filter** via dataset parameter folder switching batch files — simplified lab: copy from `incremental/customers_batch2.csv` only on run 2).
6. **Copy** or **Set Variable** + **Copy** to write updated watermark `2025-06-08` after batch2.

> 💡 TIP: Full MS Learn tutorial uses Azure SQL watermark table; FinLedger uses JSON file in lake — same pattern, different store.

### A3 — Runs

7. Run 1: process batch1 — 5 rows; update watermark to `2025-06-05`.
8. Run 2: process batch2 — **3 rows** only (CUS-014..016).

## Part B — JSON (`bronze/_control/watermark.json`)

```json
{
  "customers": {
    "last_signup_date": "2025-06-07",
    "last_customer_id": "CUS-015"
  }
}
```

`pipeline/pl_incremental_customers.json` — include Lookup + Copy with `dependsOn` chain (see 01-05 pattern).

## Part C — Python

Session 2 [`watermark_store.py`](../../scripts/watermark_store.py) — same FinLedger control file pattern; ADF pipeline orchestrates reads/writes.

## Part D — Verify

| Run | Rows copied | Watermark after |
|---|---|---|
| 1 (batch1) | 5 | `2025-06-05` |
| 2 (batch2) | 3 | `2025-06-08` |

Tick [VERIFICATION-CHECKLIST §01-07](../docs/VERIFICATION-CHECKLIST.md).

## Common errors

Watermark not updated — sink copy path wrong. All rows re-copied — filter expression used `>=` instead of `>`.

## Next

[01-08 · Change tracking & CDC intro](01-08-change-tracking-multiple-tables-cdc.md)
