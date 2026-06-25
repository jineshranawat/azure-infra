# 02-03 · Expressions, schema drift & derived columns

> Module 2 · Time budget: 35 min · Source: [Data flow expression language](https://learn.microsoft.com/en-us/azure/data-factory/concepts-data-flow-expression-language)
> Prereqs: [02-02 · Transform at scale](02-02-code-free-transformation-at-scale.md), [`transactions_messy.csv`](../data/module-02-data-flows/transactions_messy.csv)

## What you'll build

Data flow **`df_handle_messy_transactions`**: read **`transactions_messy.csv`** (row TXN-20003 has `INVALID` amount) → **Derived Column** `amount_gbp_clean` → **Filter** valid amounts → Sink **`silver/transactions/validated/`**. **7 valid rows** output.

## Why this matters

Real FinLedger feeds have **schema drift** and bad values. Data flow expression language (`toDecimal()`, `isNull()`, `iif()`) cleanses without redeploying pipelines when a new column appears — if **Allow schema drift** is on.

## Part A — UI (click by click)

1. Upload `transactions_messy.csv` → `bronze/incoming/transactions/transactions_messy.csv`.
2. New data flow `df_handle_messy_transactions`.
3. **Source** → path messy file → **Allow schema drift:** ON → **Infer drifted column types:** ON.
4. **Derived Column** `CleanAmount`:
   - Expression: `toDecimal(amount_gbp, 12, 2)` with error handling — or:
   - `iif(like(amount_gbp, '^[0-9]+(\\.[0-9]+)?$'), toDecimal(amount_gbp), toDecimal(null()))`
5. **Filter** `ValidAmounts`: `!isNull(CleanAmount)` AND `CleanAmount > 0`.
6. **Select** → map `amount_gbp_clean` = `CleanAmount`.
7. **Sink** → CSV or Parquet → `silver/transactions/validated/`.
8. **Debug** → confirm TXN-20003 excluded → **7 rows**.
9. **Stop debug**.

## Part B — JSON (Derived Column fragment)

```json
{
  "name": "CleanAmount",
  "type": "DerivedColumn",
  "expression": "iif(like(amount_gbp, '^[0-9]+(\\\\.[0-9]+)?$'), toDecimal(amount_gbp), toDecimal(null()))",
  "columns": [
    { "name": "amount_gbp_clean", "expression": "CleanAmount" }
  ]
}
```

## Part C — Code

Expressions authored in Studio; export full data flow JSON to Git for review.

## Part D — Verify

| Check | Expected |
|---|---|
| Invalid row dropped | TXN-20003 absent |
| Valid rows | 7 |
| Drift | Source allows extra columns without failure |

## Common errors

`toDecimal` fails on INVALID — use `iif`/`like` guard. Drift off — new column breaks mapping.

## Next

[02-04 · Delta Lake transformations](02-04-delta-lake-transformations.md)
