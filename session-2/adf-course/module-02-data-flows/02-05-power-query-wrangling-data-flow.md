# 02-05 · Power Query wrangling

> Module 2 · Time budget: 30 min · Source: [Wrangling data flows tutorial](https://learn.microsoft.com/en-us/azure/data-factory/wrangling-tutorial)
> Prereqs: [02-04 · Delta](02-04-delta-lake-transformations.md), [`returns_raw.csv`](../data/module-02-data-flows/returns_raw.csv)

## What you'll build

**Power Query** wrangling data flow **`pq_finledger_returns`**: ingest **`returns_raw.csv`** → normalise `reason` values → aggregate refund totals by `channel` → sink **`silver/returns/summary/`**. **6 input rows** → summary with grouped totals.

## Why this matters

Analysts know Excel/Power Query M language better than Spark expressions. **Wrangling data flows** target self-service prep; mapping data flows target engineer-led ETL. FinLedger finance team can own return reason mapping in PQ.

## Part A — UI (click by click)

1. Upload `returns_raw.csv` → `bronze/incoming/returns/returns_raw.csv`.
2. **Author** → **Power Query** → **+** → **Wrangling data flow** (or **New** → **Power Query data flow**).
   > ⚠️ VERIFY: Menu label may be **Data flows** → **+** → **Power Query** depending on Studio version.
3. Name **`pq_finledger_returns`**.
4. **Get data** → **Azure Data Lake Storage Gen2** → `ls_adls_main` → `bronze/incoming/returns/returns_raw.csv`.
5. Power Query editor opens → **Transform**:
   - **Replace values** on `reason`: `changed_mind` → `Changed Mind`, `defective` → `Defective`, etc.
   - **Group by** `channel` → Sum `refund_gbp`.
6. **Save and close** / **Done**.
7. Configure **Sink** to `silver/returns/summary/`.
8. Create pipeline **`pl_wrangle_returns`** → **Execute Wrangling Data Flow** (or mapping wrapper per portal).
9. **Trigger** → verify summary output.

## Part B — JSON

Power Query flows export as `PowerQueryDataFlow` type — commit exported JSON from **{}** view after save.

## Part D — Verify

| Check | Expected |
|---|---|
| Input rows | 6 |
| Reasons normalised | Title case categories |
| Summary | One row per channel (card, fps) |

## Common errors

PQ not available in region — use mapping data flow fallback. Sink path 403 — RBAC.

## Cost & tear-down

Wrangling uses Spark backend — same vCore billing; limit runs.

## Next

[02-06 · Writing to the lake](02-06-writing-to-lake-partitioning-best-practices.md)
