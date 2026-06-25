# 01-08 · Change tracking & CDC intro

> Module 1 · Time budget: 30 min · Source: [Incremental copy with change tracking](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-change-tracking-feature-portal)
> Prereqs: [01-07 · Watermark](01-07-incremental-copy-patterns.md), [`store_locations.csv`](../data/module-01-copy-ingest/store_locations.csv), [`products.csv`](../data/module-01-copy-ingest/products.csv)

## What you'll build

Conceptual + portal walkthrough of **SQL Server change tracking** and **multi-table copy** for FinLedger `products` + `stores` — pipeline **`pl_bulk_master_data`** copying **10 + 5 rows** to `bronze/loaded/`. Azure SQL optional; **bulk copy** path uses ForEach preview (full ForEach in Module 3).

## Why this matters

Watermarks suit file feeds; **change tracking** suits OLTP sources where the database records row versions. **CDC** (change data capture) is the enterprise upgrade path for FinLedger ERP SQL (Module 5 private link).

## Part A — Multi-table bulk copy (portal)

1. Upload `store_locations.csv` → `bronze/incoming/stores/`.
2. **Copy Data tool** or manual pipeline with two **Copy** activities in sequence:
   - `Copy_products` (10 rows) — already from 01-03
   - `Copy_stores` (5 rows) — `ds` paths `incoming/stores` → `loaded/stores`
3. **Publish** → trigger → verify both sink folders.

### A2 — Change tracking (Azure SQL lab — optional)

4. Deploy **Azure SQL Database** `sqldb-finledger-{learner}` (Basic tier, UK South) — or trainer demo only.
5. Enable **Change tracking** on `dbo.Customers` per MS Learn tutorial.
6. ADF source with **Change tracking** incremental partition.
7. Copy deltas to `bronze/loaded/sql_customers/`.

> ⚠️ WARNING: SQL Basic tier costs ~£4/month — tear down after lab. **MPN skip:** study MS Learn screenshots; complete bulk CSV path only.

## Part B — JSON

`pipeline/pl_bulk_master_data.json` — two Copy activities:

```json
{
  "name": "pl_bulk_master_data",
  "properties": {
    "activities": [
      {
        "name": "Copy_products",
        "type": "Copy",
        "dependsOn": [],
        "typeProperties": { "source": { "type": "DelimitedTextSource" }, "sink": { "type": "DelimitedTextSink" } },
        "inputs": [{ "referenceName": "ds_bronze_csv_source", "type": "DatasetReference", "parameters": { "entity_name": { "value": "products", "type": "Expression" }, "file_name": { "value": "products.csv", "type": "Expression" } } }],
        "outputs": [{ "referenceName": "ds_bronze_csv_sink", "type": "DatasetReference", "parameters": { "entity_name": { "value": "products", "type": "Expression" }, "file_name": { "value": "products.csv", "type": "Expression" } } }]
      },
      {
        "name": "Copy_stores",
        "type": "Copy",
        "dependsOn": [{ "activity": "Copy_products", "dependencyConditions": ["Succeeded"] }],
        "typeProperties": { "source": { "type": "DelimitedTextSource" }, "sink": { "type": "DelimitedTextSink" } },
        "inputs": [{ "referenceName": "ds_bronze_csv_source", "type": "DatasetReference", "parameters": { "entity_name": { "value": "stores", "type": "Expression" }, "file_name": { "value": "store_locations.csv", "type": "Expression" } } }],
        "outputs": [{ "referenceName": "ds_bronze_csv_sink", "type": "DatasetReference", "parameters": { "entity_name": { "value": "stores", "type": "Expression" }, "file_name": { "value": "store_locations.csv", "type": "Expression" } } }]
      }
    ]
  }
}
```

## Part C — Python

Bulk deploy two copy activities in one `PipelineResource` — `create_or_update` idempotent.

## Part D — Verify

| Entity | Rows in `loaded/` |
|---|---|
| products | 10 |
| stores | 5 |

Module 1 complete — bronze ingest foundation ready for Module 2 data flows.

## Common errors

Change tracking disabled on SQL table — enable at DB and table level. Second copy runs before first finishes — add `dependsOn` Succeeded.

## Cost & tear-down

Delete Azure SQL if created. Keep CSV pipelines.

## Recap

Module 1 covered: wizard copy, manual copy, parameters, self-hosted IR, error handling, S3, watermark incremental, bulk/CDC intro.

## Next

[02-01 · Data flow fundamentals](../module-02-data-flows/02-01-data-flow-fundamentals-debug-canvas.md)
