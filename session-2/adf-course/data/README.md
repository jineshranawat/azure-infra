# Sample data — FinLedger UK case study

Upload these files to ADLS Gen2 during the matching lesson. Paths assume storage account `stadfcourse{learner}` and container `bronze` unless the lesson says otherwise.

**Rule:** After every upload or pipeline run, complete the **Verify** table in that lesson (or [`docs/VERIFICATION-CHECKLIST.md`](../docs/VERIFICATION-CHECKLIST.md)).

---

## Folder map

| Folder | Used in module | Upload target (bronze) | Lesson |
|---|---|---|---|
| [`module-00-foundations/`](module-00-foundations/) | 0 | *(upload in 00-01)* `incoming/_seed/` | 00-01 |
| [`module-01-copy-ingest/`](module-01-copy-ingest/) | 1 | `incoming/transactions/`, `customers/`, `products/` | 01-01 – 01-08 |
| [`module-02-data-flows/`](module-02-data-flows/) | 2 | `incoming/returns/`, `incoming/transactions/` | 02-01 – 02-06 |
| [`module-03-control-flow/`](module-03-control-flow/) | 3 | `incoming/manifests/` | 03-01 – 03-05 |
| [`module-04-external-compute/`](module-04-external-compute/) | 4 | `silver/scoring_input/` | 04-01 – 04-03 |
| [`module-06-governance/`](module-06-governance/) | 6 | Git repo / ARM — not uploaded to bronze | 06-01 – 06-02 |

Module 5 uses secured SQL connectivity — no flat files in this repo.

---

## How to upload (portal — every module)

1. Azure portal → storage account `stadfcourse{learner}`.
2. **Data storage** → **Containers** → **bronze**.
3. **Upload** → **Browse for files** → pick file from this repo folder.
4. **Advanced** → **Upload to folder** → enter path from table above (no leading slash).
5. Click **Upload**.
   → Blob appears at `bronze/<folder>/<filename>`.

**Verify:** Open blob → **Preview** or **Edit** → row count matches file README in subfolder.

---

## How to upload (Azure CLI — optional)

```text
az storage blob upload ^
  --account-name stadfcourse{learner} ^
  --container-name bronze ^
  --name incoming/transactions/daily/transactions_daily.csv ^
  --file data\module-01-copy-ingest\transactions_daily.csv ^
  --auth-mode login ^
  --overwrite
```

Replace `{learner}` and run from `adf-course` directory.

---

## File inventory

| File | Rows | Purpose |
|---|---|---|
| `module-00-foundations/upload_manifest.txt` | — | Checklist for first container seed |
| `module-01-copy-ingest/transactions_daily.csv` | 12 | Primary copy / incremental source |
| `module-01-copy-ingest/customers.csv` | 8 | Master data copy |
| `module-01-copy-ingest/products.csv` | 10 | Bulk copy multi-table |
| `module-01-copy-ingest/store_locations.csv` | 5 | Second table same pipeline |
| `module-01-copy-ingest/incremental/customers_batch1.csv` | 5 | Watermark lesson batch 1 |
| `module-01-copy-ingest/incremental/customers_batch2.csv` | 3 | Watermark lesson batch 2 |
| `module-02-data-flows/transactions_messy.csv` | 8 | Schema drift / derived columns |
| `module-02-data-flows/returns_raw.csv` | 6 | Power Query wrangling |
| `module-03-control-flow/file_manifest.json` | 4 entries | ForEach over files |
| `module-03-control-flow/pipeline_parameters.json` | — | Lookup / Set Variable |
| `module-04-external-compute/scoring_input.csv` | 6 | Databricks / Spark input |
| `module-06-governance/sample_arm_parameters.json` | — | CI/CD parameter file example |

---

## Schema reference

### transactions_daily.csv

```text
transaction_id,account_id,amount_gbp,value_date,channel,status,store_id
```

Compatible with Session 2 `sample_transactions.csv` with added `store_id` column.

### customers.csv

```text
customer_id,full_name,email,signup_date,segment
```

### products.csv

```text
sku,product_name,category,unit_price_gbp,is_active
```

---

## Regenerating data

All files are hand-authored CSV/JSON — safe to commit. No PII: emails use `@example.finledger.uk`.
