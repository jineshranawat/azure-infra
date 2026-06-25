# Module 1 sample data — copy & ingest

Upload before lessons **01-01** through **01-08**.

| File | Upload to (container `bronze`) | Rows |
|---|---|---|
| `transactions_daily.csv` | `incoming/transactions/daily/` | 12 |
| `customers.csv` | `incoming/customers/` | 8 |
| `products.csv` | `incoming/products/` | 10 |
| `store_locations.csv` | `incoming/stores/` | 5 |
| `incremental/customers_batch1.csv` | `incoming/customers/incremental/` | 5 |
| `incremental/customers_batch2.csv` | `incoming/customers/incremental/` | 3 |

**Verify:** Storage → blob **Preview** → row count matches table.

**MS Learn:** [Copy Data tool](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-copy-data-tool)
