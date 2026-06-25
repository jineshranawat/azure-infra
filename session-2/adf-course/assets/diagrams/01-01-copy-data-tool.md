# 01-01 · Copy Data tool — FinLedger transactions

```mermaid
flowchart LR
    SRC["bronze/incoming/transactions/daily/transactions_daily.csv"]
    COPY[Copy Data tool pipeline]
    SNK["bronze/loaded/transactions/"]
    SRC --> COPY --> SNK
```

**Lesson:** [01-01-copy-data-tool.md](../../module-01-copy-ingest/01-01-copy-data-tool.md)
