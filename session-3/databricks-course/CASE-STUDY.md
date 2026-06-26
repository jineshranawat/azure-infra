# FinLedger Session 3 — case study continuation

Session 2 landed **raw transactions** in bronze via ADF. Session 3 implements the **transformation contract**:

| Layer | FinLedger artefact | Quality rule |
|-------|---------------------|--------------|
| Bronze | `sample_transactions.csv` | Immutable raw feed |
| Silver | `transactions` Delta | Typed amounts; invalid rows quarantined |
| Gold | `daily_channel_summary` Delta | One row per date × channel for MI dashboards |

**Regulatory angle:** TXN-10003 (£50,000 pending wire) must appear in silver and contribute to `pending_count` in gold — fraud ops review queue.

**Data files:** `session-3/data/sample_transactions.csv`, `transactions_messy.csv`
