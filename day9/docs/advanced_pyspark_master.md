# Trainer notes — Advanced PySpark Master (200+ commands)

**Notebook:** `/Shared/day9/advanced_pyspark_master`  
**Open:** https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/advanced_pyspark_master  
**Build:** `python day9/scripts/build_advanced_pyspark_notebook.py`  
**Verify:** `python scripts/run_advanced_pyspark_master.py`  
**Compute:** classic cluster `0703-105931-31juyffm` (not Serverless)

## What this session covers (beyond teach-all / 50-problems)

| Block | Focus |
|-------|--------|
| 01–03 | struct / array / map, explode, higher-order `transform`/`filter`/`exists`/`aggregate` |
| 04–08 | Advanced windows, stack unpivot, approx sketches, semi/anti joins, set ops |
| 09–11 | Delta history/CDF/MERGE delete/constraints/generated columns/OPTIMIZE ZORDER |
| 12–17 | CTE/CUBE/ROLLUP/QUALIFY, pandas_udf, AQE explain, rate streaming |
| 18–20 | **system tables** + **cost** charts + FinLedger KPI dashboards (`display` + matplotlib) |
| 21–25 | dbutils, JSON schema, masking, information_schema, interview drill (≥200 `cmd` APIs) |
| **26–27** | **Event Hubs use case** — send dummy FinLedger pulses + consume → silver KPI + chart |

## Event Hubs (once per estate)

```cmd
python scripts\ensure_eventhub_lab.py
```

Creates `ehns-shared-finledger` / `finledger-payments` (Basic SKU) and stores `finledger/eventhub-*` secrets. Notebook cells 26–27 send + process; if secrets missing they **simulate** the same JSON bus so Run all still works.

## Re-run

```cmd
python scripts\run_advanced_pyspark_master.py
```
