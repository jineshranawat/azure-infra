# Databricks System Tables — Complete Master Lab

**Notebook:** https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/system_tables_master  

## What & why

System tables are Databricks-hosted, **read-only** operational diaries in catalog `system`.  
Learners use them to answer: cost, security, jobs, compute waste, query slowness, lineage, and catalog inventory — without inventing custom logging first.

**Master analogy:** Databricks is a **bank hotel** — audit = CCTV, billing = utility bill, compute = kitchen stoves, lakeflow = staff shifts, query = room-service tickets, information_schema = library card catalog.

## Dual audience

| Reader | Gets |
|--------|------|
| **Novice** | Step-by-step schemas, plain English, synthetic fallbacks if ACL blocks |
| **Architect** | Retention/scope, SCD2 vs timelines, join recipes, grants, streaming gotchas |

## Coverage (Jul 2026 public/preview set)

`access`, `billing`, `compute`, `lakeflow`, `query`, `information_schema`, `storage`, `sharing`, `marketplace`, `mlflow`, `serving`, `ai_gateway`, `data_classification`, `data_quality_monitoring`, `replication` (+ Zerobus beta tables).

Every live query is **soft-try**: deny/error → tiny synthetic sample so class never stops.

## Requirements

- Unity Catalog workspace  
- Grants: `USE CATALOG` + **`BROWSE`** on `system`, then `USE SCHEMA` + `SELECT` on schemas you need  
- Prefer date filters (`lookback_days` widget, default 7)

### If you see `PERMISSION_DENIED: User does not have BROWSE on Catalog 'system'`

This is a **Unity Catalog grant** issue (not Serverless, not the notebook).

1. A user who is **both Account Admin and Metastore Admin** must run grants in SQL Editor.  
2. Helper script (same SQL): `python scripts\grant_system_tables_access.py`  
   - Lab PAT usually fails with *not an account admin* — that confirms you need the account owner.  
3. Until grants land, the notebook **soft-skips** and shows synthetic samples so teaching continues.

```sql
GRANT USE CATALOG ON CATALOG system TO `account users`;
GRANT BROWSE ON CATALOG system TO `account users`;
GRANT USE SCHEMA ON SCHEMA system.billing TO `account users`;
GRANT SELECT ON SCHEMA system.billing TO `account users`;
-- repeat for access, compute, lakeflow, query, ...
```

#### Why Azure Resource Group Owner is not enough

| Role you may have | Unlocks system tables? |
|-------------------|------------------------|
| Azure **subscription / RG Owner** | **No** — Azure RBAC ≠ Databricks account roles |
| Databricks **workspace admin** (`admins` group) | **No** — can manage clusters/notebooks, not `system` catalog |
| Databricks **Account Admin** | Required to assign Metastore Admin / enable system access |
| Databricks **Metastore Admin** (or MANAGE on `system`) | Required to `GRANT` on catalog `system` |

This shared lab metastore (`metastore_azure_eastus`) is auto-provisioned with owner **`System user`** (no human metastore admin). Only an **Account Admin** can assign a Metastore Admin in the [account console](https://accounts.azuredatabricks.net).

**Bootstrap Account Admin (Microsoft rule):** an Entra ID **Global Administrator** must sign in once to `https://accounts.azuredatabricks.net`, then turn on **Account admin** for `v-jinesh@mastekus.onmicrosoft.com`. After that, Jinesh can set himself as Metastore Admin and run the GRANTs above.

Official: [Establish first account admin](https://learn.microsoft.com/en-us/azure/databricks/admin/admin-concepts#--establish-your-first-account-admin) · [System tables — grant access](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/#grant-access-to-system-tables)

## Deploy / re-run

```cmd
python scripts\deploy_system_tables_lab.py
```

Idempotent: re-import overwrites the same path `/Shared/day9/system_tables_master`.

## Pair with

- `/Shared/day9/perf_databricks_adf_lab` — NFR + ADF correlation  
- `/Shared/day9/spark_dag_problems_lab` — Spark DAG smell → fix  

## Official reference

[System tables reference — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/)
