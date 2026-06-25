# 05-03 · On-prem SQL via managed private endpoint

> Module 5 · Time budget: 35 min · Source: [On-prem SQL managed private endpoint](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-managed-virtual-network-on-premise-sql-server)
> Prereqs: [05-02 · Managed VNet copy](05-02-copy-pipeline-managed-vnet.md)

## What you'll build

**Managed private endpoint** to **Azure SQL** or simulated on-prem SQL via self-hosted IR + private link pattern. Linked service **`ls_sql_finledger`** → copy to `bronze/loaded/sql_customers/`.

## Part A — UI

1. Deploy **Azure SQL Database** `sqldb-finledger-{learner}` (Basic, UK South) OR use trainer shared SQL.
2. Create table `dbo.Customers` from `customers.csv` data.
3. ADF **Managed private endpoint** → SQL server → approve.
4. **Linked service** SQL with managed VNet IR.
5. **Copy** pipeline `pl_sql_to_bronze` → verify rows in ADLS.

> ⚠️ WARNING: SQL Basic ~£4/month — delete after lab. **Skip:** read MS Learn + diagram only.

## Part D — Verify

| Check | Expected |
|---|---|
| PE approved | SQL private endpoint |
| Copy rows | 8 customers in bronze |

## Tear-down

Delete SQL database/server if created for lab.

## Next

[05-04 · Key Vault & RBAC](05-04-key-vault-managed-identity-rbac.md)
