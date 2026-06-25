# 05-02 · Copy pipeline in managed VNet

> Module 5 · Time budget: 35 min · Source: [Copy with managed VNet](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-copy-data-portal-private)
> Prereqs: [05-01 · Managed VNet](05-01-managed-vnet-private-endpoints-concepts.md)

## What you'll build

**Managed private endpoint** from ADF to `stadfcourse{learner}`, approve on storage, **Integration runtime** in managed VNet, copy `customers.csv` via **`pl_copy_customers_private`**.

## Part A — UI (click by click)

1. **Manage** → **Managed private endpoints** → **+ New** → **Azure Data Lake Storage Gen2** → select storage account → **resource group** `rg-adf-course-{learner}`.
2. Status **Pending** → storage account → **Networking** → **Private endpoint connections** → **Approve**.
3. **Linked service** `ls_adls_main` → **Connect via** → **AutoResolveIntegrationRuntime** with **Integration runtime setup** → use **Managed Virtual Network** IR (create **Managed** IR `ir-managed-finledger` in managed VNet).
4. Copy pipeline reusing customers datasets — **Trigger** → Succeeded only over private path.
5. Optional: disable storage public access — verify copy still works.

## Part B — JSON

Managed IR reference on linked service `connectVia` → `ir-managed-finledger`.

## Part D — Verify

| Check | Expected |
|---|---|
| Private endpoint | Approved |
| Copy | Succeeded without public storage access |

## Next

[05-03 · On-prem SQL private endpoint](05-03-on-prem-sql-managed-private-endpoint.md)
