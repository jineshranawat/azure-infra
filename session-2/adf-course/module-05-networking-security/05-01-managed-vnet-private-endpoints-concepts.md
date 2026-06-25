# 05-01 · Managed VNet & private endpoints

> Module 5 · Time budget: 30 min · Source: [Managed virtual network](https://learn.microsoft.com/en-us/azure/data-factory/concepts-managed-virtual-network)
> Prereqs: Module 4 complete

## What you'll build

Conceptual model + enable **Managed virtual network** on `df-adf-course-{learner}` — no pipeline yet. Understand **Managed private endpoints** to ADLS/SQL for FinLedger compliance.

## Why this matters

Public endpoints expose data planes to internet path. **Managed VNet** isolates ADF integration runtime traffic; **managed private endpoints** connect to PaaS without public access — required for FinLedger PCI-scoped data later.

## Part A — UI

1. ADF Studio → **Manage** → **Managed virtual networks** (or factory **Networking** in portal).
2. **+ New** → enable managed VNet for factory — **UK South**.
3. Review **Managed private endpoints** blade — empty list.
4. Whiteboard: ADF IR → private endpoint → storage account (no public internet).

## Part B — ARM snippet

```json
{
  "properties": {
    "publicNetworkAccess": "Disabled",
    "managedVirtualNetwork": {
      "alias": "default"
    }
  }
}
```

## Part D — Verify

| Check | Expected |
|---|---|
| Managed VNet | Enabled on factory |
| Can explain | Why copy needed private endpoint in 05-02 |

## Next

[05-02 · Copy in managed VNet](05-02-copy-pipeline-managed-vnet.md)
