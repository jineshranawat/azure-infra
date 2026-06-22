# Purview, Synapse & Fabric — deploy status and manual steps

## What was deployed on `TrainerJinesh -MPN`

| Service | Status | Resource |
|---------|--------|----------|
| **Microsoft Purview** | **Created** | `pviewjineshwgepcx` (eastus) |
| **Azure Data Factory** | Created earlier | `adf-jinesh-wgepcx` |
| **Azure Databricks** | Created earlier | `dbw-jinesh-wgepcx` |
| **Synapse (serverless SQL)** | **Blocked** | MPN subscription: `SqlServerRegionDoesNotAllowProvisioning` |
| **Microsoft Fabric capacity** | **Blocked** | Fabric quota = 0; capacity not allowed in tested regions |

---

## Portal links

**Purview (governance catalog):**  
https://web.purview.azure.com/resource/subscriptions/a802ddef-155b-481f-9796-fac7318a749f/resourceGroups/rg-jinesh-class1/providers/Microsoft.Purview/accounts/pviewjineshwgepcx/overview

**Resource group:**  
https://portal.azure.com/#@/resource/subscriptions/a802ddef-155b-481f-9796-fac7318a749f/resourceGroups/rg-jinesh-class1/overview

**Cost Explorer (RG):**  
https://portal.azure.com/#view/Microsoft_Azure_CostManagement/Menu/~/costanalysis/open/scope/%2Fsubscriptions%2Fa802ddef-155b-481f-9796-fac7318a749f%2FresourceGroups%2Frg-jinesh-class1

---

## Synapse — why blocked and what to do

Azure returned:

```text
SqlServerRegionDoesNotAllowProvisioning
Location 'uksouth' / 'ukwest' / 'eastus' is not accepting creation of new SQL servers
for subscription a802ddef-155b-481f-9796-fac7318a749f
```

This is common on **MSDN/MPN** dev subscriptions.

**Options:**

1. Request SQL/Synapse quota on a production or CSP subscription.
2. Ask your Azure admin to enable SQL provisioning on this subscription.
3. Use **serverless SQL in Microsoft Fabric** (after Fabric trial — see below) instead of standalone Synapse.

Re-deploy when unblocked:

```powershell
az deployment group create -g rg-jinesh-class1 -n synapse-only `
  --template-file infra/platform-services.bicep `
  --parameters location=uksouth learner=jinesh ownerEmail=<email> `
               storageAccountName=stjineshfqdcgg synapseSqlPassword=<pwd> `
               deployPurview=false deploySynapse=true deployFabric=false
```

---

## Microsoft Fabric — why blocked and manual workspace setup

Azure returned:

```text
RegionalQuota: 0 for Fabric capacities
Capacity provisioning is not currently allowed in this region
```

**Manual Fabric trial (recommended for MPN):**

1. Open https://app.fabric.microsoft.com/
2. Sign in as `v-jinesh@mastekus.onmicrosoft.com`
3. Click **Start trial** (creates trial Fabric capacity in your tenant — no Azure F-SKU quota needed)
4. **Workspaces** → **New workspace** → assign to trial capacity
5. Optional: run workspace API after trial capacity exists:

```powershell
# Get capacity ID from Fabric Admin portal URL: .../capacities/<guid>
.\.venv\Scripts\python.exe scripts\fabric_workspace.py `
  --workspace-name ws-jinesh-class1 `
  --capacity-id <fabric-capacity-guid-from-admin-portal>
```

**Request paid Fabric quota (if needed):**  
https://learn.microsoft.com/fabric/enterprise/fabric-quotas

---

## Re-run automation

```powershell
.\orchestrate.ps1 --platforms-only --skip-setup
```

Code paths:

| File | Purpose |
|------|---------|
| `infra/platform-services.bicep` | ADF, Synapse, Purview, Fabric, Databricks |
| `scripts/fabric_workspace.py` | Creates Fabric workspace via REST API |
| `scripts/orchestrate.py` | Full lab orchestrator (default) or `--platforms-only` |
