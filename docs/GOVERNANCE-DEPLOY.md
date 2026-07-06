# Purview, Synapse & Fabric — deploy status and manual steps

## Shared class estate (`AzureSponsorship-DataTraining`)

| Service | Status | Resource |
|---------|--------|----------|
| **Resource group** | Deployed | `rg-shared-class1` (eastus) |
| **ADLS Gen2 storage** | Deployed | `stsharedqgr7mj` |
| **Azure Data Factory** | Deployed | `adf-shared-qgr7mj` |
| **Azure Databricks** | Deployed | `dbw-shared-qgr7mj` |
| **Synapse (serverless SQL)** | Skipped | MPN/Sponsorship often blocks SQL provisioning |
| **Microsoft Fabric capacity** | Skipped | Use Fabric trial in portal if needed |

Deploy / re-run: `provision-shared.cmd`

---

## Portal links

**Resource group:**  
https://portal.azure.com/#@/resource/subscriptions/a64c0dd2-3a31-4604-bde3-3d40c7d5e8be/resourceGroups/rg-shared-class1/overview

**Databricks workspace:**  
https://adb-7405613791235979.19.azuredatabricks.net

**Cost Explorer (RG):**  
https://portal.azure.com/#view/Microsoft_Azure_CostManagement/Menu/~/costanalysis/open/scope/%2Fsubscriptions%2Fa64c0dd2-3a31-4604-bde3-3d40c7d5e8be%2FresourceGroups%2Frg-shared-class1

---

## Synapse — why blocked and what to do

Azure may return:

```text
SqlServerRegionDoesNotAllowProvisioning
```

This is common on **MSDN/MPN/Sponsorship** dev subscriptions.

**Options:**

1. Request SQL/Synapse quota on a production or CSP subscription.
2. Ask your Azure admin to enable SQL provisioning on this subscription.
3. Use **serverless SQL in Microsoft Fabric** (after Fabric trial) instead of standalone Synapse.

Re-deploy when unblocked:

```powershell
az deployment group create -g rg-shared-class1 -n synapse-only `
  --template-file infra/platform-services.bicep `
  --parameters location=eastus learner=shared ownerEmail=<email> `
               storageAccountName=stsharedqgr7mj synapseSqlPassword=<pwd> `
               deployPurview=false deploySynapse=true deployFabric=false
```

---

## Microsoft Fabric — manual workspace setup

**Manual Fabric trial (recommended for training subscriptions):**

1. Open https://app.fabric.microsoft.com/
2. Sign in with your training account
3. Click **Start trial** (creates trial Fabric capacity in your tenant)
4. **Workspaces** → **New workspace** → assign to trial capacity
5. Optional: run workspace API after trial capacity exists:

```powershell
.\.venv\Scripts\python.exe scripts\fabric_workspace.py `
  --workspace-name ws-shared-class1 `
  --capacity-id <fabric-capacity-guid-from-admin-portal>
```

**Request paid Fabric quota (if needed):**  
https://learn.microsoft.com/fabric/enterprise/fabric-quotas

---

## Re-run automation

```powershell
provision-shared.cmd
```

Or full per-learner lab (UK regions only):

```powershell
.\orchestrate.cmd --platforms-only --skip-setup
```
