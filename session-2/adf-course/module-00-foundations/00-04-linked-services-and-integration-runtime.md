# 00-04 · Linked services & Integration Runtime

> Module 0 · Time budget: 25 min · Source: [Linked services in ADF](https://learn.microsoft.com/en-us/azure/data-factory/concepts-linked-services)
> Prereqs: [00-03 · Studio tour](00-03-studio-tour-every-pane.md)

## What you'll build in this lesson

You will deepen the **conceptual model** of linked services, datasets, and integration runtimes — and inspect **AutoResolveIntegrationRuntime** in ADF Studio. You will create a **draft** linked service to Azure SQL (cancel before save) to see all authentication fields FinLedger will use later. Lesson 00-05 creates the real `ls_adls_main` to storage.

## Why this matters (the concept)

A **linked service** is a connection recipe: server name, authentication method, optional integration runtime. A **dataset** is a pointer into that connection — which folder, table, or file pattern. Activities reference **datasets**, not linked services directly (copy activity is the exception where source/sink datasets each carry a linked service).

**Integration Runtime (IR)** is the compute bridge. **Azure IR** (AutoResolve) runs cloud copy in Microsoft's network. **Self-hosted IR** runs on your VM for on-prem SQL (Module 1, lesson 01-04). **Azure-SSIS IR** lifts SSIS packages (Module 6). Picking the wrong IR is the #1 cause of "connection timeout" when data is on-premises.

**Analogy:** Linked service = phone number saved in contacts. Dataset = "call Amelia at her mobile, extension 402." IR = which cell tower routes the call.

## Key terms (first appearance)

| Term | Meaning in one line | Linked in GLOSSARY |
|---|---|---|
| Linked service | Connection/auth to external system | [Linked service](../GLOSSARY.md#linked-service) |
| Dataset | Structure + path within a linked service | [Dataset](../GLOSSARY.md#dataset) |
| Azure IR | Microsoft-managed compute for cloud workloads | [IR](../GLOSSARY.md#integration-runtime-ir) |
| Self-hosted IR | Agent on your network for on-prem sources | [IR](../GLOSSARY.md#integration-runtime-ir) |
| Connect via IR | Field linking linked service to an IR | [AutoResolveIntegrationRuntime](../GLOSSARY.md#autoresolveintegrationruntime) |

## Architecture at a glance

```mermaid
flowchart LR
    subgraph Activity["Copy activity"]
        SRC[Source dataset]
        SNK[Sink dataset]
    end
    SRC --> LS1[ls_adls_main]
    SNK --> LS1
    LS1 --> IR[AutoResolve IR]
    IR --> ADLS[(stadfcourse{learner})]
```

## Part A — Do it in the UI (click by click)

### A1 — Review linked services list

1. ADF Studio → **Manage** → **Linked services**.
   → List empty (FinLedger connections start in 00-05).
2. Click **+ New**.
   → **New linked service** blade slides in from the right.

### A2 — Explore ADLS Gen2 connector (preview for 00-05)

3. Search box → type `data lake`.
   → Tiles filter to ADLS-related connectors.
4. Select **Azure Data Lake Storage Gen2** → click **Continue**.
   → Form: **Name**, **Connect via integration runtime**, **Authentication type**, **Storage account name**, **Test connection**, **Create**.
5. Read each field — do **not** click Create yet:
   - **Connect via integration runtime:** default `AutoResolveIntegrationRuntime`.
   - **Authentication type** options: Account key, System-assigned managed identity, Service principal, User-assigned MI.
   → FinLedger uses **System-assigned managed identity** in 00-05.
6. Click **Back** (top-left of blade) or **Cancel**.
   → Return to linked services list.

### A3 — Explore a SQL connector (contrast authentication)

7. Click **+ New** again.
8. Search `azure sql` → select **Azure SQL Database** → **Continue**.
   → SQL connection form appears.
9. Note fields: **Server name**, **Database name**, **Authentication type** (SQL, System-assigned MI, Service principal).
10. Click **Cancel**.
    → Reinforces: linked service = connector + auth, not data path.

### A4 — Integration runtimes deep dive

11. Manage → **Integration runtimes**.
12. Click **AutoResolveIntegrationRuntime**.
    → Pane shows **Status: Running**, **Type: Azure**, **Region: Auto**.
13. Note tabs: **Nodes** (empty for AutoResolve), **Monitor** (metrics when jobs run).
14. Click **+ New** (top) — preview IR types.
    → Options include **Azure**, **Self-hosted**, **Azure-SSIS**. Click **Cancel**.
    → Self-hosted used in 01-04; SSIS in 06-04.

### A5 — How datasets reference linked services (Author)

15. **Author** → **Datasets** → **+** → search `delimited`.
16. Select **Delimited Text** → **Continue**.
    → **Linked service** dropdown (empty until 00-05).
17. **Cancel** wizard.
    → Dataset wizard always asks for linked service first — confirms separation of concerns.

> 🧪 LAB CHECK: Explain aloud: "Linked service connects to the account; dataset picks the file path."

## Part B — The JSON behind it

Reference linked service (created in 00-05) — study structure now:

`linkedService/ls_adls_main.json`

```json
{
  "name": "ls_adls_main",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "annotations": ["finledger", "module-00"],
    "type": "AzureBlobFS",
    "typeProperties": {
      "url": "https://stadfcoursejinesh.dfs.core.windows.net",
      "accountKey": null,
      "servicePrincipalId": null,
      "servicePrincipalCredentialType": null,
      "servicePrincipalCredential": null,
      "tenant": null,
      "azureCloudType": null,
      "accountKind": null
    },
    "connectVia": {
      "referenceName": "AutoResolveIntegrationRuntime",
      "type": "IntegrationRuntimeReference"
    },
    "description": "FinLedger ADLS Gen2 — MSI auth"
  }
}
```

After 00-05 with MSI, portal stores auth in `credential` / `authentication` blocks — UI may hide secrets. Dataset referencing it:

`dataset/ds_bronze_transactions_csv.json`

```json
{
  "name": "ds_bronze_transactions_csv",
  "properties": {
    "linkedServiceName": {
      "referenceName": "ls_adls_main",
      "type": "LinkedServiceReference"
    },
    "parameters": {
      "folder_path": { "type": "String" }
    },
    "type": "DelimitedText",
    "typeProperties": {
      "location": {
        "type": "AzureBlobFSLocation",
        "fileSystem": "bronze",
        "folderPath": { "value": "@dataset().folder_path", "type": "Expression" }
      },
      "columnDelimiter": ",",
      "firstRowAsHeader": true
    }
  }
}
```

## Part C — Do it in code (Python / REST / ARM)

**Chosen:** Python — get default integration runtime (maps to Manage hub).

```python
"""Get AutoResolve IR — lesson 00-04."""
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

SUBSCRIPTION_ID = "00000000-0000-0000-0000-000000000000"
RG = "rg-adf-course-jinesh"
FACTORY = "df-adf-course-jinesh"
IR_NAME = "AutoResolveIntegrationRuntime"

client = DataFactoryManagementClient(DefaultAzureCredential(), SUBSCRIPTION_ID)
ir = client.integration_runtimes.get(RG, FACTORY, IR_NAME, if_none_match="")
print(f"Name: {ir.name}")
print(f"Type: {ir.properties.type}")
print(f"State: {ir.properties.state}")
```

Prefer code when auditing IR health across dev/test/prod factories nightly.

## Part D — Run, validate, and read the output

| # | Check | Where | Expected |
|---|---|---|---|
| 1 | ADLS connector fields | New linked service blade | MSI option visible |
| 2 | AutoResolve IR | Manage → IR | **Running** |
| 3 | IR types preview | + New IR | Azure, Self-hosted, SSIS |
| 4 | Dataset wizard | Author → New dataset | Requires linked service |
| 5 | Python/REST IR get | Script output | `Managed`, state `Started` |

## Common errors & fixes

| Symptom | Likely cause | Fix |
|---|---|---|
| Test connection greyed out | Required fields empty | Fill name + storage account first |
| IR **Stopped** | Factory paused / rare glitch | Click **Start** if available; reopen Studio |
| Dataset cannot pick LS | LS not published | 00-05 publishes `ls_adls_main` |
| Self-hosted IR install fails | TLS / proxy | Use MSI installer; corporate firewall rules |
| Confused LS vs dataset | Concept gap | LS = account; dataset = path/table |

## Cost & tear-down

**Cost:** £0 — no IR nodes provisioned beyond free AutoResolve dispatch.

## Recap & self-check

- Linked service = connection; dataset = data address; activity = work unit.
- **AutoResolveIntegrationRuntime** suffices for FinLedger cloud copy (Modules 0–1).
- MSI auth avoids keys in JSON — configured in 00-05.
- Self-hosted IR only when source is on-prem or in private network.

**Self-check:** Why does copy activity use two datasets but one linked service for ADLS-to-ADLS?

<details><summary>Answer</summary>Source and sink paths differ (incoming vs loaded) — two datasets, same storage account and linked service.</details>

## Next

[00-05 · Link ADF to storage, click-by-click](00-05-link-adf-to-storage-step-by-step.md)
