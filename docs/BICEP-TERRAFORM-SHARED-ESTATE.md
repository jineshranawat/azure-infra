# Bicep ↔ Terraform — same shared estate, two IaC languages

**Open in class:** [infra-cicd-walkthrough.html](infra-cicd-walkthrough.html) (section Terraform) · this file for deep detail.

FinLedger ships **both**:

| Path | Language | Entry command |
|------|----------|---------------|
| `infra/shared-eastus.bicep` | Bicep / ARM Incremental | `provision-shared.cmd` |
| `infra/terraform/shared-eastus/` | Terraform (azurerm) | `provision-shared-tf.cmd` |
| `infra/shared-sql-westus.bicep` | Bicep | via `deploy-shared-adf-lab.cmd` |
| `infra/terraform/shared-sql-westus/` | Terraform | optional / teaching |

**Same case:** same subscription, same RG `rg-shared-class1`, same region `eastus`, same resource types and tags.  
**Name parity:** set Terraform `name_hash = "qgr7mj"` to match the live Bicep estate (`stsharedqgr7mj`, …).

---

## 0. Why teach both?

| Question | Answer |
|----------|--------|
| Do enterprises use only one? | No — many shops standardise on **Terraform** or **Bicep/ARM**; banks often know both. |
| Does TF “call” the `.bicep` file? | **No.** Terraform does **not** invoke Bicep. Both describe the **same Azure resources** and talk to ARM. Think two blueprints of the same house. |
| Which should students run day-to-day? | Either. Class labs were born on **Bicep** (`provision-shared.cmd`). Use **Terraform** to learn the industry-common DSL and to compare side-by-side. |
| Can both manage the same RG? | Yes if names match (`name_hash`) and you **import** existing resources into TF state — or use TF only on a greenfield hash. Do **not** apply both blindly without state/import (drift). |

**Analogy:** Bicep is the architect’s CAD file in Microsoft’s format; Terraform is AutoCAD from HashiCorp. Same walls and plumbing — different drawing tool. Neither “runs” the other.

---

## 0.5 Benefits of Terraform over Bicep — each mapped to a runnable command here

> **Verified on this repo, 19 Jul 2026:** Terraform v1.15.8 (`winget install Hashicorp.Terraform`) →
> `terraform validate` = Success on both modules → `provision-shared-tf.cmd --plan-only` =
> **Plan: 14 to add, 0 to change, 0 to destroy** with the exact live names
> (`stsharedqgr7mj`, `kv-shared-qgr7mj`, `adf-shared-qgr7mj`, `dbw-shared-qgr7mj`).

| # | Benefit | Meaning | Executable proof in this repo |
|---|---------|---------|-------------------------------|
| 1 | **Plan before apply** | Full create/change/destroy diff approved *before* Azure is touched (Bicep `what-if` is optional, less exact) | `provision-shared-tf.cmd --plan-only` |
| 2 | **State = drift detection** | `terraform.tfstate` remembers what TF built; portal clicks show up as drift on next plan. Bicep has no state | `terraform plan` in `infra\terraform\shared-eastus` after apply/import |
| 3 | **Multi-provider** | Azure + Entra ID (+ Databricks/GitHub/AWS) in one HCL codebase; Bicep is Azure-only | `main.tf` loads `azurerm` + `azuread` together |
| 4 | **Precise destroy** | `terraform destroy` removes exactly the state contents; Bicep teardown = delete whole RG | `terraform destroy` (sandbox only — NEVER `rg-shared-class1`) |
| 5 | **Import existing** | Adopt Bicep/portal resources into code without recreating | §E below |
| 6 | **Provider lock file** | `.terraform.lock.hcl` pins plugin versions on every machine | committed in `shared-eastus/` |
| 7 | **HCL loops + validation** | `for_each` containers, `validation` locks region to `eastus` | `azurerm_storage_container.medallion`, `var.location` |
| 8 | **Registry ecosystem** | Huge community module registry | `terraform init` pulls signed providers |
| 9 | **Job-market standard** | Most-requested IaC skill cross-cloud | `infra-walkthrough.cmd --phase 1t` |

**When Bicep wins:** no state file to lose, day-0 Azure feature support, native `az` tooling, zero extra installs — which is why the class default remains `provision-shared.cmd`.

---

## A. Theory — how each talks to Azure

```text
                 ┌─────────────────┐
  Engineer       │  Git (main)     │
                 └────────┬────────┘
            ┌─────────────┴─────────────┐
            ▼                           ▼
   infra/shared-eastus.bicep    infra/terraform/shared-eastus/*.tf
            │                           │
            ▼                           ▼
   az deployment group create    terraform apply
   --mode Incremental            (azurerm provider)
            │                           │
            └─────────────┬─────────────┘
                          ▼
              Azure Resource Manager (ARM)
                          │
                          ▼
         rg-shared-class1  (KV, ADLS, ADF, Databricks, …)
```

| Concept | Bicep | Terraform |
|---------|-------|-----------|
| Declarative desired state | Yes | Yes |
| Incremental / update in place | `--mode Incremental` | Plan then apply (updates existing by ID in state) |
| State file | Azure remembers deployment history | Local/remote **terraform.tfstate** (must not lose) |
| Auth | `az login` / SPN | `az login` (`ARM_USE_CLI=true`) or SPN env vars |
| Modules | modules / nested templates | modules / registries |
| Secrets | `@secure()` params | `sensitive = true` + Key Vault data sources |

Microsoft Learn:

- [Bicep](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/)
- [ARM Incremental](https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/deployment-modes)
- [Terraform on Azure](https://learn.microsoft.com/en-us/azure/developer/terraform/overview)
- [azurerm provider](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs)

---

## B. Resource map — Bicep line ↔ Terraform resource (same case)

| What | Bicep (`shared-eastus.bicep`) | Terraform (`infra/terraform/shared-eastus/main.tf`) |
|------|-------------------------------|-----------------------------------------------------|
| Resource group | Created by `provision_shared.py` then Bicep targets it | `azurerm_resource_group.shared` |
| Tags | `var tags` | `local.tags` (+ `iac=terraform`) |
| Name hash | `uniqueString(rg.id, learner, 'shared-eastus')` → 6 chars | `var.name_hash` or `sha1(...)` slice |
| Storage | `Microsoft.Storage/storageAccounts` | `azurerm_storage_account.shared` |
| Containers | `bronze/silver/gold/audit` loop | `azurerm_storage_container.medallion` for_each |
| Lifecycle | `managementPolicies` cool 30d + delete 7d | `azurerm_storage_management_policy.lifecycle` |
| Key Vault | `Microsoft.KeyVault/vaults` RBAC | `azurerm_key_vault.shared` |
| KV RBAC | roleAssignment Secrets Officer | `azurerm_role_assignment.kv_secrets_officer` |
| Storage RBAC | Blob Data Contributor | `azurerm_role_assignment.storage_blob_contributor` |
| ADF | `Microsoft.DataFactory/factories` + MSI | `azurerm_data_factory.shared` |
| Linked service | `AdlsBronzeLinkedService` AzureBlobFS | `azurerm_data_factory_linked_service_data_lake_storage_gen2.bronze` |
| Databricks | `Microsoft.Databricks/workspaces` premium | `azurerm_databricks_workspace.shared` |
| Managed RG | `rg-{learner}-dbw-{hash}` | `managed_resource_group_name` |
| Budget (optional) | `Microsoft.Consumption/budgets` if `deployBudget` | `azurerm_consumption_budget_resource_group` if `deploy_budget` |
| Outputs | `storageAccountName`, `keyVaultName`, … | same output names |

### SQL westus twin

| What | Bicep `shared-sql-westus.bicep` | Terraform `shared-sql-westus/main.tf` |
|------|--------------------------------|----------------------------------------|
| SQL server | `Microsoft.Sql/servers` westus | `azurerm_mssql_server.shared` |
| Firewall | AllowAllAzureIPs 0.0.0.0 | `azurerm_mssql_firewall_rule.azure_services` |
| Database | Basic `finledger` | `azurerm_mssql_database.finledger` |
| KV secret | `sql-admin-password` | `azurerm_key_vault_secret.sql_password` |

---

## C. Naming / hash — critical for “same case”

Bicep:

```bicep
var nameHash = substring(uniqueString(resourceGroup().id, learner, 'shared-eastus'), 0, 6)
var storageAccountName = toLower('st${learner}${nameHash}')
```

Terraform **cannot** reproduce Azure `uniqueString` bit-for-bit. For the **existing class estate**:

```hcl
name_hash = "qgr7mj"   # from stsharedqgr7mj / adf-shared-qgr7mj / …
```

`provision-shared-tf.cmd` defaults to `--name-hash qgr7mj` so apply targets the same names as Bicep.

Greenfield TF-only (new names): pass `--name-hash ""` empty and TF uses `substr(sha1(...),0,6)` — **different** storage/ADF names (fine for a sandbox, not for shared class).

---

## D. Commands — Bicep path vs Terraform path

### D.1 Prerequisites (both)

```cmd
cd D:\azure
az login
az account set --subscription a64c0dd2-3a31-4604-bde3-3d40c7d5e8be
```

Ensure `.env` has `CLASS_OWNER_EMAIL` or `OWNER_EMAIL`.

### D.2 Deploy with **Bicep** (original lab path)

```cmd
.\provision-shared.cmd
.\infra-walkthrough.cmd --phase 1
```

Child: `scripts\provision_shared.py` → `az deployment group create --mode Incremental --template-file infra\shared-eastus.bicep`

### D.3 Deploy with **Terraform** (twin path)

```cmd
winget install Hashicorp.Terraform
.\provision-shared-tf.cmd --plan-only
.\provision-shared-tf.cmd --auto-approve
```

Tested 19 Jul 2026 with Terraform v1.15.8: validate = Success, plan-only = `14 to add, 0 to change, 0 to destroy` (same names as live Bicep estate). Note: provider is pinned to azurerm `~> 3.117`, so 3.x attribute names apply (e.g. `enable_rbac_authorization`, not the 4.x `rbac_authorization_enabled`).

Child: `scripts\provision_shared_tf.py` → `terraform init` + `terraform apply` in `infra\terraform\shared-eastus\`

Plan only (safe classroom demo):

```cmd
.\provision-shared-tf.cmd --plan-only
```

### D.4 After either IaC — same app release path

IaC only builds the house. Notebooks / ADF pipelines still use:

```cmd
.\deploy-shared-lab.cmd
.\release.cmd adf
.\release.cmd medallion-deploy
```

---

## E. Import existing Bicep estate into Terraform — now AUTOMATIC

### The error a learner will see without import

```text
Error: A resource with the ID "/subscriptions/.../resourceGroups/rg-shared-class1" already exists -
to be managed via Terraform this resource needs to be imported into the State.
```

**Why:** the estate was built by Bicep, so the learner's local `terraform.tfstate` is empty.
Terraform refuses to adopt resources it did not create — safety by design, nothing was harmed.

### The fix (built into the driver, tested 19 Jul 2026)

`scripts/provision_shared_tf.py` now does **check-before-create** (guardrail 2) on every apply:

1. **Auto-import** — for each expected resource (RG, KV, storage + 4 containers + lifecycle,
   ADF + linked service, Databricks, 3 role assignments): if it exists in Azure but not in state,
   run `terraform import` automatically. Role-assignment IDs are rebuilt with proper-case
   `/resourceGroups/` (az CLI returns lowercase, which would force a bogus recreate).
2. **Destroy-gate** — plan is saved to a file and inspected as JSON; if ANY resource would be
   deleted or recreated, the run ABORTS before touching Azure and tells you to review with
   `--plan-only`. On the shared class estate destroys are never automatic.
3. **Apply the saved plan** — exactly what was inspected, nothing else.

So the failing command simply works now:

```cmd
git pull
.\provision-shared-tf.cmd --auto-approve
```

Verified sequence on the live estate: run 1 = imported 14 resources, gate blocked two
role-assignment recreates (then fixed in code); run 2 = converged with 6 in-place updates
(tags only + ADF linked-service managed identity), **0 destroyed**; run 3 =
**No changes. 0 added, 0 changed, 0 destroyed** — idempotent.

### Team rule — one applier, everyone else plans

State is **local per laptop** (`infra/terraform/shared-eastus/terraform.tfstate`, gitignored).
Two people applying from different laptops both work (auto-import rebuilds state), but the
`owner` tag flips to whoever ran last (it comes from their `.env` OWNER_EMAIL). In class:
**students run `--plan-only`; the trainer applies.** Enterprises solve this properly with a
remote state backend (Azure Storage + state locking).

---

## F. When to use which (classroom rule)

| Situation | Prefer |
|-----------|--------|
| Follow Day-1 shared provision as designed | **Bicep** `provision-shared.cmd` |
| Teach Terraform / interview prep / enterprise DSL | **Terraform** `provision-shared-tf.cmd --plan-only` then apply |
| Compare side-by-side on projector | Open this doc + both files |
| Daily notebook/ADF release | Neither — use `release.cmd` (app layer) |

---

## G. File map

| File | Role |
|------|------|
| `infra/shared-eastus.bicep` | Canonical Bicep shared estate |
| `infra/shared-sql-westus.bicep` | SQL Basic westus |
| `infra/terraform/shared-eastus/main.tf` | Terraform twin |
| `infra/terraform/shared-eastus/terraform.tfvars.example` | Example vars + live hash |
| `infra/terraform/shared-sql-westus/main.tf` | SQL twin |
| `scripts/provision_shared.py` | Bicep driver |
| `scripts/provision_shared_tf.py` | Terraform driver |
| `provision-shared.cmd` | Windows entry Bicep |
| `provision-shared-tf.cmd` | Windows entry Terraform |
| `docs/BICEP-TERRAFORM-SHARED-ESTATE.md` | This guide |

---

## H. Guardrails

- Region: shared estate **eastus** (documented exception); SQL **westus** (documented exception).
- SKUs: Standard_LRS storage, KV Standard, ADF, Databricks Premium (lab), SQL Basic.
- Idempotent: Bicep Incremental / Terraform apply both update in place when names match.
- Never teardown `rg-shared-class1` in class.
- Do not commit `terraform.tfstate`, `.terraform/`, or `terraform.tfvars` with secrets.

### Four re-run scenarios (Terraform)

| Scenario | Safe? | Notes |
|----------|-------|-------|
| Fresh machine, nothing deployed | Yes | `provision-shared-tf.cmd --auto-approve` creates RG + resources |
| Re-run after full success | Yes | Plan shows no/minimal drift; apply is no-op |
| Re-run after partial/failed apply | Yes | Resume — Terraform continues from state; fix errors then re-apply |
| Re-run after teardown | Yes | Empty state + missing Azure resources → creates again (same `name_hash`) |

**Re-run command:** `provision-shared-tf.cmd` (or `--plan-only` first).

**Warning:** If the live estate was created by **Bicep** and TF has **empty** state, a blind apply with `name_hash=qgr7mj` may try to create resources that already exist → import first (section E) or stick to Bicep for that RG.

---

## I. Quick desk card

```text
SAME HOUSE, TWO BLUEPRINTS
  Bicep:       provision-shared.cmd      → infra/shared-eastus.bicep
  Terraform:   provision-shared-tf.cmd   → infra/terraform/shared-eastus/

SAME HASH FOR LIVE CLASS
  name_hash = qgr7mj

THEN APP RELEASE (unchanged)
  release.cmd databricks | adf | medallion-deploy
```
