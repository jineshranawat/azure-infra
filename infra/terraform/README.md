# Terraform twin of shared Bicep estate

**Does not call Bicep.** Same Azure resources via the azurerm provider. Detail + resource map: [docs/BICEP-TERRAFORM-SHARED-ESTATE.md](../../docs/BICEP-TERRAFORM-SHARED-ESTATE.md).

| Folder | Bicep twin | Entry |
|--------|------------|--------|
| `shared-eastus/` | `infra/shared-eastus.bicep` | `provision-shared-tf.cmd` |
| `shared-sql-westus/` | `infra/shared-sql-westus.bicep` | teaching / optional apply |

```bat
provision-shared-tf.cmd --plan-only
provision-shared-tf.cmd --auto-approve
infra-walkthrough.cmd 1t
```

Live class names: default `name_hash=qgr7mj`. Install Terraform once (`winget install Hashicorp.Terraform`), then use the `.cmd` above (loads `.env`, runs `init` / `plan` / `apply`).
