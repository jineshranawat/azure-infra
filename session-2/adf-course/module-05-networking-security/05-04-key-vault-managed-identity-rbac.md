# 05-04 · Key Vault, managed identity & RBAC

> Module 5 · Time budget: 30 min · Source: [Store credentials in Key Vault](https://learn.microsoft.com/en-us/azure/data-factory/store-credentials-in-key-vault)
> Prereqs: [05-03 · SQL private endpoint](05-03-on-prem-sql-managed-private-endpoint.md)

## What you'll build

Key Vault **`kv-adf-course-{learner}`**, store secret `s3-partner-access-key` (placeholder), **Key Vault linked service** `ls_keyvault_finledger`, reference in **`ls_s3_partner_feed`** from lesson 01-06 instead of plain-text keys.

## Part A — UI (click by click)

1. Portal → **Key Vault** → create `kv-adf-course-{learner}` UK South.
2. **Secrets** → **+ Generate/Import** → name `s3-partner-access-key`, value placeholder.
3. **Access policies** or **RBAC** → grant factory MI **Key Vault Secrets User**.
4. ADF **Manage** → **Linked services** → **Azure Key Vault** → `ls_keyvault_finledger`.
5. Edit `ls_s3_partner_feed` → **Authentication** → **Key Vault** secret reference for access key.
6. **Test connection** → **Publish**.

## Part B — JSON

```json
{
  "name": "ls_s3_partner_feed",
  "properties": {
    "type": "AmazonS3",
    "typeProperties": {
      "serviceUrl": "https://s3.eu-west-2.amazonaws.com",
      "accessKeyId": {
        "type": "AzureKeyVaultSecret",
        "store": { "referenceName": "ls_keyvault_finledger", "type": "LinkedServiceReference" },
        "secretName": "s3-partner-access-key-id"
      },
      "secretAccessKey": {
        "type": "AzureKeyVaultSecret",
        "store": { "referenceName": "ls_keyvault_finledger", "type": "LinkedServiceReference" },
        "secretName": "s3-partner-access-key"
      }
    }
  }
}
```

## Part D — Verify

| Check | Expected |
|---|---|
| No secrets in Git JSON | Key Vault references only |
| MI RBAC | Secrets User on vault |

## Module 5 recap

Managed VNet, private endpoints, Key Vault secrets — FinLedger production security baseline.

## Next

[06-01 · Git integration](../module-06-governance-cicd-ops/06-01-git-integration-azure-devops-github.md)
