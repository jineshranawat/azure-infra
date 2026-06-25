# 01-06 · Amazon S3 to ADLS Gen2

> Module 1 · Time budget: 25 min · Source: [Load data from Amazon S3 to ADLS Gen2](https://learn.microsoft.com/en-us/azure/data-factory/load-azure-data-lake-storage-gen2)
> Prereqs: [01-05 · Error handling](01-05-conditional-execution-error-handling.md)

## What you'll build

Linked service **`ls_s3_partner_feed`** (FinLedger partner bucket or **S3-compatible emulator**), pipeline **`pl_s3_to_bronze`** copying partner `transactions_daily.csv` to `bronze/incoming/partner_s3/`. Complete JSON + portal path per MS Learn hybrid cloud ingest.

## Part A — UI (summary path)

1. **Manage** → **+ New** → **Amazon S3** → name `ls_s3_partner_feed`.
2. **Authentication:** Access key (training bucket only) or **Anonymous** for public read test bucket.
3. **Service URL:** `https://s3.eu-west-2.amazonaws.com` (partner region — document exception: S3 not UK; FinLedger partner is EU).
4. **Test connection** → **Create** → **Publish**.
5. Source dataset on S3 bucket/key; sink `ls_adls_main` → `bronze/incoming/partner_s3/`.
6. Pipeline `pl_s3_to_bronze` → **Copy data** → **Trigger now** → verify rows in ADLS.

> ⚠️ WARNING: No AWS account? Trainer skip: read linked service blade + JSON only; use second ADLS folder as "pseudo S3" with manual upload labelled partner feed.

## Part B — JSON

`linkedService/ls_s3_partner_feed.json`

```json
{
  "name": "ls_s3_partner_feed",
  "properties": {
    "type": "AmazonS3",
    "typeProperties": {
      "serviceUrl": "https://s3.eu-west-2.amazonaws.com",
      "accessKeyId": "<use-key-vault-in-prod>",
      "secretAccessKey": "<use-key-vault-in-prod>",
      "authenticationType": "AccessKey"
    },
    "connectVia": {
      "referenceName": "AutoResolveIntegrationRuntime",
      "type": "IntegrationRuntimeReference"
    }
  }
}
```

> ℹ️ NOTE: Module 5 moves secrets to Key Vault — never commit real keys.

## Part C — Python

Use `AmazonS3LinkedService` in `azure.mgmt.datafactory.models` with credentials from environment variables.

## Part D — Verify

| Check | Expected |
|---|---|
| Copy Succeeded | Rows in `bronze/incoming/partner_s3/` |
| No keys in Git | Placeholder credentials only |

## Common errors

403 on S3 — IAM policy; wrong region URL. ADLS 403 — MSI RBAC from 00-05.

## Cost & tear-down

S3 egress + copy DIU — small for one CSV. Remove S3 linked service after lab.

## Next

[01-07 · Incremental copy patterns](01-07-incremental-copy-patterns.md)
