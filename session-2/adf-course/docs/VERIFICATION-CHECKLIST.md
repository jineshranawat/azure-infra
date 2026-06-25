# FinLedger UK — master verification checklist

Tick each box **after** the listed lesson. If a lesson is not yet generated (`manifest.json` status `todo`), use [TRAINER-GUIDE.md](../TRAINER-GUIDE.md) module table + Microsoft Learn tutorial until the lesson file exists.

**Portal:** [https://portal.azure.com](https://portal.azure.com)  
Replace `{learner}` with your ID from [SETUP.md](../SETUP.md).

---

## Module 0 — Foundations

### 00-00 Overview
- [ ] I can explain linked service vs dataset vs pipeline without notes
- [ ] I found **Data factories** in the portal search
- [ ] Python factory list script runs (or shows "none yet")

### 00-01 ADLS Gen2
- [ ] Storage account `stadfcourse{learner}` exists in **UK South**
- [ ] **Hierarchical namespace** = Enabled
- [ ] Containers: `bronze`, `silver`, `gold` exist
- [ ] Uploaded `data/module-00-foundations/upload_manifest.txt` → `bronze/incoming/_seed/`
- [ ] Blob **Preview** opens the manifest text

### 00-02 Data Factory
- [ ] Factory `df-adf-course-{learner}` status **Succeeded**
- [ ] Region = **UK South**
- [ ] **Open Azure Data Factory Studio** loads

### 00-03 Studio tour
- [ ] I can open **Author**, **Manage**, **Monitor** from the left rail
- [ ] I located **Copy Data tool** under Author
- [ ] I located **Linked services** under Manage

### 00-04 Linked services & IR
- [ ] I can name three IR types (Azure, self-hosted, SSIS)
- [ ] I know when AutoResolve IR is sufficient (cloud-to-cloud copy)

### 00-05 Link ADF to storage
- [ ] Linked service `ls_adls_main` exists
- [ ] Authentication = **System assigned managed identity**
- [ ] **Test connection** = green
- [ ] Factory MI has **Storage Blob Data Contributor** on storage account (IAM)

---

## Module 1 — Copy & ingest

**Data folder:** [`data/module-01-copy-ingest/`](../data/module-01-copy-ingest/)

### 01-01 Copy Data tool
- [ ] Uploaded `transactions_daily.csv` to `bronze/incoming/transactions/daily/`
- [ ] Wizard created pipeline + datasets
- [ ] Monitor shows copy **Succeeded**
- [ ] Output files under `bronze/loaded/transactions/` (or path from lesson)
- [ ] Row count = **12** data rows (+ header in source file)

### 01-02 Copy activity manual
- [ ] Pipeline `pl_*` contains single **Copy data** activity
- [ ] `customers.csv` copied — **8** rows
- [ ] Part B JSON pasted into Code view validates

### 01-03 Parameters deep dive
- [ ] Pipeline parameter changes sink path without republishing datasets
- [ ] `products.csv` — **10** rows landed

### 01-04 Self-hosted IR
- [ ] Self-hosted IR registered (or documented skip with trainer approval)
- [ ] IR status **Running** during copy

### 01-05 Error handling
- [ ] Failed activity triggers secondary path or notification
- [ ] Retry policy visible on activity **Settings**

### 01-06 S3 → ADLS
- [ ] Partner file landed in bronze (or emulator path documented)

### 01-07 Incremental / watermark
- [ ] `customers_batch1.csv` then `customers_batch2.csv` uploaded to incremental folder
- [ ] `bronze/_control/watermark.json` updated after second run
- [ ] Second run copied **only new** customers (3 rows from batch2)

### 01-08 Change tracking / CDC intro
- [ ] Incremental pipeline copies delta only (per lesson SQL setup)

---

## Module 2 — Mapping data flows

**Data folder:** [`data/module-02-data-flows/`](../data/module-02-data-flows/)

### 02-01 Fundamentals
- [ ] Data flow created; **Debug** session turned **off** after lab
- [ ] Debug cluster not left running

### 02-02 Transformation at scale
- [ ] `df_clean_transactions` published; `pl_silver_transactions` Succeeded
- [ ] Filter preview: **10** posted rows (excludes TXN-10003 pending, TXN-10012 failed)
- [ ] Parquet output in `silver/transactions/cleansed/`
- [ ] Debug cluster **stopped** after preview

### 02-03 Expressions & drift
- [ ] Row with `INVALID` amount filtered or flagged
- [ ] Derived column present in output schema

### 02-04 Delta Lake
- [ ] `_delta_log` directory exists under silver path

### 02-05 Power Query wrangling
- [ ] `returns_raw.csv` uploaded to `bronze/incoming/returns/`
- [ ] **6** returns in silver output

### 02-06 Write to lake
- [ ] Partition folders by date (or configured key) visible in storage

---

## Module 3 — Control flow

**Data folder:** [`data/module-03-control-flow/`](../data/module-03-control-flow/)

### 03-01 Activities catalogue
- [ ] **Lookup** reads `pipeline_parameters.json` from storage
- [ ] **Set Variable** stores a value from Lookup output

### 03-02 ForEach / If / Until / Switch
- [ ] `file_manifest.json` uploaded to `bronze/incoming/manifests/`
- [ ] ForEach copied **3** enabled entities (stores skipped — `enabled: false`)

### 03-03 Parameters & expressions
- [ ] Dynamic content `@pipeline().parameters` resolves at runtime

### 03-04 Triggers
- [ ] Schedule trigger shows **Next trigger** time
- [ ] Manual **Trigger now** still works

### 03-05 Monitoring & email
- [ ] Failed/succeeded pipeline sends notification (Logic App or alert)

---

## Module 4 — External compute

**Data folder:** [`data/module-04-external-compute/`](../data/module-04-external-compute/)

### 04-01 Databricks
- [ ] `scoring_input.csv` in `silver/scoring_input/`
- [ ] Notebook activity **Succeeded**
- [ ] Output in `gold/` path

### 04-02 HDInsight Spark
- [ ] Spark activity **Succeeded**; cluster **deleted** after lab

### 04-03 Hive
- [ ] Hive query activity **Succeeded**; cluster **deleted** after lab

---

## Module 5 — Networking & security

### 05-01 Managed VNet concepts
- [ ] I can draw managed VNet between ADF and data store

### 05-02 Copy in managed VNet
- [ ] Copy activity uses managed IR / managed VNet

### 05-03 On-prem SQL private endpoint
- [ ] Managed private endpoint **Approved**; connection test green

### 05-04 Key Vault
- [ ] Secret referenced from Key Vault — not plain text in linked service

---

## Module 6 — Governance & CI/CD

**Data folder:** [`data/module-06-governance/`](../data/module-06-governance/)

### 06-01 Git integration
- [ ] ADF Studio **Git status** shows connected repo
- [ ] Publish creates PR or commit

### 06-02 ARM CI/CD
- [ ] `sample_arm_parameters.json` used in deploy pipeline
- [ ] Dev and prod factories receive same template

### 06-03 Purview lineage
- [ ] Lineage visible in Purview for at least one pipeline

### 06-04 SSIS IR
- [ ] SSIS package run **Succeeded** (or skip documented)

### 06-05 Cost monitoring
- [ ] Budget or cost alert configured for resource group

---

## Capstone — end-to-end

- [ ] Schedule trigger runs nightly pipeline without manual start
- [ ] Bronze ingest → silver data flow → gold Databricks output
- [ ] Failure email received on intentional test failure
- [ ] Git repo contains pipeline JSON
- [ ] Total MTD cost reviewed in Cost Management

---

## Storage quick-reference paths

| Entity | Incoming path | Expected row/file count |
|---|---|---|
| Transactions | `bronze/incoming/transactions/daily/transactions_daily.csv` | 12 rows |
| Customers | `bronze/incoming/customers/customers.csv` | 8 rows |
| Products | `bronze/incoming/products/products.csv` | 10 rows |
| Stores | `bronze/incoming/stores/store_locations.csv` | 5 rows |
| Returns | `bronze/incoming/returns/returns_raw.csv` | 6 rows |
| Manifest | `bronze/incoming/manifests/file_manifest.json` | 4 file entries |

---

*Last updated: Phase 0+ case study scaffold. Aligns with [CASE-STUDY.md](../CASE-STUDY.md) and [manifest.json](../manifest.json).*
