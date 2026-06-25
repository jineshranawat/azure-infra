# Session 2 — Manual portal lab (read, do, verify)

> **Classroom:** use [SESSION2-STUDENT-GUIDE.md](SESSION2-STUDENT-GUIDE.md) first — it orchestrates this lab, the scripts, and the 20h [adf-course](adf-course/README.md). This file has **extra portal detail** per section.

Follow these steps in the **Azure Portal** and **ADF Studio**. You can run them **after** `orchestrate.cmd` (verify what the script built) or use **Section B** as a full hands-on path without the script.

Replace `<learner>` with your `.env` value (e.g. `jinesh`).

**Portal:** [https://portal.azure.com](https://portal.azure.com)

---

## How this fits the 2-hour session

| Order | Path | When |
|-------|------|------|
| 1 | Run `orchestrate.cmd` | Automates RBAC, upload, pipeline deploy |
| 2 | **This guide — Sections C–F** | Read each step → do in portal → tick verify |
| 3 | Optional **Section B** | Build the same pipeline by hand in ADF Studio |

---

## A. Find your resources (5 min)

### Do

1. Sign in to [portal.azure.com](https://portal.azure.com) (same account as `az login`).
2. Top search bar → type `rg-<learner>-class1` → open the resource group.
3. Note these names from the resource list:

| Resource type | Name pattern | You need this |
|---------------|--------------|---------------|
| Storage account | `st<learner><hash>` | Yes |
| Data factory | `adf-<learner>-<hash>` | Yes |

### Verify

- [ ] Resource group **Location** = **UK South** or **UK West**
- [ ] **Tags** blade shows `env=training`, `owner=<your email>`, `course=azure-etl-boe`
- [ ] Storage account and Data factory status = **Succeeded**

---

## B. Manual path — Storage upload (15 min)

*Skip if you already ran `orchestrate.cmd` — go to Section C to verify the script upload.*

### B1. Open the bronze container

1. Resource group → click **storage account** (`st...`).
2. Left menu → **Data storage** → **Containers**.
3. Open container **`bronze`** (created in Class-1).

### B2. Create folder path and upload

1. Click **Upload**.
2. Click **Browse for files** → select `session-2\data\sample_transactions.csv` from your repo clone.
3. Under **Advanced** → **Upload to folder**, enter:

   ```text
   incoming/transactions/manual-run/sample_transactions.csv
   ```

4. Click **Upload**.

### B3. Verify in Storage

| Check | Where | Expected |
|-------|--------|----------|
| File exists | bronze → `incoming` → `transactions` → `manual-run` | `sample_transactions.csv` |
| File size | Click blob → **Properties** | ~300 bytes, 6 data rows + header |
| Tier | Properties → Access tier | **Hot** |

- [ ] Blob path matches: `bronze/incoming/transactions/manual-run/sample_transactions.csv`
- [ ] You can **Preview** the CSV in the portal (first lines visible)

### B4. Optional — manual watermark file

1. In container **bronze**, **Upload** a new file (or use **Edit** in Storage Explorer).
2. Path: `_control/watermark.json`
3. Content:

   ```json
   {
     "last_run_id": "manual-run",
     "last_loaded_path": "bronze/incoming/transactions/manual-run",
     "updated_utc": "2026-06-22T12:00:00Z",
     "feed": "sample_transactions",
     "note": "created manually in portal"
   }
   ```

### Verify

- [ ] `_control/watermark.json` exists under bronze
- [ ] JSON opens and shows `last_run_id`

---

## C. Verify Storage after `orchestrate.cmd` (10 min)

*Run from `session-2` folder:*

```text
orchestrate.cmd
```

### C1. Incoming feed

1. Storage account → **Containers** → **bronze**.
2. Navigate: `incoming` → `transactions` → folder named like `20260622T143052Z` (UTC timestamp).
3. Open `sample_transactions.csv`.

### Verify

| # | Check | Pass? |
|---|--------|-------|
| 1 | Folder under `incoming/transactions/<run_id>/` exists | [ ] |
| 2 | File name = `sample_transactions.csv` | [ ] |
| 3 | Preview shows columns: `transaction_id`, `account_id`, `amount_gbp`, … | [ ] |
| 4 | Row count = 5 transactions (+ header) | [ ] |

### C2. Watermark control file

1. bronze → `_control` → `watermark.json`
2. Click → **Edit** or **View** (Storage Explorer).

### Verify

| # | Check | Pass? |
|---|--------|-------|
| 1 | `last_run_id` matches the folder name in incoming | [ ] |
| 2 | `last_loaded_path` contains `loaded/run=` | [ ] |
| 3 | `updated_utc` is today (UTC) | [ ] |

### C3. Loaded path (after `--run-pipeline` only)

1. bronze → `loaded` → `run=<run_id>` → `sample_transactions.csv`

### Verify

| # | Check | Pass? |
|---|--------|-------|
| 1 | File exists after successful pipeline run | [ ] |
| 2 | Same row count as incoming file | [ ] |

---

## D. ADF — open studio & linked service (10 min)

### D1. Open Data Factory

1. Resource group → click **Data factory** (`adf-...`).
2. Click **Open Azure Data Factory Studio** (or **Author & Monitor**).

### D2. Inspect linked service

1. In ADF Studio → left **Manage** (toolbox icon).
2. **Linked services** → open **`AdlsBronzeLinkedService`**.

### Verify

| # | Check | Pass? |
|---|--------|-------|
| 1 | Type = **Azure Data Lake Storage Gen2** | [ ] |
| 2 | URL = `https://st<learner><hash>.dfs.core.windows.net` | [ ] |
| 3 | Test connection → **Successful** (or **Connected** after save) | [ ] |

**If test fails:** wait 2 minutes after `orchestrate.cmd` (RBAC propagation), then **Test connection** again. Re-run `orchestrate.cmd` if still failing.

### D3. ADF managed identity on storage (read-only check)

1. Portal → storage account → **Access control (IAM)**.
2. **Role assignments** → search for your ADF name.
3. Confirm role **Storage Blob Data Contributor** on the storage account for the ADF **managed identity**.

### Verify

- [ ] ADF system-assigned identity has **Storage Blob Data Contributor** on the storage account

---

## E. ADF — datasets & pipeline (15 min)

### E1. Verify script-deployed artefacts

In ADF Studio → **Author** (pencil icon):

1. **Datasets** → confirm:
   - `ds_bronze_incoming_csv`
   - `ds_bronze_loaded_csv`
2. Open `ds_bronze_incoming_csv`:
   - Linked service: `AdlsBronzeLinkedService`
   - File system: `bronze`
   - Parameter: `incoming_folder`
3. **Pipelines** → open **`pl_bronze_copy`**
4. Click activity **CopyIncomingToLoaded** → **Source** / **Sink** tabs

### Verify

| # | Check | Pass? |
|---|--------|-------|
| 1 | Both datasets exist | [ ] |
| 2 | Pipeline `pl_bronze_copy` exists | [ ] |
| 3 | Copy activity source = `ds_bronze_incoming_csv` | [ ] |
| 4 | Copy activity sink = `ds_bronze_loaded_csv` | [ ] |
| 5 | Pipeline has parameters `incoming_folder` and `loaded_folder` | [ ] |

---

## F. Manual ADF — trigger pipeline run (15 min)

You can trigger from **portal** or **script**:

```text
orchestrate.cmd --run-pipeline
```

### F1. Trigger in ADF Studio (manual)

1. **Author** → pipeline **`pl_bronze_copy`**.
2. Click **Add trigger** → **Trigger now**.
3. Enter parameters (use your run_id from Section C):

   | Parameter | Example value |
   |-----------|----------------|
   | `incoming_folder` | `incoming/transactions/20260622T143052Z` |
   | `loaded_folder` | `loaded/run=20260622T143052Z` |

4. Click **OK** → note the **Run ID** in the notification.

### F2. Monitor the run

1. Left **Monitor** → **Pipeline runs**.
2. Find your run → status should move **Queued** → **In progress** → **Succeeded**.
3. Click the run → open **CopyIncomingToLoaded** → **Input** / **Output** → **Data read** / **Data written**.

### Verify

| # | Check | Pass? |
|---|--------|-------|
| 1 | Pipeline run status = **Succeeded** | [ ] |
| 2 | Data read &gt; 0 bytes | [ ] |
| 3 | Data written &gt; 0 bytes | [ ] |
| 4 | Storage: `bronze/loaded/run=<run_id>/sample_transactions.csv` exists | [ ] |
| 5 | Incoming and loaded files have same content (preview both) | [ ] |

### F3. If run **Failed** — common fixes

| Error hint | Portal action |
|------------|----------------|
| 403 / Authorization | Storage → IAM → confirm ADF MI role; wait 2 min, **Trigger now** again |
| Path not found | Check `incoming_folder` matches exact folder under bronze |
| Linked service fail | Manage → Linked services → **Test connection** |

---

## G. Manual ADF — build copy pipeline by hand (optional, 30 min)

*Do this only if your trainer assigns a portal-only exercise. Skip if you used `orchestrate.cmd`.*

### G1. Create datasets (if not present)

**Source dataset**

1. **Author** → **+** → **Dataset**.
2. **Azure Data Lake Storage Gen2** → **DelimitedText** → **Linked service:** `AdlsBronzeLinkedService`.
3. File path: file system `bronze`, directory `incoming/transactions/manual-run`, file `sample_transactions.csv`.
4. Name: `ds_manual_incoming` → **OK**.

**Sink dataset**

1. New dataset → same linked service.
2. Directory `loaded/run=manual-run`, file `sample_transactions.csv`.
3. Name: `ds_manual_loaded` → **OK**.

### G2. Create pipeline

1. **Author** → **+** → **Pipeline** → name `pl_manual_copy`.
2. **Activities** → drag **Copy data** to canvas.
3. **Source** tab → source dataset `ds_manual_incoming`.
4. **Sink** tab → sink dataset `ds_manual_loaded`.
5. **Mapping** → **Import schemas** (if prompted).
6. **Publish all** (top bar).

### Verify

- [ ] **Publish** succeeded (no errors)
- [ ] **Trigger now** on `pl_manual_copy` → **Succeeded**
- [ ] Loaded file appears in bronze (`loaded/run=manual-run/`)

---

## H. Morning check — script vs portal (10 min)

### H1. Script

```text
orchestrate.cmd
```

Terminal Phase 6 lists pipeline runs (last 24h).

### H2. Portal

1. ADF Studio → **Monitor** → **Pipeline runs**.
2. Filter last 24 hours; compare run IDs and status to terminal output.

### Verify

| # | Check | Pass? |
|---|--------|-------|
| 1 | Same pipeline name `pl_bronze_copy` in portal and script output | [ ] |
| 2 | Status in portal matches script `[OK]` / `[FAIL]` lines | [ ] |
| 3 | Run timestamp is today | [ ] |

---

## I. End-to-end verification checklist

Complete before leaving Session 2:

### Storage

- [ ] `bronze/incoming/transactions/<run_id>/sample_transactions.csv` exists
- [ ] `bronze/_control/watermark.json` exists and valid JSON
- [ ] After pipeline run: `bronze/loaded/run=<run_id>/sample_transactions.csv` exists
- [ ] `silver`, `gold`, `audit` containers still empty (Class-1 only — no accidental uploads)

### ADF

- [ ] Factory opens in ADF Studio
- [ ] Linked service `AdlsBronzeLinkedService` tests OK
- [ ] Datasets `ds_bronze_incoming_csv` and `ds_bronze_loaded_csv` present
- [ ] Pipeline `pl_bronze_copy` published
- [ ] At least one pipeline run **Succeeded** (after `--run-pipeline` or Trigger now)

### Cost

- [ ] Cost Management → filter RG `rg-<learner>-class1` → MTD still pennies
- [ ] No self-hosted integration runtime created
- [ ] No Mapping Data Flow activities added

### Code map (what you did manually vs script)

| Portal step | Automated by |
|-------------|--------------|
| Upload CSV to bronze | `bronze_loader.py` |
| ADF MI storage RBAC | `adf_rbac.py` |
| Linked service + datasets + pipeline | `adf_pipeline.py` |
| Watermark JSON | `watermark_store.py` |
| Trigger pipeline | `orchestrate.cmd --run-pipeline` OR **Trigger now** |
| Run history report | `morning_check.py` OR **Monitor** blade |

---

## J. Quick links

| Item | URL |
|------|-----|
| Resource group | `https://portal.azure.com` → search `rg-<learner>-class1` |
| ADF Studio | RG → Data factory → **Open Azure Data Factory Studio** |
| Storage containers | RG → Storage → Containers → **bronze** |
| Cost analysis | Cost Management → filter by resource group |

---

## K. Troubleshooting (portal)

| Symptom in portal | What to do |
|-------------------|------------|
| Cannot upload blob | Storage → IAM → your user needs **Storage Blob Data Contributor** — re-run root `orchestrate.cmd` |
| ADF Test connection fails | Confirm MI role on storage; wait 2 min |
| Pipeline 404 path | Parameter `incoming_folder` must not start with `/` or `bronze/` |
| Preview empty file | Re-upload CSV from `session-2\data\sample_transactions.csv` |
| No pipeline in Author | Run `orchestrate.cmd` from `session-2` folder first |

For script errors, see [README.md](README.md) Section G.
