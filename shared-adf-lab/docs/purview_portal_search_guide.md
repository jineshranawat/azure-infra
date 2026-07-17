# Microsoft Purview — what to search and where to click (FinLedger shared lab)

**For:** Trainers and learners using `rg-shared-class1` + Purview account `pviewrohan4hnv7s`

---

## ⚠️ CRITICAL — why `adf-shared-qgr7mj` shows “No search results”

You are not doing anything wrong. Three things cause this:

### 1. Wrong portal mode (most common)

The toggle **“New Microsoft Purview portal”** (top of screen) must be **OFF** for **Azure Data Factory lineage**.

| Portal mode | ADF pipelines & lineage |
|-------------|-------------------------|
| **New** portal ON (`purview.microsoft.com`) | ❌ **ADF not supported** — factory name will not appear |
| **Classic** Governance portal | ✅ ADF registration, lineage, Data Map connections |

**Fix:** Turn **OFF** the “New Microsoft Purview portal” toggle → you land in the **classic** Purview Governance portal (`web.purview.azure.com` style).

Or open directly:

[https://web.purview.azure.com/resource/subscriptions/a64c0dd2-3a31-4604-bde3-3d40c7d5e8be/resourceGroups/rg-rohan-class1/providers/Microsoft.Purview/accounts/pviewrohan4hnv7s/catalog/overview](https://web.purview.azure.com/resource/subscriptions/a64c0dd2-3a31-4604-bde3-3d40c7d5e8be/resourceGroups/rg-rohan-class1/providers/Microsoft.Purview/accounts/pviewrohan4hnv7s/catalog/overview)

### 2. Wrong search box

| Search box | What it finds |
|------------|----------------|
| **Global** search (`purview.microsoft.com/home?globalsearchquery=...`) | Navigation, users, Azure resources — **not ADF pipelines** |
| **Data catalog** search (classic portal) | Data assets: files, tables, paths |

**Do not** search for the ADF factory name in global search. Search for **data paths** instead (see section B).

### 3. ADF factory name is not a catalog asset

Purview indexes **datasets** (ADLS paths, tables), not the string `adf-shared-qgr7mj`.

ADF appears as a **process node on the Lineage tab** of a data asset — e.g. `sample_transactions.csv` or `loaded/run=session3-lab`.

**Search this instead:** `stsharedqgr7mj`, `sample_transactions`, `loaded`, `cleaned`, `aggregates`

### 4. ADF must have Data Curator on Purview (one-time)

Lineage push fails silently if the ADF managed identity lacks **Data Curator** on the Purview **root collection**.

**Fix (classic portal):**

1. **Data map** → **Access control** (or **Policy** → **Access control**)
2. **Root collection** → **Role assignments** → **Add**
3. Role: **Data Curator**
4. Member: **`adf-shared-qgr7mj`** (managed identity / service principal)
5. Save

**Verify in ADF Studio:**

1. **Manage** → **Microsoft Purview**
2. Status must be **Connected** (not Disconnected)

---

## A. WHAT & WHY — before you search

### Microsoft Purview Data Catalog

**What:** A searchable index of data assets (files, tables, pipelines) plus **lineage** (how data flows).

**Why:** Answers “where did this report come from?” and “what breaks if I change this column?”

**Analogy:** Purview is **Google for your data estate** — ADF and Databricks are two of the systems it indexes.

### What this lab registers automatically vs manually


| Source                                | Appears in Purview?        | How                                      |
| ------------------------------------- | -------------------------- | ---------------------------------------- |
| **ADF Copy / Data Flow** (`pl_gov_`*) | Often yes (after 5–30 min) | ADF factory linked to Purview            |
| **ADLS** (`stsharedqgr7mj`)           | After **scan**             | Data Map → Register source → Scan        |
| **Databricks notebooks**              | Not automatic              | Manifest JSON on ADLS + optional UC link |
| **50-problems** `prob`* **tables**    | After ADLS/UC scan         | Under silver `demo_50_problems` paths    |


---



## B. Your estate — names to search

Copy these into the **Search catalog** box on the Overview page:


| Search term | What you hope to find |
|-------------|----------------------|
| `stsharedqgr7mj` | Storage account + containers — **search this first** |
| `sample_transactions` | Bronze CSV → open **Lineage** tab to see ADF |
| `loaded` | Bronze loaded folder |
| `cleaned` | Silver cleansed (pl_gov_02) |
| `aggregates` | Gold aggregates (pl_gov_03) |
| `finledger` | SQL database (if scanned) |
| `bronze` / `silver` | Container paths |
| `daily_channel_summary` | Gold Delta (Databricks) |
| `governance` | Audit governance JSON (after scan) |
| `adf-shared-qgr7mj` | **Usually zero results** — not a catalog asset; see Lineage on paths above |

**Subscription:** `a64c0dd2-3a31-4604-bde3-3d40c7d5e8be`  
**Shared RG:** `rg-shared-class1`  
**Purview account RG:** `rg-rohan-class1` → `pviewrohan4hnv7s`

---



## C. Step-by-step — classic Purview portal (for ADF lineage)

> **Use classic portal** (toggle **New Microsoft Purview portal** = **OFF**).

### Step 1 — Register ADF in Purview (one-time)

1. Classic portal → **Data map** → **Lineage connections** → **Data Factory**
2. **New** → select **`adf-shared-qgr7mj`** → **OK**

### Step 2 — Grant ADF Data Curator (one-time)

1. **Data map** → **Access control** → **Root collection**
2. **Role assignments** → **Add** → Role: **Data Curator** → Member: **`adf-shared-qgr7mj`**

### Step 3 — Search catalog (not global home search)

1. **Data catalog** → **Overview** → **Search catalog** box
2. Type **`stsharedqgr7mj`** or **`sample_transactions`**
3. Open result → **Lineage** tab → ADF pipeline appears as process node

### Step 4 — Register + scan ADLS (if no storage assets)

1. **Data map** → **Sources** → **Register** → **Azure Data Lake Storage Gen2**
2. Account **`stsharedqgr7mj`** → **New scan** → containers `bronze`, `silver`, `audit`

---

## C2. Step-by-step — new Purview portal (limited — no ADF)



### Step 1 — Open Data Catalog Overview

1. Go to [https://purview.microsoft.com](https://purview.microsoft.com)
2. Left menu → **Data catalog** → **Overview**
3. You should see **“18+ assets”** (or more after scans)



### Step 2 — Search the catalog (fastest)

1. Click the large **Search catalog** box
2. Type: `stsharedqgr7mj`
3. Press Enter
4. Open any result → tabs: **Overview**, **Lineage**, **Properties**, **Classifications**

Repeat with: `adf-shared-qgr7mj`, then `pl_gov`

### Step 3 — Browse by Azure hierarchy

1. Left menu → **Data catalog** → **Browse**
2. Or on Overview → **Explore your data** → **Microsoft Azure**
3. Drill down:
  ```text
   Subscriptions
     └── a64c0dd2-3a31-4604-bde3-3d40c7d5e8be
           └── Resource groups
                 └── rg-shared-class1
                       ├── stsharedqgr7mj (Storage)
                       ├── adf-shared-qgr7mj (Data Factory)
                       └── dbw-shared-qgr7mj (Databricks)
  ```



### Step 4 — View lineage (ADF medallion flow)

1. Search `adf-shared-qgr7mj` or a path like `loaded/run=session3-lab`
2. Open the asset → **Lineage** tab
3. You should see edges from ADF activities (Copy, Data Flow) if:
  - `pl_gov_06_master_medallion_governance` has **Succeeded** in ADF Monitor
  - You waited **5–30 minutes** after the run

**Expected medallion chain (logical):**

```text
bronze/incoming  →  bronze/loaded  →  silver/cleaned  →  silver/aggregates
     (pl_gov_01)      (Copy)           (pl_gov_02)         (pl_gov_03)
```



### Step 5 — Data Map (register ADLS if assets are missing)

If search returns little or nothing for storage paths:

1. Left menu → **Data map** (bottom of sidebar)
2. **Sources** → **Register**
3. Source type: **Azure Data Lake Storage Gen2**
4. Account: `stsharedqgr7mj`
5. **New scan** → scope: containers `bronze`, `silver`, `audit` → **Run**
6. Wait for scan complete → return to **Data catalog** → search again



### Step 6 — Glossary & classifications (teaching)

1. **Data catalog** → **Glossary** (or **Business concepts** in new UI)
2. Search terms the lab uses:
  - `Medallion.Bronze`
  - `Medallion.Silver`
  - `Medallion.Gold`
  - `FinLedger.Transaction`
3. **Classifications:** search `FinLedger.Internal`

*Note:* Glossary terms in our lab are defined in `audit/governance/purview_catalog_template.json` and the Databricks manifest — they appear in Purview only after manual creation or automated publish (not all are pre-seeded in your tenant).

---



## D. ADF + Databricks — where to see each flow



### ADF flow (Purview)


| Step | Where                                                                                                                                                                                                                    | What to verify                               |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------- |
| 1    | [ADF Studio](https://adf.azure.com/en/authoring?factory=%2Fsubscriptions%2Fa64c0dd2-3a31-4604-bde3-3d40c7d5e8be%2FresourceGroups%2Frg-shared-class1%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2Fadf-shared-qgr7mj) | Folder `13-governance-purview` → `pl_gov_06` |
| 2    | ADF **Monitor**                                                                                                                                                                                                          | Last run **Succeeded**                       |
| 3    | Purview **Search catalog**                                                                                                                                                                                               | `adf-shared-qgr7mj` or `pl_gov_06`           |
| 4    | Asset **Lineage** tab                                                                                                                                                                                                    | Copy + Data Flow edges                       |


**Re-run ADF master pipeline:**

```cmd
cd d:\azure\shared-adf-lab
.\orchestrate.cmd --run-only --run-pipeline pl_gov_06_master_medallion_governance
```



### Databricks flow (Purview + ADLS manifest)


| Step | Where       | What to verify                                                                                                                                            |
| ---- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | Databricks  | [medallion_governance_e2e](https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/medallion_governance_e2e)                           |
| 2    | ADLS        | `audit/governance/databricks_governance_manifest.json`                                                                                                    |
| 3    | Purview     | After **ADLS scan**, search `transactions/run=session3-lab` or `daily_channel_summary`                                                                    |
| 4    | 50 problems | [50_data_engineering_problems](https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/50_data_engineering_problems) → Part K capstone |


**Databricks lineage in Purview is limited** unless Unity Catalog is linked to Purview. For class demo, also show the **JSON manifest** on ADLS.

### SQL metadata model


| Step        | Where                                                                      |
| ----------- | -------------------------------------------------------------------------- |
| Azure SQL   | `sql-shared-qgr7mj.database.windows.net` / database `finledger`            |
| Tables      | `dbo.de_table_registry`, `dbo.de_pipeline_run_log`, `dbo.adf_job_metadata` |
| ADLS export | `audit/governance/de_table_registry.csv`                                   |


---



## E. Troubleshooting



### “AxiosError: Network Error” on old URL

- **Do not use** truncated URLs like `web.purview.azure.com/resource/subscriptions/?feature.tenant=...`
- Use [purview.microsoft.com](https://purview.microsoft.com) or Portal → **pviewrohan4hnv7s** → **Open Purview Studio**



### Purview not in `rg-shared-class1`

- Purview account is in `rg-rohan-class1` — search global top bar: `pviewrohan4hnv7s`



### Search returns no ADF lineage

1. Confirm ADF link: factory `adf-shared-qgr7mj` has Purview configuration pointing to `pviewrohan4hnv7s`
2. Re-run `pl_gov_06` and wait 30 minutes
3. Search `Copy` or filter by **Data Factory** asset type



### Search returns no storage paths

1. **Data map** → register `stsharedqgr7mj` → run scan
2. Re-search `bronze`, `silver`, `governance`



### Two SQL tiles in Azure Portal

- **One server** (`sql-shared-qgr7mj`) + **one lab database** (`finledger`) — normal Azure display, not two servers

---



## F. Trainer demo script (15 minutes)

1. **Purview Overview** → Search `stsharedqgr7mj` → show asset count
2. **Browse** → Microsoft Azure → `rg-shared-class1` → storage + ADF
3. Open asset → **Lineage** (ADF)
4. **ADF Studio** → `pl_gov_06` Monitor → show same pipeline names
5. **Storage** → `audit/governance/databricks_governance_manifest.json` → show 50-problem assets + medallion paths
6. **Databricks** → open medallion notebook → Run → refresh Purview after scan

---



## G. Quick links


| Tool                         | URL                                                                                                                                                                                                                             |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Purview (new)**            | [https://purview.microsoft.com/datacatalog/governance/main/catalog/home](https://purview.microsoft.com/datacatalog/governance/main/catalog/home)                                                                                |
| **Purview account (Portal)** | Search `pviewrohan4hnv7s` in Azure Portal                                                                                                                                                                                       |
| **ADF Studio**               | [adf-shared-qgr7mj](https://adf.azure.com/en/authoring?factory=%2Fsubscriptions%2Fa64c0dd2-3a31-4604-bde3-3d40c7d5e8be%2FresourceGroups%2Frg-shared-class1%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2Fadf-shared-qgr7mj) |
| **Databricks workspace**     | [dbw-shared-qgr7mj](https://adb-7405613791235979.19.azuredatabricks.net)                                                                                                                                                        |
| **Governance ADF guide**     | [adf_purview_medallion_governance_guide.md](./adf_purview_medallion_governance_guide.md)                                                                                                                                        |
| **Medallion CI/CD**          | [../../day9/docs/medallion_governance_cicd.md](../../day9/docs/medallion_governance_cicd.md)                                                                                                                                    |


---



## H. Re-run commands

```cmd
cd d:\azure\shared-adf-lab
.\orchestrate.cmd --run-only --run-pipeline pl_gov_06_master_medallion_governance

cd d:\azure
release-medallion-governance.cmd --run-only
```

---

*Last aligned with shared class estate:* `stsharedqgr7mj`*,* `adf-shared-qgr7mj`*,* `pviewrohan4hnv7s`*,* `finledger`*.*