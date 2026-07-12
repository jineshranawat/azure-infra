# ADF Folders, Data Flows & Advanced Concepts — Trainer Catalog

**Purpose:** Cover every important Azure Data Factory concept for data engineering that goes beyond the core `pl_01`–`pl_12` pipelines — including **where to create folders in ADF Studio**, **mapping data flows**, and **12 example scenarios per concept** for classroom discussion.

**Code:** `scripts/adf_advanced.py` (data flows + `pl_13`–`pl_26`)  
**Deploy:** `shared-adf-lab\orchestrate.cmd`

---

## Part A — Where is “Create folder”? (ADF Studio UI)

Your screenshot shows the **`+` (plus) menu** on the **Pipelines** header. That menu creates **Pipeline**, **Dataset**, **Data flow**, etc. — **not folders**.

### Create a pipeline folder

| Step | Action |
|------|--------|
| 1 | **Author** tab → left pane **Factory resources** |
| 2 | Hover the **Pipelines** section header (where `pl_01` … `pl_11` are listed) |
| 3 | Click the **`⋯` (ellipsis)** on the **Pipelines** row — **not** the `+` button |
| 4 | Choose **New folder** |
| 5 | Name it (e.g. `06-dataflows`) → **Create** |

### Create a subfolder

| Step | Action |
|------|--------|
| 1 | Right-click an existing folder **or** click its `⋯` menu |
| 2 | **New subfolder** |

### Move pipelines into folders

| Method | Steps |
|--------|-------|
| Drag and drop | Drag `pl_07_databricks_notebook` onto folder `04-integration-databricks` |
| Move item | Right-click pipeline → **Move item** → pick destination folder |

### Create data flow in a folder

| Step | Action |
|------|--------|
| 1 | Click `+` next to **Pipelines** **or** expand **Data flows** section |
| 2 | Under **Data flow** → **Mapping data flow** (or **Wrangling data flow** for Power Query) |
| 3 | If you created folder `06-dataflows` first, select it before saving — or move the data flow after creation |

### Same rules for Datasets

- **Datasets** header also has `⋯` → **New folder** (e.g. `bronze`, `silver`, `audit`).

> **This lab deploys folders automatically** via `PIPELINE_FOLDERS` in `adf_advanced.py`. After `orchestrate.cmd`, refresh ADF Studio — pipelines appear grouped under `01-core-copy` … `09-power-query`.

---

## Part B — Folder layout in this lab (deployed by code)

| ADF folder | Pipelines / data flows | Teaches |
|------------|------------------------|---------|
| `01-core-copy` | `pl_01`, `pl_06` | Copy, Wait |
| `02-discovery-control` | `pl_02`, `pl_03`, `pl_05`, `pl_11` | Metadata, If, Lookup, Filter |
| `03-iteration-orchestration` | `pl_04`, `pl_10` | ForEach, Execute Pipeline |
| `04-integration-databricks` | `pl_07` | Notebook activity |
| `05-integration-sql` | `pl_08`, `pl_09`, `pl_12` | SQL source/sink, metadata orchestration |
| `06-dataflows` | `df_01`, `df_02`, `df_04`, `pl_13`, `pl_14`, `pl_26` | Mapping data flow, join, Execute Data Flow |
| `07-advanced-activities` | `pl_15`–`pl_22` | Delete, Web, Switch, Until, variables, validation, fail, script |
| `08-incremental-cdc` | `pl_23`, `pl_24` | Incremental copy (watermark), SQL change tracking (CDC) |
| `09-power-query` | `pq_01`, `df_03`, `pl_25` | Power Query wrangling (Studio) + automated PQ-parity mapping flow |

---

## Part C — 12 ADF concept areas × 12 classroom examples

Each row is a **real-world scenario** you can map to ADF. **Bold** = implemented in this lab.

### 1. Factory folders & naming conventions

| # | Example scenario |
|---|------------------|
| 1 | **`01-core-copy`** — all bronze copy pipelines |
| 2 | **`05-integration-sql`** — cross-region SQL pipelines |
| 3 | **`06-dataflows`** — visual ETL separate from orchestration |
| 4 | Prefix `pl_` for pipelines, `df_` for data flows, `ds_` for datasets |
| 5 | Suffix with purpose: `_bronze_copy`, `_metadata_orchestrate` |
| 6 | Subfolder per medallion layer: `bronze/`, `silver/`, `gold/` under Datasets |
| 7 | Environment suffix: `_dev`, `_uat`, `_prd` (not used in shared class) |
| 8 | Domain prefix: `finledger_`, `payments_` for multi-domain factories |
| 9 | Move deprecated pipelines to `99-archive/` folder (inactive, not deleted) |
| 10 | Match folder name to team ownership: `team-a-ingest/` |
| 11 | Keep **linked services** flat in Manage (no folders) — ADF limitation |
| 12 | Document folder map in README (this file + `adf_advanced.py`) |

### 2. Mapping data flows (visual ETL)

| # | Example scenario |
|---|------------------|
| 1 | **`df_01_bronze_cleanse`** — derive `ingestion_ts`, select columns → silver |
| 2 | **`df_02_channel_aggregate`** — `groupBy(channel)`, `sum(amount_gbp)` |
| 3 | Cast `amount_gbp` from string to decimal with data type cast transform |
| 4 | Filter `status = 'posted'` before silver load |
| 5 | Join transactions to `dim_channel` reference table |
| 6 | Union card + wire feeds from two source folders |
| 7 | Pivot channel totals by month |
| 8 | Surrogate key generation with `keyGenerate()` |
| 9 | Mask `account_id` for dev environment sink |
| 10 | Conditional split: large amounts → `audit` path, rest → `silver` |
| 11 | Window: running total per account ordered by `value_date` |
| 12 | Parse JSON nested column from semi-structured landing file |

**Cost note:** Mapping data flows bill for **vCore-hours** (minimum 8 cores). Run `pl_13`/`pl_14` once in class, not in a loop.

### 3. Execute Data Flow activity

| # | Example scenario |
|---|------------------|
| 1 | **`pl_13_df_bronze_cleanse`** — pipeline triggers `df_01` |
| 2 | **`pl_14_df_channel_aggregate`** — pipeline triggers `df_02` |
| 3 | Parameterize cluster: General vs Memory Optimized |
| 4 | Set trace level `Fine` only when debugging |
| 5 | Chain: Copy raw → Execute DF cleanse → Copy to SQL |
| 6 | Parent pipeline passes `run_date` parameter into data flow |
| 7 | Run data flow on schedule trigger nightly |
| 8 | Execute DF after Validation confirms source file landed |
| 9 | Execute DF inside ForEach per country partition folder |
| 10 | Log row counts via data flow `aggregate` → audit table |
| 11 | Re-run Execute DF idempotently with `truncate: true` sink |
| 12 | Fail pipeline if data flow sinks zero rows (post-check Lookup) |

### 4. Wrangling data flows (Power Query)

| # | Example scenario |
|---|------------------|
| 1 | Business analyst cleans CSV via **Power Query** UI — no code |
| 2 | Promote headers + change types on Excel export |
| 3 | Remove duplicate rows before lake load |
| 4 | Split column `full_name` into `first_name`, `last_name` |
| 5 | Merge lookup from SharePoint list |
| 6 | Replace nulls in `channel` with `UNKNOWN` |
| 7 | Unpivot wide monthly columns to tall format |
| 8 | Data quality rules: filter invalid IBAN patterns |
| 9 | Combine 3 sheets from same workbook |
| 10 | Publish wrangling DF to pipeline for scheduled refresh |
| 11 | Export Power Query M script for code review |
| 12 | Compare wrangling DF vs mapping DF for same source (when to use which) |

*Not deployed in code — create in Studio: `+` → **Wrangling data flow**.*

### 5. Delete, move & archive files

| # | Example scenario |
|---|------------------|
| 1 | **`pl_15_delete_rejected`** — delete decoys under `incoming/.../rejected/` |
| 2 | Archive processed files to `archive/run=yyyy-MM-dd/` |
| 3 | Delete empty staging folders after successful load |
| 4 | Remove GDPR right-to-erasure files by prefix |
| 5 | Purge temp Parquet after merge to Delta |
| 6 | Delete source only after Validation + Copy both succeed |
| 7 | Move (copy + delete) from `landing/` to `bronze/` |
| 8 | Delete virus-scan quarantine files older than 7 days |
| 9 | Clean up failed run artifacts in `errors/` folder |
| 10 | Delete duplicate uploads keeping latest timestamp |
| 11 | Log every delete path to SQL audit table |
| 12 | Use lifecycle management policy on storage + ADF delete for hot paths |

### 6. Web activity & REST integration

| # | Example scenario |
|---|------------------|
| 1 | **`pl_16_web_ping`** — GET `https://jsonplaceholder.typicode.com/todos/1` (public REST demo API) |
| 2 | POST row count to internal metrics API |
| 3 | Call Databricks Jobs API 2.1 `/run-now` |
| 4 | Refresh Power BI dataset after load |
| 5 | Send Slack/Teams webhook on failure |
| 6 | OAuth token fetch before subsequent Web call |
| 7 | GET watermarks from external metadata service |
| 8 | Trigger Logic App HTTP endpoint |
| 9 | Poll REST job status until `completed` |
| 10 | Register dataset in Purview via REST |
| 11 | Call Azure Function HTTP trigger for lightweight transform |
| 12 | Paginated GET — append pages in Until loop |

### 7. Switch & dynamic routing

| # | Example scenario |
|---|------------------|
| 1 | **`pl_17_switch_channel`** — route by `wire` / `card` / default |
| 2 | Switch on `file_type` parameter: `csv`, `parquet`, `json` |
| 3 | Route per `source_system` code from SQL metadata |
| 4 | Country switch: UK → `uksouth` copy path, US → `eastus` |
| 5 | Switch on pipeline trigger name (scheduled vs tumbling window) |
| 6 | Priority queue: `P1` fast path, else standard path |
| 7 | Switch after Lookup on `environment` column |
| 8 | Holiday calendar branch — skip markets closed |
| 9 | Schema version `v1` vs `v2` transform pipelines |
| 10 | Error code switch for different notification templates |
| 11 | Combine Switch + Execute Pipeline per branch |
| 12 | Default branch logs “unhandled” to quarantine |

### 8. Until loops & polling

| # | Example scenario |
|---|------------------|
| 1 | **`pl_18_until_file_ready`** — poll folder until `childItems` > 0 |
| 2 | Wait until upstream SFTP drop completes |
| 3 | Poll SQL `control_table.status` until `READY` |
| 4 | Retry Web GET until HTTP 200 |
| 5 | Wait for Databricks run to finish (with timeout) |
| 6 | Until file size stabilizes (two metadata checks equal) |
| 7 | Poll Event Grid–delayed blob appearance |
| 8 | Wait for partition folder `dt=2026-07-09/` to exist |
| 9 | Until variable `retry_count` reaches max then Fail |
| 10 | Combine Until + Wait for backoff pattern |
| 11 | Break early when `force_skip=true` parameter set |
| 12 | Document max timeout to avoid runaway IR cost |

### 9. Variables — Set & Append

| # | Example scenario |
|---|------------------|
| 1 | **`pl_05`** — Set Variable from Lookup watermark |
| 2 | **`pl_19_append_audit_log`** — build array of step names |
| 3 | Accumulate failed file names across ForEach |
| 4 | Store `pipeline().RunId` for audit correlation |
| 5 | Append each SQL batch number to log array |
| 6 | Set `processed_count` from activity output |
| 7 | Build dynamic SQL `IN (...)` list via Append |
| 8 | Track branch taken in Switch for metrics |
| 9 | Reset counters at pipeline start (default values) |
| 10 | Pass variable into Web activity body JSON |
| 11 | Append Variable in ForEach — **must** use sequential ForEach |
| 12 | Output final array to pipeline return / logging sink |

### 10. Validation & dependencies

| # | Example scenario |
|---|------------------|
| 1 | **`pl_20_validate_incoming`** — Validation wraps Get Metadata |
| 2 | Wait until daily file exists before 06:00 SLA |
| 3 | Validate upstream pipeline run succeeded (depends on) |
| 4 | Ensure minimum 1000 rows in staging |
| 5 | Validate schema columns match expected list |
| 6 | Check no zero-byte files in folder |
| 7 | Validate partition date = `@utcnow('yyyy-MM-dd')` |
| 8 | Cross-pipeline dependency via Execute Pipeline `waitOnCompletion` |
| 9 | Validation sleep 60s between checks for slow landings |
| 10 | Timeout → alert + skip downstream |
| 11 | Validate SQL linked service before bulk insert |
| 12 | Combine Validation + If Condition on output |

### 11. Fail activity & error handling

| # | Example scenario |
|---|------------------|
| 1 | **`pl_21_fail_on_bad_param`** — `force_fail=true` demo path |
| 2 | Fail with `errorCode` for ops triage |
| 3 | Fail when Lookup returns zero rows |
| 4 | Fail if amount_gbp > threshold (fraud hold) |
| 5 | Fail fast on missing required parameter |
| 6 | Fail after Web activity returns 4xx |
| 7 | Use Fail in false branch instead of silent skip |
| 8 | Parent monitors child Failed → notification pipeline |
| 9 | Fail message includes `@pipeline().RunId` |
| 10 | Inactive activities with `onInactiveMarkAs: Succeeded` for feature flags |
| 11 | Retry policy on Copy; Fail only after retries exhausted |
| 12 | Document difference: **Failed** (activity) vs **Fail** (explicit) |

### 12. SQL Script & stored procedures

| # | Example scenario |
|---|------------------|
| 1 | **`pl_12`** — `usp_adf_mark_job_status` stored proc |
| 2 | **`pl_22_sql_script_count`** — Script activity `SELECT COUNT(*)` |
| 3 | Pre-copy script: `TRUNCATE stg_*` before load |
| 4 | Post-load: `MERGE` staging into dimension |
| 5 | Script activity runs `UPDATE` watermark table |
| 6 | Stored proc with TVP bulk parameter |
| 7 | `sp_executesql` dynamic SQL from pipeline parameter |
| 8 | Script returns result set → Set Variable |
| 9 | Transaction wrap in stored proc for atomic promote |
| 10 | Script checks `sys.tables` before first deploy |
| 11 | Cross-region SQL westus ← eastus ADF (this lab) |
| 12 | Use Script vs Stored Proc: ad-hoc SQL vs governed API surface |

---

## Part D — Quick run commands (advanced pipelines)

```cmd
cd shared-adf-lab
.\orchestrate.cmd --run-pipeline pl_13_df_bronze_cleanse
.\orchestrate.cmd --run-pipeline pl_16_web_ping
.\orchestrate.cmd --run-pipeline pl_17_switch_channel
.\orchestrate.cmd --run-pipeline pl_21_fail_on_bad_param
```

| Pipeline | Trigger params | Expected outcome |
|----------|----------------|------------------|
| `pl_13_df_bronze_cleanse` | (none) | `silver/cleaned/.../cleaned_transactions.csv` |
| `pl_14_df_channel_aggregate` | (none) | `silver/aggregates/.../channel_totals.csv` |
| `pl_15_delete_rejected` | (none) | Removes `incoming/.../rejected/*` |
| `pl_16_web_ping` | (none) | HTTP 200 from jsonplaceholder REST API |
| `pl_17_switch_channel` | `channel`: `wire` / `card` / other | Sets `route_message` variable |
| `pl_18_until_file_ready` | `incoming_folder` | Exits when folder has files |
| `pl_19_append_audit_log` | (none) | `audit_steps` = 2 items |
| `pl_20_validate_incoming` | `incoming_folder` | Succeeds when files present |
| `pl_21_fail_on_bad_param` | `force_fail`: `false` (default) | Succeeds; set `true` to demo Fail |
| `pl_22_sql_script_count` | (none) | Returns metadata row count |
| `pl_23_incremental_watermark_copy` | `incoming_folder`, `loaded_folder` | Copies CSV only if blob modified after `audit/cdc_watermark.json` → `last_modified_after` |
| `pl_24_sql_change_tracking_copy` | (none) | Enables SQL change tracking, simulates row change, exports `CHANGETABLE` deltas to `audit/sql_cdc_export/` |
| `pl_25_power_query_returns` | (none) | Runs `df_03_returns_by_channel` (PQ group-by parity) → `silver/returns/summary/` |
| `pl_26_join_returns_txn` | (none) | Inner join returns ↔ transactions → `silver/returns/joined/` |

---

## Part G — Incremental CDC, Power Query & remaining ADF concepts

### 13. Incremental load / watermark (`pl_23`)

| # | Example scenario |
|---|------------------|
| 1 | **`pl_23`** — Lookup `cdc_watermark.json` → Copy with `modifiedDatetimeStart` |
| 2 | Watermark stores `last_successful_watermark` date for row-level filters in data flows |
| 3 | Re-upload bronze file each lab run so modified timestamp advances |
| 4 | Audit table records last pipeline run id |
| 5 | High-water mark column (`value_date`) in SQL source query |
| 6 | Merge upsert into silver Delta using watermark |
| 7 | Compare Get Metadata `lastModified` vs watermark before copy |
| 8 | Fail pipeline if watermark file missing (Validation activity) |
| 9 | Parameterize watermark path per environment |
| 10 | Store watermark in Azure SQL instead of JSON file |
| 11 | Incremental folder copy (`incoming/dt=2026-06-01/`) with ForEach |
| 12 | Native ADF **Change Data Capture** resource (Studio **+** → CDC) for managed CDC — see Part H |

### 14. SQL change tracking / CDC (`pl_24`)

| # | Example scenario |
|---|------------------|
| 1 | **`pl_24`** — `ALTER DATABASE … CHANGE_TRACKING` + `CHANGETABLE` copy |
| 2 | Export insert/update/delete ops (`SYS_CHANGE_OPERATION`) to lake |
| 3 | Watermark `last_sync_version` in `cdc_watermark.json` |
| 4 | Simulate source mutation (`fps` channel) each run |
| 5 | SQL Server CDC (on-prem) via self-hosted IR |
| 6 | Debezium → Event Hub → ADF copy |
| 7 | Merge CDC files into slowly changing dimension |
| 8 | Retention window vs `CHANGE_RETENTION` days |
| 9 | Full load once, then CDC incremental forever |
| 10 | Compare change tracking vs temporal tables |
| 11 | ADF Copy `SqlServerSource` with `changeTracking` property |
| 12 | Purview lineage tags on CDC landing path |

### 15. Power Query / wrangling (`pl_25`, `pq_01`)

| # | Example scenario |
|---|------------------|
| 1 | **`pl_25`** — automated lab runs mapping flow `df_03` (same outcome as PQ group-by) |
| 2 | **`pq_01_returns_wrangle`** — Wrangling data flow shell; complete mashup in Studio |
| 3 | Upload `bronze/incoming/returns/returns_raw.csv` (6 rows) |
| 4 | Replace values on `reason` column (title case) in PQ editor |
| 5 | Group by `channel`, sum `refund_gbp` |
| 6 | Sink to `silver/returns/summary/` — single partition for one CSV |
| 7 | Swap `pl_25` activity to **Execute Wrangling Data Flow** after Studio save |
| 8 | Analyst-owned transforms vs engineer mapping flows |
| 9 | PQ mashup translated to Spark at runtime |
| 10 | Combine PQ prep + mapping flow downstream |
| 11 | Power Query Online mashup editor (code-free) |
| 12 | Fallback: mapping data flow when PQ unavailable in region |

**Studio steps for `pq_01`:** Author → **Power Query** → open `pq_01_returns_wrangle` → **Get data** → `ds_returns_raw` → transform → **Save** → run **Execute Wrangling Data Flow** on `pq_01`.

### 16. Join transform (`pl_26`, `df_04`)

| # | Example scenario |
|---|------------------|
| 1 | **`pl_26`** — inner join returns to `sample_transactions.csv` on `transaction_id` |
| 2 | Left outer join to keep unmatched returns |
| 3 | Join to `dim_channel` reference for channel name |
| 4 | Multi-stream join (3+ sources) |
| 5 | Broadcast join hint for small dimension |
| 6 | Join + aggregate in one data flow |
| 7 | Exists anti-join for orphan detection |
| 8 | Cross join for calendar spine (careful with row explosion) |
| 9 | Join in Copy activity (limited) vs data flow |
| 10 | Stream split after join to multiple sinks |
| 11 | Join on composite key (`account_id`, `value_date`) |
| 12 | Surrogate key generation post-join |

### Part H — ADF concepts documented but not automated in this lab

| Concept | Why / where to learn |
|---------|----------------------|
| **Native Change Data Capture** (ADF resource) | Studio **+** → **Change Data Capture** — preview; pairs source SQL with lake sink |
| **Tumbling / schedule triggers** | Author → Triggers — time windows for `pl_04` batch dates |
| **Event / storage triggers** | Blob created → start `pl_01` |
| **Azure Function activity** | Serverless HTTP transform between copy steps |
| **Custom activity** | HDInsight / batch exe on Azure IR |
| **Self-hosted IR** | On-prem SQL Server without public endpoint |
| **Flowlets** | Reusable data-flow subgraphs |
| **Global parameters** | Factory-wide `environment` switch |
| **Git publish branch** | `adf_publish` vs collaboration `main` |
| **Managed private endpoints** | Lock down SQL/storage from ADF managed VNet |
| **Data flow debug cluster** | Interactive debug in Studio (cost) |
| **CI/CD ARM export** | Publish from Git → deploy to prod factory |

---

## Part E — Manual Studio: build `df_01_bronze_cleanse` from scratch

1. **Author** → `06-dataflows` folder → `+` → **Data flow** → **Mapping data flow** → name `df_01_bronze_cleanse`
2. **Source settings** → dataset `ds_df_bronze_source` (bronze CSV)
3. **+** transform → **Derived column** → `ingestion_ts` = `currentTimestamp()`
4. **+** → **Select** → keep `transaction_id`, `account_id`, `amount_gbp`, `channel`, `status`, `ingestion_ts`
5. **Sink** → dataset `ds_silver_cleaned_csv` → **Truncate table** = true
6. **Validate** → **Debug** (uses debug cluster — cost warning)
7. New pipeline `pl_13_df_bronze_cleanse` → drag **Execute Data Flow** → pick `df_01_bronze_cleanse`
8. **Compute** → General, 8 vCores → **Trigger now**

---

## Part F — Concept coverage map (full lab)

| Concept | Pipeline / data flow |
|---------|----------------------|
| Copy | `pl_01`, `pl_06`, `pl_23`, `pl_24` |
| Get Metadata | `pl_02`, `pl_11`, `pl_18`, `pl_20` |
| If Condition | `pl_03`, `pl_11`, `pl_12`, `pl_21` |
| ForEach | `pl_04` |
| Lookup | `pl_05`, `pl_12`, `pl_23`, `pl_24` |
| Set Variable | `pl_05`, `pl_11`, `pl_17`, `pl_18`, `pl_21` |
| Append Variable | `pl_19` |
| Wait | `pl_06` |
| Filter | `pl_11` |
| Execute Pipeline | `pl_10` |
| Databricks Notebook | `pl_07`, `pl_10`, `pl_11`, `pl_12` |
| SQL source/sink | `pl_08`, `pl_09` |
| Stored Procedure | `pl_12` |
| Script (SQL) | `pl_22`, `pl_24` |
| Incremental copy / watermark | `pl_23` |
| SQL change tracking (CDC) | `pl_24` |
| Power Query (wrangling) | `pq_01`, `pl_25`, `df_03` |
| Join (data flow) | `df_04`, `pl_26` |
| Mapping Data Flow | `df_01`–`df_04` |
| Execute Data Flow | `pl_13`, `pl_14`, `pl_25`, `pl_26` |
| Delete | `pl_15` |
| Web | `pl_16` |
| Switch | `pl_17` |
| Until | `pl_18` |
| Validation | `pl_20` |
| Fail | `pl_21` |
| **Folders** | All — via `PIPELINE_FOLDERS` |

*Last aligned with code: `adf_advanced.py` — 14 advanced pipelines (`pl_13`–`pl_26`), 4 mapping data flows, 1 wrangling shell, 9 ADF folders.*
