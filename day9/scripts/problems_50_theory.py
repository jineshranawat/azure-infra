"""Trainer theory — file formats foundation + step-by-step explanation for all 50 problems."""

from pathlib import Path

_PRIMER_PATH = Path(__file__).resolve().parent.parent / "docs" / "connectivity_and_key_classes_primer.md"
CONNECTIVITY_PRIMER_MD = (
    _PRIMER_PATH.read_text(encoding="utf-8") if _PRIMER_PATH.is_file() else ""
)

# Inserted after notebook index, before bronze auth cell
FOUNDATIONS_MD = """# Part 0 — Foundations (read this first)

Before the 50 problems, understand **how data is stored** on the FinLedger lake. Every problem below assumes this stack.

## Timeline — when each format appears

| Era | Format | Where in FinLedger | Why it exists |
|-----|--------|-------------------|---------------|
| Always | **CSV** | Bronze landing (`bronze/loaded/`) | Systems export spreadsheets; human-readable; cheap to land |
| 2013+ | **Parquet** | Silver files (inside Delta too) | Columnar analytics — read only columns you need |
| 2019+ | **Delta Lake** | Silver + Gold tables | Parquet **plus** transaction log = ACID, time travel, MERGE |
| Optional | **JSON** | APIs, semi-structured feeds | Nested data; flexible schema |

## What is a CSV file? (Bronze)

- **What:** Plain text — one row per line, commas between columns.
- **Example:** `transaction_id,amount_gbp,channel` then `TXN-1,100.50,card`
- **When:** File lands from bank feed, ADF copy, or manual upload.
- **Problem:** No types (everything is text), no schema enforcement, slow for big analytics.

## What is Parquet? (Columnar storage)

- **What:** **Parquet** is a **binary columnar** file format. Data is stored **by column**, not by row.
- **Born:** Open-sourced ~**2013** (Twitter + Cloudera) for Hadoop / Spark ecosystems.
- **Analogy:** Instead of filing whole receipts in date order (row store), Parquet files all amounts in one drawer, all channels in another — Spark reads only the drawers it needs.
- **When it enters FinLedger:**
  1. Bronze may stay CSV (raw).
  2. When you **write silver** with `.format("parquet")` or `.format("delta")`, the **data files** under the folder are Parquet.
  3. **OPTIMIZE** (Problem 5) compacts many small Parquet files into fewer larger ones.
- **Benefits:** Compression (Snappy/GZIP), predicate pushdown, fast aggregations.
- **Limits:** No built-in UPDATE/DELETE across files; no transaction log — if write fails mid-way, you can get corrupt partial folders.

## What is Delta Lake? (Parquet + transaction log)

- **What:** **Delta** = **Parquet data files** + a **`_delta_log`** folder (JSON commit files listing which Parquet files belong to the table).
- **Born:** **Databricks, 2019** — “ACID on the data lake.”
- **When it enters FinLedger:**
  1. **Silver** cleansed tables (`silver/transactions`) — Delta recommended.
  2. **Gold** aggregates (`gold/daily_channel_summary`) — Delta for MERGE and reporting.
  3. Problems 3, 5, 6, 15, 36, 49 all use Delta features (CDC, OPTIMIZE, schema evolution, MERGE, SCD2, time travel).
- **What you see on disk:**
  ```
  prob05_optimized/
    _delta_log/          ← transaction log (who wrote what, when)
    part-00000-....parquet
    part-00001-....parquet
  ```
- **Benefits:** ACID writes, `MERGE`, `DESCRIBE HISTORY`, `RESTORE`, schema evolution, Change Data Feed.

## Medallion + formats (FinLedger)

```
BRONZE (CSV/raw)  →  SILVER (Delta/Parquet)  →  GOLD (Delta)
     land               cleanse + type            aggregate + serve
```

## How to use this notebook in class

1. Read **Part 0** (this section) — 15 minutes.
2. For each problem: read markdown (problem → solution → steps), then run code.
3. In Catalog, open `main.demo_problems` (or ADLS paths) to see outputs.

---
"""

# Step-by-step trainer theory per problem (replaces short theory in notebook markdown)
DETAILED_THEORY: dict[str, str] = {
    "01": """### Step-by-step: Deduplication

1. **Land data** in bronze (often CSV) — may contain duplicate `transaction_id` if ADF re-runs.
2. **Identify key** — business natural key (`transaction_id`), not row number.
3. **Count before** — `duped.count()` so learners see the problem.
4. **Deduplicate** — `dropDuplicates(["transaction_id"])` keeps first occurrence per key.
5. **Write silver** — save as **Delta** (Parquet files + log) so downstream is idempotent.
6. **Verify** — count after; duplicates removed.

**Formats:** Read CSV bronze → write **Delta** silver.""",

    "02": """### Step-by-step: Watermarking (late data)

1. **Event time** — use `value_date` as when payment happened (not when file arrived).
2. **Set watermark** — “accept events up to 48 hours late” (streaming: `.withWatermark()`).
3. **Split** — on-time vs too-late rows.
4. **Aggregate** only on-time in production; late goes to reconciliation.

**Formats:** Applies to streaming and batch; stored result in **Delta**.""",

    "03": """### Step-by-step: Change Data Feed (CDC)

1. **Write Delta** with `delta.enableChangeDataFeed=true` — still **Parquet** underneath.
2. **Change data** — `UPDATE` one row in Delta.
3. **Read CDF** — `readChangeFeed` shows insert/update/delete rows.
4. **Downstream** — gold consumes CDF instead of full table scan.

**When Delta vs Parquet alone:** Need CDC → must use **Delta**, not plain Parquet.""",

    "04": """### Step-by-step: Salting (skew)

1. **Detect skew** — one `channel` has most rows.
2. **Add salt** — random 0–4 so work splits across executors.
3. **Partial aggregate** per (channel, salt).
4. **Final aggregate** per channel.

**Formats:** In-memory transform; output to **Delta**.""",

    "05": """### Step-by-step: Small files → OPTIMIZE

1. **Problem** — many micro-batches create dozens of tiny **Parquet** files inside Delta.
2. **Why slow** — Spark opens each file separately; metadata overhead.
3. **Simulate** — append 5 small writes to `prob05_small_files/`.
4. **OPTIMIZE** — run `OPTIMIZE` on the Delta table path to merge Parquet files inside the table.
5. **Register** — `write_demo_table` saves optimized table for queries.

**Key teaching point:** Delta **is** Parquet files; OPTIMIZE compacts those Parquet files. This is when **Parquet file layout** directly affects performance.""",

    "06": """### Step-by-step: Schema evolution

1. **First batch** — write Delta with columns A, B, C.
2. **Second batch** — new column `source_system` arrives.
3. **mergeSchema=true** — append adds column; old rows get null for new col.
4. **No pipeline crash** — evolution is explicit and logged in `_delta_log`.

**CSV vs Delta:** CSV has no schema enforcement; **Delta** tracks schema in the log.""",

    "07": """### Step-by-step: Dead Letter Queue (DLQ)

1. **Parse/cast** — `amount_gbp` to double.
2. **Route valid** — rows that pass rules → silver **Delta**.
3. **Route invalid** — bad rows → `audit/quarantine` path (still Delta or CSV for ops).
4. **Pipeline succeeds** — DLQ holds bad rows for manual review.

**Formats:** Bronze CSV → silver Delta + audit folder.""",

    "08": """### Step-by-step: Data validation framework

1. **Define rules** — null IDs, negative amounts.
2. **Compute metrics** — one-row summary DataFrame.
3. **Filter passed** — only clean rows to silver.
4. **Store metrics** — Delta table for audit trail.

**Teaching:** Validation runs **before** gold; often on Delta silver.""",

    "09": """### Step-by-step: Imputation / defaults

1. **Find nulls** — `channel` or `amount_gbp` missing.
2. **coalesce / fillna** — `UNKNOWN`, `0.0`.
3. **Document** — defaults are business decisions, not silent fixes.
4. **Write cleansed** Delta silver.

**Formats:** Transform in Spark; persist **Delta**.""",

    "10": """### Step-by-step: Standardization

1. **trim + upper** on `channel`.
2. **to_date** on `value_date`.
3. **Compare** before/after in `.show()`.
4. **Silver** stores canonical forms for joins.

**Why:** CSV text varies; silver enforces types in **Delta schema**.""",

    "11": """### Step-by-step: Partitioning (large volume)

1. **Choose key** — `value_date` (common for payments).
2. **partitionBy** on write — folder per date: `txn_date=2026-06-01/`.
3. **Each partition** holds Parquet files (inside Delta if format=delta).
4. **Queries** filter one date → skip other folders.

**When Parquet appears:** Partition folders contain Parquet (or Delta-managed Parquet).""",

    "12": """### Step-by-step: Partition pruning

1. **Load** partitioned table from Problem 11.
2. **Filter** `WHERE txn_date = one day`.
3. **explain()** — look for `PartitionFilters` in plan.
4. **Teach:** pruning avoids reading all Parquet files.

**Link:** Parquet + partitioning = fast scans.""",

    "13": """### Step-by-step: Caching

1. **Expensive transform** — `groupBy` once.
2. **cache()** — store in executor memory after first action.
3. **Reuse** — second action reads cache, not re-shuffle from **Delta/Parquet**.
4. **unpersist()** — free memory when done.

**Formats:** Cache is in Spark memory; source is still Delta on disk.""",

    "14": """### Step-by-step: Incremental processing

1. **Watermark date** — last successful run (control table).
2. **Filter bronze** — only `value_date >= watermark`.
3. **Process subset** — not full history.
4. **Update watermark** after success.

**Formats:** Read bronze CSV or Delta; write **Delta** silver incrementally.""",

    "15": """### Step-by-step: Incremental MERGE (not full load)

1. **Target** — Delta table with 50 rows.
2. **Updates** — 5 changed amounts from new batch.
3. **MERGE** — match on `transaction_id`; update matched, insert new.
4. **Avoid** — dropping and rewriting entire table (full load).

**Delta feature:** `MERGE` requires **Delta**, not plain Parquet.""",

    "16": """### Step-by-step: Automation (widgets / jobs)

1. **Widgets** — `run_id`, `env` as job parameters.
2. **ADF / Job** passes same values every night.
3. **No manual** path edits in notebook.
4. **Log** run to Delta control table.

**Ops:** Notebook → Databricks Job → ADF trigger.""",

    "17": """### Step-by-step: Logging & alerting

1. **Structured log** — JSON with row counts, status.
2. **Append** to `pipeline_metrics` Delta table.
3. **Morning script** — read metrics; alert if threshold breached.
4. **Link Problem 50** — end-to-end visibility.

**Formats:** Metrics as small **Delta** tables.""",

    "18": """### Step-by-step: Batch backfill

1. **List dates** to reprocess after bug fix.
2. **Loop** each date — filter bronze, write silver partition.
3. **Idempotent** per day — safe to re-run one date.
4. **Avoid** one giant job for all history.

**Formats:** Read bronze; write **Delta** per day partition.""",

    "19": """### Step-by-step: Checkpointing

1. **Streaming** — checkpoint path on ADLS records offsets.
2. **Restart** — resume from checkpoint, no duplicate read.
3. **Batch analogy** — MERGE on keys for idempotency.
4. **mkdirs** checkpoint folder in lab.

**Formats:** Checkpoint is JSON metadata; data still **Delta**.""",

    "20": """### Step-by-step: Event-time processing

1. **Use** `value_date` as event time, not `current_timestamp()`.
2. **Window** — `groupBy(window(event_ts, "7 days"))`.
3. **Correct totals** when events arrive out of order.
4. **Write** windowed aggregates to Delta gold.

**Streaming:** `withWatermark` + event time windows.""",

    "21": """### Step-by-step: Trigger optimization (streaming backlog)

1. **Backlog** — micro-batches fall behind.
2. **Tune trigger** — `processingTime` vs `availableNow`.
3. **Catch-up** — drain backlog in one shot when needed.
4. **Document** trade-offs in Delta control table.

**Lab:** Concept table in **Delta**.""",

    "22": """### Step-by-step: OOM / right-sizing

1. **Never collect()** millions of rows on driver.
2. **aggregate** then small `.show()`.
3. **Right-size** cluster memory in Compute UI.
4. **spill** — if shuffle too large, add partitions or salt.

**Formats:** Read **Delta/Parquet** in executors, not driver.""",

    "23": """### Step-by-step: Shuffle + AQE

1. **Enable AQE** — `spark.sql.adaptive.enabled=true`.
2. **Wide transform** — repartition + groupBy.
3. **AQE coalesces** partitions after shuffle statistics.
4. **Write** result to Delta.

**Link:** Shuffle happens in memory; output Parquet via Delta.""",

    "24": """### Step-by-step: Broadcast join

1. **Large fact** — bronze transactions.
2. **Small dim** — distinct channels (few rows).
3. **broadcast(dim)** — copy small table to all executors.
4. **Avoid shuffle** on join.

**Formats:** Join in Spark; save joined **Delta** table.""",

    "25": """### Step-by-step: Large joins (bucketing / co-partition)

1. **Both sides large** — broadcast not possible.
2. **partitionBy(channel)** — co-locate same channel in same folder.
3. **Classic:** `bucketBy` on Hive tables.
4. **Join** on partition key reduces shuffle.

**Parquet:** Co-partitioned folders hold Parquet files.""",

    "26": """### Step-by-step: Cost control (cluster)

1. **Job clusters** — terminate after job.
2. **Right SKU** — smallest node that doesn't OOM.
3. **Photon** — if licensed, for SQL/ETL speed.
4. **Document** tips in Delta table.

**Not a file format** — infrastructure cost.""",

    "27": """### Step-by-step: Autoscale + auto-terminate

1. **min_workers=1, max_workers=4** — scale with load.
2. **auto-terminate 30 min** — interactive clusters don't run overnight.
3. **JSON config** stored for learners to copy to Compute UI.

**Ops concept** — complements Delta storage cost (Problem 46).""",

    "28": """### Step-by-step: Instance pools (warm VMs)

1. **Cold start** — new cluster waits for VMs.
2. **Pool** — idle VMs ready.
3. **Trade-off** — faster start vs idle VM cost.
4. **Attach** pool in cluster policy.

**When:** SLA-critical jobs only.""",

    "29": """### Step-by-step: Unity Catalog RBAC

1. **Catalog** — `main` or `finledger`.
2. **Schema** — `demo_problems`, `bronze`, `silver`, `gold`.
3. **GRANT** USE SCHEMA, SELECT on tables.
4. **Separate** who sees bronze PII vs gold aggregates.

**UC** governs **Delta** tables in the lake.""",

    "30": """### Step-by-step: Fine-grained access

1. **Masked gold** — aggregates only, no `transaction_id`.
2. **Views** — `CREATE VIEW` with column subset.
3. **GRANT** on view to analysts.
4. **Write** masked table to Delta.

**Governance** on top of **Delta** gold.""",

    "31": """### Step-by-step: Secret scopes

1. **Never** paste storage keys in notebook.
2. **Scope `finledger`** — `storage-account`, `storage-key`.
3. **Setup** — `session-3\\orchestrate.cmd --setup-secrets` on Windows.
4. **Read** — `dbutils.secrets.get()` in cell 0.

**Enables** secure read of bronze **CSV** from ADLS.""",

    "32": """### Step-by-step: Data lineage (explain clearly in class)

**What is lineage?** A map of **data flow**: which tables/files feed which tables, notebooks, and dashboards.

1. **Upstream** — `prob01_deduped` (silver deduped transactions from Problem 01).
2. **Transform** — take 10 rows and write a **downstream** table `prob32_lineage_demo`.
3. **Register** — Unity Catalog records that `prob32` was created from `prob01` (when UC writes work).
4. **View in UI** — Catalog Explorer → select `prob32_lineage_demo` → **Lineage** tab → see arrow from upstream.
5. **Why it matters** — Compliance asks: "If we change `transaction_id` in silver, which gold reports break?" Lineage answers that without tribal knowledge.
6. **Purview** — enterprise tool that scans ADLS + Databricks and shows lineage across ADF, Power BI, etc. (Session 24+).

**Formats:** Lineage tracks **Delta** table relationships (Parquet files underneath). CSV bronze is usually tracked at the **path** level in Purview.""",

    "33": """### Step-by-step: Documentation (UC comments)

1. **COMMENT ON TABLE** — business description.
2. **COMMENT ON COLUMN** — what `transaction_id` means.
3. **Visible** in Catalog UI and `DESCRIBE EXTENDED`.
4. **Skip** if no UC write permission (ADLS fallback mode).

**Metadata** on **Delta** tables.""",

    "34": """### Step-by-step: Metric standardization

1. **One canonical name** — `metric_amount_gbp`.
2. **Gold layer** enforces naming dictionary.
3. **Avoid** `amount` vs `amt` vs `gbp` across teams.
4. **Write** canonical **Delta** gold.

**Semantic layer** on Parquet/Delta files.""",

    "35": """### Step-by-step: Data drift detection

1. **Profile** each run — row count, avg amount, nulls.
2. **Append** to `drift_profile` Delta table.
3. **Compare** week-over-week.
4. **Alert** if avg shifts > 20%.

**Quality** metadata in **Delta**.""",

    "36": """### Step-by-step: SCD Type 2

1. **Dimension changes** — channel renamed or reclassified.
2. **Columns** — `effective_from`, `effective_to`, `is_current`.
3. **MERGE** — close old row, insert new version.
4. **Point-in-time** reports use date filter.

**Classic warehouse pattern on Delta.**""",

    "37": """### Step-by-step: Reference data refresh

1. **Small lookup** — channel reference table.
2. **Full overwrite** daily if < 100k rows.
3. **MERGE** if large slowly-changing ref.
4. **Join** facts to latest ref in silver.

**Ref dim as small **Delta** table.**""",

    "38": """### Step-by-step: Retry with backoff

1. **API call fails** — transient network error.
2. **Retry** 3 times with exponential sleep.
3. **Fail** to DLQ after max attempts.
4. **Log** success to Delta.

**Driver-side** pattern before landing CSV bronze.""",

    "39": """### Step-by-step: Rate limiting / throttle

1. **Partner API** — max 100 req/min.
2. **Batch + sleep** between batches.
3. **Respect** `Retry-After` header in production.
4. **Log** throttle stats to Delta.

**Ingest** pattern before bronze file exists.""",

    "40": """### Step-by-step: File integrity (checksum)

1. **Landing file** — partner sends SHA256 in manifest.
2. **Compute hash** after upload (lab: row-count hash proxy).
3. **Reject** ingest if mismatch — corrupt **CSV**.
4. **Log** integrity row to Delta.

**Before** parsing CSV to bronze.""",

    "41": """### Step-by-step: UTC timestamps

1. **Store** all times in UTC in silver.
2. **to_utc_timestamp** from `Europe/London` local.
3. **Consistent** global reporting.
4. **Write** UTC column to **Delta**.

**Type enforcement** in Delta schema.""",

    "42": """### Step-by-step: Time zone normalization

1. **Read** UTC from Problem 41 table.
2. **from_utc_timestamp** for London display.
3. **Storage UTC, display local** — best practice.
4. **Write** local display column to Delta.

**Presentation** layer on **Delta** gold.""",

    "43": """### Step-by-step: Orphan records (referential integrity)

1. **Facts** — bronze transactions.
2. **Dims** — valid channels.
3. **left_anti join** — facts with no matching dim = orphans.
4. **Quarantine** orphans; write valid to silver **Delta**.

**Data quality** before gold.""",

    "44": """### Step-by-step: Idempotent processing

1. **pipeline_run_id** column on every batch.
2. **MERGE or overwrite** same run — ADF retry safe.
3. **No duplicate** gold rows on retry.
4. **Link** Problem 15 MERGE.

**Reliability** on **Delta** writes.""",

    "45": """### Step-by-step: Optimize ETL code

1. **Select early** — only needed columns.
2. **Filter early** — reduce shuffle size.
3. **Native Spark** — avoid Python UDFs.
4. **One write** at end to **Delta**.

**Performance** reading **Parquet** via Delta.""",

    "46": """### Step-by-step: Compression / I/O cost

1. **Delta** uses **Parquet** with Snappy by default.
2. **Smaller files** — less ADLS scan cost.
3. **compression** option on write (lab demo).
4. **Compare** to raw CSV size on bronze.

**When Parquet enters:** Every Delta write creates compressed Parquet files — this problem makes cost explicit.""",

    "47": """### Step-by-step: Lifecycle / retention

1. **Bronze CSV** — move to cool tier after 90 days (ADLS lifecycle).
2. **Audit** — delete after 365 days per policy.
3. **Delta VACUUM** — remove old files no longer referenced by log.
4. **Legal hold** check before VACUUM.

**Storage policy** on CSV + **Delta** folders.""",

    "48": """### Step-by-step: Governance tags

1. **UC tags** — `domain=payments`, `classification=internal`.
2. **Purview scan** — classify columns.
3. **SHOW TBLPROPERTIES** — verify tags.
4. **Compliance** reporting from **Delta** metadata.

**Governance** on catalog-registered Delta tables.""",

    "49": """### Step-by-step: Disaster recovery (time travel)

1. **DESCRIBE HISTORY** — list versions of Delta table.
2. **Bad MERGE** — note version before mistake.
3. **RESTORE** or `versionAsOf` read.
4. **DR** — geo-redundant ADLS + Delta history.

**Delta-only:** time travel needs `_delta_log`, not plain Parquet.""",

    "50": """### Step-by-step: End-to-end monitoring

1. **Stages** — bronze ingest, tables created, notebook complete.
2. **Metrics row** per stage in `prob50_e2e_monitoring`.
3. **List** all tables/paths written this run.
4. **Morning dashboard** — one query on Delta control tables.

**Ties all 50 problems** — formats, Delta, UC, ops.""",
}

# Concepts that tie multiple problems to file formats (for cross-reference in class)
FORMAT_CONCEPT_MAP = """## Which problems teach which format?

| Concept | Problems | Remember |
|---------|----------|----------|
| **CSV bronze** | 1, 7, 10, 14, 40 | Raw land; text; no ACID |
| **Parquet files** | 5, 11, 12, 25, 45, 46 | Columnar; inside Delta folders |
| **Delta Lake** | 3, 5, 6, 15, 19, 36, 44, 49 | Parquet + `_delta_log`; MERGE, OPTIMIZE, CDF |
| **Unity Catalog** | 29, 30, 31, 32, 33, 48 | Governance over Delta tables |
| **Streaming** | 2, 19, 20, 21 | Watermark, checkpoint, triggers |
| **Ops / cost** | 16, 17, 26, 27, 28, 50 | Jobs, metrics, clusters |
"""
