#!/usr/bin/env python3
"""Generate Day 9+ notebooks — one file per session (9–15), small cells + diagrams.

Run: python day9/scripts/build_all_notebooks.py
Deploy: deploy-shared-lab.cmd
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent.parent
sys.path.insert(0, str(ROOT))

from bronze_auth_cell import BRONZE_AUTH_CELL
from commented_snippets import (
    BRIDGE_PRINT,
    CAST_PREVIEW_CODE,
    TYPED_SPLIT_CODE,
    VERIFY_SILVER_CODE,
    WRITE_QUARANTINE_CODE,
    WRITE_SILVER_CODE,
)
from session_diagrams import (
    ADLS_CONTAINERS,
    ASCII_DELTA_LOG,
    ASCII_MEDALLION,
    ASCII_QUARANTINE,
    CAST_DETAIL,
    CAST_FLOW,
    DEAD_LETTER,
    DELTA_LOG,
    DELTA_VS_PARQUET,
    GOLD_ROLLUP,
    INCREMENTAL_BATCH,
    MEDALLION,
    MERGE_FLOW,
    ORCHESTRATE_STACK,
    PIPELINE_E2E,
    QUARANTINE_FLOW,
    RUN_MEDALLION,
    SESSION09_OVERVIEW,
    SILVER_WRITE_SEQUENCE,
    SNAPSHOT_SAFETY,
    SQL_VS_PYSPARK,
    TIME_TRAVEL,
    UNITY_CATALOG,
    WRITE_ACTION,
    WRITE_MODES,
)
from notebook_helpers import ascii_flow, box_diagram, code, concept, finish, lesson, md
from paths_cell import IMPORTS_CELL, PATHS_CELL

OUT_DIR = ROOT.parent / "notebooks"

NOTEBOOKS: list[tuple[str, str, str]] = [
    ("nb_session09_silver_quarantine.py", "session09_silver_quarantine", "Session 9"),
    ("nb_session10_delta_basics.py", "session10_delta_basics", "Session 10"),
    ("nb_session11_time_travel.py", "session11_time_travel", "Session 11"),
    ("nb_session12_merge_upsert.py", "session12_merge_upsert", "Session 12"),
    ("nb_session13_medallion_e2e.py", "session13_medallion_e2e", "Session 13"),
    ("nb_session14_gold_reporting.py", "session14_gold_reporting", "Session 14"),
    ("nb_session15_sql_jobs_adf.py", "session15_sql_jobs_adf", "Session 15"),
]


def _setup(p: list[str]) -> None:
    code(p, BRONZE_AUTH_CELL)
    code(p, PATHS_CELL)
    code(p, IMPORTS_CELL)


def _typed_split_code() -> str:
    return TYPED_SPLIT_CODE


def build_session09() -> list[str]:
    p: list[str] = ["# Databricks notebook source", ""]
    md(
        p,
        """# Session 9 — Clean, validate & quarantine (FinLedger silver rules)

**After Day 8:** you can *transform* in memory. **Today:** split good vs bad rows and **write** them to the lake.

| Container | Path purpose |
|-----------|----------------|
| `bronze` | Raw CSV as landed |
| `silver` | Cleansed, typed transactions (Delta) |
| `audit` | Quarantined bad rows — pipeline keeps running |

**Cell guide:** markdown = theory + diagrams · Python = commented steps you run on cluster.""",
    )
    box_diagram(p, SESSION09_OVERVIEW, caption="Session 9 in the FinLedger story")
    box_diagram(p, ADLS_CONTAINERS, caption="ADLS containers on shared storage")

    _setup(p)

    concept(
        p,
        "9.1 — Bridge from Day 8",
        "Day 8 ended with `silver_good` and `silver_bad` **in memory**. Session 9 **persists** them to ADLS so other teams can query them tomorrow.",
        "Banks must not lose bad rows (audit) and must not block the pipeline on one messy file.",
        "Cast → filter → **write** is an **action**. Two writes: silver + quarantine.",
        "**Airport security:** good bags fly; suspicious bags go to a side room for inspection — the flight still departs.",
        BRIDGE_PRINT,
        mermaid_diagram=MEDALLION,
        ascii_diagram=ASCII_MEDALLION,
    )

    concept(
        p,
        "9.2 — Cast before quality rules",
        "Bronze stores `amount_gbp` as **text**. Cast to `double` so comparisons like `> 0` work.",
        "Invalid text (`INVALID`) becomes **null** after cast — those rows need quarantine.",
        "`withColumn` + `cast` is lazy until an action (`count`, `write`).",
        "**Weighing luggage:** text label 'heavy' means nothing until you put it on the scale (cast to number).",
        CAST_PREVIEW_CODE,
        mermaid_diagram=CAST_FLOW,
    )
    box_diagram(p, CAST_DETAIL, caption="Cast outcomes — valid number vs null")

    concept(
        p,
        "9.3 — Quarantine pattern (dead-letter for data)",
        "**Quarantine** = isolate bad rows in `audit` container. The pipeline **succeeds**; ops fixes source data later.",
        "Failing the whole job on one bad CSV costs money and blocks downstream gold reports.",
        "Split with two `filter` branches: `silver_good` and `silver_bad`. Never `collect()` bad rows to driver only — write them.",
        "**Hospital triage:** stable patients to ward; critical cases to ICU — both are recorded, neither is thrown away.",
        _typed_split_code(),
        mermaid_diagram=QUARANTINE_FLOW,
        ascii_diagram=ASCII_QUARANTINE,
        extra_code=["silver_bad.show(truncate=False)", "silver_good.show(truncate=False)"],
    )

    concept(
        p,
        "9.4 — Write is an ACTION",
        "`.write.format(...).save(path)` executes the full lazy chain and creates folders + files on ADLS.",
        "Until you write, cleansed data exists only in the cluster memory for this session.",
        "Spark writes parquet files + `_delta_log`. `mode('overwrite')` replaces this run's snapshot.",
        "**Saving a Word doc:** Ctrl+S actually hits disk — transforms alone are just typing.",
        'print("Next: write silver_good to", SILVER_PATH)',
        mermaid_diagram=WRITE_ACTION,
        ascii_diagram=None,
    )
    box_diagram(p, SILVER_WRITE_SEQUENCE, caption="Write + verify sequence")

    lesson(
        p,
        "9.5 — Write silver (good rows)",
        "First production write: cleansed rows land under `abfss://silver/.../transactions/run=<id>/`.",
        WRITE_SILVER_CODE,
        extra_code=[VERIFY_SILVER_CODE],
    )

    lesson(
        p,
        "9.6 — Write quarantine (bad rows)",
        "Bad rows go to `abfss://audit/.../quarantine/run=<id>/` — separate container for governance.",
        WRITE_QUARANTINE_CODE,
        mermaid_diagram=DEAD_LETTER,
    )

    md(p, "### 9.7 — Trainer: verify in Azure Portal")
    md(
        p,
        """Open **Storage account** → **Containers**:
1. `silver` → `transactions/run=session3-lab/` → `_delta_log/` + `part-*.parquet`
2. `audit` → `quarantine/run=session3-lab/` → contains TXN-BAD row""",
    )
    ascii_flow(
        p,
        """Portal check
─────────────
Storage → stsharedqgr7mj → silver → transactions → run=session3-lab
Storage → stsharedqgr7mj → audit  → quarantine → run=session3-lab""",
        caption="Where to click in Azure Portal",
    )

    md(p, "## Session 9 checklist")
    md(
        p,
        """- [ ] Cast `amount_gbp` → `amount`
- [ ] Split good vs quarantine
- [ ] Wrote silver to `abfss://silver/...`
- [ ] Wrote quarantine to `abfss://audit/...`
- [ ] Verified in Portal or `read.format('delta')`
- [ ] **Next:** Session 10 — why Delta beats raw Parquet""",
    )
    return finish(p)


def build_session10() -> list[str]:
    p: list[str] = ["# Databricks notebook source", ""]
    md(
        p,
        """# Session 10 — Delta Lake basics (ACID, schema, read back)

**Goal:** Understand **why** FinLedger stores silver/gold as **Delta**, not plain CSV or Parquet.""",
    )
    _setup(p)
    code(p, _typed_split_code())

    concept(
        p,
        "10.1 — Why Delta Lake?",
        "**Delta Lake** = Parquet files + a **transaction log** (`_delta_log`). Gives ACID, time travel, schema enforcement.",
        "Plain Parquet folders have no single source of truth when two jobs write at once. Banks need **atomic** commits.",
        "Every write adds a JSON commit entry. Readers always see a consistent **version**.",
        "**Bank ledger book:** Parquet is the pages; Delta is the numbered ledger entries that say which pages are official.",
        "print('Delta = Parquet + _delta_log')",
        mermaid_diagram=DELTA_VS_PARQUET,
        ascii_diagram=ASCII_DELTA_LOG,
    )

    concept(
        p,
        "10.2 — Write silver as Delta",
        "Use `.format('delta')` on write and read. Same path as Session 9 — now we understand the log underneath.",
        "Delta is the default lakehouse format on Databricks — one API for batch and (later) streaming.",
        "`save(path)` creates `part-*.parquet` plus `_delta_log/`.",
        "**Git commits:** each write is a commit; the log is the history.",
        """(silver_good.dropDuplicates(["transaction_id"]).write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH))""",
        mermaid_diagram=DELTA_LOG,
    )

    lesson(
        p,
        "10.3 — Read Delta back",
        "Prove round-trip: `spark.read.format('delta').load(path)`.",
        'back = spark.read.format("delta").load(SILVER_PATH)\nback.printSchema()\nback.show()',
        extra_code=['print("Row count:", back.count())'],
    )

    concept(
        p,
        "10.4 — Write modes",
        "| mode | Meaning |\n|------|--------|\n| `overwrite` | Replace this table snapshot (our batch default) |\n| `append` | Add new files (use when keeping history) |\n| `error` | Fail if path exists |\n| `ignore` | Do nothing if exists |",
        "Wrong mode = duplicate data or accidental wipe. Batch snapshots usually use **overwrite** per partition folder.",
        "Mode is decided at **write** time — part of the action.",
        "**Notebook save:** 'Save' replaces vs 'Save As new file' — same idea.",
        'print("FinLedger batch uses overwrite per run= folder")',
        mermaid_diagram=WRITE_MODES,
    )

    lesson(
        p,
        "10.5 — Schema enforcement preview",
        "Delta can **reject** writes that don't match the table schema (production safety).",
        'print("If you add a column with mergeSchema=True you can evolve — Session 11+")',
    )

    md(p, "## Session 10 checklist")
    md(p, "- [ ] Wrote and read Delta silver\n- [ ] Can explain `_delta_log`\n- [ ] **Next:** Session 11 — time travel")
    return finish(p)


def build_session11() -> list[str]:
    p: list[str] = ["# Databricks notebook source", ""]
    md(
        p,
        """# Session 11 — Snapshots & time travel (safe re-runs)

**Goal:** Every Delta write is a **version**. You can read yesterday's data or **RESTORE** after a mistake.""",
    )
    _setup(p)

    lesson(
        p,
        "11.1 — Ensure silver Delta exists",
        "Run after Session 9/10 or create silver now.",
        _typed_split_code(),
        extra_code=[
            """(silver_good.dropDuplicates(["transaction_id"]).write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH))""",
        ],
    )

    concept(
        p,
        "11.2 — Delta versions",
        "Each write increments **version** 0, 1, 2… Stored in `_delta_log`.",
        "Batch jobs re-run. Without versions you cannot prove what changed or roll back.",
        "`DESCRIBE HISTORY` lists operations, timestamps, row counts.",
        "**Google Docs version history:** scroll back to any edit.",
        f'spark.sql(f"DESCRIBE HISTORY delta.`{{SILVER_PATH}}`").show(truncate=False)',
        mermaid_diagram=TIME_TRAVEL,
    )

    lesson(
        p,
        "11.3 — Read an older version",
        "Use `option('versionAsOf', n)` to read a snapshot without changing the table.",
        """v0 = (
    spark.read.format("delta")
    .option("versionAsOf", 0)
    .load(SILVER_PATH)
)
print("Version 0 rows:", v0.count())""",
        mermaid_diagram=SNAPSHOT_SAFETY,
    )

    lesson(
        p,
        "11.4 — Second write (creates version 1)",
        "Re-run overwrite to simulate tomorrow's batch.",
        """(silver_good.write.format("delta").mode("overwrite").save(SILVER_PATH))
spark.sql(f"DESCRIBE HISTORY delta.`{SILVER_PATH}`").select("version", "operation", "operationMetrics").show()""",
    )

    concept(
        p,
        "11.5 — RESTORE (roll back)",
        "`RESTORE TABLE ... TO VERSION AS OF n` makes the latest version match an older snapshot.",
        "When a bad deploy lands bad data, restore beats manual file copy.",
        "RESTORE is itself a new version in history — audit trail stays intact.",
        "**Undo in Excel:** revert to backup; Delta does it in one SQL command.",
        'spark.sql(f"RESTORE TABLE delta.`{SILVER_PATH}` TO VERSION AS OF 0")\nprint("Restored to version 0")',
    )

    md(p, "## Session 11 checklist")
    md(p, "- [ ] Ran DESCRIBE HISTORY\n- [ ] Read versionAsOf\n- [ ] Understand RESTORE\n- [ ] **Next:** Session 12 — MERGE")
    return finish(p)


def build_session12() -> list[str]:
    p: list[str] = ["# Databricks notebook source", ""]
    md(
        p,
        """# Session 12 — Batch incremental: MERGE upsert

**Goal:** Load today's file and **merge** into silver — update existing keys, insert new ones. **Run twice = same row count** (idempotent).""",
    )
    _setup(p)
    code(p, _typed_split_code())

    concept(
        p,
        "12.1 — Why MERGE?",
        "Full **overwrite** replaces the whole table. **MERGE** upserts by business key (`transaction_id`) — true incremental batch.",
        "Daily files overlap yesterday's keys (amendments). Overwrite duplicates or loses history.",
        "Delta `merge` matches `ON` key → `whenMatchedUpdate`, `whenNotMatchedInsert`.",
        "**Contact list sync:** update existing people, add new numbers — don't delete the whole list.",
        "print('MERGE = match on txn_id, upsert rows')",
        mermaid_diagram=MERGE_FLOW,
        ascii_diagram=None,
    )

    lesson(
        p,
        "12.2 — Create target silver table",
        "First load seeds the target (overwrite once).",
        """(silver_good.dropDuplicates(["transaction_id"]).write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH))""",
    )

    lesson(
        p,
        "12.3 — MERGE upsert",
        "Import `DeltaTable`, alias target `t` and source `s`, merge on `transaction_id`.",
        """from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, SILVER_PATH)
updates = silver_good.dropDuplicates(["transaction_id"])

(target.alias("t")
    .merge(updates.alias("s"), "t.transaction_id = s.transaction_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
print("MERGE complete — rows:", spark.read.format("delta").load(SILVER_PATH).count())""",
        mermaid_diagram=INCREMENTAL_BATCH,
    )

    lesson(
        p,
        "12.4 — Idempotency demo",
        "Run the same MERGE again — row count must stay the same.",
        """before = spark.read.format("delta").load(SILVER_PATH).count()
(target.alias("t")
    .merge(updates.alias("s"), "t.transaction_id = s.transaction_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
after = spark.read.format("delta").load(SILVER_PATH).count()
print("before:", before, "after:", after, "| idempotent:", before == after)""",
    )

    md(p, "## Session 12 checklist")
    md(p, "- [ ] MERGE on transaction_id\n- [ ] Re-run did not duplicate rows\n- [ ] **Next:** Session 13 — full pipeline function")
    return finish(p)


def build_session13() -> list[str]:
    p: list[str] = ["# Databricks notebook source", ""]
    md(
        p,
        """# Session 13 — Medallion end-to-end in one batch run

**Goal:** One function `run_medallion(run_date)` chains bronze → silver → gold — what ADF/Jobs will call later.""",
    )
    _setup(p)

    concept(
        p,
        "13.1 — Pipeline as a function",
        "Wrap steps in `def run_medallion(run_date):` so Jobs/ADF pass **parameters** instead of copy-paste.",
        "Production pipelines are scheduled, monitored, retried — they need one entry point.",
        "Read bronze → cast → quarantine split → write silver → aggregate → write gold → return metrics.",
        "**One recipe card:** head chef follows the same steps every night; only the delivery date changes.",
        "print('We will define run_medallion(run_date) below')",
        mermaid_diagram=RUN_MEDALLION,
        ascii_diagram=ASCII_MEDALLION,
    )

    code(
        p,
        '''def run_medallion(run_date: str) -> dict:
    """Bronze → silver (good) → gold channel totals. Returns row counts."""
    path = (
        f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
        f"/loaded/run={run_date}/sample_transactions.csv"
    )
    raw = spark.read.option("header", True).option("inferSchema", True).csv(path)
    typed = raw.withColumn("amount", F.col("amount_gbp").cast("double"))
    good = typed.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
    bad = typed.filter(F.col("amount").isNull() | (F.col("amount") <= 0))
    good = good.dropDuplicates(["transaction_id"])

    silver_run = (
        f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
        f"/transactions/run={run_date}"
    )
    quarantine_run = (
        f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net"
        f"/quarantine/run={run_date}"
    )
    good.write.format("delta").mode("overwrite").save(silver_run)
    if bad.count() > 0:
        bad.write.format("delta").mode("overwrite").save(quarantine_run)

    gold = good.groupBy("channel").agg(
        F.count("*").alias("txns"),
        F.sum("amount").alias("total_gbp"),
    )
    gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
    return {
        "bronze": raw.count(),
        "silver": good.count(),
        "quarantine": bad.count(),
        "gold_channels": gold.count(),
    }''',
    )

    lesson(
        p,
        "13.2 — Run the pipeline",
        "One action call — trainer demos full medallion.",
        'metrics = run_medallion(RUN_ID)\nprint(metrics)',
        mermaid_diagram=PIPELINE_E2E,
        extra_code=[
            'spark.read.format("delta").load(GOLD_PATH).orderBy(F.desc("total_gbp")).show()',
        ],
    )

    md(p, "## Session 13 checklist")
    md(p, "- [ ] `run_medallion` returns metrics\n- [ ] Silver + gold paths written\n- [ ] **Next:** Session 14 — gold reporting shapes")
    return finish(p)


def build_session14() -> list[str]:
    p: list[str] = ["# Databricks notebook source", ""]
    md(
        p,
        """# Session 14 — Gold: aggregations & reporting tables

**Goal:** Build **analyst-ready** tables — daily rollups, pivot, partitioned gold writes.""",
    )
    _setup(p)
    lesson(
        p,
        "14.0 — Load silver (from Session 13 or build now)",
        "Gold builds on cleansed silver — read Delta if Session 13 already ran.",
        _typed_split_code(),
        extra_code=[
            """(silver_good.dropDuplicates(["transaction_id"]).write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH))""",
            'silver = spark.read.format("delta").load(SILVER_PATH)\nprint("Silver rows:", silver.count())',
        ],
    )

    concept(
        p,
        "14.1 — Gold vs silver",
        "**Silver** = row-level cleansed facts. **Gold** = business aggregates analysts query (sums, counts, KPIs).",
        "Analysts should not scan billions of silver rows for a dashboard — pre-aggregate in gold.",
        "`groupBy` + `agg` then `write` partitioned Delta to `abfss://gold/...`.",
        "**Supermarket:** silver = every receipt line; gold = daily sales by aisle.",
        "print('Gold = fewer rows, higher business value')",
        mermaid_diagram=GOLD_ROLLUP,
    )

    lesson(
        p,
        "14.2 — Channel rollup (already familiar)",
        "Same pattern as Day 8 Part 7 — now persisted.",
        """by_channel = silver.groupBy("channel").agg(
    F.count("*").alias("txns"),
    F.sum("amount").alias("total_gbp"),
)
by_channel.orderBy(F.desc("total_gbp")).show()""",
    )

    lesson(
        p,
        "14.3 — Daily rollup with date",
        "Add `value_date` as `day` for time-series reports.",
        """daily = (
    silver.withColumn("day", F.col("value_date"))
    .groupBy("channel", "day")
    .agg(F.sum("amount").alias("total_gbp"))
)
daily.show()""",
    )

    lesson(
        p,
        "14.4 — Pivot (wide report)",
        "One row per day, columns per channel — easy for Excel/BI.",
        """pivoted = daily.groupBy("day").pivot("channel").sum("total_gbp")
pivoted.show()""",
    )

    lesson(
        p,
        "14.5 — Partitioned gold write",
        "`partitionBy('day')` — one folder per day under gold (faster filters).",
        """(daily.write.format("delta")
    .mode("overwrite")
    .partitionBy("day")
    .save(GOLD_DAILY_PATH))
print("Partitioned gold written:", GOLD_DAILY_PATH)""",
    )

    md(p, "## Session 14 checklist")
    md(p, "- [ ] Channel + daily gold\n- [ ] Pivot demo\n- [ ] Partitioned write\n- [ ] **Next:** Session 15 — SQL, catalog, Jobs, ADF")
    return finish(p)


def build_session15() -> list[str]:
    p: list[str] = ["# Databricks notebook source", ""]
    md(
        p,
        """# Session 15 — SQL, Unity Catalog, Jobs & ADF (watch / demo)

**Goal:** See how the **same lake** is queried with SQL, registered in **Unity Catalog**, scheduled as a **Job**, and orchestrated by **ADF**.

*Concepts + short demo cells — trainer shows UI where noted.*""",
    )
    _setup(p)

    concept(
        p,
        "15.1 — PySpark vs SQL (same engine)",
        "DataFrame API and `%sql` compile to the **same Catalyst plan** — use what your team prefers.",
        "SQL analysts should not re-learn PySpark for dashboards.",
        "Register Delta path as a table, then `SELECT ... GROUP BY`.",
        "**Two keyboards, same computer:** QWERTY vs Dvorak — same CPU.",
        "print('Catalyst optimizes both APIs')",
        mermaid_diagram=SQL_VS_PYSPARK,
    )

    lesson(
        p,
        "15.2 — Register Delta table (SQL)",
        "External table points at our gold path.",
        '''spark.sql(f"""
CREATE TABLE IF NOT EXISTS finledger_gold_channel
USING delta
LOCATION '{GOLD_PATH}'
""")''',
        extra_code=[
            "spark.sql('SELECT * FROM finledger_gold_channel ORDER BY total_gbp DESC').show()",
        ],
    )

    concept(
        p,
        "15.3 — Unity Catalog",
        "**Catalog.schema.table** naming; permissions (`GRANT SELECT`); lineage in UI.",
        "Banks need one place to audit who can read PII and where tables came from.",
        "Create catalog → schema → table or external location.",
        "**Library card catalog:** books (tables) organised by section (schema), borrow rules (grants).",
        "print('UC: finledger.gold.channel_daily')",
        mermaid_diagram=UNITY_CATALOG,
    )

    lesson(
        p,
        "15.4 — UC SQL (demo)",
        "If Unity Catalog enabled on workspace — trainer runs in Catalog UI.",
        '''-- %sql
-- CREATE CATALOG IF NOT EXISTS finledger;
-- CREATE SCHEMA IF NOT EXISTS finledger.gold;
-- GRANT SELECT ON TABLE finledger.gold.channel_daily TO `analysts`;
print("Trainer: show Catalog Explorer + lineage tab")''',
    )

    concept(
        p,
        "15.5 — Databricks Jobs",
        "A **Job** runs a notebook on a schedule with retries, alerts, and parameters (`run_date`).",
        "Interactive 'Run all' does not scale — production needs scheduling.",
        "Job → cluster → notebook → `dbutils.widgets.get('run_date')` → `run_medallion()`.",
        "**Alarm clock for the recipe:** same `run_medallion`, runs at 6am daily.",
        'dbutils.widgets.text("run_date", RUN_ID)\nrun_date = dbutils.widgets.get("run_date")\nprint("Job would call run_medallion(", run_date, ")")',
    )

    concept(
        p,
        "15.6 — Azure Data Factory",
        "**ADF** orchestrates: copy activities, dependencies, triggers. **Databricks notebook activity** runs our pipeline.",
        "Enterprise batch often chains: land file → run Databricks → send email on failure.",
        "ADF passes `run_date` into notebook base parameters.",
        "**Conductor + orchestra:** ADF waves the baton; Databricks plays the music.",
        "print('ADF pipeline: Trigger → DatabricksNotebook(run_medallion)')",
        mermaid_diagram=ORCHESTRATE_STACK,
    )

    lesson(
        p,
        "15.7 — Widgets + exit value",
        "How Jobs/ADF pass parameters and read success/failure.",
        """dbutils.widgets.text("run_date", RUN_ID)
run_date = dbutils.widgets.get("run_date")
print("Job calls: run_medallion(", run_date, ")")
dbutils.notebook.exit("ok")""",
    )

    md(p, "## Session 15 checklist")
    md(
        p,
        """- [ ] SQL query on gold Delta
- [ ] Saw Unity Catalog concept
- [ ] Understand Job vs interactive notebook
- [ ] Understand ADF → Databricks activity
- [ ] **Capstone preview:** new CSV → bronze → `run_medallion` → ADF trigger""",
    )
    return finish(p)


BUILDERS = {
    "nb_session09_silver_quarantine.py": build_session09,
    "nb_session10_delta_basics.py": build_session10,
    "nb_session11_time_travel.py": build_session11,
    "nb_session12_merge_upsert.py": build_session12,
    "nb_session13_medallion_e2e.py": build_session13,
    "nb_session14_gold_reporting.py": build_session14,
    "nb_session15_sql_jobs_adf.py": build_session15,
}


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for filename, workspace_name, label in NOTEBOOKS:
        builder = BUILDERS[filename]
        lines = builder()
        out = OUT_DIR / filename
        out.write_text("\n".join(lines) + "\n", encoding="utf-8")
        cells = sum(1 for line in lines if line == "# COMMAND ----------") + 1
        print(f"Wrote {out.name} ({cells} cells) -> /Shared/day9/{workspace_name}")
    formats_script = ROOT / "build_file_formats_notebook.py"
    if formats_script.is_file():
        subprocess.run([sys.executable, str(formats_script)], cwd=REPO_ROOT, check=True)
    print(f"\nDone — {len(NOTEBOOKS)} session notebooks + file_formats in {OUT_DIR}")


if __name__ == "__main__":
    main()
