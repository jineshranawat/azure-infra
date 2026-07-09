#!/usr/bin/env python3
"""Generate nb_50_data_engineering_problems.py — all 50 problems with UC tables.

Run:  python day9/scripts/build_50_problems_notebook.py
Deploy: deploy-shared-lab.cmd
Run all 50 on cluster: python scripts/run_50_problems_notebook.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent.parent
sys.path.insert(0, str(ROOT))

from bronze_auth_cell import BRONZE_AUTH_CELL
from demo_table_helpers import DEMO_TABLE_HELPERS
from notebook_helpers import code, finish, md
from problems_50_data import ALL_PROBLEMS_50
from problems_50_theory import (
    CONNECTIVITY_PRIMER_MD,
    DETAILED_THEORY,
    FORMAT_CONCEPT_MAP,
    FOUNDATIONS_MD,
)
from problems_50_use_cases import PROBLEM_GROUPS, USE_CASES

OUT = ROOT.parent / "notebooks" / "nb_50_data_engineering_problems.py"

# Apply BEFORE any regex — exact multiline replacements
_MULTILINE_FIXES: list[tuple[str, str]] = [
    (
        'spark.read.format("delta").load(small_path).write.format("delta").mode("overwrite").saveAsTable(\n'
        '    f"{UC_TABLE}.prob05_optimized"\n)',
        'write_demo_table(spark.read.format("delta").load(small_path), "prob05_optimized")',
    ),
    (
        'spark.read.format("delta").load(part_path).write.format("delta").mode("overwrite").saveAsTable(\n'
        '    f"{UC_TABLE}.prob11_partitioned"\n)',
        'write_demo_table(spark.read.format("delta").load(part_path), "prob11_partitioned")',
    ),
    (
        'spark.read.format("delta").load(comp_path).write.format("delta").mode("overwrite").saveAsTable(\n'
        '    f"{UC_TABLE}.prob46_compressed"\n)',
        'write_demo_table(spark.read.format("delta").load(comp_path), "prob46_compressed")',
    ),
    (
        'spark.createDataFrame(cost_tips, ["tip", "detail"]).write.format("delta").mode("overwrite").saveAsTable(\n'
        '    f"{UC_TABLE}.prob26_cost_tips"\n)',
        'write_demo_table(spark.createDataFrame(cost_tips, ["tip", "detail"]), "prob26_cost_tips")',
    ),
    (
        'spark.createDataFrame([(json.dumps(cluster_policy),)], ["cluster_config_json"]).write.format(\n'
        '    "delta"\n'
        ').mode("overwrite").saveAsTable(f"{UC_TABLE}.prob27_autoscale_config")',
        'write_demo_table(spark.createDataFrame([(json.dumps(cluster_policy),)], ["cluster_config_json"]), "prob27_autoscale_config")',
    ),
    (
        'spark.createDataFrame([("instance_pool", "reduces cold start", RUN_ID)], ["component", "benefit", "run_id"]).write.format(\n'
        '    "delta"\n'
        ').mode("overwrite").saveAsTable(f"{UC_TABLE}.prob28_pool_guide")',
        'write_demo_table(spark.createDataFrame([("instance_pool", "reduces cold start", RUN_ID)], ["component", "benefit", "run_id"]), "prob28_pool_guide")',
    ),
    (
        'spark.createDataFrame([(result, "ok")], ["rows", "status"]).write.format("delta").mode("overwrite").saveAsTable(\n'
        '    f"{UC_TABLE}.prob38_retry_result"\n)',
        'write_demo_table(spark.createDataFrame([(result, "ok")], ["rows", "status"]), "prob38_retry_result")',
    ),
    (
        'spark.createDataFrame([(total, BATCH_SIZE, batches)], ["total_rows", "batch_size", "batch_count"]).write.format(\n'
        '    "delta"\n'
        ').mode("overwrite").saveAsTable(f"{UC_TABLE}.prob39_throttle_log")',
        'write_demo_table(spark.createDataFrame([(total, BATCH_SIZE, batches)], ["total_rows", "batch_size", "batch_count"]), "prob39_throttle_log")',
    ),
    (
        'spark.createDataFrame([(BRONZE_PATH, row_count, checksum)], ["path", "rows", "checksum"]).write.format(\n'
        '    "delta"\n'
        ').mode("overwrite").saveAsTable(f"{UC_TABLE}.prob40_file_integrity")',
        'write_demo_table(spark.createDataFrame([(BRONZE_PATH, row_count, checksum)], ["path", "rows", "checksum"]), "prob40_file_integrity")',
    ),
    (
        'spark.createDataFrame([\n'
        '    ("bronze", 90, "cool"),\n'
        '    ("audit", 365, "delete"),\n'
        '], ["layer", "days", "action"]).write.format("delta").mode("overwrite").saveAsTable(\n'
        '    f"{UC_TABLE}.prob47_retention_policy"\n)',
        'write_demo_table(spark.createDataFrame([("bronze", 90, "cool"), ("audit", 365, "delete")], ["layer", "days", "action"]), "prob47_retention_policy")',
    ),
    (
        'spark.createDataFrame([("finledger", acct, key_len)], ["scope", "storage_account", "key_chars"]).write.format(\n'
        '    "delta"\n'
        ').mode("overwrite").saveAsTable(f"{UC_TABLE}.prob31_secrets_audit")',
        'write_demo_table(spark.createDataFrame([("finledger", acct, key_len)], ["scope", "storage_account", "key_chars"]), "prob31_secrets_audit")',
    ),
    (
        'bronze.select("transaction_id").dropDuplicates().write.format("delta").mode("overwrite").saveAsTable(\n'
        '    f"{UC_TABLE}.prob19_idempotent_keys"\n)',
        'write_demo_table(bronze.select("transaction_id").dropDuplicates(), "prob19_idempotent_keys")',
    ),
]


def patch_problem_code(raw: str) -> str:
    """Replace saveAsTable / spark.table with permission-safe helpers."""
    code = raw

    # 1) Exact multiline replacements FIRST (before regex can corrupt chained .load().write)
    for old, new in _MULTILINE_FIXES:
        code = code.replace(old, new)

    # 2) spark.read.load(...).write...saveAsTable (single line)
    code = re.sub(
        r'spark\.read\.format\("delta"\)\.load\(([^)]+)\)\.write\.format\("delta"\)\.mode\("overwrite"\)'
        r'\.saveAsTable\(\s*f"\{UC_TABLE\}\.([^"]+)"\s*\)',
        r'write_demo_table(spark.read.format("delta").load(\1), "\2")',
        code,
    )

    # 3) spark.createDataFrame(...).write...saveAsTable (single line, simple args)
    code = re.sub(
        r"(spark\.createDataFrame\([^)]+\))\.write\.format\(\"delta\"\)\.mode\(\"(overwrite|append)\"\)"
        r'\.saveAsTable\(\s*f"\{UC_TABLE\}\.([^"]+)"\s*\)',
        r'write_demo_table(\1, "\3", mode="\2")',
        code,
    )

    # 4) Simple DataFrame variable .write...saveAsTable (word chars only — NOT ) or .)
    code = re.sub(
        r"^(\s*)([a-zA-Z_][a-zA-Z0-9_]*)\.write\.format\(\"delta\"\)\.mode\(\"(overwrite|append)\"\)"
        r'\.saveAsTable\(\s*f"\{UC_TABLE\}\.([^"]+)"\s*\)',
        r'\1write_demo_table(\2, "\4", mode="\3")',
        code,
        flags=re.MULTILINE,
    )

    # 5) Method-chain DataFrame .write...saveAsTable (e.g. bronze.select(...).write)
    code = re.sub(
        r"^(\s*)((?:bronze|typed|valid|invalid|passed|checks|imputed|std|on_time|late|duped|deduped|"
        r"skew_safe|evolved|incremental|merged|auto_log|metrics_df|by_event|triggers|summary|shuffled|"
        r"joined|masked|canonical|profile|dim|ref|run_df|optimized|utc|local|orphans|e2e|cdf)"
        r"(?:\.[a-zA-Z_][\w.()\",: ]*)?)\.write\.format\(\"delta\"\)\.mode\(\"(overwrite|append)\"\)"
        r'\.saveAsTable\(\s*f"\{UC_TABLE\}\.([^"]+)"\s*\)',
        r'\1write_demo_table(\2, "\4", mode="\3")',
        code,
        flags=re.MULTILINE,
    )

    code = re.sub(
        r'spark\.table\(f"\{UC_TABLE\}\.([^"]+)"\)',
        r'read_demo_table("\1")',
        code,
    )

    return code


def _problem_md(p: dict[str, str]) -> str:
    use_case = USE_CASES.get(p["num"], "")
    finledger = use_case.split("**You run this when:**")[0].strip() if use_case else ""
    when = ""
    if "**You run this when:**" in use_case:
        when = use_case.split("**You run this when:**")[1].strip()

    detailed = DETAILED_THEORY.get(p["num"], p["theory"])

    return (
        f"## Problem {p['num']} — {p['problem']}\n\n"
        f"| | |\n"
        f"|:---|:---|\n"
        f"| **The problem** | {p['problem']} |\n"
        f"| **The solution** | **{p['solution']}** |\n"
        f"| **When to apply** | {when or 'See steps below'} |\n\n"
        f"### FinLedger use case\n\n{finledger}\n\n"
        f"### Trainer: explain step-by-step\n\n{detailed}\n\n"
        f"### Quick reference (WHAT / WHY / HOW)\n\n{p['theory']}\n\n"
        f"### Run the next cell\n\n"
        f"Code demonstrates **{p['solution']}**. Read the banner comments, then execute."
    )


def _code_banner(p: dict[str, str]) -> str:
    return (
        f'# {"=" * 72}\n'
        f"# PROBLEM {p['num']}: {p['problem']}\n"
        f"# SOLUTION: {p['solution']}\n"
        f'# {"-" * 72}\n'
        f"# WHAT GOES WRONG: {p['problem']} breaks or slows the pipeline.\n"
        f"# WHAT THIS CODE DOES: Demonstrates {p['solution']} on FinLedger bronze data.\n"
        f"# OUTPUT: Table prob{p['num']}_* in Unity Catalog or ADLS (see write_demo_table log).\n"
        f'# {"=" * 72}'
    )


def build() -> Path:
    parts: list[str] = [
        "# Databricks notebook source",
        "# MAGIC %md",
        "# MAGIC # 50 Data Engineering Problems — Runnable Solutions",
        "# MAGIC",
        "# MAGIC Read **each markdown cell** before the code — it explains the problem, solution, and use case.",
        "# MAGIC Every code cell starts with a **PROBLEM / SOLUTION** banner.",
        "# MAGIC",
        "# MAGIC ## How to run",
        "# MAGIC",
        "# MAGIC 1. `session-3\\orchestrate.cmd --setup-secrets` on Windows  ",
        "# MAGIC 2. Start cluster → attach → **Run all**  ",
        "# MAGIC 3. Or: `run-50-problems.cmd` from repo root",
        "# MAGIC",
        "# MAGIC ## Problem groups",
    ]

    for letter, title, nums in PROBLEM_GROUPS:
        parts.append(f"# MAGIC - **{letter}.** {title}")

    parts.extend([
        "# MAGIC",
        "# MAGIC ## Full index",
        "# MAGIC",
        "# MAGIC | # | Problem | Solution |",
        "# MAGIC |---|---------|----------|",
    ])

    for p in ALL_PROBLEMS_50:
        parts.append(f"# MAGIC | {p['num']} | {p['problem']} | {p['solution']} |")

    parts.extend(["", "# COMMAND ----------", ""])
    md(parts, FOUNDATIONS_MD.strip())
    parts.extend(["", "# COMMAND ----------", ""])
    parts.extend(BRONZE_AUTH_CELL.strip().splitlines())
    parts.extend(["", "# COMMAND ----------", ""])
    if CONNECTIVITY_PRIMER_MD.strip():
        md(parts, CONNECTIVITY_PRIMER_MD.strip())
        parts.extend(["", "# COMMAND ----------", ""])
    parts.extend(DEMO_TABLE_HELPERS.strip().splitlines())
    parts.extend(["", "# COMMAND ----------", ""])

    md(parts, FORMAT_CONCEPT_MAP.strip())
    parts.extend(["", "# COMMAND ----------", ""])

    current_group = None
    for p in ALL_PROBLEMS_50:
        for letter, title, nums in PROBLEM_GROUPS:
            if p["num"] in nums and letter != current_group:
                current_group = letter
                md(parts, f"# Part {letter} — {title}")
                break

        md(parts, _problem_md(p))
        patched = patch_problem_code(p["code"])
        # Prepend banner; strip duplicate old header if present
        patched = re.sub(r"^# --- Problem \d+.*---\n", "", patched)
        code(parts, _code_banner(p) + "\n\n" + patched.strip())
        if p.get("uc_table"):
            code(
                parts,
                f'''# Verify problem {p["num"]} output
try:
    _df = read_demo_table("{p['uc_table']}")
    print("VERIFY OK:", "{p['uc_table']}", "| rows =", _df.count())
except Exception as _exc:
    print("VERIFY FAIL:", _exc)''',
            )

    md(parts, "## Final — all tables created this run")
    code(
        parts,
        '''if UC_WRITE_ENABLED:
    display(spark.sql(f"SHOW TABLES IN {UC_TABLE}"))
    for row in sorted(spark.sql(f"SHOW TABLES IN {UC_TABLE}").collect(), key=lambda r: r.tableName):
        try:
            n = spark.table(f"{UC_TABLE}.{row.tableName}").count()
            print(f"  {UC_TABLE}.{row.tableName:42s} {n:>8} rows")
        except Exception as exc:
            print(f"  {UC_TABLE}.{row.tableName:42s}  error: {exc}")
else:
    print("ADLS Delta folders under", DEMO_BASE)
    for item in sorted(dbutils.fs.ls(DEMO_BASE)):
        print(" ", item.path)
print("-" * 70)
print("Written this run:", len(WRITTEN_TABLES), "tables/paths")
print("NOTEBOOK COMPLETE — 50 problems demonstrated.")''',
    )

    OUT.write_text("\n".join(finish(parts)) + "\n", encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB)")
    broken = path.read_text(encoding="utf-8").count(".load(write_demo_table")
    if broken:
        print(f"WARNING: {broken} corrupted .load(write_demo_table) patterns remain!")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
