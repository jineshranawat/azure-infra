#!/usr/bin/env python3
"""Build nb_file_formats_and_secrets.py — simple guide: formats + secret scope.

Run: python day9/scripts/build_file_formats_notebook.py
"""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from notebook_helpers import ascii_flow, box_diagram, code, concept, finish, lesson, md
from session_diagrams import (
    ASCII_FORMATS,
    DELTA_LOG,
    DELTA_VS_PARQUET,
    FORMAT_LAYERS,
    FORMAT_PICKER,
    SECRET_SCOPE_FLOW,
)

OUT = ROOT.parent / "notebooks" / "nb_file_formats_and_secrets.py"
WS_NAME = "file_formats_and_secrets"


def build() -> list[str]:
    p: list[str] = ["# Databricks notebook source", ""]

    md(
        p,
        """# File formats & secret scope — simple FinLedger guide

**Two topics in one short notebook:**
1. How to store **secrets** in scope `finledger` (safe storage keys)
2. **File formats** you meet on the lake: CSV, JSON, Parquet, Delta

**Cells:** small theory + diagram + one code example each.""",
    )

    # ═══════════════════════════════════════════════════════════════════════
    # PART A — SECRET SCOPE
    # ═══════════════════════════════════════════════════════════════════════
    md(p, "## Part A — Secret scope `finledger`")
    box_diagram(p, SECRET_SCOPE_FLOW, caption="How secrets reach your notebook")

    concept(
        p,
        "A.1 — What is a secret scope?",
        "A **secret scope** is a named vault inside Databricks (scope `finledger`) that holds keys like `storage-account` and `storage-key`.",
        "Storage keys must **never** appear in notebook code or git — they leak in logs and version control.",
        "Notebooks call `dbutils.secrets.get(scope, key)` at runtime. Databricks decrypts the value on the cluster.",
        "**Hotel safe:** you keep the PIN in the safe, not written on the room door.",
        'print("Scope name we use in every lab: finledger")',
    )

    md(p, "### A.2 — Step-by-step: add secrets (Windows, recommended)")
    md(
        p,
        """| Step | Where | What to do |
|------|--------|------------|
| **1** | Azure Portal | Storage account → **Access keys** → copy **key1** |
| **2** | Repo `.env` | Add `STORAGE_ACCOUNT_KEY=...` and `DATABRICKS_TOKEN=...` |
| **3** | Command Prompt | `cd session-3` then `orchestrate.cmd --setup-secrets` |
| **4** | Databricks | Run cell A.4 below to verify `dbutils.secrets.get` works |

**Get DATABRICKS_TOKEN:** Workspace → your name → **User settings** → **Developer** → **Access tokens** → **Generate new token**.""",
    )

    ascii_flow(
        p,
        """Windows laptop (one time)
─────────────────────────
1. Paste key1 into d:\\azure\\.env
2. cd session-3
3. orchestrate.cmd --setup-secrets
   → creates scope finledger
   → stores storage-account + storage-key

Databricks notebook (every lab)
───────────────────────────────
dbutils.secrets.get("finledger", "storage-key")""",
        caption="Secret setup flow",
    )

    lesson(
        p,
        "A.3 — CLI commands (trainer laptop)",
        "Run these in PowerShell after setting `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in `.env`.",
        """# List scopes
# d:\\azure\\.venv\\Scripts\\databricks.exe secrets list-scopes

# List key names (not values) in finledger
# d:\\azure\\.venv\\Scripts\\databricks.exe secrets list --scope finledger

# Put/update a secret (trainer only)
# databricks secrets put --scope finledger --key storage-account --string-value stsharedqgr7mj

print("Trainer runs CLI on laptop — learners only verify in notebook")""",
    )

    lesson(
        p,
        "A.4 — Verify secrets in this notebook",
        "If this cell works, your scope is ready for all FinLedger labs.",
        """SECRET_SCOPE = "finledger"

STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account").strip()
_storage_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-key")

# Prove key exists — Databricks redacts the value if you print it directly
print("Scope          :", SECRET_SCOPE)
print("Storage account:", STORAGE_ACCOUNT)
print("Key loaded     :", "yes" if len(_storage_key) > 10 else "NO — run orchestrate.cmd --setup-secrets")""",
        extra_code=[
            """# Optional: list bronze folder (needs spark.conf on classic cluster)
try:
    spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        _storage_key,
    )
    root = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    for item in dbutils.fs.ls(root):
        print(" bronze/", item.name)
except Exception as e:
    print("Storage list skipped (Serverless may need different auth):", str(e)[:80])""",
        ],
    )

    md(
        p,
        """### A.5 — If `dbutils.secrets.put` fails in notebook

Many workspaces **block** writing secrets from notebooks (`SecretsHandler has no attribute put`).

**Fix:** always use **orchestrate.cmd --setup-secrets** on your Windows PC — not `dbutils.secrets.put` in a cell.""",
    )

    code(p, "from pyspark.sql import functions as F")

    # ═══════════════════════════════════════════════════════════════════════
    # PART B — FILE FORMATS
    # ═══════════════════════════════════════════════════════════════════════
    md(p, "## Part B — File formats on the data lake")
    box_diagram(p, FORMAT_PICKER, caption="Which format when?")
    box_diagram(p, FORMAT_LAYERS, caption="FinLedger medallion → format")
    ascii_flow(p, ASCII_FORMATS, caption="Quick comparison table")

    concept(
        p,
        "B.1 — CSV (Comma-Separated Values)",
        "**CSV** = text file, one row per line, columns separated by commas. First row often has column names.",
        "Sources (banks, ERP exports) still deliver CSV. Easy for humans to open in Excel.",
        "Bronze layer lands CSV unchanged. Spark: `spark.read.option('header', True).csv(path)`.",
        "**Paper ledger:** simple, universal, not great for huge scale or strict types.",
        f"""bronze_csv = (
    f"abfss://bronze@{{STORAGE_ACCOUNT}}.dfs.core.windows.net"
    f"/loaded/run=session3-lab/sample_transactions.csv"
)
df_csv = spark.read.option("header", True).option("inferSchema", True).csv(bronze_csv)
df_csv.printSchema()
df_csv.show(3)""",
    )

    concept(
        p,
        "B.2 — JSON (JavaScript Object Notation)",
        "**JSON** = nested text format `{ \"key\": \"value\" }`. Common for REST APIs and event payloads.",
        "Some feeds arrive as JSON lines (one JSON object per row).",
        "Spark: `spark.read.json(path)` infers schema from structure. Good for semi-structured bronze.",
        "**Post-it stack:** flexible shape, harder to query than flat tables without flattening.",
        """# Demo JSON in memory (real lake path would be abfss://bronze@.../events.json)
df_json = spark.read.json(
    spark.sparkContext.parallelize([
        '{"transaction_id":"TXN-1","amount_gbp":"100.00","channel":"wire"}',
    ])
)
df_json.show(truncate=False)""",
    )

    concept(
        p,
        "B.3 — Parquet (columnar files)",
        "**Parquet** = binary columnar format. Stores data by column (not row), compressed — fast for analytics.",
        "Much smaller and faster than CSV for large scans. No transaction log — just files in a folder.",
        "Spark: `spark.read.parquet(path)` and `.write.parquet(path)`. Used inside Delta tables as the data files.",
        "**Filing cabinet by column:** pull only the columns you need — less IO.",
        """# Parquet write/read demo (temp folder on cluster — production silver uses Delta)
tmp = "dbfs:/tmp/finledger_parquet_demo"
df_csv.select("transaction_id", "channel", "amount_gbp").write.mode("overwrite").parquet(tmp)
df_parquet = spark.read.parquet(tmp)
print("Parquet rows:", df_parquet.count())
df_parquet.show(3)""",
    )

    concept(
        p,
        "B.4 — Delta Lake (Parquet + transaction log)",
        "**Delta** = Parquet data files + `_delta_log/` folder. Adds **ACID**, schema enforcement, **time travel**.",
        "FinLedger **silver** and **gold** use Delta — not plain CSV or loose Parquet folders.",
        "Write: `.format('delta').save(path)`  Read: `spark.read.format('delta').load(path)`.",
        "**Parquet + bank ledger:** every change is a numbered version you can audit or restore.",
        f"""silver_path = (
    f"abfss://silver@{{STORAGE_ACCOUNT}}.dfs.core.windows.net"
    f"/transactions/run=session3-lab"
)
# Read Delta if Session 9 already wrote it; else show message
try:
    df_delta = spark.read.format("delta").load(silver_path)
    print("Delta silver rows:", df_delta.count())
    df_delta.show(3)
except Exception:
    print("Silver Delta not written yet — run Session 9 notebook first")""",
        mermaid_diagram=DELTA_VS_PARQUET,
        ascii_diagram=None,
    )
    box_diagram(p, DELTA_LOG, caption="Inside a Delta table folder")

    md(
        p,
        """### B.5 — Other formats (awareness only)

| Format | Typical use |
|--------|-------------|
| **Avro** | Kafka schemas, row-based with schema embedded |
| **ORC** | Hive legacy, columnar (less common in new Azure lakes) |
| **XML** | Old enterprise feeds — parse then convert to Delta |

**FinLedger rule:** bronze = **CSV** (today) → silver/gold = **Delta**.""",
    )

    lesson(
        p,
        "B.6 — Side-by-side row count",
        "Same logical data — different formats, different layers.",
        """print("CSV bronze rows :", df_csv.count())
print("Format lesson complete — use CSV ingest, Delta for curated tables")""",
    )

    md(p, "## Checklist")
    md(
        p,
        """**Secrets**
- [ ] Ran `orchestrate.cmd --setup-secrets` on Windows
- [ ] Cell A.4 shows storage account name
- [ ] Never committed keys to git

**Formats**
- [ ] Can explain CSV vs Parquet vs Delta in one sentence each
- [ ] Know bronze = CSV, silver/gold = Delta
- [ ] **Next:** Session 9 notebook — write silver Delta + quarantine""",
    )

    return finish(p)


def main() -> None:
    lines = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    cells = sum(1 for line in lines if line == "# COMMAND ----------") + 1
    print(f"Wrote {OUT} ({cells} cells) -> /Shared/day9/{WS_NAME}")


if __name__ == "__main__":
    main()
