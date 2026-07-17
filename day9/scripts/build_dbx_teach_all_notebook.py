#!/usr/bin/env python3
"""Generate nb_dbx_teach_all_topics.py — large FinLedger Databricks teaching notebook.

Style: same as 50-problems lab (theory → analogy → commented code).
Run:  python day9/scripts/build_dbx_teach_all_notebook.py
Deploy: deploy-shared-lab.cmd → /Shared/day9/dbx_teach_all_topics
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent.parent
sys.path.insert(0, str(ROOT))

from bronze_auth_cell import BRONZE_AUTH_CELL
from demo_table_helpers import DEMO_TABLE_HELPERS
from dbx_topics_teach import TOPIC_GROUPS, TOPICS
from notebook_helpers import code, finish, md

OUT = ROOT.parent / "notebooks" / "nb_dbx_teach_all_topics.py"


def _topic_md(t: dict[str, str]) -> str:
    return (
        f"## Topic {t['num']} — {t['title']}\n\n"
        f"| | |\n"
        f"|:---|:---|\n"
        f"| **Hard problem** | {t['hard_problem']} |\n"
        f"| **Easy approach** | **{t['easy_approach']}** |\n\n"
        f"### Analogy\n\n{t['analogy']}\n\n"
        f"### Trainer: WHAT / WHY / HOW\n\n{t['theory']}\n\n"
        f"### Run the next cell\n\n"
        f"Banner comments explain every block. Read them out loud while you demo."
    )


def _banner(t: dict[str, str]) -> str:
    return (
        f'# {"=" * 72}\n'
        f"# TOPIC {t['num']}: {t['title']}\n"
        f"# HARD: {t['hard_problem'][:70]}\n"
        f"# EASY: {t['easy_approach'][:70]}\n"
        f"# ANALOGY: {t['analogy'][:70]}\n"
        f'# {"=" * 72}'
    )


def build() -> Path:
    parts: list[str] = [
        "# Databricks notebook source",
        "# MAGIC %md",
        "# MAGIC # FinLedger Databricks — Teach-All Topics",
        "# MAGIC",
        "# MAGIC A **large teaching notebook** (same spirit as the 50-problems lab):",
        "# MAGIC **50 topics** — complicated industry problems explained the **easy** way with **analogies** + **commented code**.",
        "# MAGIC",
        "# MAGIC **Audience:** Trainers walking Sessions 8–19 / 27–30, or learners revising Spark + Delta + Events + Jobs.",
        "# MAGIC",
        "# MAGIC ## How to run",
        "# MAGIC",
        "# MAGIC 1. Prefer **classic** cluster; Serverless works via RBAC → SDK → DBFS fallback (account keys blocked)  ",
        "# MAGIC 2. `deploy-shared-lab.cmd` once  ",
        "# MAGIC 3. Open `/Shared/day9/dbx_teach_all_topics` → **Run all** from the top  ",
        "# MAGIC 4. Trainer script: `day9/docs/dbx_teach_all_topics_transcript.md`  ",
        "# MAGIC",
        "# MAGIC ## Companion notebooks",
        "# MAGIC",
        "# MAGIC | Notebook | Role |",
        "# MAGIC |----------|------|",
        "# MAGIC | `50_data_engineering_problems` | 50 failure modes |",
        "# MAGIC | `finledger_event_processors` | Zach Event runner deep-dive |",
        "# MAGIC | **`dbx_teach_all_topics`** | **This — curated teach path with analogies** |",
        "# MAGIC",
        "# MAGIC ## Topic groups",
    ]
    for letter, title, _ in TOPIC_GROUPS:
        parts.append(f"# MAGIC - **{letter}.** {title}")

    parts.extend([
        "# MAGIC",
        "# MAGIC ## Full index",
        "# MAGIC",
        "# MAGIC | # | Topic | Easy approach |",
        "# MAGIC |---|-------|---------------|",
    ])
    for t in TOPICS:
        parts.append(f"# MAGIC | {t['num']} | {t['title']} | {t['easy_approach'][:60]} |")

    parts.extend(["", "# COMMAND ----------", ""])

    md(
        parts,
        """# How to teach with this notebook

1. **Read the markdown** (hard problem → easy approach → analogy) **before** clicking Run.
2. In the code cell, read the `# WHAT / WHY / HOW` banner aloud.
3. Run the cell; point at the printed output.
4. Ask: *"Where would this break in production?"* — then point back at the hard problem.

**Analogy rule:** if you can't explain it with a kitchen / airport / library story, the topic isn't ready.""",
    )

    parts.extend(BRONZE_AUTH_CELL.strip().splitlines())
    parts.extend(["", "# COMMAND ----------", ""])

    # Widget + RUN_ID alias if bronze cell used fixed RUN_ID
    code(
        parts,
        '''# Teaching widgets — ADF/Jobs can override
dbutils.widgets.text("run_id", "session3-lab", "Pipeline run id")
RUN_ID = dbutils.widgets.get("run_id").strip() or "session3-lab"
print("Teaching RUN_ID =", RUN_ID)''',
    )

    parts.extend(DEMO_TABLE_HELPERS.strip().splitlines())
    parts.extend(["", "# COMMAND ----------", ""])

    md(
        parts,
        """## Foundations in 60 seconds

```
bronze CSV  →  typed silver  →  gold KPI
     ↑              ↑                ↑
  land raw     Event/quality      pivot/report
```

Everything below is a **story fragment** you can drop into a 2-hour class.""",
    )

    current = None
    for t in TOPICS:
        for letter, title, nums in TOPIC_GROUPS:
            if t["num"] in nums and letter != current:
                current = letter
                md(parts, f"# Part {letter} — {title}")
                break
        md(parts, _topic_md(t))
        code(parts, _banner(t) + "\n\n" + t["code"].strip())

    md(parts, "## Final — tables written this run")
    code(
        parts,
        '''print("=" * 70)
print("TEACH-ALL COMPLETE —", len(WRITTEN_TABLES), "outputs")
for p in WRITTEN_TABLES[-15:]:
    print(" ", p)
if len(WRITTEN_TABLES) > 15:
    print(" ...", len(WRITTEN_TABLES) - 15, "earlier outputs")
print("Next: finledger_event_processors OR 50_data_engineering_problems")
print("=" * 70)''',
    )

    OUT.write_text("\n".join(finish(parts)) + "\n", encoding="utf-8")
    return OUT


def main() -> int:
    path = build()
    print(f"Wrote {path} ({path.stat().st_size // 1024} KB) — {len(TOPICS)} topics")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
