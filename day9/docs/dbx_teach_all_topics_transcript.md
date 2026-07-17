# Trainer transcript — `dbx_teach_all_topics` (50 topics)

**Notebook:** `/Shared/day9/dbx_teach_all_topics`  
**Open:** https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/dbx_teach_all_topics  
**Source:** `day9/notebooks/nb_dbx_teach_all_topics.py`  
**Duration:** 2–3 hours full walk · or 45–60 min “greatest hits” (Parts A, C, D, E, L)  
**Prereq:** `deploy-shared-lab.cmd` once (bronze CSV + `finledger` secrets)  
**Companion:** `50_data_engineering_problems` (failure modes) · `finledger_event_processors` (Zach deep-dive)

---

## Opening (3 min)

> “This notebook is **not** fifty random APIs. Each topic is one **hard production problem**,
> one **easy approach**, one **analogy**, then **commented code** you can read aloud.
> Same spirit as the 50-problems lab — but optimized for *teaching*, not debugging.”

Write on board:

```text
HARD problem  →  EASY approach  →  ANALOGY  →  Run code  →  “Where breaks in prod?”
```

**Analogy rule for trainers:** if you can’t explain it with kitchen / airport / library, skip until the story is crisp.

---

## Compute tip (say before Cell 0) — 2 min

> “Prefer the **classic** FinLedger cluster for ADLS demos.
> **Serverless** blocks `fs.azure.account.key`. Our auth cell is **RBAC first**, then key, then driver SDK.
> Setup writes go to Unity Catalog if allowed, else ADLS, else **DBFS** so Serverless never dies on `mkdirs`.”

| Auth mode printed | Meaning |
|-------------------|---------|
| `azure_rbac_passthrough` | Best — identity has Blob Data access |
| `storage_key_spark_conf` | Classic cluster + key |
| `driver_sdk` | CSV loaded via Python SDK (fine for teaching) |

**If you still see:** `Invalid configuration value detected for fs.azure.account.key`  
→ Detach Serverless → attach classic cluster **or** pull latest notebook (RBAC-first + DBFS fallback) → **Run all from top**.

---

## Cell walkthrough

### Roadmap + how to teach (markdown) — 2 min

> “Every topic cell: read the table (hard / easy), say the analogy, then the banner comments, then Run.”

**Do:** Don’t skip the “How to teach” markdown — it trains the *habit*, not only the APIs.

---

### Cell 0 — Bronze auth — 3 min

> “Same `finledger` secret scope as Sessions 9–15. We never paste storage keys into git.
> Shopping-list analogy: `.select` is free; `.count` is checkout — that’s Topic 01 next.”

**Do:** Run. Confirm:

- `Auth mode` prints one of the three modes above  
- `Bronze rows` > 0  

**Ask:** “Why three auth methods?”  
**Answer:** Serverless security + shared class estates — one chain, no portal clicking mid-class.

---

### Widget cell — 1 min

> “`run_id` is the kitchen ticket number. ADF/Jobs pass the same key as `baseParameters`.”

**Do:** Leave `session3-lab` unless you staged another bronze folder.

---

### Setup cell (UC / ADLS / DBFS) — 3 min  ← former “third cell” failure

> “This used to explode on Serverless at `dbutils.fs.mkdirs(abfss://…)`.
> Now: probe Unity Catalog → soft ADLS → **DBFS fallback**. Look for `SETUP COMPLETE` and `Demo base`.”

**Success looks like:**

```text
SETUP COMPLETE
  Catalog       : … | UC writes: True/False
  Demo base     : abfss://…  OR  dbfs:/FileStore/demo_50_problems
  Bronze rows   : N
  Auth mode     : …
```

**Ask:** “Why DBFS is OK for class?”  
**Answer:** Teaching Spark verbs and Delta APIs; medallion *paths* still show on classic. Don’t pretend DBFS is the production lake.

---

## Greatest hits (60 min cut)

| Time | Part | Topics | Analogy you must say |
|------|------|--------|----------------------|
| 5 | A | 01 lazy vs action | Shopping list vs checkout |
| 5 | A | 04 groupBy | Bowl per dish, not every grain |
| 5 | B | 06 medallion | Raw → prep → plated |
| 8 | C | 09–11 Delta / travel / MERGE | Photo album + Git checkout + UPSERT |
| 8 | D | 13–15 Event model | Same passport desk, different embassies |
| 5 | E | 16 quality assert | Pre-flight checklist |
| 5 | F | 20 broadcast | Postage stamp to every worker |
| 5 | H | 27–28 widgets + exit | Order form + receipt for ADF |
| 5 | L | 49–50 interview + checklist | 90-second recipe story |

Skip J–K detail on the short track; assign as homework with the transcript index.

---

## Full track — say one line per part

### Part A — Spark fundamentals (01–05)

> “Stay in DataFrame verbs. Collect is opening every envelope yourself — mail-room rules instead.”

### Part B — Medallion (06–08)

> “Bronze is the loading dock. Silver is the clean prep. Gold is what goes to the board.”

### Part C — Delta (09–12)

> “Parquet is loose photos. Delta adds the album contents page — time travel and MERGE.”

### Part D — Zach Event (13–15)

> “Many sources, one Event grain. Processors don’t union; the runner does.”

### Part E — Quality (16–18)

> “Empty gold must fail the job. Idempotency = re-run doesn’t double the bank.”

### Part F — Performance (19–22)

> “Shuffle is the expensive airport layover. Broadcast the tiny dim. Don’t invent UDFs.”

### Part G — SQL / UC / secrets (23–26)

> “SQL and DataFrames are the same engine. Secrets are hotel key cards — never under the mat (git).”

### Part H — Jobs / ADF (27–29)

> “Notebook is the recipe. Job/ADF is the alarm clock. Exit JSON is the receipt.”

### Part I — Hard ideas (30–34)

> “SCD2 = passport history. Late data = box office grace window. CDC = motion clips only.”

### Part J — Reshape (35–40)

> “Windows rank without kicking rows out. Pivot = crossword grid. Cache = photocopy the map once.”

### Part K — Ops (41–45)

> “VACUUM is shredding tax returns — only after retention. Streaming = conveyor sushi, same fish API.”

### Part L — Ready (46–50)

> “Multi-task Jobs = restart at frosting, not flour. Purview needs spine labels (paths). Capstone = road test.”

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Cell 3 / setup: `fs.azure.account.key` invalid | Latest notebook + RBAC-first; or switch to **classic** cluster; Run from Cell 0 |
| `mkdirs` on `abfss://` fails | Expected soft-skip → `Demo base` should be `dbfs:/FileStore/demo_50_problems` |
| Bronze rows = 0 / read fail | Re-run `deploy-shared-lab.cmd` (uploads bronze); check secret scope `finledger` |
| UC `PERMISSION_DENIED` | Normal on shared workspace catalog — path/DBFS fallback is OK |
| Widget missing / wrong run | Widget name must be `run_id`; value `session3-lab` |
| Topic Delta write fails mid-book | Re-run setup cell; confirm `DEMO_BASE` printed |

---

## Re-run scenarios

| Scenario | Safe? |
|----------|-------|
| Fresh machine + deploy | Yes |
| Re-run after full success | Yes — overwrite / UC overwriteSchema |
| Partial (stop after Part C) | Yes — resume any topic cell after setup |
| After Serverless fail, switch cluster | **Detach → classic → Run all from top** (don’t resume mid-poison) |

**Re-run command:**

```cmd
cd d:\azure
deploy-shared-lab.cmd
```

Then open `/Shared/day9/dbx_teach_all_topics` → **Run all**.

---

## Source map

| File | Role |
|------|------|
| `day9/scripts/dbx_topics_teach.py` | 50 topics (theory + analogy + code) |
| `day9/scripts/build_dbx_teach_all_notebook.py` | Generator |
| `day9/scripts/bronze_auth_cell.py` | Cell 0 — RBAC-first bronze |
| `day9/scripts/demo_table_helpers.py` | Setup — UC / ADLS / DBFS |
| `day9/notebooks/nb_dbx_teach_all_topics.py` | Generated SOURCE notebook |
| `day9/docs/dbx_teach_all_topics_transcript.md` | **This file** |

---

## Closing (2 min)

> “If you can teach Topic 01 and Topic 49 without notes, you’re ready for the interview story.
> Failures live in the 50-problems notebook. Depth on Events lives in `finledger_event_processors`.
> This book is the **map** — walk it with analogies.”
