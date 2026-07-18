# Coverage map — where to go, step by step

**Start here if you are lost.** This file is the master index for the whole repo: which **session**, which **document**, which **command**, and which **portal** blade — in order.

> **Tomorrow’s infra session (20+ h shared estate):**  
> **[docs/INFRA-WALKTHROUGH-20H.md](docs/INFRA-WALKTHROUGH-20H.md)** · run `.\infra-walkthrough.cmd --list` then `--phase N`  
> Links every child script (provision → ADF → Day 6–9 → medallion CI/CD → cost) with analogies + incremental DevOps.

**Curriculum source:** [azure.html](azure.html) (60h programme, Day 0–7)
**Student course (Data Engineering):** [docs/data-engineering-course.html](docs/data-engineering-course.html) — 60h practical programme; Storage, Data Factory, Databricks, Purview, SQL — theory + hands-on

| Role | Start |
|------|--------|
| **Learner** | Row 1 below → follow steps 1–12 in order |
| **Trainer** | [docs/TRAINER-NOTES.md](docs/TRAINER-NOTES.md) + session GUIDE files |

---

## 1. Programme at a glance

| Session | Folder | azure.html | Duration | Deploy command | UI walkthrough doc |
|---------|--------|------------|----------|----------------|-------------------|
| **Session 1** — Class-1 landing zone | repo root | Day 1 (Hours 9–16) | ~6 h class | `orchestrate.cmd` | [README §10](README.md#10-azure-portal--complete-novice-practical-guide) + [WORKFLOW-AND-CODE](docs/WORKFLOW-AND-CODE.md) |
| **Session 2** — ADF ingestion | `session-2/` | Day 2 (Hours 17–24) | 2 h | `session-2\orchestrate.cmd` | [session-2/MANUAL-LAB.md](session-2/MANUAL-LAB.md) |
| **Session 3** — Databricks lakehouse | `session-3/` | Day 3 (Hours 25–32) | 2 h | `session-3\orchestrate.cmd` | [session-3/MANUAL-LAB.md](session-3/MANUAL-LAB.md) |
| **Day 6** — Python for data engineers | `day6/` | [data-engineering-course.html](docs/data-engineering-course.html) S6 | 2 h | `day6\orchestrate.cmd` | [day6/MANUAL-LAB.md](day6/MANUAL-LAB.md) |
| **Day 7** — Storage + read lake (PySpark) | `day7/` | [data-engineering-course.html](docs/data-engineering-course.html) S7 | 2 h | `day7\orchestrate.cmd` | [day7/MANUAL-LAB.md](day7/MANUAL-LAB.md) |
| **Day 8** — PySpark transformations | `day8/` | [data-engineering-course.html](docs/data-engineering-course.html) S8 | 2 h | `day8\orchestrate.cmd` | [day8/MANUAL-LAB.md](day8/MANUAL-LAB.md) |
| Session 4 — Purview | *planned* | Day 4 | — | — | — |
| Session 5+ — Operate / Engineering | *planned* | Day 5–7 | — | — | — |

---

## 2. Learner journey — step by step (root level)

Do these in order. **Read** the doc → **Run** the command → **Open** the portal section → **Verify** the checklist.

| Step | When | Read (open this file) | Run (Command Prompt) | Portal — where to click | Verify |
|------|------|------------------------|-------------------------|-------------------------|--------|
| **1** | First time on PC | [README §1](README.md#1-how-to-run-on-windows-step-by-step) Prerequisites | Install Python + Git | — | Python on PATH |
| **2** | Clone repo | [README §1 Step 0](README.md#step-0--clone-the-repo-and-open-a-terminal) | `git clone` … `cd azure-infra-class` | — | Folder contains `orchestrate.cmd` |
| **3** | Create config | [README §1 Step 1](README.md#step-1--first-run-creates-env) | `orchestrate.cmd --install-cli` | — | `.env` file exists |
| **4** | Fill secrets | [README §1 Step 1](README.md#step-1--first-run-creates-env) | Edit `.env` in Notepad | [portal.azure.com](https://portal.azure.com) → **Subscriptions** → copy ID | `.env` saved |
| **5** | Deploy Class-1 + platforms | [README §2](README.md#2-theory--what--why-concepts-before-code) theory optional first | `orchestrate.cmd --install-cli` then `orchestrate.cmd` | Wait for `az login` browser | Terminal: `Orchestration complete` |
| **6** | Understand what deployed | [README §2](README.md#2-theory--what--why-concepts-before-code) | — | — | Can name RG, budget, KV, storage |
| **7** | Portal tour Class-1 | [README §10](README.md#10-azure-portal--complete-novice-practical-guide) | — | RG → tags → Budget → KV → Storage → containers | [README §10.6](README.md#106-safe-novice-checklist-do--dont) |
| **8** | Code + portal map | [WORKFLOW-AND-CODE §3](docs/WORKFLOW-AND-CODE.md#3-step-by-step-code--terminal--portal) | — | Same blades per block | Match code lines to live resources |
| **9** | Timed class (optional) | [CLASS-GUIDE](docs/CLASS-GUIDE.md) Blocks 0–7 | Per block | “Learner sees in portal” column | Block checkpoints |
| **10** | **Session 2** start | [session-2/README](session-2/README.md) §A theory | `cd session-2` | — | Class-1 + ADF exist in RG |
| **11** | Session 2 automate | [session-2/README](session-2/README.md) §B | `orchestrate.cmd` | — | Phases 1–6 OK in terminal |
| **12** | Session 2 UI lab | [session-2/MANUAL-LAB](session-2/MANUAL-LAB.md) §A→I | `orchestrate.cmd --run-pipeline` (optional) | Storage **bronze** + ADF Studio | [MANUAL-LAB §I](session-2/MANUAL-LAB.md#i-end-to-end-verification-checklist) |
| **13** | **Session 3** start | [session-3/README](session-3/README.md) §A theory | `cd session-3` | — | Class-1 + Databricks workspace exist |
| **14** | Session 3 prep | [session-3/README](session-3/README.md) §B | `orchestrate.cmd` | — | Prints `abfss://` paths |
| **15** | Session 3 UI lab | [session-3/MANUAL-LAB](session-3/MANUAL-LAB.md) §A→I | `orchestrate.cmd --verify-storage` (after notebooks) | Databricks workspace + Storage silver/gold | [MANUAL-LAB §I](session-3/MANUAL-LAB.md#lab-i) |
| **16** | **Day 6** Python | [day6/README](day6/README.md) §A | `cd day6` → `orchestrate.cmd` | — | Unit tests OK + notebook list |
| **17** | **Day 7** venv + lake read | [day7/README](day7/README.md) + [SESSION7-STUDENT-GUIDE](day7/SESSION7-STUDENT-GUIDE.md) | `python -m venv .venv` then `cd day7` → `orchestrate.cmd` | Databricks: nb_01→nb_04 | Bronze row count matches Session 3 |
| **18** | **Day 8** PySpark transforms | [day8/PYSPARK-REFERENCE](day8/PYSPARK-REFERENCE.md) then [SESSION8-STUDENT-GUIDE](day8/SESSION8-STUDENT-GUIDE.md) | `cd day8` → `orchestrate.cmd` | Databricks: nb_00→nb_04 | All 8 transforms + lazy vs action |
| **19** | Teardown | [README §1 teardown](README.md#other-commands) | `orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes` | RG deleted | RG gone in portal |

**Re-run anytime (Session 1):** `orchestrate.cmd` — [README §C](README.md#c-the-re-run-command)  
**Re-run anytime (Session 2):** `cd session-2` → `orchestrate.cmd`

---

## 3. Document index — what each file is for

| File | Audience | Use when |
|------|----------|----------|
| **[COVERAGE-MAP.md](COVERAGE-MAP.md)** | Everyone | **Lost? Start here.** |
| **[README.md](README.md)** | Learners | Run guide, theory, failures, portal §10 |
| **[docs/WORKFLOW-AND-CODE.md](docs/WORKFLOW-AND-CODE.md)** | Learners + trainers | **Code file ↔ portal blade** per Class-1 step |
| **[docs/CLASS-GUIDE.md](docs/CLASS-GUIDE.md)** | Trainers + learners | **Timed 6h class** blocks with TODOs |
| **[docs/TRAINER-NOTES.md](docs/TRAINER-NOTES.md)** | Trainers | One orchestrator, demo script, MPN limits |
| **[docs/GOVERNANCE-DEPLOY.md](docs/GOVERNANCE-DEPLOY.md)** | Trainers | Synapse / Fabric / Purview MPN workarounds |
| **[azure.html](azure.html)** | Trainers | Full 60h curriculum outline |
| **[session-2/README.md](session-2/README.md)** | Learners | Session 2 theory + 2h schedule |
| **[session-2/MANUAL-LAB.md](session-2/MANUAL-LAB.md)** | Learners | **Session 2 UI: read → do → verify** |
| **[session-2/GUIDE.md](session-2/GUIDE.md)** | Trainers | Session 2 timed blocks |
| **[session-3/README.md](session-3/README.md)** | Learners | Session 3 theory + 2h schedule |
| **[session-3/MANUAL-LAB.md](session-3/MANUAL-LAB.md)** | Learners | **Session 3 UI: Databricks + Storage** |
| **[session-3/UI-OVERVIEW.md](session-3/UI-OVERVIEW.md)** | Learners | **Theory + UI graphs** |
| **[session-3/SESSION3-STUDENT-GUIDE.md](session-3/SESSION3-STUDENT-GUIDE.md)** | Learners | **Session 3 classroom handout** |
| **[session-3/GUIDE.md](session-3/GUIDE.md)** | Trainers | Session 3 timed blocks |
| **[day6/README.md](day6/README.md)** | Learners | Day 6 Python theory + run |
| **[day6/MANUAL-LAB.md](day6/MANUAL-LAB.md)** | Learners | Day 6 step-by-step |
| **[day7/README.md](day7/README.md)** | Learners | Day 7 venv → pandas/Polars → PySpark read |
| **[day7/SESSION7-STUDENT-GUIDE.md](day7/SESSION7-STUDENT-GUIDE.md)** | Learners | Day 7 classroom handout |
| **[day7/MANUAL-LAB.md](day7/MANUAL-LAB.md)** | Learners | Day 7 step-by-step |
| **[day8/README.md](day8/README.md)** | Learners | Day 8 transforms + run |
| **[day8/PYSPARK-REFERENCE.md](day8/PYSPARK-REFERENCE.md)** | Learners | **Complete PySpark theory (14 sections)** |
| **[day8/SESSION8-STUDENT-GUIDE.md](day8/SESSION8-STUDENT-GUIDE.md)** | Learners | Day 8 classroom handout |
| **[day8/MANUAL-LAB.md](day8/MANUAL-LAB.md)** | Learners | Day 8 step-by-step |

---

## 4. Session 1 — Class-1: read / run / portal map

**Command:** `orchestrate.cmd` (repo root)  
**Resource group:** `rg-<learner>-class1`

| Block | Read | Run | Portal (verify) | Doc section |
|-------|------|-----|-----------------|-------------|
| Setup | README §1 | `orchestrate.cmd --install-cli` | Subscriptions → ID | CLASS-GUIDE Block 0 |
| Tags & RG | `infra/main.bicep` tags | `orchestrate.cmd` | RG → **Tags** | WORKFLOW Step 1, README §10.2 step 1 |
| Budget | `infra/main.bicep` budget | (same deploy) | Cost Management → **Budgets** | WORKFLOW Step 2, README §10.2 step 2 |
| Key Vault | `infra/main.bicep` KV | (same deploy) | KV → Properties, IAM | WORKFLOW Step 3, README §10.2 step 3 |
| ADLS + medallion | `infra/main.bicep` storage | (same deploy) | Storage → **Containers** bronze/silver/gold/audit | WORKFLOW Step 4, README §10.2 step 4 |
| Lifecycle | `infra/main.bicep` lifecycle | (same deploy) | Storage → **Lifecycle management** | README §10.2 step 4 |
| RBAC | `infra/main.bicep` + `orchestrate.py` | (same deploy) | Storage/KV → **Access control (IAM)** | WORKFLOW Step 5, README §10.2 step 5 |
| Verify cost | `scripts/verify_cost.py` | (orchestrator phase) | Cost Management → **Cost analysis** filter RG | WORKFLOW Step 6, CLASS-GUIDE Block 6 |
| Platforms (ADF, etc.) | `infra/platform-services.bicep` | `orchestrate.cmd` (full lab) | RG → ADF, Databricks, Purview | README §10.3, TRAINER-NOTES §4 |

**Portal-only deep dive:** [README §10.7](README.md#107-suggested-90-minute-portal-lab-after-deploy) (90 min)

---

## 5. Session 2 — ADF: read / run / portal map

**Prerequisite:** Session 1 complete (`orchestrate.cmd` at root).  
**Command:** `session-2\orchestrate.cmd`

| Block | Read | Run | Portal (verify) | Doc section |
|-------|------|-----|-----------------|-------------|
| Find resources | MANUAL-LAB §A | — | RG → storage + ADF names | MANUAL-LAB §A verify |
| Bronze upload | `bronze_loader.py` | `orchestrate.cmd` | Storage → bronze → **incoming** | MANUAL-LAB §B or §C |
| Watermark | `watermark_store.py` | (same) | bronze → **_control/watermark.json** | MANUAL-LAB §C2 |
| ADF linked service | `adf_pipeline.py` + Bicep | `orchestrate.cmd` | ADF Studio → **Manage** → linked services | MANUAL-LAB §D |
| Datasets + pipeline | `adf_pipeline.py` | (same) | ADF Studio → **Author** | MANUAL-LAB §E |
| Trigger run | MANUAL-LAB §F | `orchestrate.cmd --run-pipeline` OR Trigger now | ADF Studio → **Monitor** | MANUAL-LAB §F2 |
| Morning check | `morning_check.py` | `orchestrate.cmd` | Monitor vs terminal | MANUAL-LAB §H |
| Full checklist | — | — | All above | MANUAL-LAB §I |

**Optional:** Build pipeline by hand in portal — [MANUAL-LAB §G](session-2/MANUAL-LAB.md#g-manual-adf--build-copy-pipeline-by-hand-optional-30-min)

---

## 6. Session 3 — Databricks: read / run / portal map

**Prerequisite:** Session 1 complete (Databricks workspace in RG).  
**Command:** `session-3\orchestrate.cmd`

| Block | Read | Run | Portal / Databricks (verify) | Doc section |
|-------|------|-----|------------------------------|-------------|
| Find resources | MANUAL-LAB §A | — | RG → storage + Databricks | MANUAL-LAB §A verify |
| Bronze prep | `bronze_prep.py` | `orchestrate.cmd` | Storage → bronze → **loaded/run=session3-lab** | MANUAL-LAB §E |
| Workspace tour | SESSION3-STUDENT-GUIDE Block 1 | — | Launch Databricks → Compute | MANUAL-LAB §B–C |
| Read bronze | `nb_01_read_bronze.py` | Run all in UI | Notebook output 5 rows | MANUAL-LAB §E |
| Silver Delta | `nb_02_bronze_to_silver.py` | Run all | silver → **transactions/_delta_log** | MANUAL-LAB §F |
| Gold Delta | `nb_03_silver_to_gold.py` | Run all | gold → **daily_channel_summary** | MANUAL-LAB §G |
| Verify + cost | MANUAL-LAB §I | `orchestrate.cmd --verify-storage` | Terminate cluster | MANUAL-LAB §I |
| ADF optional | module-04-adf | — | ADF Notebook activity | databricks-course/04-01 |

---

## 6. Day 6 — Python: read / run / map

**Prerequisite:** Session 3 secrets recommended.  
**Command:** `day6\orchestrate.cmd`

| Block | Read | Run | Verify |
|-------|------|-----|--------|
| Local Python | README §A | `orchestrate.cmd` | `clean_amount` demo + tests OK |
| Databricks | SESSION6-STUDENT-GUIDE | Import `nb_01`, `nb_02` | Secrets scope `finledger` OK |

---

## 6b. Day 7 — Storage + PySpark read: read / run / map

**Prerequisite:** Day 6 + Session 3 bronze landed + `--setup-secrets`.  
**Command:** `day7\orchestrate.cmd`

| Block | Read | Run | Verify |
|-------|------|-----|--------|
| venv first | README §B Step 1 | `python -m venv .venv` + `pip install -r day7\requirements.txt` | pandas, polars, pyspark installed |
| Local script | MANUAL-LAB §A–B | `cd day7` → `orchestrate.cmd` | pandas/Polars rows printed |
| PySpark foundation | `nb_03_spark_concepts.py` | Databricks Run All | lazy vs action explained |
| Read bronze | `nb_04_read_bronze.py` | Databricks Run All | row count matches Session 3 |
| Full PySpark theory | [day8/PYSPARK-REFERENCE.md](day8/PYSPARK-REFERENCE.md) | Read sections 1–6 | Ready for Day 8 |

---

## 6c. Day 8 — PySpark transforms: read / run / map

**Prerequisite:** Day 7 complete.  
**Command:** `day8\orchestrate.cmd`

| Block | Read | Run | Verify |
|-------|------|-----|--------|
| Full theory | [PYSPARK-REFERENCE.md](day8/PYSPARK-REFERENCE.md) | Read §1–12 | Can explain lazy vs action |
| Local script | MANUAL-LAB §A–B | `orchestrate.cmd` | pandas/Polars + foundation + transforms |
| Foundation nb | `nb_00_pyspark_foundation.py` | Databricks | SQL groupBy works |
| Transforms | `nb_01` → `nb_04` | Databricks | 8 ops + window rank |

---

## 7. Commands quick reference

| Goal | Where | Command |
|------|-------|---------|
| First-time setup | repo root | `orchestrate.cmd --install-cli` |
| Full Class-1 + platforms | repo root | `orchestrate.cmd` |
| Class-1 only | repo root | `orchestrate.cmd --class1-only` |
| Re-run (safe) | repo root | `orchestrate.cmd` |
| Session 2 lab | `session-2\` | `orchestrate.cmd` |
| Session 2 + pipeline run | `session-2\` | `orchestrate.cmd --run-pipeline` |
| Session 3 lab | `session-3\` | `orchestrate.cmd` |
| Session 3 verify Delta | `session-3\` | `orchestrate.cmd --verify-storage` |
| Session 3 secrets | `session-3\` | `orchestrate.cmd --setup-secrets` |
| Day 6 lab | `day6\` | `orchestrate.cmd` |
| Day 7 lab (venv + read lake) | `day7\` | `orchestrate.cmd` |
| Day 7 skip Spark (no Java) | `day7\` | `orchestrate.cmd --skip-spark` |
| Day 8 lab (transforms) | `day8\` | `orchestrate.cmd` |
| Day 8 PySpark theory only | — | Read `day8/PYSPARK-REFERENCE.md` |
| Teardown | repo root | `orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes` |

---

## 8. Portal URLs (after deploy)

| What | How to open |
|------|-------------|
| Resource group | [portal.azure.com](https://portal.azure.com) → search `rg-<learner>-class1` |
| Cost analysis | Cost Management → filter resource group |
| Storage bronze | RG → storage → Containers → **bronze** |
| Data Factory | RG → ADF → **Open Azure Data Factory Studio** |
| Databricks | RG → Databricks workspace → **Launch workspace** |
| Purview | [web.purview.azure.com](https://web.purview.azure.com) |
| Fabric (MPN workaround) | [app.fabric.microsoft.com](https://app.fabric.microsoft.com) |

---

## 9. When things go wrong

| Situation | Go to |
|-----------|--------|
| Setup / login / deploy errors | [README §1 failures](README.md#failures--workarounds-short-guide) |
| Session 2 script errors | [session-2/README §G](session-2/README.md#g-failures--workarounds) |
| Session 2 portal errors | [session-2/MANUAL-LAB §K](session-2/MANUAL-LAB.md#k-troubleshooting-portal) |
| Session 3 Databricks errors | [session-3/MANUAL-LAB §K](session-3/MANUAL-LAB.md#lab-k) |
| Session 3 script errors | [session-3/README §G](session-3/README.md#g-failures--workarounds) |
| Day 7 JAVA not found | Install Java 11+ (adoptium.net) or `orchestrate.cmd --skip-spark` |
| Day 7/8 secrets error | `session-3\orchestrate.cmd --setup-secrets` |
| Day 8 PySpark concepts | [day8/PYSPARK-REFERENCE.md](day8/PYSPARK-REFERENCE.md) |
| MPN Synapse / Fabric blocked | [docs/GOVERNANCE-DEPLOY.md](docs/GOVERNANCE-DEPLOY.md) |
| Explain an error with AI | [README — Cursor / VS Code AI](README.md#when-to-use-cursor--vs-code-ai-chat) |

---

## 10. Trainer one-page flow

```text
Day A (Session 1):  README §1 → orchestrate.cmd → README §10 + WORKFLOW + CLASS-GUIDE
Day B (Session 2):  session-2/README → orchestrate.cmd → MANUAL-LAB (learners tick §I)
Day C (Session 3):  session-3/README → orchestrate.cmd → Databricks notebooks + MANUAL-LAB §I
Day D (Day 6):      day6/README → orchestrate.cmd → nb_01 + nb_02
Day E (Day 7):      day7/README → venv + orchestrate.cmd → nb_01→nb_04 (nb_03 = PySpark foundation)
Day F (Day 8):      day8/PYSPARK-REFERENCE → orchestrate.cmd → nb_00→nb_04
Wrap-up:            teardown demo → COVERAGE-MAP step 19
```

---

*Last aligned with: Session 1–3 + Day 6 + Day 7 + Day 8.*
