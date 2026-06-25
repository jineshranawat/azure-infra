# FinLedger ADF — Multi-Class Delivery Plan (20+ hours, hands-on)

**Goal:** Run Azure Data Factory as a *series of enjoyable, do-it-in-the-portal classes* — not one 2-hour session. Every class has a **look-and-feel "wow" moment**, a thing students **build**, and screens they **watch happen live**.

| | |
|---|---|
| **Format** | 12 classes × ~2 hours = **~24 hours** (drop/extend any class) |
| **Style** | Portal-first. Students click in Azure; scripts are backstage only. |
| **Story** | One company — **FinLedger UK** — built lake-to-gold across all classes ([CASE-STUDY.md](CASE-STUDY.md)) |
| **Per class** | Read (5 min) → **Do in Azure** (the bulk) → See it run → Checkpoint |
| **Student handout** | [STUDENT-GUIDE.md](STUDENT-GUIDE.md) — the single doc students open (Open / Execute / Show + theory) |
| **Trainer notes** | [TRAINER-GUIDE.md](TRAINER-GUIDE.md) · timings, blockers, rubric |

> **Golden rule for every class:** open the portal in the first 5 minutes. Students should be *clicking and watching Azure react* for 80% of the time. Concept slides are short; the screen is the star.

---

## The "look and feel" moments (why students enjoy it)

These are the visual highlights — schedule classes so each one lands at least one:

| Moment | Class | Why it lands |
|---|---|---|
| First time inside **ADF Studio** (Author / Manage / Monitor) | 2 | "So this is the cockpit." |
| **Copy Data wizard** auto-builds a pipeline | 3 | Drag-free pipeline appears on canvas. |
| **Monitor gantt** turns green as a run finishes | 3, 8, 12 | Live proof it worked. |
| **Data flow canvas + live Data Preview** | 5 | Watch rows transform step-by-step. |
| **Debug cluster** spins up and data flows through | 5, 6 | Spark power, no code. |
| **ForEach** fans one pipeline into many parallel copies | 7 | "One pipeline did 10 files." |
| **Dynamic content / expression builder** | 7 | Pipelines that configure themselves. |
| **Trigger** shows the next scheduled run time | 8 | "It runs itself at 2 a.m." |
| **Purview lineage graph** of the whole pipeline | 11 | The map of everything you built. |
| **Capstone** end-to-end run in one Monitor view | 12 | Victory lap. |

---

## Class-by-class plan

Each class below = one 2-hour session. Lessons referenced are in this `adf-course/`. Where a lesson is currently a **stub** (overview only), the plan says **trainer-demo**; see [Lesson depth status](#lesson-depth-status) and deepen before teaching.

---

### Class 1 — On-ramp: your first ingest (Session 2)
**This is the existing 2-hour Session 2.** Use it as the gateway.

- **Wow moment:** first time in ADF Studio + a green run in **Monitor**.
- **You build:** a file landed in `bronze`, a watermark, a triggered `pl_bronze_copy` run.
- **You see:** Storage containers, ADF Studio panes, Monitor gantt going green.
- **Run it:** [`../SESSION2-STUDENT-GUIDE.md`](../SESSION2-STUDENT-GUIDE.md) (Blocks 0–5).
- **Checkpoint:** student explains *land → copy → watermark* in one sentence.
- **Depth:** ✅ full click-by-click.

> **Bridge to Class 2:** "Session 2 used a pre-built factory. Now you'll build the whole lake yourself, from an empty resource group."

---

### Class 2 — Build the lake & the factory from scratch
- **Wow moment:** create real **folders** in a data lake (HNS) and open the empty **ADF Studio** cockpit for the first time.
- **You build:** resource group → ADLS Gen2 (bronze/silver/gold) → Data Factory → linked service with managed identity.
- **You see:** hierarchical-namespace folders you can browse; Studio Author/Manage/Monitor; a green **Test connection**.
- **Do in Azure (lessons in order):**
  - [00-00 Overview & mental model](module-00-foundations/00-00-overview.md) (15 min, whiteboard)
  - [00-01 Create ADLS Gen2 storage](module-00-foundations/00-01-create-storage-adls-gen2.md) (30 min)
  - [00-02 Create the Data Factory](module-00-foundations/00-02-create-data-factory.md) (20 min)
  - [00-03 Studio tour — every pane](module-00-foundations/00-03-studio-tour-every-pane.md) (30 min)
  - [00-04 Linked services & IR concepts](module-00-foundations/00-04-linked-services-and-integration-runtime.md) (25 min)
  - [00-05 Link ADF to storage, click-by-click](module-00-foundations/00-05-link-adf-to-storage-step-by-step.md) (30 min)
- **Checkpoint:** bronze/silver/gold visible; linked service **Connection successful**.
- **Depth:** ✅ full click-by-click.

---

### Class 3 — Copy data two ways (wizard + by hand)
- **Wow moment:** the **Copy Data wizard** builds a working pipeline for you; then you build the same thing by dragging a **Copy activity** onto the canvas.
- **You build:** `transactions_daily.csv` → `bronze/loaded/`; a manual pipeline; parametrised datasets.
- **You see:** wizard screens, the pipeline canvas, **Debug** run, Monitor turning green.
- **Do in Azure:**
  - [01-01 Copy Data tool (wizard)](module-01-copy-ingest/01-01-copy-data-tool.md) (35 min)
  - [01-02 Copy activity built by hand](module-01-copy-ingest/01-02-copy-activity-manual-pipeline.md) (35 min)
  - [01-03 Datasets, linked services, parameters](module-01-copy-ingest/01-03-datasets-linked-services-parameters.md) (30 min)
- **Checkpoint:** file in `bronze/loaded/transactions/`; one parametrised pipeline copies any folder.
- **Depth:** ✅ 01-01/01-02 full; 01-03 solid.

---

### Class 4 — Ingest like the real world (hybrid, errors, incremental)
- **Wow moment:** inject a bad path and watch the pipeline take a **red failure branch**, then recover; watch a **watermark** advance.
- **You build:** error-handling pipeline; S3 → ADLS copy; watermark incremental load.
- **You see:** If-condition branches, retry policy, `_control/watermark.json` updating.
- **Do in Azure:**
  - [01-04 On-prem → cloud with self-hosted IR](module-01-copy-ingest/01-04-on-prem-to-cloud-self-hosted-ir.md) — *trainer-demo* (needs a VM)
  - [01-05 Conditional execution & error handling](module-01-copy-ingest/01-05-conditional-execution-error-handling.md) (30 min)
  - [01-06 Amazon S3 → ADLS Gen2](module-01-copy-ingest/01-06-amazon-s3-to-adls-gen2.md) — *deepen* (currently short)
  - [01-07 Incremental copy patterns (watermark)](module-01-copy-ingest/01-07-incremental-copy-patterns.md) — *deepen*
  - [01-08 Change tracking, CDC intro](module-01-copy-ingest/01-08-change-tracking-multiple-tables-cdc.md) — *deepen*
- **Checkpoint:** failure path triggers cleanly; second incremental run copies only new rows.
- **Depth:** ⚠️ 01-06/07/08 are short — deepen or run as guided demo.

---

### Class 5 — Data flows I: the visual transformation studio *(the big "wow")*
- **Wow moment:** the **mapping data flow canvas** + **Data Preview** — watch rows change shape as they pass through Filter → Select → Sink, no code.
- **You build:** clean FinLedger transactions into `silver/` with a code-free data flow.
- **You see:** debug cluster starting, drag-to-add transformations, live row preview at each step.
- **Do in Azure:**
  - [02-01 Data flow fundamentals, debug, canvas](module-02-data-flows/02-01-data-flow-fundamentals-debug-canvas.md) (30 min) — *deepen*
  - [02-02 Code-free transformation at scale](module-02-data-flows/02-02-code-free-transformation-at-scale.md) (40 min) ✅ full
- **Checkpoint:** 10 posted transactions land in `silver/transactions/` (the pending row filtered out).
- **Cost:** ⚠️ data flows bill per vCore-hour — **turn off Debug** at the end.
- **Depth:** ✅ 02-02 full; ⚠️ 02-01 deepen.

---

### Class 6 — Data flows II: expressions, Delta, partitioning
- **Wow moment:** the **expression builder** with live preview; **Delta** `_delta_log` appearing; partition folders by date.
- **You build:** derived columns, schema-drift handling, a Delta sink, partitioned output.
- **You see:** expression editor autocompletion, Power Query grid, partition folders in Storage.
- **Do in Azure:**
  - [02-03 Expressions, schema drift, derived columns](module-02-data-flows/02-03-expressions-schema-drift-derived-columns.md) — *deepen*
  - [02-04 Delta Lake transformations](module-02-data-flows/02-04-delta-lake-transformations.md) — *deepen*
  - [02-05 Power Query (wrangling) data flow](module-02-data-flows/02-05-power-query-wrangling-data-flow.md) — *deepen*
  - [02-06 Writing to the lake — partitioning](module-02-data-flows/02-06-writing-to-lake-partitioning-best-practices.md) — *deepen*
- **Checkpoint:** `_delta_log` exists; multiple `value_date` partition folders.
- **Depth:** ⚠️ all four are short — deepen before teaching hands-on.

---

### Class 7 — Orchestration: pipelines that drive themselves
- **Wow moment:** **ForEach** fans one pipeline into many parallel copies from a manifest; **dynamic content** makes paths build themselves.
- **You build:** a manifest-driven loader that copies only `enabled:true` files.
- **You see:** ForEach iteration list, dynamic-content expression builder, parallel activity runs in Monitor.
- **Do in Azure:**
  - [03-01 Pipeline activities catalogue](module-03-control-flow-orchestration/03-01-pipeline-activities-catalogue.md) — *deepen*
  - [03-02 ForEach, If, Until, Switch](module-03-control-flow-orchestration/03-02-control-flow-foreach-if-until-switch.md) (40 min)
  - [03-03 Parameters, variables, dynamic content](module-03-control-flow-orchestration/03-03-parameters-variables-expressions-dynamic-content.md) — *deepen*
- **Checkpoint:** ForEach copies exactly the enabled files from `file_manifest.json`.
- **Depth:** ✅ 03-02 decent; ⚠️ 03-01/03 deepen.

---

### Class 8 — Schedules & monitoring (it runs at 2 a.m.)
- **Wow moment:** a **trigger** shows its next run time; a failed run fires a **real email**.
- **You build:** a schedule/tumbling-window trigger; an email alert via Logic App / alert rule.
- **You see:** trigger config, Monitor gantt over time, an email landing in your inbox.
- **Do in Azure:**
  - [03-04 Triggers: schedule, tumbling, event, custom](module-03-control-flow-orchestration/03-04-triggers-schedule-tumbling-event-custom.md) — *deepen*
  - [03-05 Monitoring + email notifications](module-03-control-flow-orchestration/03-05-monitoring-email-notifications.md) — *deepen*
- **Checkpoint:** trigger shows next run; test alert received.
- **Depth:** ⚠️ deepen both.

---

### Class 9 — External compute: Spark/Databricks from ADF
- **Wow moment:** ADF launches a **Databricks notebook** and you watch the job run from the pipeline.
- **You build:** a Databricks Notebook activity scoring high-value accounts into `gold/`.
- **You see:** notebook activity settings, the linked Databricks run, output in `gold/`.
- **Do in Azure:**
  - [04-01 Databricks notebook activity](module-04-external-compute/04-01-databricks-notebook-activity.md) — *deepen*
  - [04-02 HDInsight Spark](module-04-external-compute/04-02-hdinsight-spark-transformation.md) — *trainer-demo* (cost)
  - [04-03 Hive transformation](module-04-external-compute/04-03-hive-transformation.md) — *trainer-demo* (cost)
- **Checkpoint:** notebook output appears in `gold/`.
- **Cost:** ⚠️ highest-cost module — smallest clusters; **delete clusters** after. MPN quota 0 → portal-only demo.
- **Depth:** ⚠️ deepen 04-01; 04-02/03 demo.

---

### Class 10 — Security & networking (no public exposure)
- **Wow moment:** a pipeline reads a secret from **Key Vault** (no password in JSON); a **managed private endpoint** approval.
- **You build:** Key Vault linked service; copy inside a managed VNet; private endpoint to SQL.
- **You see:** Key Vault secret reference, managed VNet toggle, private endpoint approval state.
- **Do in Azure:**
  - [05-01 Managed VNet & private endpoints concepts](module-05-networking-security/05-01-managed-vnet-private-endpoints-concepts.md) — *deepen*
  - [05-02 Copy pipeline inside a managed VNet](module-05-networking-security/05-02-copy-pipeline-managed-vnet.md) — *deepen*
  - [05-03 On-prem SQL via managed private endpoint](module-05-networking-security/05-03-on-prem-sql-managed-private-endpoint.md) — *deepen*
  - [05-04 Key Vault, managed identity, RBAC](module-05-networking-security/05-04-key-vault-managed-identity-rbac.md) — *deepen*
- **Checkpoint:** pipeline runs with **zero secrets** in its JSON.
- **Depth:** ⚠️ deepen all four.

---

### Class 11 — Ship it: Git, CI/CD, governance, cost
- **Wow moment:** **publish to Git** and see the branch/commit; the **Purview lineage graph** of everything you built; the **cost chart**.
- **You build:** Git integration, ARM export for dev→prod, Purview lineage push, a budget alert.
- **You see:** Git mode in Studio, ARM template export, lineage graph, Cost Management chart.
- **Do in Azure:**
  - [06-01 Git integration (DevOps / GitHub)](module-06-governance-cicd-ops/06-01-git-integration-azure-devops-github.md) — *deepen*
  - [06-02 ARM templates & CI/CD](module-06-governance-cicd-ops/06-02-arm-templates-cicd-dev-test-prod.md) — *deepen*
  - [06-03 Push lineage to Purview](module-06-governance-cicd-ops/06-03-push-lineage-to-purview.md) — *deepen*
  - [06-04 SSIS IR (lift & shift)](module-06-governance-cicd-ops/06-04-ssis-integration-runtime.md) — *trainer-demo* (cost)
  - [06-05 Cost monitoring, alerts & runbook](module-06-governance-cicd-ops/06-05-cost-monitoring-alerts-runbook.md) — *deepen*
- **Checkpoint:** commit visible in Git; lineage graph renders; budget alert configured.
- **Depth:** ⚠️ deepen most; 06-04 demo.

---

### Class 12 — Capstone: the nightly FinLedger pipeline
- **Wow moment:** one **Monitor** view shows the whole estate run end-to-end — ingest → data flow → orchestration → gold.
- **You build:** a master pipeline that ties every module together, on a schedule.
- **You see:** the full pipeline graph and a single green end-to-end run.
- **Do in Azure:** [CAPSTONE.md](CAPSTONE.md) (open-ended build, ~40 min reading + build time).
- **Checkpoint:** unattended end-to-end run **Succeeded**; gold output populated.
- **Depth:** ✅ scaffold present; build is open-ended.

---

## Suggested calendars (mix & match)

| Cadence | Plan |
|---|---|
| **Weekly evening (2 h)** | One class per week → 12 weeks. |
| **Bootcamp (full days)** | Day 1: Classes 1–4 · Day 2: 5–8 · Day 3: 9–12. |
| **Half-day workshops** | Classes 1–3, then 4–6, then 7–9, then 10–12. |
| **Fast track (data engineers)** | 2 → 3 → 5 → 7 → 8 → 12 (skip hybrid/SSIS/HDInsight). |

Each class ends with the next class's **bridge line** so the story carries over.

---

## Lesson depth status

So you know exactly where the click-by-click steps already exist vs. where to deepen before teaching hands-on:

| Depth | Lessons | Action |
|---|---|---|
| ✅ **Full click-by-click** | 00-00 … 00-05, 01-01, 01-02, 02-02, 03-02 | Teach as-is |
| 🟡 **Solid, light on screens** | 01-03, 02-01 | Add a few "→ you'll see" lines |
| 🔴 **Stub (overview only) — deepen** | 01-06, 01-07, 01-08, 02-03, 02-04, 02-05, 02-06, 03-01, 03-03, 03-04, 03-05, 04-01, 05-01…05-04, 06-01…06-05 | Expand to Part A click-by-click before hands-on; until then run as **trainer-demo** |
| 🎬 **Trainer-demo (cost/infra)** | 01-04, 04-02, 04-03, 06-04 | Screen-share; no learner cost |

> **Want the deep versions?** Ask and I'll expand the 🔴 lessons to the same click-by-click depth as Module 0/1 — one module at a time so each stays high quality. Recommended order to deepen: **Module 2 → 3 → 4 → 5 → 6** (that matches Classes 5–11).

---

## How students stay hands-on (the experience contract)

For every class, the trainer guarantees:

1. **Portal open in 5 minutes** — concept first, but short.
2. **Students drive** — they click; the trainer narrates and circulates.
3. **One visible win** — a green run, a preview, a graph — before the break.
4. **Verify, don't assume** — tick the lesson's Part D checklist + [VERIFICATION-CHECKLIST.md](docs/VERIFICATION-CHECKLIST.md).
5. **Cost safety** — tear down data flows / clusters at class end (Classes 5, 6, 9).

---

## Related docs

| Doc | Purpose |
|---|---|
| [README.md](README.md) | Full lesson index + per-lesson anatomy |
| [TRAINER-GUIDE.md](TRAINER-GUIDE.md) | Facilitation, blockers, rubric |
| [CASE-STUDY.md](CASE-STUDY.md) | FinLedger UK business story |
| [SETUP.md](SETUP.md) | Subscription, region, naming guardrails |
| [../SESSION2-STUDENT-GUIDE.md](../SESSION2-STUDENT-GUIDE.md) | Class 1 (on-ramp) handout |
