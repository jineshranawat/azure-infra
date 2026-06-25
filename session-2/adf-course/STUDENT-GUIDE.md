# FinLedger ADF — Student Guide (the one doc you open in class)

**This is the only file you need open during class.** Each class below tells you exactly what to **Open**, what to **Execute**, and what to **Show**, with the **theory** right beside it. Deeper click-by-click steps live in the linked lessons — open those only when you want every micro-click.

| | |
|---|---|
| **You are** | A data engineer at **FinLedger UK** building their lake ([CASE-STUDY.md](CASE-STUDY.md)) |
| **Region** | UK South only |
| **Rhythm** | **Theory (5 min) → Open → Execute → Show → tick the checkpoint** |
| **Trainer map** | [CLASS-PLAN.md](CLASS-PLAN.md) · timing & facilitation in [TRAINER-GUIDE.md](TRAINER-GUIDE.md) |
| **Look up a word** | [GLOSSARY.md](GLOSSARY.md) · **Verify everything** [docs/VERIFICATION-CHECKLIST.md](docs/VERIFICATION-CHECKLIST.md) |

---

## How to read every class block

Each class uses the same four moves so you always know what to do:

| Move | What it means |
|---|---|
| 🧠 **Theory** | The concept *before* you click — why this exists. |
| 📂 **Open** | The exact thing to open (portal blade / ADF Studio pane / lesson). |
| ▶️ **Execute** | The hands-on actions. Summary here; full clicks in the linked lesson. |
| 👀 **Show / See** | The visual "wow" + the checkpoint that proves it worked. |
| 📓 **Your notes** | Blank space — add screenshots, gotchas, your own steps. |

---

## Standing setup — open these every class

Keep these tabs open all course long:

| Tab | URL / path | Used for |
|---|---|---|
| Azure Portal | `https://portal.azure.com` | Create + browse resources |
| Your resource group | Portal → `rg-adf-course-<you>` | Everything lives here |
| Storage (the lake) | RG → storage account → **Containers** → bronze/silver/gold | See files land |
| ADF Studio | RG → Data factory → **Open Azure Data Factory Studio** | Author / Manage / Monitor |
| This guide | `adf-course/STUDENT-GUIDE.md` | What to do next |

> First time? Do [SETUP.md](SETUP.md) once (subscription, naming, region), then Class 1.

---

# Class 1 — Your first ingest (on-ramp / Session 2)

🧠 **Theory.** Data pipelines move data from where it's *produced* (files, apps) to where it's *analysed* (a lake). The pattern is **land → copy → track**: drop a raw file (bronze), copy it under control of a pipeline, and record a **watermark** so next time you only take what's new. ADF is the orchestrator that runs this on a schedule.

📂 **Open.** [`../SESSION2-STUDENT-GUIDE.md`](../SESSION2-STUDENT-GUIDE.md) · Storage **bronze** container · ADF Studio **Monitor**.

▶️ **Execute.**
1. Upload `sample_transactions.csv` to `bronze/incoming/`.
2. Trigger pipeline `pl_bronze_copy` (Studio → **Author** → pipeline → **Add trigger → Trigger now**).
3. Watch it in **Monitor**.

👀 **Show / See.** The **Monitor gantt** turns green; the file appears in `bronze/loaded/`; `_control/watermark.json` updates.
✅ *Checkpoint:* you can say *land → copy → watermark* in one breath.

📓 **Your notes:**

---

# Class 2 — Build the lake & the factory from scratch

🧠 **Theory.** Before any pipeline, you need **storage** and a **factory**. **ADLS Gen2** = Blob storage + a *hierarchical namespace* (real folders), which analytics tools expect. **Azure Data Factory** is the managed service that authors and runs pipelines. They connect through a **linked service** (a saved connection) authenticated by a **managed identity** (a passwordless cloud identity for your factory).

📂 **Open.** Lessons in order:
[00-00 mental model](module-00-foundations/00-00-overview.md) → [00-01 storage](module-00-foundations/00-01-create-storage-adls-gen2.md) → [00-02 factory](module-00-foundations/00-02-create-data-factory.md) → [00-03 Studio tour](module-00-foundations/00-03-studio-tour-every-pane.md) → [00-04 linked services & IR](module-00-foundations/00-04-linked-services-and-integration-runtime.md) → [00-05 link ADF to storage](module-00-foundations/00-05-link-adf-to-storage-step-by-step.md).

▶️ **Execute.** Create `rg-adf-course-<you>` → ADLS Gen2 account with **HNS on** → containers `bronze`, `silver`, `gold` → Data Factory (V2, UK South) → linked service to storage using **managed identity** → **Test connection**.

👀 **Show / See.** Browsable HNS **folders**; the three **Studio panes** (Author ✏️ / Manage 🧰 / Monitor 📈); a green **Connection successful**.
✅ *Checkpoint:* bronze/silver/gold exist; linked service tests green.

📓 **Your notes:**

---

# Class 3 — Copy data two ways (wizard + by hand)

🧠 **Theory.** The **Copy activity** is ADF's workhorse: read from a **source** dataset, write to a **sink** dataset, both defined by **linked service + dataset + (optional) parameters**. The **Copy Data tool** is a wizard that builds all of this for you; building it by hand teaches what the wizard hid. **Parameters** let one pipeline copy *any* file instead of a hard-coded one.

📂 **Open.** [01-01 Copy Data wizard](module-01-copy-ingest/01-01-copy-data-tool.md) · [01-02 Copy activity by hand](module-01-copy-ingest/01-02-copy-activity-manual-pipeline.md) · [01-03 datasets & parameters](module-01-copy-ingest/01-03-datasets-linked-services-parameters.md).

▶️ **Execute.** Run the wizard to copy `transactions_daily.csv` → `bronze/loaded/`. Then build the same copy manually on the pipeline canvas. Finally, parametrise the dataset path.

👀 **Show / See.** The wizard screens; the **pipeline canvas** with a Copy activity; a **Debug** run; Monitor green.
✅ *Checkpoint:* one parametrised pipeline can copy any folder you pass it.

📓 **Your notes:**

---

# Class 4 — Ingest like the real world (errors, hybrid, incremental)

🧠 **Theory.** Production ingest must handle failure and growth. **Error handling** routes failures down a separate path (success/failure/completion dependencies) and **retries** transient errors. A **self-hosted IR** reaches data behind a firewall (on-prem). **Incremental copy** uses a **watermark** (last value seen) so you re-load only new rows — cheaper and faster than full reloads. **CDC / change tracking** is the database-native version of that idea.

📂 **Open.** [01-04 self-hosted IR](module-01-copy-ingest/01-04-on-prem-to-cloud-self-hosted-ir.md) (demo) · [01-05 error handling](module-01-copy-ingest/01-05-conditional-execution-error-handling.md) · [01-06 S3 → ADLS](module-01-copy-ingest/01-06-amazon-s3-to-adls-gen2.md) · [01-07 watermark](module-01-copy-ingest/01-07-incremental-copy-patterns.md) · [01-08 CDC intro](module-01-copy-ingest/01-08-change-tracking-multiple-tables-cdc.md).

▶️ **Execute.** Add an **If** + failure branch; inject a bad path and watch it fail safely. Copy a partner feed (S3). Run a watermark pipeline twice and confirm the second run takes only new rows.

👀 **Show / See.** The **red failure branch**; the **retry** counter; `_control/watermark.json` advancing.
✅ *Checkpoint:* second incremental run copies *only* new rows.

📓 **Your notes:**

---

# Class 5 — Data flows I: the visual transformation studio

🧠 **Theory.** A **mapping data flow** is code-free Spark: you draw **source → transformations → sink**, and ADF compiles it to a Spark job on a managed cluster. **Debug mode** spins up a small cluster so you can preview real data at every step. This is where raw **bronze** becomes clean **silver**.

📂 **Open.** [02-01 fundamentals & debug canvas](module-02-data-flows/02-01-data-flow-fundamentals-debug-canvas.md) · [02-02 source→transform→sink](module-02-data-flows/02-02-code-free-transformation-at-scale.md).

▶️ **Execute.** Turn on **Data flow debug**. Build Source (bronze transactions) → **Filter** (posted only) → **Select** (tidy columns) → **Sink** (silver). Preview data at each node.

👀 **Show / See.** The **data flow canvas**; **Data Preview** showing rows change shape live; the debug cluster status.
✅ *Checkpoint:* posted transactions land in `silver/transactions/` (pending row filtered).
💷 **Cost:** turn **Debug off** at the end — data flows bill per vCore-hour.

📓 **Your notes:**

---

# Class 6 — Data flows II: expressions, Delta, partitioning

🧠 **Theory.** **Derived columns** compute new fields with the expression language; **schema drift** lets flows tolerate changing columns. **Delta Lake** adds ACID transactions + time travel (a `_delta_log` folder). **Partitioning** writes data into folders (e.g. by date) so downstream queries scan less.

📂 **Open.** [02-03 expressions & drift](module-02-data-flows/02-03-expressions-schema-drift-derived-columns.md) · [02-04 Delta](module-02-data-flows/02-04-delta-lake-transformations.md) · [02-05 Power Query](module-02-data-flows/02-05-power-query-wrangling-data-flow.md) · [02-06 partitioning](module-02-data-flows/02-06-writing-to-lake-partitioning-best-practices.md).

▶️ **Execute.** Add a **Derived Column** with a live-previewed expression; write a **Delta** sink; partition output by `value_date`.

👀 **Show / See.** The **expression builder** with autocomplete + preview; the `_delta_log` folder; multiple partition folders in Storage.
✅ *Checkpoint:* `_delta_log` exists; several `value_date` folders.

📓 **Your notes:**

---

# Class 7 — Orchestration: pipelines that drive themselves

🧠 **Theory.** **Control flow** activities make pipelines smart: **Lookup** reads config, **ForEach** loops over a list (in parallel), **If/Switch** branch, **Until** repeats. **Dynamic content** (`@pipeline().parameters...`, `@item()`) lets a single pipeline configure itself per file — the key to scaling from 1 file to 1,000.

📂 **Open.** [03-01 activities catalogue](module-03-control-flow-orchestration/03-01-pipeline-activities-catalogue.md) · [03-02 ForEach/If/Until/Switch](module-03-control-flow-orchestration/03-02-control-flow-foreach-if-until-switch.md) · [03-03 dynamic content](module-03-control-flow-orchestration/03-03-parameters-variables-expressions-dynamic-content.md).

▶️ **Execute.** **Lookup** a `file_manifest.json` → **ForEach** the files → **If** `enabled == true` → Copy. Drive the copy path with **dynamic content** `@item()`.

👀 **Show / See.** The **ForEach** fanning into parallel iterations in Monitor; the **dynamic-content** expression builder.
✅ *Checkpoint:* only `enabled:true` files get copied.

📓 **Your notes:**

---

# Class 8 — Schedules & monitoring (it runs at 2 a.m.)

🧠 **Theory.** A **trigger** starts pipelines without you: **schedule** (wall clock), **tumbling window** (fixed, replayable slices), **event** (a file lands), **custom**. **Monitoring** shows run history; alerts/Logic Apps notify humans when something breaks.

📂 **Open.** [03-04 triggers](module-03-control-flow-orchestration/03-04-triggers-schedule-tumbling-event-custom.md) · [03-05 monitoring & email](module-03-control-flow-orchestration/03-05-monitoring-email-notifications.md).

▶️ **Execute.** Attach a **schedule trigger** to your loader. Configure an **alert/Logic App** email on failure. Force a failure and confirm the email.

👀 **Show / See.** The trigger's **next run time**; the **Monitor gantt** over time; a real **email** arriving.
✅ *Checkpoint:* trigger shows next run; test alert received.

📓 **Your notes:**

---

# Class 9 — External compute: Spark / Databricks from ADF

🧠 **Theory.** When transformation needs full Spark or ML, ADF *orchestrates* external compute instead of doing it itself: the **Databricks Notebook activity** runs your notebook on a Databricks cluster and returns control to the pipeline. ADF stays the conductor; Databricks/HDInsight are the engines.

📂 **Open.** [04-01 Databricks notebook](module-04-external-compute/04-01-databricks-notebook-activity.md) · [04-02 HDInsight Spark](module-04-external-compute/04-02-hdinsight-spark-transformation.md) (demo) · [04-03 Hive](module-04-external-compute/04-03-hive-transformation.md) (demo).

▶️ **Execute.** Link a Databricks workspace; add a **Notebook activity** that scores high-value accounts; write output to `gold/`.

👀 **Show / See.** The notebook activity launching the Databricks run; output landing in `gold/`.
✅ *Checkpoint:* notebook output appears in `gold/`.
💷 **Cost:** smallest clusters; **delete clusters** after class. (MPN quota 0 → portal-only demo.)

📓 **Your notes:**

---

# Class 10 — Security & networking (no public exposure)

🧠 **Theory.** Secrets never belong in pipeline JSON. **Key Vault** stores them; a linked service references them by name. A **managed VNet** + **managed private endpoints** keep traffic off the public internet, and **RBAC** grants the factory's managed identity least-privilege access.

📂 **Open.** [05-01 managed VNet concepts](module-05-networking-security/05-01-managed-vnet-private-endpoints-concepts.md) · [05-02 copy in managed VNet](module-05-networking-security/05-02-copy-pipeline-managed-vnet.md) · [05-03 private endpoint to SQL](module-05-networking-security/05-03-on-prem-sql-managed-private-endpoint.md) · [05-04 Key Vault & RBAC](module-05-networking-security/05-04-key-vault-managed-identity-rbac.md).

▶️ **Execute.** Store a secret in **Key Vault**; create a Key Vault-backed linked service; enable managed VNet; approve a **private endpoint**.

👀 **Show / See.** The **secret reference** (no plaintext), the managed VNet toggle, the private endpoint **Approved** state.
✅ *Checkpoint:* a working pipeline with **zero secrets** in its JSON.

📓 **Your notes:**

---

# Class 11 — Ship it: Git, CI/CD, governance, cost

🧠 **Theory.** Real factories are **version-controlled**: Git mode saves every change; **ARM templates** promote dev → test → prod; **Purview** captures **lineage** (what flows where); **budgets/alerts** keep spend safe. This is how a lab becomes production.

📂 **Open.** [06-01 Git](module-06-governance-cicd-ops/06-01-git-integration-azure-devops-github.md) · [06-02 ARM CI/CD](module-06-governance-cicd-ops/06-02-arm-templates-cicd-dev-test-prod.md) · [06-03 Purview lineage](module-06-governance-cicd-ops/06-03-push-lineage-to-purview.md) · [06-04 SSIS IR](module-06-governance-cicd-ops/06-04-ssis-integration-runtime.md) (demo) · [06-05 cost & alerts](module-06-governance-cicd-ops/06-05-cost-monitoring-alerts-runbook.md).

▶️ **Execute.** Connect ADF to Git and publish; export the **ARM template**; push **lineage** to Purview; set a **budget alert**.

👀 **Show / See.** The Git branch/commit; the ARM export; the **Purview lineage graph** of everything you built; the **cost chart**.
✅ *Checkpoint:* commit visible in Git; lineage graph renders; budget alert set.

📓 **Your notes:**

---

# Class 12 — Capstone: the nightly FinLedger pipeline

🧠 **Theory.** Everything you learned composes into one **master pipeline**: ingest (Copy) → cleanse (data flow) → orchestrate (control flow) → score (Databricks) → publish (gold), on a **trigger**, version-controlled, monitored. That is a production data platform.

📂 **Open.** [CAPSTONE.md](CAPSTONE.md).

▶️ **Execute.** Assemble the end-to-end pipeline and run it on a schedule, unattended.

👀 **Show / See.** One **Monitor** view of the whole estate running green, end to end; populated `gold/`.
✅ *Checkpoint:* unattended end-to-end run **Succeeded**.

📓 **Your notes:**

---

## Quick reference (all classes)

| Need | Open |
|---|---|
| Business story | [CASE-STUDY.md](CASE-STUDY.md) |
| Every term defined | [GLOSSARY.md](GLOSSARY.md) |
| Master tick-list | [docs/VERIFICATION-CHECKLIST.md](docs/VERIFICATION-CHECKLIST.md) |
| Sample data to upload | [data/README.md](data/README.md) |
| Class 1 portal micro-steps | [`../MANUAL-LAB.md`](../MANUAL-LAB.md) |
| Full lesson index | [README.md](README.md) |
| Trainer plan & timing | [CLASS-PLAN.md](CLASS-PLAN.md) · [TRAINER-GUIDE.md](TRAINER-GUIDE.md) |

## Make this guide yours

This file is meant to grow. Add under any class's **📓 Your notes**:
- Screenshots of your own runs.
- Errors you hit + the fix (so the next cohort skips the pain).
- Extra challenges ("now copy a second file type", "add a Switch on region").

Commit your edits so the whole class shares one living guide.
