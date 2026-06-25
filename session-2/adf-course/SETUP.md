# ADF Course — One-Time Setup

> Read this once before [Module 0](module-00-foundations/00-00-overview.md). Every lesson assumes the same resource names defined here.

## What you need before lesson 00-00

| Requirement | Why |
|---|---|
| Azure subscription (free trial or pay-as-you-go) | Hosts all course resources |
| Owner or Contributor on the subscription | Create resource groups, assign RBAC |
| Windows 10/11 PC with a browser | ADF Studio is browser-based; code examples target Windows |
| SQL literacy + basic Python | Course does not teach SQL or Python from zero |
| ~2 hours uninterrupted for Module 0 | Storage + factory + first linked service |

> ℹ️ NOTE: If you already completed Class-1 in this repo (`orchestrate.cmd` at repo root), you may **reuse** `rg-{learner}-class1`, the existing storage account, and Data Factory. Map the course names below to your deployed resources — the lessons still apply; only the literal names in JSON/code blocks change.

---

## Canonical resource naming (never deviate)

All lessons, JSON artifacts, and Python samples use this convention. Replace `{learner}` with your 2–10 character lowercase ID (e.g. `jinesh`).

| Resource | Name pattern | Example |
|---|---|---|
| Resource group | `rg-adf-course-{learner}` | `rg-adf-course-jinesh` |
| Region (primary) | `uksouth` | UK South only |
| Region (failover, if used) | `ukwest` | UK West only |
| ADLS Gen2 storage account | `stadfcourse{learner}` | `stadfcoursejinesh` (3–24 chars, lowercase, globally unique) |
| Data Factory | `df-adf-course-{learner}` | `df-adf-course-jinesh` |
| Key Vault (Module 5+) | `kv-adf-course-{learner}` | `kv-adf-course-jinesh` |

### ADF artifact prefixes

| Prefix | Artifact | Example |
|---|---|---|
| `ls_` | Linked service | `ls_adls_main` |
| `ds_` | Dataset | `ds_bronze_incoming_csv` |
| `pl_` | Pipeline | `pl_bronze_copy` |
| `df_` | Mapping data flow | `df_clean_transactions` |
| `tr_` | Trigger | `tr_daily_6am` |
| `ir_` | Integration runtime (if named) | `ir_selfhosted_win` |

### Storage layout (containers / paths)

| Path | Purpose |
|---|---|
| `bronze/incoming/` | Raw landing zone |
| `bronze/loaded/` | Post-copy confirmation |
| `bronze/_control/watermark.json` | Incremental load watermark |
| `silver/` | Cleansed / conformed (data flows) |
| `gold/` | Business-ready outputs |

---

## Subscription & account setup (click-by-click summary)

Full click-level steps appear in lesson **00-01** and **00-02**. At a high level:

1. Sign in at [https://portal.azure.com](https://portal.azure.com) with an account that has subscription access.
2. Confirm your subscription is active: search **Subscriptions** → select yours → note the **Subscription ID** (GUID).
3. Set a **cost budget** (recommended £25/month alert): **Cost Management + Billing** → **Budgets** → **Add**. Lessons that spin up billable compute include tear-down blocks.
4. Record these values in a local notes file (not committed to git):

```text
LEARNER=jinesh
AZURE_SUBSCRIPTION_ID=<your-guid>
LOCATION=uksouth
OWNER_EMAIL=you@example.com
```

> ⚠️ WARNING: Never commit subscription IDs, keys, or connection strings to git. Course JSON uses managed identity where possible.

---

## Region & cost guardrails

| Rule | Value |
|---|---|
| Primary region | `uksouth` |
| Failover region | `ukwest` |
| Non-UK regions | Only where a service is unavailable in UK — each exception is called out in the lesson |
| SKUs | Cheapest viable tier (Standard LRS storage, serverless/public ADF IR unless lesson requires otherwise) |
| Billable lessons | Data flows (vCore clusters), Databricks, HDInsight, SSIS IR — each lesson ends with **Cost & tear-down** |

**Expected steady-state cost (Module 0–1 only):** pennies per pipeline run (activity runs + minimal storage). Mapping data flows and external compute modules can cost pounds per hour if left running.

---

## Optional: align with this repo's Class-1 lab

If you are taking this course inside the Stalwart Learning repo:

| Course name | Class-1 equivalent |
|---|---|
| `rg-adf-course-{learner}` | `rg-{learner}-class1` |
| `stadfcourse{learner}` | from `.env` / Bicep output |
| `df-adf-course-{learner}` | from `platform-services.bicep` |

From repo root (already deployed):

```text
orchestrate.cmd
```

From `session-2` (bronze loader + pipeline SDK):

```text
cd session-2
orchestrate.cmd
```

The 20-hour course is **self-contained** in `adf-course/`; the repo scripts are a code-first parallel track, not a prerequisite.

---

## Tools to install (for Part C code sections)

| Tool | Version | Install |
|---|---|---|
| Python | 3.10+ | [python.org](https://www.python.org/downloads/) or via repo `orchestrate.cmd` venv |
| Azure CLI | latest | `winget install Microsoft.AzureCLI` or repo bootstrap |
| VS Code or Cursor | any | Optional; for editing JSON and Python |

Python packages used across lessons (install once):

```text
pip install azure-identity azure-mgmt-datafactory requests
```

---

## How to use this course

1. Read [README.md](README.md) for the full table of contents and time budgets.
2. Work lessons in order — each file links to the next.
3. Track progress in [manifest.json](manifest.json) (`status` field updated as lessons are generated).
4. Look up terms in [GLOSSARY.md](GLOSSARY.md).
5. Say **`continue`** after each lesson (or **`auto`** to generate without pausing).

---

## Case study data (all modules)

Sample CSV and JSON files live under [`data/`](../data/README.md). Upload paths match [CASE-STUDY.md](../CASE-STUDY.md) lake layout. Module 1 primary file: `transactions_daily.csv` (12 rows) — same schema as Session 2 `sample_transactions.csv` with a `store_id` column.

---

## Re-run scenarios (idempotency mindset)

| Scenario | Expected behaviour |
|---|---|
| Fresh subscription, nothing deployed | Module 0 creates all resources from scratch |
| Re-run after success | Portal "already exists" or update-in-place; no duplicates |
| Re-run after partial failure | Resume from last incomplete step; check-before-create |
| Tear-down then re-run | Same as fresh — deterministic names allow clean rebuild |

---

## Next

Start the course: [00-00 · Overview & mental model of ADF](module-00-foundations/00-00-overview.md)
