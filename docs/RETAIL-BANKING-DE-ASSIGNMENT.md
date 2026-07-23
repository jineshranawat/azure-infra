# FinTrust Retail Banking — Data Engineering Capstone Assignment

**Audience:** Students who completed (or are completing) this repo’s Azure ETL / lakehouse labs  
**Estate:** Shared class resources (`rg-shared-class1`, ADF, ADLS, Databricks `dbw-shared-qgr7mj`, Purview, Genie, notify mocks) — **uksouth**, cheapest viable SKUs  
**Deliverable style:** Working pipelines + notebooks + docs + demo — not slides alone  
**Projector twin:** [retail-banking-de-assignment.html](retail-banking-de-assignment.html)

---

## 0. One-line goal

Design and deliver an **end-to-end retail-banking data platform** for fictional UK bank **FinTrust Bank plc** that ingests multi-source operational data, enforces quality and quarantine, builds a governed medallion lakehouse, alerts on failure, enables natural-language analytics via Genie, and proves lineage/cost discipline — using **only patterns and services already present in this repository**.

---

## 1. Why this assignment (beyond the repo labs)

Repo labs teach **pieces** (ADF copy, Databricks silver/gold, Purview, Genie, Jira notify, DQ playbook).  
This assignment forces you to **own a bank’s overnight book**: multiple products, regulatory heat, late files, bad cards, incremental loads, ops tickets, and MI questions from the CFO — in one coherent solution.

You must invent **richer business rules and datasets** than the sample CSVs in the repo, while **reusing** the shared Azure estate and the lab patterns (do not provision a second region or expensive SKUs).

---

## 2. Company & problem statement

### 2.1 Who you are

You are the **Lead Data Engineer** hired by **FinTrust Bank plc** (fictional UK retail bank, HQ London, primary cloud region **UK South**).

FinTrust serves:

| Segment | Products |
|---------|----------|
| Mass retail | Current accounts, debit cards, FPS / Faster Payments |
| Affluent | Savings pots, ISA (simplified), personal loans |
| SME lite | Business current accounts, card acquiring (simplified) |
| Channels | Branch (declining), mobile app, open-banking AISP feeds (batch file) |

### 2.2 The crisis (business problem)

Overnight, Risk & Finance cannot answer four questions without manual Excel:

1. **What is yesterday’s authorised card spend by channel and MCC group?**  
2. **Which payments failed DQ or arrived late — and are they quarantined or still polluting silver?**  
3. **Which customers breached velocity rules (e.g. > N FPS outbound in 1 hour) for fraud review?**  
4. **If the ADF gold pipeline fails at 02:10, who gets a ticket with the run log attached?**

Today’s pain:

- Files land in SFTP-style drops as CSV/JSON with **schema drift** and **bad encodings**.  
- Full reloads of the payments history every night — cost and SLA breaches.  
- No single **audit trail** of what ran, with which watermark.  
- No **Purview**-visible lineage from raw file → gold KPI.  
- Business users ping analysts on Teams instead of using a **Genie Agent** over trusted gold tables.  
- Failures are discovered by humans at 09:00 — too late for the fraud queue.

### 2.3 Your mandate (assignment charter)

In **10–15 working days** (or as scheduled by your trainer), deliver **FinTrust Nightly Bank Platform v1** on the class Azure estate such that:

- A **single orchestrated path** (ADF master + Databricks where needed) produces bronze → silver → gold.  
- **Bad data never silently lands in silver** (gate + quarantine + optional notify).  
- **Incremental** processing uses watermarks (no full history reload for transactions).  
- **Incidents** open Jira-style tickets + email (local mock or Studio folder `15-enterprise-notify`).  
- **Governance**: Purview (or documented pl_gov path) shows searchable assets / lineage intent.  
- **Self-serve**: a Genie Agent answers at least 10 approved MI questions on gold.  
- **Cost**: uksouth only, cheapest SKUs, warehouse/cluster auto-stop, budget awareness.  
- **Docs**: a student README so a peer can re-run with **one Windows command** or a clearly documented Studio path.

---

## 3. Scope — what you must build

### 3.1 In scope (mandatory)

| # | Capability | Must demonstrate |
|---|------------|------------------|
| A | Multi-source ingest | ≥ **3** source types (e.g. CSV card auth, JSON FPS, CSV customer/account dim) into bronze |
| B | Medallion lake | bronze (raw immutable) → silver (typed, cleansed) → gold (KPI / fraud features) |
| C | Data quality gate | Schema + domain rules; promote vs block (playbook UC02 style) |
| D | Quarantine / dead-letter | Failed files or rows → `reject/` (or equivalent); good → `loaded/` / silver |
| E | Incremental watermark | Only new events since last success (UC03 / pl_05 style) |
| F | Config-driven entities | `entities.json` (or ADF ForEach) for ≥ 2 entities without cloning whole pipelines |
| G | Run audit trail | Per-run JSON (run_id, watermarks, counts, pass/fail) — UC06 style |
| H | Orchestration | **ADF** master pipeline(s) **and** at least one **Databricks** notebook/job activity — parameterized `run_id` |
| I | Failure notify | force-fail path → Jira + attach + email (`enterprise-notify` / notebook) |
| J | Genie | Genie Agent over **gold** (and optionally silver dims); ≥ 10 curated questions + examples |
| K | Governance / **Purview** | Purview account search + ADF–Purview link story **or** `pl_gov_06` + screenshots |
| N | **Azure SQL** | Use class Azure SQL (`ls_azure_sql_westus` / `finledger` DB) for ≥1 of: watermark table, reference/dim seed, or ADF Lookup/Copy sink — document westus exception |
| L | Cost & region | uksouth primary; SQL westus only as documented lab exception; auto-stop; spend note |
| M | Idempotent re-run | Same command/Studio run twice → no duplicate chaos |

### 3.1b Four platform pillars (all required — not optional)

Students must deliver **evidence for each pillar**. Skipping any pillar fails acceptance.

| Pillar | What FinTrust must use | Repo / class artefacts | Minimum student proof |
|--------|------------------------|------------------------|------------------------|
| **1. Azure Data Factory** | Ingest + orchestration hub | `shared-adf-lab/`, `adf-shared-*`, session-2, Monitor deep-dive, folder `15-enterprise-notify` | ≥1 Copy (or Data Flow) into bronze; ≥1 master pipeline with parameters; Monitor screenshot of success + one failure/notify path |
| **2. Azure Databricks** | Silver/gold transforms, optional Genie compute path | `dbw-shared-qgr7mj`, session-3, day7/8, `/Shared/shared-adf`, Genie lab | ≥1 notebook building silver and/or gold Delta/UC tables; ADF Databricks activity **or** Job run evidence |
| **3. Microsoft Purview** | Catalog + lineage / governance narrative | `purview-demo.cmd`, `pview*`, `pl_gov_*`, Purview teach HTML | Purview portal search of ≥2 FinTrust assets **or** documented MPN workaround + `pl_gov_06` outputs + lineage explanation |
| **4. Azure SQL** | Relational control / reference | `sql-shared-*`, DB `finledger`, `scripts/azure_sql.py`, linked service `ls_azure_sql_westus`, pipelines `pl_08`/`pl_09` style | Watermark **or** reference table in Azure SQL read by ADF Lookup/Copy; note **westus** as explicit documented region exception (class pattern) |

**Architecture sentence students must write in README:**

> ADF moves and orchestrates; ADLS holds the lake; Databricks transforms to silver/gold; Azure SQL holds control/reference state; Purview governs discovery/lineage; Genie serves MI questions; notify closes the ops loop.

### 3.2 Out of scope (do not boil the ocean)

- Real Open Banking APIs / live card networks  
- Real Jira Cloud / Outlook (local mocks or class Web activities are enough)  
- Multi-region active-active  
- Full IFRS9 / Basel models  
- Building a second subscription or non-UK primary region  

### 3.3 Stretch (bonus marks)

- Genie **Agent mode** report for fraud investigation narrative  
- SCD2 on customer or product dimension  
- Late-arriving data handling with event-time vs process-time  
- CI snippet or `release.cmd`-style documented promotion of notebooks  
- Knowledge Assistant over a short FinTrust runbook PDF (only if Model Serving budget allowed by trainer)

---

## 4. Domain model (you must flesh this out)

Minimum entities (extend freely — **more content than repo samples**):

### 4.1 Dimensions

| Entity | Example attributes | Notes |
|--------|--------------------|-------|
| Customer | customer_id, segment, onboarding_ts, risk_band, postcode_area | PII: mask in silver teaching views if shared |
| Account | account_id, customer_id, product_code, opened_ts, status | |
| Channel | channel_code (card, fps, wire, branch, app) | Align with lab channel vocab |
| Calendar | date_key, is_uk_bank_holiday | Optional helper |
| MCC / merchant group | mcc, mcc_group | For card spend |

### 4.2 Facts / events

| Entity | Grain | Key fields |
|--------|-------|------------|
| Card authorisation | 1 auth attempt | auth_id, account_id, amount_gbp, mcc, channel, event_ts, auth_status |
| FPS payment | 1 payment | fps_id, debtor_account, creditor_sort_ref, amount_gbp, direction (in/out), event_ts, status |
| Account balance snapshot | 1 account × day | account_id, as_of_date, available_balance, ledger_balance |
| DQ rejection event | 1 reject | source_file, rule_id, severity, raw_payload_ref |

### 4.3 Gold marts (minimum)

1. `gold_daily_channel_spend` — date × channel × sum(amount), txn_count  
2. `gold_fraud_velocity_alerts` — customer/account flags for velocity breaches  
3. `gold_dq_daily_summary` — files received / passed / quarantined  
4. `gold_ops_run_audit` — last N pipeline runs (or lake audit folder)

Students must **document business definitions** (e.g. what “authorised” means, timezone = Europe/London vs UTC storage).

---

## 5. Non-functional & regulatory flavour (UK retail bank)

Write short “control statements” in your README (1–2 paragraphs each):

1. **Data residency** — UK South primary; no eastus “because Genie” without trainer exception.  
2. **Least privilege** — UC grants / MSI patterns as in class.  
3. **PII** — do not put real personal data; synthetic only; mask in shared demos.  
4. **Auditability** — every gold number must be traceable to a run_id and source file name.  
5. **SLA** — declare a fictional SLA (e.g. gold ready by 06:00 UK). Show how watermark + notify support it.  
6. **Cost** — warehouse/cluster auto-stop; no needless serverless spend.

---

## 6. How to use **this repo** (mandatory mapping)

Your solution must **explicitly map** each deliverable to a repo artefact (copy/adapt, don’t ignore):

| Your FinTrust capability | Repo place to start |
|--------------------------|---------------------|
| Estate / one-command culture | `orchestrate.cmd`, README, `.cursorrules` |
| **ADF** ingest / medallion / Monitor | `shared-adf-lab/`, session-2 ADF course, `deploy-shared-adf-lab.cmd`, `docs/adf-monitoring-deep-dive.html` |
| **Databricks** transforms / Jobs / Genie | session-3, day7/day8, `/Shared/day7-day8`, `/Shared/shared-adf`, `genie-lab.cmd`, Genie teach HTML |
| **Azure SQL** control/reference | `shared-adf-lab/scripts/azure_sql.py`, `infra/shared-sql-westus.bicep`, LS `ls_azure_sql_westus`, DB `finledger` |
| **Purview** governance | `purview-demo.cmd`, `docs/purview-teach-demo.html`, `pl_gov_06`, Purview portal search guide |
| DQ / watermark / quarantine / ForEach / audit | `enterprise-de.cmd`, `docs/enterprise-de-playbook.html` |
| Incident → Jira + email | `enterprise-notify.cmd`, folder `15-enterprise-notify`, `nb_incident_jira_email.py` |
| Cost / region | `verify_cost.py` patterns; uksouth primary; SQL westus = documented exception |
| Infra as code awareness | Bicep/Terraform docs (describe; don’t redeploy whole estate unless asked) |

**Rule:** Prefer **extending** shared estate over creating parallel resource groups.

---

## 7. Phased delivery plan (suggested)

### Phase 0 — Charter (Day 1)
- Problem restatement in your words  
- Architecture diagram (sources → ADF → ADLS → Databricks → gold → Genie / notify / Purview)  
- Data dictionary v0  

### Phase 1 — Ingest & bronze (Days 2–3)
- Sample datasets (≥ 3 sources, **≥ 500 rows** total synthetic — richer than repo’s tiny samples)  
- Landing zones + ADF copy (or documented equivalent)  
- Prove re-run idempotency  

### Phase 2 — Quality & silver (Days 4–6)
- DQ rules catalogue (≥ 8 rules)  
- Quarantine path demo with intentional bad file  
- Typed silver tables/views in UC  

### Phase 3 — Incremental & gold (Days 7–9)
- Watermark store + delta load  
- Gold marts + fraud velocity logic  
- Audit JSON per run  

### Phase 4 — Ops & self-serve (Days 10–12)
- Notify on forced failure  
- Genie Agent + example SQL + 10 questions  
- Purview / governance evidence  

### Phase 5 — Defence (Days 13–15)
- Peer re-run from your README  
- Cost note + known limitations  
- 15-minute live demo to trainer  

---

## 8. Acceptance criteria (trainer checklist)

Mark **PASS** only if all mandatory items hold:

- [ ] **ADF** ingest + master orchestration + Monitor evidence  
- [ ] **Databricks** silver/gold notebook (ADF activity or Job)  
- [ ] **Azure SQL** watermark or reference used by ADF  
- [ ] **Purview** search evidence or pl_gov + lineage write-up  
- [ ] Architecture + dictionary  
- [ ] ≥ 3 sources → silver typed → ≥ 3 gold marts  
- [ ] DQ gate + quarantine demo with before/after proof  
- [ ] Watermark incremental proof (second run processes fewer/new rows only)  
- [ ] Config-driven multi-entity or ForEach evidence  
- [ ] Audit artefact per run with run_id  
- [ ] Notify path demonstrated (ticket key or local FIN-* + mail outbox)  
- [ ] Genie answers ≥ 5 gold questions correctly with visible SQL  
- [ ] README: one re-run path, uksouth/cost notes (+ SQL westus exception), mapping table to repo labs  
- [ ] Re-run twice without duplicate gold grain breakage  

---

## 9. Rubric (100 points)

| Area | Points | What “excellent” looks like |
|------|-------:|-----------------------------|
| Platform pillars (ADF + DBX + Purview + SQL) | 20 | All four evidenced |
| Medallion + domain richness | 20 | Bank story + bronze/silver/gold contracts |
| DQ + quarantine | 12 | Rules documented; bad data isolated |
| Incremental + audit | 12 | Watermark + run audit proven |
| Orchestration + notify | 12 | ADF master + incident path |
| Genie | 8 | Curated agent on gold |
| Cost/region/idempotency | 8 | Guardrails + SQL westus note |
| Demo + docs | 8 | Peer can re-run from README |

---

## 10. Suggested Genie question pack (students customise)

1. Yesterday’s total authorised card spend by channel  
2. Top 10 MCC groups by spend last 7 days  
3. Count of DQ quarantined files this week  
4. Accounts with velocity alerts in the last 24 hours  
5. Compare FPS inbound vs outbound totals for last Monday  
6. Trend of gold daily spend for card channel last 14 days  
7. How many silver transactions failed amount numeric checks (if tracked)  
8. Latest pipeline run_id and its status from audit  
9. Customers in risk_band High with spend > £X yesterday  
10. Out-of-scope guardrail: “What is Apple’s share price?” (must refuse)

---

## 11. Sample DQ rules (minimum — add more)

| Rule ID | Layer | Rule |
|---------|-------|------|
| DQ-AMT-01 | silver | amount_gbp numeric and ≥ 0 |
| DQ-CH-01 | silver | channel in allowed set |
| DQ-TS-01 | silver | event_ts parseable ISO-8601 |
| DQ-ID-01 | silver | primary keys non-null |
| DQ-FX-01 | silver | currency = GBP or documented FX |
| DQ-LATE-01 | ops | file event_date &lt; process_date − 2 → late flag |
| DQ-DUP-01 | silver | no duplicate auth_id / fps_id in batch |
| DQ-REF-01 | silver | account_id exists in account dim (or quarantine) |

---

## 12. Fraud velocity (example business logic)

Implement **at least one** rule in gold (tune thresholds):

- **VEL-FPS-01:** same `account_id`, &gt; 5 outbound FPS in any 60-minute window → alert row  
- **VEL-CARD-01:** same `account_id`, sum(authorised) &gt; £3,000 in calendar day → alert row  

Document why the threshold is fictional and how ops would consume the alert table (Genie + CSV extract is enough).

---

## 13. Demo script (15 minutes)

1. Show bad file → quarantine (Monitor / lake path).  
2. Show watermark before/after.  
3. Show gold KPI table.  
4. Force failure → Jira + mail.  
5. Genie: ask 2 gold questions; show SQL.  
6. Point to Purview or governance pipeline evidence.  
7. Re-run command / Studio run — idempotent.

---

## 14. Student submission pack

Submit a folder or PR section containing:

1. `docs/FINTRUST-README.md` — how to run, architecture, mapping to repo  
2. `docs/FINTRUST-DATA-DICTIONARY.md`  
3. `docs/FINTRUST-DQ-RULES.md`  
4. Pipelines / notebooks / sample data paths  
5. Screenshots or `out/` evidence (Genie, notify, quarantine)  
6. (Optional) short Loom/video link if trainer requests  

---

## 15. Integrity & collaboration

- Synthetic data only.  
- Do not commit secrets (`.env`, tokens).  
- You may collaborate on ideas; **implementation and demo must be your team’s**.  
- Cite which repo lab you copied (e.g. “notify flow adapted from `enterprise-notify.cmd`”).

---

## 16. Trainer notes (short)

- Grade on **integration and judgment**, not on reinventing Bicep.  
- Shared estate contention: stagger Genie/warehouse demos.  
- If Purview blocked on MPN, accept documented workaround + pl_gov evidence.  
- Prefer teams of 2–3 for Phases 1–5.

---

## Related repo docs

- [COVERAGE-MAP.md](../COVERAGE-MAP.md)  
- [enterprise-de-playbook.html](enterprise-de-playbook.html)  
- [enterprise-jira-email-demo.html](enterprise-jira-email-demo.html)  
- [databricks-genie-agents-teach.html](databricks-genie-agents-teach.html)  
- [purview-teach-demo.html](purview-teach-demo.html)  
- session-2 / session-3 CASE-STUDY.md (smaller FinLedger stories — this assignment **supersedes** them in scope)
