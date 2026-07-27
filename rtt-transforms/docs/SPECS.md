# RTT-Programme — Platform specs (Automation, Security, Visibility, Workshop, AIP)

These problems are best expressed as precise, reproducible Foundry configuration specs
rather than repository code. Each names the surface, the exact config, and the test that
proves it. *Verify SDK/UI names against the current tenant.*

<a id="automation"></a>
## Automation & Orchestration (7, 16, 17, 38, 39) — Foundry Schedules + Automate (no cron/Airflow)

**7 — Scheduled builds across the graph.** Schedule `rtt-nightly`: target = `rtt_ptl` +
`rtt_trust_specialty_perf`, mode *build required inputs*, so the whole graph
(seed→…→marts) builds in dependency order. Trigger: time, 06:00 Europe/London.
*Test:* schedule run history shows one green build touching all 13+ datasets in order.

**39 — Dependency-aware cascade.** Same schedule uses "build downstream of" the seeds; no
manual ordering. *Test:* editing one upstream transform and running the schedule rebuilds
only the affected subgraph (verified via build report).

**16 — Event/condition trigger.** Automate monitor `rtt-on-new-data`: *event* = new
transaction committed to `trust_fdp_rtt_pathways_raw` → *effect* = build `rtt_ptl`.
*Test:* commit a transaction to raw → monitor fires → downstream build starts.

**38 — SLA monitoring + alerting.** Automate monitor `rtt-ptl-sla`: *condition* =
`freshness_report.days_since_latest > 1` OR `rtt_ptl` not rebuilt by 08:00 → *effect* =
notify Clare (+ Nitin if `rtt_pathways_rejects` row-count spikes > 5% of input).
*Test:* hold the build → at 08:00 the notification fires.

**17 — Connector retry + throttling.** Data Connection batch sync (source system → raw):
`maxRetries=3`, exponential backoff (2s→30s), concurrency/throttle cap, incremental
high-water on `source_load_ts`. *Test:* inject a transient 5xx in the source stub → sync
retries and succeeds; sync log shows backoff.

<a id="security"></a>
## Security & Governance (29, 30, 31, 47, 48)

**29 — Markings on PII.** Apply Marking **"NHS Patient PII"** to every dataset that still
carries `nhs_number`: `trust_fdp_rtt_pathways_raw`, `rtt_pathways_typed`,
`rtt_pathways_clean`, `rtt_pathways_minimised`, `rtt_pathways_metrics`,
`rtt_pathways_rejects`. Markings propagate to downstream outputs automatically.
The Ontology backing (`rtt_patient_pseudonymised`) is deliberately PII-free and needs no
marking. *Test:* a user without the marking is denied read on `rtt_pathways_clean` but can
read `rtt_patient_pseudonymised` (verify with `check_principal_access`).

**30 — Restricted Views (row-level).** Restricted View `rtt_ptl_rv` over `rtt_ptl` with a
row policy: `row.region == user.attribute('nhs_region')` (or group-per-region mapping).
RVs cannot be transform outputs, so this sits beside the mart. *Test:* a South West user
sees only South West trusts' pathways.

**31 — Property-level column security.** Two layers: (a) in-pipeline — `nhs_number` never
reaches the Ontology (pseudonymised + dropped in `security.py`, verified); (b) platform —
a property security group on any surfaced sensitive property restricts column visibility.

**47 — Encrypted secrets.** Source credentials live in a Data Connection **secret** (never
in code); egress via an approved network policy. *Test:* the sync runs without any secret
in the repo; secret is write-only in the UI.

**48 — Audit trails.** `sae_audit_log` (who/what/when for IG minimisation),
`recon_control_totals` (row-loss proof), the **Triage Pathway** action's edit history, and
Foundry's immutable transaction log together give end-to-end audit. *Test:* every executed
Triage action appears in the object's edit history; SAE log has one row per dropped column.

<a id="visibility"></a>
## Visibility & Documentation (32, 49, 50)

**32 — Automatic lineage source→Ontology.** Foundry captures lineage with zero config:
`seed → raw → typed → clean → minimised → metrics → patient_pseudonymised → Pathway object`.
*Test:* `get_resource_lineage` on `rtt_ptl` / the Pathway object type returns the full chain.

**49 — Data Health dashboards.** Every `Check(...)` in the transforms publishes to Data
Health (pass/fail, history). Group them into an "RTT Data Health" dashboard: PK uniqueness,
weeks range, ref-integrity, schema contract, freshness, recon. *Test:* the dashboard shows
all checks green except the deliberate freshness WARN.

**50 — Disaster recovery.** The immutable transaction model + branches give point-in-time
recovery: every build is an addressable transaction; roll back by re-pointing to a prior
transaction, or rebuild deterministically from `seed` (Problem 19). *Test:* delete the
latest `rtt_ptl` transaction and restore the previous one; row counts match.

<a id="workshop"></a>
## Workshop — "RTT Command Centre" (for Clare)

Backed by the **Pathway** object type (`921abe22`) + **Trust** (`da8bc071`).

- **Header KPI row** (metric widgets over the Active-pathway object set): % within 18 weeks
  (constitutional 92% gauge), count of 52-week breaches, count of 65-week breaches, total
  active PTL size.
- **Filters:** Trust, Specialty (property), Wait Band, RAG status, Pathway Status (default
  = Active).
- **PTL Object Table:** Pathway objects sorted by `weeksWaited` desc; RAG + 65-week columns
  render red/amber/green via the conditional formatting already configured; columns:
  pathwayId, trust→trustName, specialtyName, referralDate, weeksWaited, waitBand, ragStatus.
- **Detail panel + action:** selecting a pathway shows detail and a **Triage Pathway**
  action button (`897cd6d1`) writing `reviewStatus`/`triageNote`.
- **Trust drill-down:** clicking a Trust filters the table to that trust's pathways.
- **Events/variables:** `selectedTrust`, `selectedPathway` object-set variables wire the
  table, KPIs and detail panel together.

*Validation (Clare):* one screen to run the weekly PTL — longest waiters first, breaches
flagged, one click to record a validation decision.

<a id="aip"></a>
## AIP — controlled, human-in-the-loop (never a free agent on reportable data)

**Problem 40 assist — `standardise_specialty` (AIP Logic).** For specialty free-text that
`lib.canonicalise` cannot map, an AIP Logic function proposes the closest of the 10
canonical specialty names. **Constrained output** (enum of the 10 names or `UNMAPPED`);
**confidence gate** (< threshold ⇒ `UNMAPPED`); result is written to a *suggestion*
column, and only a human promotes it into `_SPECIALTY_SYNONYMS`. LLM never writes reportable
fields directly. *Test:* a golden set of dirty strings; assert enum-only outputs and that
low-confidence inputs return `UNMAPPED`.

**RTT triage assistant (Agent).** Read-only tools: object-set search over Pathway (e.g.
"longest-waiting 65-week breaches in Cardiology at TR005"). It *drafts* a triage note but the
human submits the **Triage Pathway** action — human-in-the-loop, no autonomous edits.
*Test:* the agent cannot mutate objects; every change flows through the audited action.

> All AIP steps: constrained outputs + confidence gates + human approval, so LLM assistance
> never compromises a national statutory return.
