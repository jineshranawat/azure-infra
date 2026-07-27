# RTT-Programme — "The 50 Problems, Solved"

A single, coherent Foundry reference implementation proving Palantir Foundry natively
solves 50 recurring data-engineering problems, using the NHS RTT dataset. Primary
business user: **Clare** (RTT operations manager). IG owner: **Nitin** (IG Lead).

Domain constants used verbatim: RTT clock target = **18 weeks**; **52** and **65**-week
waits are nationally reportable breaches; constitutional standard = **92%** within 18
weeks; **PTL** = Patient Tracking List.

## Medallion pipeline (all built & verified on the feature branch)

```
seed ─┬─ provider_reference ────────────────────────────────┐
      ├─ specialty_reference ──────────────────┐            │
      ├─ sae_column_rules ─────────┐           │            │
      └─ trust_fdp_rtt_pathways_raw│           │            │
             │ (all-string landing)│           │            │
   raw_ingest▼ (type/parse, P6/P10)│           │            │
      rtt_pathways_typed           │           │            │
   clean_silver▼ (canonicalise+quarantine, lib.dq_rules)    │
      rtt_pathways_clean ─ rtt_pathways_rejects ─ recon_control_totals
   sae_minimise▼ (drop sex/age/source_load_ts, audit)       │
      rtt_pathways_minimised ─ sae_audit_log                │
   metrics▼ (breach flags/bands/RAG, lib.rtt_metrics)       │
      rtt_pathways_metrics                                   │
   marts▼ ── rtt_trust_specialty_perf ── rtt_ptl ◄──────────┘
   security▼ (pseudonymise, drop nhs_number) ── rtt_patient_pseudonymised ──► Ontology
```

**Verified control totals:** typed 1,215 = clean 1,205 + rejects 10 (reconciled=1);
clean PK unique 1,205/1,205; 9 distinct reject reasons; SAE removes 3 columns;
freshness monitor flags data stale (541 days).

## Traceability matrix (Problem | Category | Artifact | Test | Status)

### Data Quality & Consistency
| # | Problem | Artifact | Test | Status |
|---|---|---|---|---|
|1|Duplicate records|`lib/dq_rules.add_reject_reason` + `clean_silver`|`test_dq_rules`|✅ Built|
|6|Ingest-time type/schema validation|`raw_ingest` + `lib/canonicalise.to_int/parse_date`|`test_canonicalise`|✅ Built|
|8|Null / missing mandatory values|`lib/dq_rules` (null_pathway_id)|`test_dq_rules`|✅ Built|
|9|Schema drift at ingest|`problems/dq.p9_schema_drift` + `lib/schema_contract`|`test_schema_contract`|✅ Built (schema_drift_report)|
|10|Timestamp/date format inconsistency|`lib/canonicalise.parse_date`|`test_canonicalise`|✅ Built|
|34|Referential integrity / orphans|`lib/dq_rules` (orphan_trust_code)|`test_dq_rules`|✅ Built|
|35|Range / domain validation|`lib/dq_rules` + `clean` FAIL checks|`test_dq_rules`,`test_rtt_metrics`|✅ Built|
|40|Free-text canonicalisation|`lib/canonicalise.canonical_specialty` + `clean_silver`|`test_canonicalise`|✅ Built|
|41|Cross-field / temporal consistency|`lib/dq_rules` + `lib/dq_checks.referral_not_after`|`test_dq_checks`,`test_dq_rules`|✅ Built|
|42|PK uniqueness enforcement|`E.primary_key` on clean + `lib/dq_rules`|`test_dq_rules`|✅ Built|
|43|Freshness / staleness|`problems/dq.p43_freshness` + `lib/freshness`|`test_freshness`|✅ Built (freshness_report)|

### Performance & Scalability
| # | Problem | Artifact | Test | Status |
|---|---|---|---|---|
|4|Adaptive Query Execution|`problems/perf.perf_spark_techniques` (spark.conf adaptive)|Spark build|✅ Built|
|5|Broadcast join|`perf` `F.broadcast(dims)`|Spark build|✅ Built|
|11|Partitioning|`perf` `partition_cols=['region']`|perf_trust_region|✅ Built|
|12|File compaction / small files|`perf` `coalesce(1)`|perf_trust_region|✅ Built|
|13|Column pruning / pushdown|`perf` early `select`/`filter`|Spark build|✅ Built|
|22|Data skew (salting)|`perf` `pmod(hash)` + `lib/perf_utils.salt_bucket`|`test_perf_utils`|✅ Built|
|23|Bucketing|`perf` `bucket_cols=['trust_code']`|perf_pathway_bucketed|✅ Built|
|24|Caching reused intermediate|`perf` `.cache()`|Spark build|✅ Built|
|25|Repartition vs coalesce sizing|`perf` + `lib/perf_utils.target_partitions`|`test_perf_utils`|✅ Built|
|45|Polars for small workloads|entire lightweight foundation|all `lib` tests|✅ Built|
|46|DuckDB small aggregation|`problems/perf.perf_duckdb_specialty`|perf_specialty_duckdb|✅ Built|

### Incremental & Streaming
| # | Problem | Artifact | Test | Status |
|---|---|---|---|---|
|2|Incremental append framework|`problems/incremental.inc_pathways_appended` `@incremental`|`test_scd`|✅ Built|
|3|Watermarking|`inc_pathways_appended` ('added') + `lib/scd.high_water_mark`|`test_scd`|✅ Built|
|14|CDC change feed|`incremental.cdc_change_feed` + `lib/scd.classify_cdc`|`test_scd`|✅ Built (1205 INSERT)|
|15|Dedup latest-wins|`lib/scd.dedupe_latest`|`test_scd`|✅ Built|
|18|SCD Type 2 history|`incremental.status_history`/`status_scd2` + `lib/scd.build_scd2`|`test_scd`|✅ Built (1205 current)|
|19|Idempotent replay / backfill|`lib/scd.dedupe_latest` + deterministic seed|`test_scd`|✅ Built|
|20|Late / out-of-order events|`lib/scd.dedupe_latest` (by event ts)|`test_scd`|✅ Built|
|21|Windowing|`incremental.window_rank_longest_waiters` + `lib/scd.rolling_sum`|`test_scd`|✅ Built|
|36|Snapshot→incremental conversion|`incremental.status_history` (`snapshot_inputs`)|`test_scd`|✅ Built|
|37|Incremental + reference lookups|`inc_pathways_appended` (`snapshot_inputs=['specialties']`)|`test_scd`|✅ Built|
|44|Backfill-safe reprocessing|`incremental.status_history` append pattern|`test_scd`|✅ Built|

### Automation & Orchestration — see SPECS.md (Foundry Schedules + Automate, no cron/Airflow)
| # | Problem | Artifact | Test | Status |
|---|---|---|---|---|
|7|Scheduled builds across the graph|`docs/SPECS.md#automation` schedule spec|schedule run history|📋 Spec|
|16|Event/condition-triggered build|Automate monitor spec|monitor fire|📋 Spec|
|17|Connector retry + throttling|Data Connection sync spec|sync retry log|📋 Spec|
|38|SLA monitoring + alerting|Automate + Data Health spec|alert fire|📋 Spec|
|39|Dependency-aware cascade builds|Schedule graph spec|cascade run|📋 Spec|

### Cost Optimization
| # | Problem | Artifact | Test | Status |
|---|---|---|---|---|
|26|Right-sized profiles|`problems/cost.cost_optimised_summary` `@lightweight(1c/2GB)`|cost_region_summary|✅ Built|
|27|Auto-scaling / warm pools|`@lightweight` scale-to-zero (see SPECS)|—|✅ Built / 📋 Spec|
|28|Lightweight vs Spark|`cost.py`/foundation (Polars/DuckDB) vs `perf.py` (Spark)|`test_perf_utils`|✅ Built|

### Security & Governance
| # | Problem | Artifact | Test | Status |
|---|---|---|---|---|
|29|Markings on PII|`problems/security.pseudonymise_patient` (removes PII) + Marking spec|patient_pseudonymised|✅ Built (data) / 📋 Spec (marking)|
|30|Restricted Views (row-level)|`docs/SPECS.md#security` region RV spec|RV access test|📋 Spec|
|31|Property-level column security|pseudonymisation + Ontology (nhs_number never modelled)|patient_pseudonymised|✅ Built / 📋 Spec|
|47|Encrypted secrets|Data Connection secret spec|—|📋 Spec|
|48|Audit trails|`sae_audit_log` + `recon` + Triage action edits + immutable transactions|SQL verify|✅ Built|

### Visibility & Documentation
| # | Problem | Artifact | Test | Status |
|---|---|---|---|---|
|32|Auto lineage source→Ontology|inherent (Data Lineage graph)|`get_resource_lineage`|✅ Native|
|33|Resource docs on every asset|descriptions on project + key datasets|metadata|✅ Built|
|49|Data Health dashboards|all `Check(...)` expectations emit to Data Health|check results|✅ Built|
|50|DR via immutable transactions|transaction model + branch snapshots|time-travel|✅ Native|

### Ontology Advantage (cross-cutting)
| Object/Action | RID | Status |
|---|---|---|
|Trust object type|`ri.ontology.main.object-type.da8bc071-…`|✅ Built|
|Pathway object type (RAG formatting, edit-only triage props)|`ri.ontology.main.object-type.921abe22-…`|✅ Built|
|Pathway→Trust link|`ri.ontology.main.relation.dcefe330-…`|✅ Built|
|Triage Pathway action|`ri.actions.main.action-type.897cd6d1-…`|✅ Built|
|Workshop RTT Command Centre|`docs/SPECS.md#workshop`|📋 Spec|
|AIP standardise_specialty + triage assistant|`docs/SPECS.md#aip`|📋 Spec|

> Ontology note: the JINESH Ontology is at its 60 object-type cap (58 pre-existing), so
> Specialty and Patient are modelled as Pathway properties (`specialty_code`/`specialty_name`,
> `patient_pseudo_id`) rather than separate objects. All logic is unaffected.

## Global test strategy
- **Unit (pytest, `src/test/`)** — pure/Polars helpers in `lib/` with deterministic fixtures;
  every rule has a **negative** case. Suites: `test_canonicalise`, `test_rtt_metrics`,
  `test_sae_minimiser`, `test_dq_checks`, `test_dq_rules`, `test_schema_contract`,
  `test_freshness`, `test_perf_utils`, `test_scd`, `test_pii`, `test_recon`.
- **In-transform Data Expectations** — `FAIL` where a bad row would corrupt keys/joins/metrics
  (PK, weeks range, specialty mapping, trust ref-integrity, schema contract); `WARN` for quality
  signals (raw nulls, freshness, unexpected columns). Bad rows are quarantined, so FAIL checks on
  `clean` stay green *because* the policy works — proven by `test_dq_rules`.
- **Integration checks on built outputs** — reconciliation (`recon_control_totals`), PK uniqueness,
  quarantine breakdown, SAE column removal, CDC/SCD2 counts, partition/bucket layout.
- **CI** — every commit to the feature branch runs the pytest suite + validates all transforms via
  `ci/foundry-publish`; merge to master only when green (trunk-based, minutes).

## Dataset RID index (branch: ai-fde/ranawatjinesh/rtt-50-problems)
| Dataset | RID |
|---|---|
|provider_reference|`ri.foundry.main.dataset.1f680428-478a-40ba-a962-971206b6178d`|
|specialty_reference|`ri.foundry.main.dataset.6d0f3f66-1d39-475e-9cc4-50c1fe9cce3d`|
|sae_column_rules|`ri.foundry.main.dataset.f9d5fad0-6435-40ae-a33d-c53f8c3bad73`|
|trust_fdp_rtt_pathways_raw|`ri.foundry.main.dataset.6fdc6f72-46cb-48ac-937b-1aa2a98f4667`|
|rtt_pathways_typed|`ri.foundry.main.dataset.387c018a-7b79-499c-a3e3-41a09574a308`|
|rtt_pathways_clean|`ri.foundry.main.dataset.a0a82863-8b8c-4cc5-9f91-31399baef9e7`|
|rtt_pathways_rejects|`ri.foundry.main.dataset.84ee3fc4-5a0f-4399-8fdc-ca658057a880`|
|recon_control_totals|`ri.foundry.main.dataset.a49bdf77-ff80-4e5e-a17c-f5a2d6ae6501`|
|rtt_pathways_minimised|`ri.foundry.main.dataset.e63934ef-afe2-46b6-8e5d-1f419b446315`|
|sae_audit_log|`ri.foundry.main.dataset.aca6c0ec-d3f0-4c44-827d-e27bea02dfbf`|
|rtt_pathways_metrics|`ri.foundry.main.dataset.04bf7522-41b5-4ddb-947c-c35ec15cf210`|
|rtt_trust_specialty_perf|`ri.foundry.main.dataset.afdb017f-9692-4217-ab63-ba28485d5907`|
|rtt_ptl|`ri.foundry.main.dataset.777a720f-9e42-46e0-a58e-8e5d0f7046b6`|
|schema_drift_report|`ri.foundry.main.dataset.9c0826be-0c89-4aae-a322-4fd63ac618d9`|
|freshness_report|`ri.foundry.main.dataset.67866ddd-c8b4-4658-98bb-2367928b231c`|
|perf_trust_region (partitioned)|`ri.foundry.main.dataset.3bd1c34e-b05e-4020-9d7f-7810d8277a57`|
|perf_pathway_bucketed|`ri.foundry.main.dataset.4ed078f2-442a-4259-8ed8-b6ceea732085`|
|perf_specialty_duckdb|`ri.foundry.main.dataset.f341f2a6-a159-432d-a567-db8b0b988e75`|
|rtt_pathways_appended|`ri.foundry.main.dataset.5496f3dc-0f8d-4a5d-967f-1257e08c2bdc`|
|rtt_pathways_cdc_feed|`ri.foundry.main.dataset.5cc6e773-2023-4596-b0b9-c84fb1a38206`|
|rtt_status_history|`ri.foundry.main.dataset.bf139890-ca15-4fe7-9e65-35bb595e44d4`|
|rtt_status_scd2|`ri.foundry.main.dataset.b850ebc2-c56d-4e34-8888-5af8e3090a9f`|
|rtt_longest_waiters_by_trust|`ri.foundry.main.dataset.c288c9c1-74a9-409e-a307-356adf477b5c`|
|rtt_cost_region_summary|`ri.foundry.main.dataset.5db8d969-47a7-44d0-a147-cfcb5800fde0`|
|rtt_patient_pseudonymised (Ontology backing)|`ri.foundry.main.dataset.800a8aaa-19f7-4525-8f77-6300f16b23c1`|
