# RTT-Programme docs

Reference implementation: **"The 50 Problems, Solved"** on NHS RTT data.

- **[`engine_command_centre.html`](./engine_command_centre.html) — START HERE.** The master
  owner's manual: where everything is (RIDs/paths), the engine in one picture, a basics→PhD
  learning path, and click-by-click **deployment runbooks** (pipeline, ontology, markings,
  OSDK React app, opening the HTMLs), glossary + FAQ.
- [`pipeline_explainer.html`](./pipeline_explainer.html) — deep theory + code flow + Mermaid graphs.
- [`../app/rtt_command_centre.html`](../app/rtt_command_centre.html) — the React app (7 tabs:
  Command Centre, Trusts, Analytics, Ontology, Data Health, Audit, Connect) with full CRUD + Triage.
- [`DEPLOYMENT.md`](./DEPLOYMENT.md) — the deployment runbook (markdown copy of the UI steps).
- [`TRACEABILITY.md`](./TRACEABILITY.md) — the 50-problem matrix (Problem | Category |
  Artifact | Test | Status), medallion diagram, global test strategy, and the dataset RID index.
- [`SPECS.md`](./SPECS.md) — precise Foundry specs for Automation, Security/Governance
  platform controls, Visibility, the Workshop "RTT Command Centre", and the guard-railed AIP.
- [`HOW_TO_OPEN.md`](./HOW_TO_OPEN.md) — how to open the HTML files.

## Repository layout
```
transforms-python/src/myproject/
  paths.py                       # single source of truth for dataset paths
  lib/                           # pure, unit-tested helpers
    canonicalise · rtt_metrics · sae_minimiser · dq_checks · dq_rules
    recon · schema_contract · freshness · perf_utils · scd · pii
  datasets/                      # medallion stages
    seed · raw_ingest · clean_silver · sae_minimise · metrics · marts
    problems/                    # one module per category
      dq · perf · incremental · security · cost
src/test/                        # pytest mirror of lib/ (positive + negative)
docs/                            # this folder
```

## Engine policy
- **Lightweight (Polars/DuckDB)** by default — the RTT volume is small (Problems 28/45/46, cost).
- **Spark** (`problems/perf.py`) only where a distributed shuffle is genuinely warranted
  (AQE, broadcast, salting, bucketing, partitioning).

## How to run
Build `rtt_ptl` + `rtt_trust_specialty_perf` (builds the whole graph). CI runs pytest +
`ci/foundry-publish` on every commit; merge to master when green.
