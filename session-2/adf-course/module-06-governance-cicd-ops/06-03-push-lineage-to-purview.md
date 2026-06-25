# 06-03 · Push lineage to Purview

> Module 6 · Time budget: 30 min · Source: [Push lineage to Purview](https://learn.microsoft.com/en-us/azure/data-factory/turorial-push-lineage-to-purview)
> Prereqs: [06-02 · CI/CD](06-02-arm-templates-cicd-dev-test-prod.md)

## What you'll build

Microsoft Purview account (if available), **connect ADF to Purview** in factory settings, run FinLedger pipelines — **lineage** from `bronze/incoming` → `silver` → `gold` visible in Purview catalog.

## Part A — UI

1. Purview account `pv-finledger-{learner}` — **UK South** if available (document exception if region limited).
2. ADF **Manage** → **Purview** → connect account.
3. Run `pl_finledger_nightly_foreach` + `pl_silver_transactions`.
4. Purview portal → **Lineage** → search `transactions_daily` → see ADF pipeline nodes.

> ℹ️ NOTE: Purview region availability varies on MPN — **skip** with MS Learn screenshot review if quota blocks.

## Part D — Verify

| Check | Expected |
|---|---|
| ADF linked to Purview | Settings show account |
| Lineage graph | Bronze → silver paths |

## Next

[06-04 · SSIS IR](06-04-ssis-integration-runtime.md)
