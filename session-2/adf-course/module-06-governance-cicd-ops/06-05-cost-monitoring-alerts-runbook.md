# 06-05 · Cost monitoring & runbook

> Module 6 · Time budget: 20 min · Source: [Plan and manage ADF costs](https://learn.microsoft.com/en-us/azure/data-factory/plan-manage-costs)
> Prereqs: [06-04 · SSIS IR](06-04-ssis-integration-runtime.md)

## What you'll build

**Budget** on `rg-adf-course-{learner}` (£25 MTD alert), **Cost analysis** filter by service (Data Factory, Storage, Databricks), operational **runbook** markdown for FinLedger on-call.

## Part A — UI

1. **Cost Management** → **Budgets** → **Create** → scope resource group → £25/month → 80% alert email.
2. **Cost analysis** → group by **Service name** → identify data flow vCore + Databricks spikes.
3. ADF **Monitor** → review pipeline run costs (activity runs meter).
4. Document runbook: stop triggers, stop debug, delete HDInsight/Databricks clusters, tear-down order.

## Runbook excerpt (FinLedger)

| Step | Action |
|---|---|
| 1 | Disable `tr_finledger_daily_6am` |
| 2 | Stop all data flow debug sessions |
| 3 | Terminate Databricks / delete HDInsight |
| 4 | Optional: delete `rg-adf-course-{learner}` |

## Part D — Verify

| Check | Expected |
|---|---|
| Budget alert | Configured |
| Cost by service | ADF + Storage visible |

## Next

[CAPSTONE.md](../CAPSTONE.md)
