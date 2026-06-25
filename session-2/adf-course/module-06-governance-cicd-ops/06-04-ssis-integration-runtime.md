# 06-04 · SSIS integration runtime

> Module 6 · Time budget: 30 min · Source: [Deploy SSIS packages in ADF](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-deploy-ssis-packages-azure)
> Prereqs: [06-03 · Purview](06-03-push-lineage-to-purview.md)

## What you'll build

Conceptual **Azure-SSIS Integration Runtime** setup for lifting legacy FinLedger SSIS packages — SSIS IR creation wizard, Azure SQL **SSISDB**, deploy `.ispac` via SSMS or Azure-SSIS IR.

## Part A — UI (overview)

1. **Manage** → **Integration runtimes** → **+** → **Azure-SSIS**.
2. Configure **Node size** (lowest for lab), **VNet** (optional), **SSISDB** on Azure SQL.
3. Deploy sample SSIS package copying CSV to ADLS.
4. ADF **Execute SSIS package** activity in pipeline `pl_ssis_legacy_load`.

> ⚠️ WARNING: SSIS IR is **expensive** — create for demo, **delete IR** same day. **MPN skip:** read tutorial + trainer demo only.

## Part D — Verify

| Check | Expected |
|---|---|
| SSIS IR | Running (or documented skip) |
| Package | Executes from ADF activity |

## Next

[06-05 · Cost monitoring](06-05-cost-monitoring-alerts-runbook.md)
