# 06-02 · ARM templates & CI/CD

> Module 6 · Time budget: 40 min · Source: [CI/CD in ADF](https://learn.microsoft.com/en-us/azure/data-factory/continuous-integration-delivery)
> Prereqs: [06-01 · Git](06-01-git-integration-azure-devops-github.md), [`sample_arm_parameters.json`](../data/module-06-governance/sample_arm_parameters.json)

## What you'll build

**ARMTemplateForFactory.json** from **Publish** export, Azure DevOps / GitHub Actions pipeline deploying to **dev** and **prod** factories with parameter file per environment.

## Part A — UI

1. After Git publish → repo `adf_publish` → `ARMTemplateForFactory.json` + `ARMTemplateParametersForFactory.json`.
2. Copy [`sample_arm_parameters.json`](../data/module-06-governance/sample_arm_parameters.json) → customise `factoryName`, `storageAccountName`.
3. DevOps **Pipeline** → task **Azure Resource Manager template deployment**:
   - Resource group `rg-adf-course-{learner}-dev`
   - Template: exported ARM
   - Parameters: JSON file
4. Repeat stage for **prod** with approval gate.
5. **Pre/post deployment** scripts: `prepostscripts/Stop-Triggers.ps1` per MS Learn.

## Part B — JSON

Use exported ARM — never hand-edit full template; use parameters for factory name and linked service properties.

## Part D — Verify

| Check | Expected |
|---|---|
| Dev deploy | Factory artefacts match Git |
| Prod gate | Manual approval before prod |

## Next

[06-03 · Purview lineage](06-03-push-lineage-to-purview.md)
