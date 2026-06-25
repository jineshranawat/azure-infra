# 06-01 · Git integration for ADF

> Module 6 · Time budget: 30 min · Source: [Source control in ADF](https://learn.microsoft.com/en-us/azure/data-factory/source-control)
> Prereqs: Module 5 complete, GitHub or Azure DevOps repo

## What you'll build

Connect ADF Studio to **GitHub** repo `adf-finledger-{learner}` — **collaboration branch** `main`, **publish branch** `adf_publish`, export FinLedger pipelines as JSON in `adf/` folder structure.

## Part A — UI (click by click)

1. ADF Studio → **Git configuration** (Manage gear) → **Setup**.
2. **Repository type:** GitHub → authorize → select repo.
3. **Branch:** `main` → **Root folder:** `/adf`.
4. **Import** existing resources or **Publish** to sync live factory to Git.
5. Edit `pl_finledger_nightly_foreach` → save → **Commit** message `feat: nightly foreach manifest`.
6. **Create pull request** (if branch policy) or commit to main.
7. **Publish** from collaboration branch → updates `adf_publish` with ARM templates.

## Part D — Verify

| Check | Expected |
|---|---|
| Git status | Connected |
| Commit | JSON files in repo under `adf/pipeline/`, `adf/dataset/` |
| Publish | `adf_publish` branch updated |

## Next

[06-02 · ARM CI/CD](06-02-arm-templates-cicd-dev-test-prod.md)
