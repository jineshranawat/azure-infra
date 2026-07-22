# Trainer notes — complete Azure ETL lab (one orchestrator)

Master guide for trainers delivering Class 1 + platform services. Learners use **`orchestrate.cmd`** only; you use this document to teach, demo, and troubleshoot.

**Related docs:** [CLASS-GUIDE.md](CLASS-GUIDE.md) | [WORKFLOW-AND-CODE.md](WORKFLOW-AND-CODE.md) | [GOVERNANCE-DEPLOY.md](GOVERNANCE-DEPLOY.md) | [CICD-INCREMENTAL-RELEASE.md](CICD-INCREMENTAL-RELEASE.md) | [BICEP-TERRAFORM-SHARED-ESTATE.md](BICEP-TERRAFORM-SHARED-ESTATE.md) | **[adf-monitoring-deep-dive.html](adf-monitoring-deep-dive.html)** | **[databricks-ui-deep-dive.html](databricks-ui-deep-dive.html)** | **[purview-teach-demo.html](purview-teach-demo.html)** / [PURVIEW-TEACH-DEMO.md](PURVIEW-TEACH-DEMO.md) (`purview-demo.cmd`) | **[enterprise-de-playbook.html](enterprise-de-playbook.html)** / [ENTERPRISE-DE-PLAYBOOK.md](ENTERPRISE-DE-PLAYBOOK.md) (`enterprise-de.cmd` — UC01–UC06) | **[enterprise-notify-20h.html](enterprise-notify-20h.html)** / [ENTERPRISE-NOTIFY-20H.md](ENTERPRISE-NOTIFY-20H.md) (`enterprise-notify-20h.cmd` · `enterprise-notify.cmd`) | [enterprise-jira-email-demo.html](enterprise-jira-email-demo.html) | [README](../README.md) | **[COVERAGE-MAP.md](../COVERAGE-MAP.md)** | **[INFRA-WALKTHROUGH-20H.md](INFRA-WALKTHROUGH-20H.md)** (`infra-walkthrough.cmd` · phase `1t` = Terraform)

**Platform:** Windows only. **Start here:** [README §1 — run guide, failures & AI chat](../README.md#1-how-to-run-on-windows-step-by-step).

---

## 1. One command runs the full lab

**Re-run safety:** Same `LEARNER` → deterministic names → ARM incremental deploys → check-before-create for RBAC and Fabric workspaces. Re-run command: **`orchestrate.cmd`**. See [README section C](../README.md#c-the-re-run-command).

### Learner commands (Windows)

```text
REM First time (clean machine):
orchestrate.cmd --install-cli
REM Edit .env, then run again:
orchestrate.cmd --install-cli

REM Full lab (default, safe to re-run):
orchestrate.cmd

REM Day 1 only:
orchestrate.cmd --class1-only

REM Teardown:
orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes
```

### What the orchestrator does (in order)

| Phase | Script function | Deploys / runs |
|-------|-----------------|----------------|
| 0 | `_ensure_dotenv`, `_ensure_venv`, `_ensure_azure_cli` | `.env`, Python venv, Azure CLI |
| 1 | `_ensure_az_no_wam_broker` or SP login, `_ensure_az_login`, `_set_subscription` | WAM off (interactive) or `az login --service-principal` (class image) |
| 2 | `_deploy_bicep`, `_ensure_rbac` | Class-1: RG, tags, budget, KV, storage, medallion, lifecycle, RBAC |
| 3 | `_deploy_platforms` | ADF, Purview (or skip if tenant catalog exists), Databricks |
| 4 | `_verify` | `verify_cost.py` — SKU allow-list + MTD cost |
| 5 | `_compare_platforms` | `compare_platform_costs.py` — list prices + Cost Explorer URLs |
| 6 | `_print_summary` | All portal links |

### Orchestrator modes

| Flag | Use when |
|------|----------|
| *(default)* | **Full lab** — Class-1 + platforms + verify + cost compare |
| `--class1-only` | Day 1 only — landing zone, no ADF/Purview/Databricks |
| `--platforms-only` | Day 2 — platforms after Class-1 already exists |
| `--skip-setup` | Optional speed — skip venv/pip when already installed |
| `--skip-verify` | Skip SKU/cost gate (not recommended) |
| `--skip-compare` | Skip list-price comparison table |
| `--use-device-code` | Headless VDI / no browser — avoids WAM and MDM enrollment entirely |
| `--install-cli` | First run — install Azure CLI via winget |

### Shared class estate (all students, one subscription)

When every learner uses the **same** resources (no per-learner RG), deploy once as trainer:

```text
provision-shared.cmd --owner-email trainer@example.com
```

| Setting | Value |
|---------|--------|
| Subscription | `a64c0dd2-3a31-4604-bde3-3d40c7d5e8be` (AzureSponsorship-DataTraining) |
| Region | `eastus` (documented exception — shared training subscription) |
| Resource group | `rg-shared-class1` |
| `LEARNER` in `.env` | `shared` |

Re-run: `provision-shared.cmd` (idempotent incremental deploy). Budget is skipped on Sponsorship subscriptions.

After provision, deploy lab assets (bronze CSV, secrets, master notebook):

```text
deploy-shared-lab.cmd
```

Master PySpark notebook (385 cells, Day 7 + Day 8): `/Shared/day7-day8/master_pyspark_complete`

**Shared ADF lab (Session 19+):** 10 teaching pipelines + SQL westus + Databricks triggers:

```text
deploy-shared-adf-lab.cmd
```

Docs: `shared-adf-lab/README.md` · Pipelines as code: `shared-adf-lab/scripts/adf_pipelines.py`

---

## 2. Four re-run scenarios (prove in class)

| # | Scenario | Commands | Safe because |
|---|----------|----------|--------------|
| 1 | Fresh machine | `orchestrate.cmd --install-cli` (×2 after `.env`) | First pass scaffolds; second deploys once |
| 2 | After full success | `orchestrate.cmd` | Incremental ARM + RBAC skip + Fabric skip-if-exists |
| 3 | After partial failure | `orchestrate.cmd` | `_discover_outputs()` resumes; MPN warns only |
| 4 | After teardown | `orchestrate.cmd teardown ... --yes` then `orchestrate.cmd` | Same learner → same `uniqueString` names |

---

## 3. Pre-class checklist (trainer)

- [ ] Learners have **Contributor** or **Owner** on a training subscription
- [ ] `.env` filled: `AZURE_SUBSCRIPTION_ID`, `LEARNER`, `LOCATION=uksouth`
- [ ] Per-learner login: `OWNER_EMAIL` in `.env` — or class mode: trainer sets `CLASS_OWNER_EMAIL` + `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` on golden image (learners skip browser login)
- [ ] Python 3.11+ on PATH
- [ ] Explain: passwords never go in chat — `az login` only
- [ ] **MPN/MSDN subscriptions:** warn that **Synapse** and **Fabric capacity** may be blocked (see Section 8)
- [ ] Budget alert email goes to `OWNER_EMAIL`
- [ ] Teardown ready: `orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes`

---

## 4. What gets deployed (full lab)

### Class-1 landing zone (`infra/main.bicep`)

| Resource | Naming | Teaching point |
|----------|--------|----------------|
| Resource group | `rg-<learner>-class1` | Blast radius; UK region only |
| Budget | `budget-<learner>-class1` | £1/month guardrail **before** storage |
| Key Vault | `kv-<learner>-<hash>` | RBAC mode, Standard SKU |
| Storage (ADLS Gen2) | `st<learner><hash>` | HNS, Standard_LRS, Hot |
| Containers | bronze, silver, gold, audit | Medallion pattern |
| Lifecycle | 7d delete, 30d Cool | Training hygiene |
| RBAC | Secrets Officer + Blob Contributor | Owner ≠ data plane |

### Platform services (`infra/platform-services.bicep`)

| Resource | Naming | Typical MPN status |
|----------|--------|------------------|
| Data Factory | `adf-<learner>-<hash>` | Deploys |
| Databricks | `dbw-<learner>-<hash>` | Deploys (Premium workspace, **no cluster**) |
| Purview | `pview<learner><hash>` | Deploys in **eastus** |
| Synapse | `syn-<learner>-<hash>` | **Skipped** on MPN |
| Fabric capacity | `fc<learner><hash>` | Often **quota 0** on MPN |
| Fabric workspace | `ws-<learner>-class1` | API skip-if-exists, or Fabric trial |

---

## 5. Teaching flow (recommended 2-day schedule)

### Day 1 — Class-1 only (~4 h)

```text
orchestrate.cmd --class1-only
```

| Block | Open in IDE | Open in portal | Concept |
|-------|-------------|----------------|---------|
| 1 | `.env`, `main.bicep` L7–37 | Subscriptions → copy ID | UK residency, tags |
| 2 | `main.bicep` L44–86 | Cost Management → Budgets | Guardrail-first |
| 3 | `main.bicep` L88–109 | Key Vault → Properties | RBAC vault |
| 4 | `main.bicep` L111–212 | Storage → Containers, Lifecycle | Medallion lake |
| 5 | `main.bicep` L214–236 | Storage/KV → IAM | Data-plane RBAC |
| 6 | `verify_cost.py` | Cost analysis (filter RG) | £0 SKU gate |

### Day 2 — Full platform lab (~4 h)

```text
orchestrate.cmd
```

| Block | Open in IDE | Open in portal | Concept |
|-------|-------------|----------------|---------|
| 1 | `platform-services.bicep` ADF section | Data Factory blade | Orchestration, linked service |
| 2 | Purview section | web.purview.azure.com | Governance catalog |
| 3 | Databricks section | Databricks workspace | Lakehouse (no cluster demo) |
| 4 | `compare_platform_costs.py` output | Cost Explorer | Cost ranking discussion |
| 5 | Manual | app.fabric.microsoft.com | Fabric trial + workspace |

---

## 6. Portal demo script

After **full lab** orchestrator completes:

1. Resource groups → `rg-<learner>-class1` → Tags
2. Cost Management → Budgets → `budget-<learner>-class1`
3. Key Vault → Permission model = **Azure RBAC**
4. Storage → HNS enabled; containers bronze/silver/gold/audit
5. Lifecycle management → 7d delete rule
6. Data Factory → Author & Monitor
7. Purview → web.purview.azure.com
8. Databricks → no cluster
9. Cost analysis → filter by RG
10. Fabric → app.fabric.microsoft.com (trial if capacity blocked)

---

## 7. Code map

```text
orchestrate.cmd              → SINGLE Windows entry (learners)
orchestrate.ps1              → internal helper (Bypass policy from .cmd)
scripts/orchestrate.py       → all phases
infra/main.bicep             → Class-1 landing zone
infra/platform-services.bicep  → ADF, Synapse, Purview, Fabric, Databricks
scripts/verify_cost.py       → SKU + MTD gate
scripts/fabric_workspace.py  → Fabric workspace (check-before-create)
scripts/teardown.py          → delete RG
```

### Build order (never change)

```text
RG + tags → budget → Key Vault → storage + containers + lifecycle → RBAC
         → ADF → (Synapse skip) → Purview (eastus) → (Fabric best-effort) → Databricks → verify
```

---

## 8. MPN / training subscription limits

| Service | Typical error | Trainer action |
|---------|---------------|----------------|
| **Synapse** | `SqlServerRegionDoesNotAllowProvisioning` | Skipped in orchestrator (`deploySynapse=false`) |
| **Fabric capacity** | `RegionalQuota: 0` | Fabric trial at app.fabric.microsoft.com |
| **Purview** | Tenant free tier in `eastus` | Orchestrator deploys Purview in **eastus** automatically |
| **Purview** | Error `35001` — tenant catalog already exists | **Automatic** — deploys ADF + Databricks only; use existing tenant Purview in portal |

Orchestrator skips Synapse and Fabric by default; retries without Purview when the tenant already has a catalog.

---

## 9. Troubleshooting

Full learner table (failures, MPN warnings, AI chat): [README §1](../README.md#failures--workarounds-short-guide).

| Symptom | Fix |
|---------|-----|
| `az` not found | `orchestrate.cmd --install-cli` |
| PowerShell blocks scripts | `orchestrate.cmd` in Command Prompt |
| `az login` MFA | `orchestrate.cmd --use-device-code` |
| MDM / `0x80192ee7` on VDI | WAM disabled automatically; `--use-device-code` if still blocked |
| RBAC `RoleDefinitionDoesNotExist` | Re-run — `_ensure_rbac` fallback |
| Fabric workspace exists | `already present` log — OK |
| Portal cost `-` | Billing lag — use verify script |

---

## 10. Teardown

```text
orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes
```

Also delete Databricks managed RG `rg-<learner>-dbw-<hash>` if orphaned.

---

## 11. Deliverables checklist

| # | Deliverable | Evidence |
|---|-------------|----------|
| 1 | Orchestrator exit 0 | `Orchestration complete (full-lab)` |
| 2 | Class-1 resources | KV + Storage + budget in portal |
| 3 | ADF + Databricks + Purview | Platform blades visible |
| 4 | verify_cost pass | MTD in terminal |
| 5 | Cost compare table | Phase 5 output |
| 6 | Fabric workspace | Screenshot or trial note |

---

## 12. Example `.env`

```env
AZURE_SUBSCRIPTION_ID=a64c0dd2-3a31-4604-bde3-3d40c7d5e8be
LEARNER=shared
LOCATION=eastus
OWNER_EMAIL=trainer@example.com
LOCATION=uksouth
```

---

## 13. One-page learner handout

```text
1. Clone repo; open Command Prompt in repo folder
2. orchestrate.cmd --install-cli
3. Edit .env — subscription, learner, email — save
4. orchestrate.cmd --install-cli   (completes deploy)
5. Re-run anytime: orchestrate.cmd
6. Teardown: orchestrate.cmd teardown --resource-group rg-<you>-class1 --yes
```
