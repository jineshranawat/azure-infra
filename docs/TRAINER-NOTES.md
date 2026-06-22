# Trainer notes — complete Azure ETL lab (one orchestrator)

Master guide for trainers delivering Class 1 + platform services. Learners use **one command**; you use this document to teach, demo, and troubleshoot.

**Related docs:** [CLASS-GUIDE.md](CLASS-GUIDE.md) (timed blocks) | [WORKFLOW-AND-CODE.md](WORKFLOW-AND-CODE.md) (code + portal map) | [GOVERNANCE-DEPLOY.md](GOVERNANCE-DEPLOY.md) (Synapse/Fabric MPN limits)

---

## 1. One command runs the full lab

**Re-run safety:** If resources already exist for the same `LEARNER`, the orchestrator uses incremental ARM deploys and skips duplicate RBAC — it does **not** create a second copy of the estate. See [README re-run flow](../README.md#re-run-flow-resources-already-exist).

### Learner command (Windows)

```powershell
# First time on a fresh VM:
.\orchestrate.ps1 --install-cli

# Every lab session (full lab — default):
.\orchestrate.ps1 --skip-setup
```

### Learner command (Linux / macOS)

```bash
./orchestrate.sh --install-cli    # first time
./orchestrate.sh --skip-setup     # full lab
```

### What the orchestrator does (in order)

| Phase | Script function | Deploys / runs |
|-------|-----------------|----------------|
| 0 | `_ensure_dotenv`, `_ensure_venv`, `_ensure_azure_cli` | `.env`, Python venv, Azure CLI |
| 1 | `_ensure_az_login`, `_set_subscription` | `az login`, subscription context |
| 2 | `_deploy_bicep`, `_ensure_rbac` | Class-1: RG, tags, budget, KV, storage, medallion, lifecycle, RBAC |
| 3 | `_deploy_platforms`, `_create_fabric_workspace` | ADF, Purview, Databricks; Synapse/Fabric (best-effort) |
| 4 | `_verify` | `verify_cost.py` — SKU allow-list + MTD cost |
| 5 | `_compare_platforms` | `compare_platform_costs.py` — list prices + Cost Explorer URLs |
| 6 | `_print_summary` | All portal links |

### Orchestrator modes

| Flag | Use when |
|------|----------|
| *(default)* | **Full lab** — Class-1 + platforms + verify + cost compare |
| `--class1-only` | Day 1 only — landing zone, no ADF/Purview/Databricks |
| `--platforms-only` | Day 2 — platforms after Class-1 already exists |
| `--skip-setup` | CLI and venv already installed |
| `--skip-verify` | Skip SKU/cost gate (not recommended) |
| `--skip-compare` | Skip list-price comparison table |
| `--use-device-code` | Headless VM / no browser for `az login` |
| `--install-cli` | First run — install Azure CLI via winget/apt/brew |

---

## 2. Pre-class checklist (trainer)

- [ ] Learners have **Contributor** or **Owner** on a training subscription
- [ ] `.env` filled: `AZURE_SUBSCRIPTION_ID`, `LEARNER`, `OWNER_EMAIL`, `LOCATION=uksouth`
- [ ] Explain: passwords never go in chat — `az login` only
- [ ] **MPN/MSDN subscriptions:** warn that **Synapse** and **Fabric capacity** may be blocked (see Section 7)
- [ ] Budget alert email goes to `OWNER_EMAIL`
- [ ] Teardown command ready: `python scripts/teardown.py --resource-group rg-<learner>-class1 --yes`

---

## 3. What gets deployed (full lab)

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
| Synapse | `syn-<learner>-<hash>` | Often **blocked** on MPN |
| Fabric capacity | `fc<learner><hash>` | Often **quota 0** on MPN |
| Fabric workspace | `ws-<learner>-class1` | Manual trial if capacity blocked |

---

## 4. Teaching flow (recommended 2-day schedule)

### Day 1 — Class-1 only (~4 h)

```powershell
.\orchestrate.ps1 --class1-only --skip-setup
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

```powershell
.\orchestrate.ps1 --skip-setup
```

| Block | Open in IDE | Open in portal | Concept |
|-------|-------------|----------------|---------|
| 1 | `platform-services.bicep` ADF section | Data Factory blade | Orchestration, linked service |
| 2 | Purview section | web.purview.azure.com | Governance catalog |
| 3 | Databricks section | Databricks workspace | Lakehouse (no cluster demo) |
| 4 | `compare_platform_costs.py` output | Cost Explorer | Cost ranking discussion |
| 5 | Manual | app.fabric.microsoft.com | Fabric trial + workspace |

---

## 5. When to open what in the portal (trainer demo script)

After **full lab** orchestrator completes, walk the portal in this order:

1. **Resource groups** → `rg-<learner>-class1` → **Tags** (seven governance tags)
2. **Cost Management** → **Budgets** → `budget-<learner>-class1` (spend = 0 or near-zero)
3. **Key Vault** → Properties → Permission model = **Azure RBAC**
4. **Storage account** → Configuration → **Hierarchical namespace = Enabled**
5. **Storage** → **Containers** → bronze, silver, gold, audit
6. **Storage** → **Lifecycle management** → 7d delete rule
7. **Data Factory** → Author & Monitor (show linked ADLS service)
8. **Purview** → Data Map (open via web.purview.azure.com link from summary)
9. **Databricks** → Workspace (show **no cluster** = no DBU burn)
10. **Cost analysis** → filter by resource group → MTD cost
11. **Fabric** → https://app.fabric.microsoft.com → Start trial → New workspace (if Azure capacity blocked)

---

## 6. Code map for trainers

```
orchestrate.ps1 / orchestrate.sh     → launchers
scripts/orchestrate.py               → ONE orchestrator (all phases)
infra/main.bicep                     → Class-1 landing zone
infra/platform-services.bicep        → ADF, Synapse, Purview, Fabric, Databricks
scripts/verify_cost.py               → Post-deploy SKU + MTD cost gate
scripts/compare_platform_costs.py    → List-price ranking + Cost Explorer URLs
scripts/fabric_workspace.py          → Fabric workspace REST API (after capacity exists)
scripts/teardown.py                  → Delete entire RG
```

### Build order (never change)

```text
RG + tags → budget → Key Vault → storage + containers + lifecycle → RBAC
         → ADF → (Synapse) → Purview → (Fabric capacity) → Databricks → verify
```

---

## 7. MPN / training subscription limits (tell learners upfront)

| Service | Typical error | Trainer action |
|---------|---------------|----------------|
| **Synapse** | `SqlServerRegionDoesNotAllowProvisioning` | Skip in Bicep (`deploySynapse=false`); use Fabric serverless SQL after trial |
| **Fabric capacity** | `RegionalQuota: 0` | Learners use **Fabric trial** at app.fabric.microsoft.com |
| **Purview** | Tenant free tier in `eastus` | Deploy Purview in **eastus** (orchestrator does this automatically) |

Orchestrator handles these gracefully: retries without Fabric, skips Synapse on MPN, still completes verify + summary.

---

## 8. Cost teaching points

Run orchestrator — Phase 5 prints list-price ranking. Key messages:

1. **Class-1 (storage + KV)** — pennies MTD when empty; no fixed monthly fee
2. **ADF factory** — free at rest; pay per pipeline/IR run
3. **Synapse serverless** — $0 when no queries (if deployable)
4. **Purview** — capacity/catalog metering; not idle-cheap
5. **Databricks** — DBU charges when clusters run; workspace alone is lower
6. **Fabric F0/F2** — capacity units; trial avoids Azure quota issues

**Cost Explorer link** (printed at end of every run):

```text
https://portal.azure.com/#view/Microsoft_Azure_CostManagement/Menu/~/costanalysis/open/scope/.../rg-<learner>-class1
```

---

## 9. Troubleshooting (trainer quick reference)

| Symptom | Fix |
|---------|-----|
| `az` not found | `.\orchestrate.ps1 --install-cli` or new terminal |
| `az login` MFA | `az login --use-device-code` |
| Bicep RBAC `RoleDefinitionDoesNotExist` | Orchestrator `_ensure_rbac` CLI fallback runs automatically |
| `verify_cost.py` fails on platforms | Re-run with platforms allowed: orchestrator passes `--include-platforms` |
| Purview EventHub error | Orchestrator registers `Microsoft.EventHub` in Phase 3 |
| Fabric workspace API 403 | Enable Fabric trial; add user as capacity admin |
| Portal cost column `-` | Normal first 24–48h; use Budget blade + verify script |

---

## 10. Teardown and hygiene

```powershell
.\.venv\Scripts\python.exe scripts\teardown.py --resource-group rg-<learner>-class1 --yes
```

Also delete Databricks **managed RG** `rg-<learner>-dbw-<hash>` if orphaned (orchestrator summary lists name).

Remind learners: never delete the subscription — delete the resource group only.

---

## 11. Deliverables checklist (end of lab)

| # | Deliverable | Evidence |
|---|-------------|----------|
| 1 | Full orchestrator exit 0 | Terminal: `Orchestration complete (full-lab)` |
| 2 | Class-1 resources | KV + Storage + budget in portal |
| 3 | ADF + Databricks + Purview | Platform blades visible |
| 4 | `verify_cost.py` pass | MTD cost in terminal output |
| 5 | Cost compare table | Orchestrator Phase 5 output |
| 6 | Fabric workspace | Portal screenshot OR manual trial note |

---

## 12. Example `.env` (trainer reference)

```env
AZURE_SUBSCRIPTION_ID=a802ddef-155b-481f-9796-fac7318a749f
LEARNER=jinesh
OWNER_EMAIL=v-jinesh@mastekus.onmicrosoft.com
LOCATION=uksouth
```

---

## 13. One-page learner handout (copy to slide)

```text
1. Clone repo, copy .env.example → .env, fill in your details
2. Run:  .\orchestrate.ps1 --install-cli   (first time only)
3. Run:  .\orchestrate.ps1 --skip-setup    (full lab)
4. Complete az login when browser opens
5. Open portal links from terminal summary
6. Teardown: python scripts/teardown.py --resource-group rg-<you>-class1 --yes
```

Trainer notes (this file): docs/TRAINER-NOTES.md
