# Bank of England Azure ETL — Class 1 Landing Zone

Training lab for a governed, low-cost Azure data platform. **Windows only.** One entry point: `orchestrate.cmd`.

**Trainers:** [docs/TRAINER-NOTES.md](docs/TRAINER-NOTES.md)  
**Timed class flow:** [docs/CLASS-GUIDE.md](docs/CLASS-GUIDE.md)  
**Code + portal map:** [docs/WORKFLOW-AND-CODE.md](docs/WORKFLOW-AND-CODE.md)

---

## A. What & why — concepts before code

Read this section first. Each component has **what it is**, **why we use it**, and **where the code lives**.

### Resource Group

**What:** A folder in Azure that holds all lab resources together.  
**Why:** One blast radius — delete the group and everything goes away. Tags on the group teach governance.  
**Analogy:** A labelled moving box for one learner's entire lab estate.  
**Code:** `scripts/orchestrate.py` → `_deploy_bicep()` creates `rg-<learner>-class1` in **uksouth** or **ukwest** only.

### Bicep (Infrastructure as Code)

**What:** A declarative language that describes Azure resources in text files.  
**Why:** The whole environment is rebuilt identically every time — no random portal clicking.  
**Analogy:** The architect's blueprint for your Azure house.  
**Code:** `infra/main.bicep` (Class-1), `infra/platform-services.bicep` (ADF, Purview, Databricks, etc.)

### Consumption Budget

**What:** A free cost guardrail that emails you when spend crosses thresholds.  
**Why:** Deployed **before** storage so learners feel cost governance before any data lands.  
**Analogy:** A £1 smoke alarm on your wallet — it does not charge you; it warns early.  
**Code:** `infra/main.bicep` — `Microsoft.Consumption/budgets`, £1/month, 50% / 90% alerts.

### Key Vault

**What:** A secure store for secrets, keys, and certificates.  
**Why:** Secrets never live in code or chat; RBAC mode means even Owner needs an explicit role.  
**Analogy:** A bank safe — being the building owner does not mean you can open every drawer.  
**Code:** `infra/main.bicep` — `Microsoft.KeyVault/vaults`, Standard SKU, RBAC enabled.

### ADLS Gen2 (Azure Data Lake Storage)

**What:** A storage account with hierarchical namespace (HNS) for data-lake workloads.  
**Why:** Bronze / silver / gold medallion containers teach the lakehouse pattern.  
**Analogy:** A warehouse with named aisles (containers) instead of one big pile.  
**Code:** `infra/main.bicep` — `StorageV2`, `Standard_LRS`, Hot tier, HNS on.

### Medallion containers (bronze, silver, gold, audit)

**What:** Four blob containers representing raw → refined → curated → audit data.  
**Why:** Industry-standard pattern for progressive data quality.  
**Analogy:** Assembly line stations — raw parts, cleaned parts, finished product, inspection log.  
**Code:** `infra/main.bicep` — `[for name in containerNames]` loop (idempotent on redeploy).

### Lifecycle management

**What:** Rules that move or delete blobs automatically by age.  
**Why:** Training data should not linger; 7-day delete keeps cost near zero.  
**Analogy:** Self-emptying bins so you never forget to take the rubbish out.  
**Code:** `infra/main.bicep` — lifecycle policy on the storage account.

### RBAC (Role-Based Access Control)

**What:** Permissions assigned to identities on specific resources.  
**Why:** Subscription Owner ≠ data-plane access. Least privilege is explicit.  
**Analogy:** Building keys vs room keys — you get only the doors you need.  
**Code:** `infra/main.bicep` + `scripts/orchestrate.py` → `_ensure_rbac()` (check-before-create).

### Azure Data Factory (ADF)

**What:** Cloud orchestration for data movement and transformation pipelines.  
**Why:** Teaches ETL orchestration; factory idle cost is ~£0.  
**Analogy:** A train timetable — schedules when data moves, does not run engines 24/7 by default.  
**Code:** `infra/platform-services.bicep` — `Microsoft.DataFactory/factories`.

### Microsoft Purview

**What:** Data governance and catalog service.  
**Why:** Teaches lineage, classification, and catalog concepts.  
**Region exception:** Deployed in **eastus** (not UK) because MPN tenant free-tier conflicts in UK regions. Documented and automated.  
**Code:** `infra/platform-services.bicep` — `purviewLocation=eastus`.

### Azure Databricks

**What:** Apache Spark analytics platform integrated with the lake.  
**Why:** Teaches lakehouse compute; **no cluster** deployed by default (no DBU burn).  
**Analogy:** A workshop with the lights off until you turn on a machine.  
**Code:** `infra/platform-services.bicep` — Premium workspace, no cluster resource.

### Synapse Analytics (serverless SQL)

**What:** SQL query engine over data lake files.  
**Why:** Alternative SQL surface; often **blocked on MPN** subscriptions — orchestrator skips cleanly.  
**Code:** `infra/platform-services.bicep` — `deploySynapse=false` by default on MPN.

### Microsoft Fabric

**What:** Unified analytics (lakehouse, warehouse, Power BI) on Fabric capacity.  
**Why:** Teaches modern Microsoft analytics stack; capacity often **quota 0** on MPN — use Fabric trial.  
**Code:** `infra/platform-services.bicep` + `scripts/fabric_workspace.py` (check-before-create).

### verify_cost.py (£0 gate)

**What:** Post-deploy script that checks every resource SKU against an allow-list and reads MTD cost.  
**Why:** Proves the lab stayed on cheapest tiers; fails loud if something expensive slipped in.  
**Code:** `scripts/verify_cost.py`

### orchestrate.cmd (single entry point)

**What:** The one Windows command learners run. Bootstraps CLI, venv, pip, `.env`, deploy, verify.  
**Why:** Guardrail-first, idempotent, no PowerShell policy changes, no bash.  
**Code:** `orchestrate.cmd` → `scripts/orchestrate.py`

---

## B. One-command quick start (Windows)

**You need:** Python 3.11+ on PATH ([python.org/downloads](https://www.python.org/downloads/) — tick **Add to PATH**).

### First time (clean machine)

```text
orchestrate.cmd --install-cli
```

| Step | What happens |
|------|----------------|
| 1 | Creates `.env` from template — **edit** `AZURE_SUBSCRIPTION_ID`, `LEARNER`, `OWNER_EMAIL`, `LOCATION=uksouth` |
| 2 | Run the **same command again** after saving `.env` |
| 3 | Installs Azure CLI (winget), creates `.venv`, pip installs, `az login`, deploys full lab |

### Full lab (default)

```text
orchestrate.cmd
```

Deploys Class-1 + ADF + Purview + Databricks + verify + cost compare. **Safe to re-run.**

### Day 1 only (landing zone)

```text
orchestrate.cmd --class1-only
```

### Teardown

```text
orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes
```

---

## C. The re-run command

There is **one** idempotent re-run command:

```text
orchestrate.cmd
```

Running it 1 or 10 times reaches the **same end state** — no duplicate resources, no crash, no manual cleanup.

| Mechanism | How idempotency is enforced |
|-----------|----------------------------|
| Deterministic names | Bicep `uniqueString(resourceGroup().id, learner)` — same learner → same KV/storage names |
| ARM incremental deploy | `--mode Incremental` on deployments `main` and `platforms` — update in place |
| Resource group | `az group create` is upsert; logs `already present` on re-run |
| RBAC | `_ensure_rbac()` lists assignments first — logs `already assigned` and skips |
| Fabric workspace | `fabric_workspace.py` lists workspaces — logs `already present` and skips |
| MPN blocks | Synapse / Fabric capacity warn and continue — lab does not hard-fail |

Optional trainer speed flag: `--skip-setup` skips venv/pip when already installed.

---

## D. Four re-run scenarios (all safe with `orchestrate.cmd`)

| # | Scenario | Command sequence | Expected outcome |
|---|----------|------------------|------------------|
| **1 — Fresh machine** | Nothing deployed yet | `orchestrate.cmd --install-cli` → edit `.env` → `orchestrate.cmd --install-cli` | Creates RG + resources once; deterministic names |
| **2 — After full success** | Estate complete | `orchestrate.cmd` | Incremental deploy no-ops; RBAC skip logs; verify re-reads MTD |
| **3 — After partial failure** | Some resources exist | `orchestrate.cmd` | `_discover_outputs()` reads live RG; Bicep fills gaps; MPN skips warn only |
| **4 — After teardown** | RG deleted | `orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes` then `orchestrate.cmd` | Full recreate with **same** names (same learner) |

---

## E. Flags reference

| Flag | Purpose |
|------|---------|
| `--install-cli` | Install Azure CLI via winget (first-time bootstrap) |
| `--class1-only` | Landing zone only — no platform services |
| `--platforms-only` | Platforms only (Class-1 must already exist) |
| `--skip-setup` | Skip venv / pip (optional speed flag) |
| `--skip-verify` | Skip SKU/cost gate (not recommended) |
| `--skip-compare` | Skip list-price comparison |
| `--use-device-code` | Headless `az login` |

---

## F. Code map — go to the file

| Component | File |
|-----------|------|
| Single entry (Windows) | `orchestrate.cmd` |
| All orchestration logic | `scripts/orchestrate.py` |
| Class-1 Bicep | `infra/main.bicep` |
| Platform Bicep | `infra/platform-services.bicep` |
| SDK provision path (optional) | `scripts/provision.py` |
| SKU + MTD cost gate | `scripts/verify_cost.py` |
| List-price comparison | `scripts/compare_platform_costs.py` |
| Fabric workspace API | `scripts/fabric_workspace.py` |
| Teardown | `scripts/teardown.py` |
| Windows bootstrap helper | `scripts/setup-windows.ps1` |
| Guardrails for AI + humans | `.cursorrules` |

---

## G. Build order (never change)

```text
RG + tags → budget → Key Vault → storage + containers + lifecycle → RBAC
         → ADF → (Synapse skip on MPN) → Purview (eastus) → (Fabric best-effort) → Databricks → verify
```

---

## H. Cost & region guardrails

| Rule | Value |
|------|-------|
| Primary region | `uksouth` |
| Failover region | `ukwest` |
| Purview exception | `eastus` — MPN tenant free-tier conflict |
| SKUs | Cheapest viable — enforced by `verify_cost.py` |
| Budget | £1/month guardrail (free resource) |
| Typical MTD | Pennies when lake is empty |

---

## I. Platform services & MPN limits

| Service | Auto-deploy | MPN note |
|---------|-------------|----------|
| Azure Data Factory | Yes | Idle ~£0 |
| Azure Databricks | Yes | No cluster by default |
| Microsoft Purview | Yes | **eastus** (documented exception) |
| Synapse serverless SQL | Skipped | `SqlServerRegionDoesNotAllowProvisioning` on MPN |
| Microsoft Fabric capacity | Best-effort | Quota 0 → use [Fabric trial](https://app.fabric.microsoft.com) |

Details: [docs/GOVERNANCE-DEPLOY.md](docs/GOVERNANCE-DEPLOY.md)

---

## J. Layout

| Path | Purpose |
|------|---------|
| `orchestrate.cmd` | **Single Windows entry point** |
| `scripts/orchestrate.py` | Full lab orchestrator |
| `infra/main.bicep` | Budget, KV, storage, medallion, lifecycle, RBAC |
| `infra/platform-services.bicep` | ADF, Synapse, Purview, Fabric, Databricks |
| `scripts/verify_cost.py` | SKU allow-list + MTD cost |
| `scripts/teardown.py` | Delete resource group |
| `docs/TRAINER-NOTES.md` | Trainer master guide |
| `docs/CLASS-GUIDE.md` | Timed learner blocks |
| `docs/WORKFLOW-AND-CODE.md` | Code + portal walkthrough |
| `.cursorrules` | Standing guardrails |
