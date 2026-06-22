# Bank of England Azure ETL — Class 1 Landing Zone

Training repository for the **Class 1** Azure data-platform landing zone. Learners provision a zero-fixed-cost medallion lake (bronze / silver / gold / audit) with Key Vault, governance tags, a consumption budget guardrail, and least-privilege RBAC.

**Trainers:** [docs/TRAINER-NOTES.md](docs/TRAINER-NOTES.md) — complete one-orchestrator guide, portal demo script, MPN limits.

**Learners:** [docs/CLASS-GUIDE.md](docs/CLASS-GUIDE.md) (timed blocks) | [docs/WORKFLOW-AND-CODE.md](docs/WORKFLOW-AND-CODE.md) (code + portal map).

## Prerequisites

- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) (`az login`)
- Python 3.11+
- Bash (for `deploy.sh`) or use the Python path on Windows via Git Bash / WSL

## Setup

### Windows (recommended for learners)

From **PowerShell** in the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup-windows.ps1
```

This script installs Azure CLI (via `winget`), creates `.venv`, installs Python dependencies, and copies `.env.example` → `.env`. Then:

1. Edit `.env` with your subscription ID, learner ID, and owner email.
2. Sign in: `az login`
3. Set subscription: `az account set --subscription <your-subscription-id>`

Re-run safely anytime — it skips steps already completed. Options: `-SkipAzureCliInstall`, `-SkipPythonSetup`.

### macOS / Linux / Git Bash

```bash
cp .env.example .env
# Edit .env with your subscription ID, learner ID, and owner email.
az login
```

## One-command orchestration (full lab)

**One script runs everything:** setup, Class-1 landing zone, ADF, Purview, Databricks, verify, cost compare, portal links.

```powershell
# First time on a fresh VM:
.\orchestrate.ps1 --install-cli

# Full lab (default):
.\orchestrate.ps1 --skip-setup

# Class-1 landing zone only (Day 1):
.\orchestrate.ps1 --class1-only --skip-setup
```

```bash
./orchestrate.sh --install-cli      # first time
./orchestrate.sh --skip-setup       # full lab
./orchestrate.sh --class1-only --skip-setup
```

| Flag | Purpose |
|------|---------|
| `--install-cli` | Install Azure CLI (winget / apt / brew) |
| `--class1-only` | Landing zone only — no platform services |
| `--platforms-only` | Platform services only (Class-1 must exist; **does not redeploy** landing zone) |
| `--skip-setup` | Skip venv / pip install |
| `--skip-compare` | Skip list-price comparison report |
| `--use-device-code` | Device-code login for headless VMs |

Trainer guide: [docs/TRAINER-NOTES.md](docs/TRAINER-NOTES.md)

## Re-run flow (resources already exist)

**Safe to run the orchestrator again.** It does **not** create duplicate resource groups, storage accounts, Key Vaults, or platform factories when the estate is already deployed for the same `LEARNER` in `.env`.

### How idempotency works

| Layer | If resource already exists | Behaviour |
|-------|---------------------------|-----------|
| Resource group | `rg-<learner>-class1` in Azure | `az group create` — no-op; tags refreshed |
| Class-1 Bicep (`deployment: main`) | KV, storage, budget, containers | **Incremental ARM deploy** — same names → update in place; missing pieces added only |
| Platform Bicep (`deployment: platforms`) | ADF, Purview, Databricks, etc. | Same incremental deploy; skipped services stay skipped |
| RBAC fallback | Role already on KV / storage | Lists assignments first — **skips** if present |
| Provider registration | `Microsoft.*` already registered | `az provider register` — no-op |
| Python venv / pip | `.venv` present | Skipped when you pass `--skip-setup` |
| Verify + cost compare | Estate unchanged | Re-reads live SKUs and MTD cost (report only) |

**Naming is deterministic:** Bicep uses `uniqueString(resourceGroup().id, learner)` so the same learner always maps to the same `kv-<learner>-<hash>` and `st<learner><hash>`. ARM matches existing resources instead of creating new ones.

### Decision flow

```text
.\orchestrate.ps1 --skip-setup
        |
        v
  .env LEARNER unchanged?
        |
   yes  |  no (different learner)
        |       └──> NEW hash suffix → NEW resources in same or new RG
        v
  rg-<learner>-class1 exists?
        |
   yes  |  no
        |       └──> Create RG → full Bicep deploy (first time)
        v
  Run deployment "main" (incremental)
        |
        +--> Budget / KV / storage / containers already OK → no recreate
        +--> Only missing child (e.g. container) → add that resource only
        v
  RBAC: assignment exists? → skip
        |
        v
  Run deployment "platforms" (incremental)
        |
        +--> ADF / Purview / Databricks exist → update in place
        +--> Synapse / Fabric blocked on MPN → warn and continue (as before)
        v
  verify_cost.py + compare_platform_costs.py → read-only checks
        |
        v
  Summary portal links (existing resource names)
```

### Which command when?

| Situation | Command |
|-----------|---------|
| First deploy (full lab) | `.\orchestrate.ps1 --skip-setup` |
| Re-run after success (verify only, no duplicate infra) | `.\orchestrate.ps1 --skip-setup` — same as first run; incremental deploy is safe |
| Class-1 done yesterday; add platforms today | `.\orchestrate.ps1 --platforms-only --skip-setup` — **skips** Class-1 Bicep; discovers storage from live RG |
| Re-check SKUs / cost without redeploying | `python scripts/verify_cost.py --resource-group rg-<learner>-class1 --include-platforms` |
| Tear down and start fresh | `python scripts/teardown.py --resource-group rg-<learner>-class1 --yes` then full orchestrator again |

### What is *not* duplicated

- No second resource group for the same learner
- No second storage account or Key Vault (fixed hash per learner + RG)
- No duplicate RBAC role assignments (checked before `role assignment create`)
- No duplicate ADF factory / Databricks workspace / Purview account names (same Bicep names on redeploy)

### What may still run on every full re-run

- **Bicep deployments** — ARM compares desired vs actual state; unchanged resources show *no change* in deployment history
- **Verify and cost compare** — always read live subscription data (no new paid resources)
- **Fabric workspace API** — only if Fabric capacity was deployed; if workspace name already exists, API may warn (create manually in [Fabric portal](https://app.fabric.microsoft.com) if needed)

**Trainer note:** Tell learners that re-running `.\orchestrate.ps1 --skip-setup` after a successful lab is expected — it confirms the estate and refreshes the summary links, it does not bill again for duplicate infrastructure. See [docs/TRAINER-NOTES.md](docs/TRAINER-NOTES.md) Section 9 for troubleshooting.

## Deploy (manual paths — optional)

**Bicep path** (recommended for IaC learners):

```bash
bash deploy.sh
```

**Python path** (SDK learners):

```bash
pip install -r requirements.txt
python scripts/provision.py --subscription-id <your-subscription-id>
```

## Verify

```bash
python scripts/verify_cost.py --resource-group rg-<learner>-class1
```

## Teardown

```bash
python scripts/teardown.py --resource-group rg-<learner>-class1
# Non-interactive:
python scripts/teardown.py --resource-group rg-<learner>-class1 --yes
```

## Layout

| Path | Purpose |
|------|---------|
| `infra/main.bicep` | Resource-group-scoped IaC: budget, Key Vault, storage, lifecycle, RBAC |
| `deploy.sh` | Creates tagged resource group, deploys Bicep |
| `scripts/provision.py` | Same estate via azure-mgmt SDKs (idempotent) |
| `scripts/verify_cost.py` | SKU allow-list check + month-to-date cost query |
| `scripts/teardown.py` | Deletes the single resource group |
| `scripts/setup-windows.ps1` | One-shot Windows setup: Azure CLI, venv, `.env` |
| `scripts/orchestrate.py` | **One orchestrator** — full lab (default) |
| `orchestrate.ps1` / `orchestrate.sh` | OS launchers |
| `infra/platform-services.bicep` | ADF, Synapse, Purview, Fabric, Databricks |
| `scripts/fabric_workspace.py` | Fabric workspace via REST API |
| `scripts/compare_platform_costs.py` | List-price compare + Cost Explorer links |
| `docs/TRAINER-NOTES.md` | **Trainer master guide** (one orchestrator, demo script) |
| `docs/CLASS-GUIDE.md` | Timed learner blocks |
| `docs/WORKFLOW-AND-CODE.md` | Code + portal walkthrough |
| `docs/GOVERNANCE-DEPLOY.md` | Synapse/Fabric MPN workarounds |

## Build order

Resource group + tags → budget guardrail → Key Vault → storage + containers + lifecycle → RBAC role assignments.

## Platform services (included in full lab)

The default orchestrator deploys `infra/platform-services.bicep` after Class-1.

| Service | Auto-deploy | MPN note |
|---------|-------------|----------|
| Azure Data Factory | Yes | Idle ~$0 |
| Azure Databricks | Yes | No cluster by default |
| Microsoft Purview | Yes | Deployed in `eastus` |
| Synapse serverless SQL | Best-effort | Often blocked on MPN |
| Microsoft Fabric | Best-effort | Use Fabric trial if quota = 0 |

See [docs/GOVERNANCE-DEPLOY.md](docs/GOVERNANCE-DEPLOY.md) and [docs/TRAINER-NOTES.md](docs/TRAINER-NOTES.md).

## Compare platform costs

List-price comparison runs automatically at the end of the **full lab** orchestrator (Phase 5).

**Training idle ranking (typical UK South list prices):** Class-1 storage + Key Vault (pennies MTD) → ADF factory idle → Synapse serverless SQL (no query = no scan charge) → Purview / SQL Database (fixed or capacity footprint) → Databricks (DBU when clusters run).

## Cost note

Every resource in this estate uses SKUs with **no fixed monthly fee**: a **StorageV2** account on **Standard_LRS** / **Hot** tier (pay-per-GB stored and per-transaction only), a **Key Vault Standard** vault (pay-per-secret-operation only), **Consumption budgets** and **role assignments** (free), and no always-on compute. With the 7-day lifecycle delete rule on training blobs, realistic month-to-date spend is **pennies** — typically well under £0.10 unless large files are uploaded and retained. The £1/month budget with 50 % / 90 % alerts is a free guardrail, not a charge.
