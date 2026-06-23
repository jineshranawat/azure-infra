# Bank of England Azure ETL — Class 1 Landing Zone

Training lab for a governed, low-cost Azure data platform. **Windows only.**

| Doc | Link |
|-----|------|
| **Coverage map (start here)** | **[COVERAGE-MAP.md](COVERAGE-MAP.md)** — step-by-step: which session, doc, command, portal |
| Trainers | [docs/TRAINER-NOTES.md](docs/TRAINER-NOTES.md) |
| Timed class | [docs/CLASS-GUIDE.md](docs/CLASS-GUIDE.md) |
| Code + portal | [docs/WORKFLOW-AND-CODE.md](docs/WORKFLOW-AND-CODE.md) |
| Session 2 (ADF) | [session-2/MANUAL-LAB.md](session-2/MANUAL-LAB.md) |

---

## 1. How to run on Windows (step by step)

Follow these steps in order. You only need **one command** after setup: `orchestrate.cmd`.

### Prerequisites (install once)

| # | Install | Link |
|---|---------|------|
| 1 | **Python 3.11+** — tick **Add python.exe to PATH** | [python.org/downloads](https://www.python.org/downloads/) |
| 2 | **Git** (to clone the repo) | [git-scm.com/download/win](https://git-scm.com/download/win) |
| 3 | **Azure training subscription** with Contributor or Owner | Your trainer provides this |

Azure CLI is installed automatically by the lab (`winget`). Manual install if needed: [Install Azure CLI on Windows](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows).

---

### Step 0 — Clone the repo and open a terminal

```text
git clone https://github.com/jineshranawat/azure-infra-class.git
cd azure-infra-class
```

Open **Command Prompt** (recommended) or **PowerShell** in the repo folder (`Shift + right-click` → *Open in Terminal*, or `cd` to the folder).

> **Use Command Prompt + `orchestrate.cmd`** when possible — you do **not** need to change PowerShell execution policy.

---

### Step 1 — First run (creates `.env`)

```text
orchestrate.cmd --install-cli
```

| What happens | You do |
|--------------|--------|
| Script copies `.env.example` → `.env` | Open `.env` in Notepad |
| Script stops and asks you to edit | Set `AZURE_SUBSCRIPTION_ID`, `LEARNER`, `OWNER_EMAIL`, `LOCATION=uksouth` |
| | Save the file |

Example `.env`:

```env
AZURE_SUBSCRIPTION_ID=your-subscription-guid-here
LEARNER=yourname
OWNER_EMAIL=you@yourcompany.com
LOCATION=uksouth
```

---

### Step 2 — Second run (bootstrap + deploy)

Run the **same command again**:

```text
orchestrate.cmd --install-cli
```

| Phase | What happens |
|-------|----------------|
| Bootstrap | Creates `.venv`, installs Python packages, installs Azure CLI (winget) if missing |
| Login | Browser opens for `az login` — sign in with your training account |
| Deploy | Class-1 landing zone + ADF + Purview + Databricks + verify + cost summary |
| Time | Allow **10–20 minutes** — do not close the window |

---

### Step 3 — Re-run anytime (safe)

```text
orchestrate.cmd
```

Safe to run again after success, after a partial failure, or to refresh verify/summary. **No duplicate resources** (incremental deploy).

---

### Other commands

| Goal | Command |
|------|---------|
| Day 1 — landing zone only | `orchestrate.cmd --class1-only` |
| Platforms only (Class-1 already exists) | `orchestrate.cmd --platforms-only` |
| Headless login (no browser) | `orchestrate.cmd --use-device-code` |
| Teardown | `orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes` |

---

### PowerShell execution policy — when you need Bypass

| How you run | Execution policy change needed? |
|-------------|--------------------------------|
| **`orchestrate.cmd` in Command Prompt** (recommended) | **No** — after `.venv` exists, `.cmd` calls Python directly |
| **First run before `.venv` exists** | **No manual change** — `orchestrate.cmd` calls PowerShell with **Bypass automatically** |
| **You run `orchestrate.ps1` directly** | **Yes** — use Bypass on that command only |

**Recommended — always use `.cmd`:**

```text
orchestrate.cmd --install-cli
orchestrate.cmd
```

**If you must run the `.ps1` file directly** (e.g. PowerShell is your only shell):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\orchestrate.ps1 --install-cli
powershell -NoProfile -ExecutionPolicy Bypass -File .\orchestrate.ps1
```

`-ExecutionPolicy Bypass` applies **only to that one process** — it does not change your machine permanently.

Read more: [about_Execution_Policies (Microsoft Learn)](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_execution_policies)

**Optional — setup script instead of first orchestrate run:**

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup-windows.ps1
```

Then edit `.env` and run `orchestrate.cmd --install-cli`.

---

### When to use Cursor / VS Code AI chat

Use **AI chat in Cursor or VS Code** (Copilot Chat) when you are **inside this repo** and need help **reading** the lab — not when you need **Azure account access**.

| Use AI chat for | Do **not** use AI for |
|-----------------|----------------------|
| "What does this error mean in `orchestrate.py`?" | Your password, MFA codes, or pasting secrets into chat |
| "Which file defines the budget?" | Choosing subscription IDs or learner names (use `.env`) |
| "Explain the medallion containers in `main.bicep`" | Fixing Azure quota blocks (MPN limits — see workarounds below) |
| "What should I run after a partial deploy?" | Deleting production resources outside your `rg-<learner>-class1` |
| "Summarise README step 2" | Sharing `.env` contents or service principal keys |

**How to ask (copy-paste template):**

```text
I'm on Windows running orchestrate.cmd for the BoE Azure ETL Class-1 lab.
Error: [paste last 10 lines from terminal]
My step: [e.g. Step 2, first deploy]
LEARNER=xxx, LOCATION=uksouth (no secrets)
What file should I open and what command should I try next?
```

**When to ask your trainer instead of AI:** login denied, no subscription access, corporate proxy blocking Azure, or the whole class hits the same MPN block.

**When to just re-run (no AI needed):** script stopped after creating `.env`; `already present` / `already assigned` logs; Synapse or Fabric **WARN** lines on MPN subs.

---

### Failures & workarounds (short guide)

#### A — Machine & setup (before Azure)

| Symptom | Likely cause | Workaround |
|---------|--------------|------------|
| `python is not recognized` | Python not on PATH | Install [Python 3.11+](https://www.python.org/downloads/); tick **Add to PATH**; **new** terminal |
| `Python 3.11+ required` | Old Python version | Upgrade Python; re-run `orchestrate.cmd --install-cli` |
| `winget not found` | Corporate image / old Windows | Install [Azure CLI manually](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows); re-run |
| `az is not recognized` (after install) | PATH not refreshed | Close terminal; open new one; or reboot once |
| PowerShell: *running scripts is disabled* | Execution policy | Use **`orchestrate.cmd`** in **Command Prompt**, or `powershell -ExecutionPolicy Bypass -File .\orchestrate.ps1` |
| Script exits immediately after creating `.env` | **Expected** first-run behaviour | Edit `.env`; run `orchestrate.cmd --install-cli` again |
| `AZURE_SUBSCRIPTION_ID required` | Empty `.env` | Fill all fields; `LEARNER` = 2–10 lowercase letters/numbers only |
| `LOCATION must be uksouth or ukwest` | Wrong region in `.env` | Set `LOCATION=uksouth` (UK residency guardrail) |

#### B — Login & subscription

| Symptom | Likely cause | Workaround |
|---------|--------------|------------|
| Browser login loop / popup blocked | Browser or SSO policy | `orchestrate.cmd --use-device-code` — sign in on phone/another device |
| Work-account / MDM prompt or error `0x80192ee7` (-2145833241) on VDI | Azure CLI WAM broker triggers Intune enrollment | Orchestrator disables WAM automatically; re-run `orchestrate.cmd`. Still stuck? `orchestrate.cmd --use-device-code` — or untick *Allow my organization to manage my device* if the GUI appears |
| `az login` timeout | VPN / proxy | Try guest network; ask IT; use device code |
| Wrong subscription in portal | Multiple subs | Check `.env` GUID; trainer runs `az account set --subscription <id>` |
| `Authorization failed` / 403 on deploy | Insufficient role | Need **Contributor** or **Owner** on training subscription — ask trainer |
| `Could not resolve signed-in user object ID` | Stale login | `az logout` then `az login`; re-run `orchestrate.cmd` |

#### C — Deploy (Bicep / ARM)

| Symptom | Likely cause | Workaround |
|---------|--------------|------------|
| Bicep error then *checking partial estate* | Transient or RBAC glitch | **Re-run** `orchestrate.cmd` — incremental deploy + `_discover_outputs` resumes |
| `RoleDefinitionDoesNotExist` (RBAC) | Known Bicep GUID edge case | Orchestrator runs `_ensure_rbac` CLI fallback automatically — re-run once |
| `ResourceGroupNotFound` with `--platforms-only` | Class-1 not deployed | Run full lab or `orchestrate.cmd --class1-only` first |
| Deployment timeout / long hang | Azure API slow | Wait 20 min; do not close window; re-run if it exits with error |
| Purview / EventHub provider error | Provider not registered | Re-run — orchestrator registers providers idempotently |
| Name already exists (different learner) | `LEARNER` clash in shared sub | Pick a **unique** `LEARNER` id in `.env` |

#### D — MPN / expected warnings (lab still succeeds)

These are **not** failures — orchestrator warns and continues. Full detail: [docs/GOVERNANCE-DEPLOY.md](docs/GOVERNANCE-DEPLOY.md).

| Message | Meaning | Workaround |
|---------|---------|------------|
| `SqlServerRegionDoesNotAllowProvisioning` | Synapse blocked on MPN | **Skip** — use Fabric trial SQL later; lab completes without Synapse |
| `RegionalQuota: 0` (Fabric) | No Fabric capacity quota | Open [app.fabric.microsoft.com](https://app.fabric.microsoft.com) → **Start trial** → new workspace |
| `Platform deploy failed — retrying without Fabric` | Fabric F-SKU blocked | **Automatic** — ADF, Purview, Databricks still deploy |
| `Fabric capacity not deployed — skipping workspace` | No capacity ID | Fabric trial path above |
| Purview in **eastus** | Tenant free-tier rule | **By design** — UK resources stay in uksouth; Purview exception documented |

#### E — Verify, cost & portal (after deploy)

| Symptom | Likely cause | Workaround |
|---------|--------------|------------|
| `verify_cost.py failed` | SKU outside allow-list | Do not upgrade SKUs in portal; re-run deploy; check `scripts/verify_cost.py` output for resource name |
| Cost shows `-` in portal | Billing lag 24–48h | Normal — trust **Budget** blade + terminal MTD from verify script |
| `Month-to-date` > £0.10 | Large uploads retained | Check storage containers; lifecycle deletes after 7 days |
| Cannot upload to blob | RBAC not applied yet | Re-run `orchestrate.cmd`; confirm RBAC roles on storage in portal |
| Databricks managed RG extra cost | Normal side-effect | Low cost with **no cluster**; delete `rg-<learner>-dbw-*` at teardown if orphaned |

#### F — Teardown & start over

| Symptom | Likely cause | Workaround |
|---------|--------------|------------|
| Teardown asks for confirmation | Missing `--yes` | `orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes` |
| Delete stuck *in progress* | Azure async delete | Wait 5–15 min in portal; RG disappears when done |
| Want clean local machine too | `.venv` still present | Delete `.venv` folder optional; re-run `--install-cli` recreates it |
| Redeploy after teardown | Fresh estate | `orchestrate.cmd` — **same** `LEARNER` → **same** resource names |

#### G — Quick decision tree

```text
Error before "Signed in as ..."     → Section A or B
Error during Bicep deploy           → Re-run orchestrate.cmd once → Section C
WARN about Synapse / Fabric         → Section D (ignore if exit 0)
Error at verify_cost                → Section E
Need to understand code / concepts  → Section 2 below, or AI chat (see above)
Still stuck after one re-run        → Trainer + paste terminal output (no secrets)
```

---

### Four re-run scenarios (all use `orchestrate.cmd`)

| # | Situation | What to run |
|---|-----------|-------------|
| 1 | Fresh machine | `orchestrate.cmd --install-cli` → edit `.env` → `orchestrate.cmd --install-cli` |
| 2 | After full success | `orchestrate.cmd` |
| 3 | After partial failure | `orchestrate.cmd` (resumes; fills gaps) |
| 4 | After teardown | `orchestrate.cmd teardown --resource-group rg-<learner>-class1 --yes` then `orchestrate.cmd` |

---

## 2. Theory — what & why (concepts before code)

Read this after you know **how to run** the lab. Each component: **what it is**, **why we use it**, **where the code lives**.

### Resource Group

**What:** A folder in Azure that holds all lab resources together.  
**Why:** One blast radius — delete the group and everything goes away. Tags teach governance.  
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
**Region exception:** Deployed in **eastus** (not UK) because MPN tenant free-tier conflicts in UK regions.  
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
**Why:** Teaches modern Microsoft analytics stack; capacity often **quota 0** on MPN — use [Fabric trial](https://app.fabric.microsoft.com).  
**Code:** `infra/platform-services.bicep` + `scripts/fabric_workspace.py` (check-before-create).

### verify_cost.py (£0 gate)

**What:** Post-deploy script that checks every resource SKU against an allow-list and reads MTD cost.  
**Why:** Proves the lab stayed on cheapest tiers; fails loud if something expensive slipped in.  
**Code:** `scripts/verify_cost.py`

### orchestrate.cmd (single entry point)

**What:** The one Windows command learners run. Bootstraps CLI, venv, pip, `.env`, deploy, verify.  
**Why:** Guardrail-first, idempotent; `.cmd` avoids PowerShell policy issues on most runs.  
**Code:** `orchestrate.cmd` → `scripts/orchestrate.py`

---

## 3. Idempotency — why re-run is safe

| Mechanism | How |
|-----------|-----|
| Deterministic names | Bicep `uniqueString(resourceGroup().id, learner)` |
| ARM incremental deploy | `--mode Incremental` on deployments `main` and `platforms` |
| Resource group | `az group create` upsert; logs `already present` |
| RBAC | Lists assignments first — `already assigned` → skip |
| Fabric workspace | Lists workspaces — `already present` → skip |
| MPN blocks | Synapse / Fabric warn and continue — lab does not hard-fail |

Optional speed flag: `--skip-setup` skips venv/pip when already installed.

---

## 4. Flags reference

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

## 5. Code map

| Component | File |
|-----------|------|
| Single entry (Windows) | `orchestrate.cmd` |
| Internal PowerShell helper | `orchestrate.ps1` (called with Bypass before `.venv` exists) |
| All orchestration logic | `scripts/orchestrate.py` |
| Class-1 Bicep | `infra/main.bicep` |
| Platform Bicep | `infra/platform-services.bicep` |
| SKU + MTD cost gate | `scripts/verify_cost.py` |
| Teardown | `scripts/teardown.py` |
| Windows setup helper | `scripts/setup-windows.ps1` |

---

## 6. Build order (never change)

```text
RG + tags → budget → Key Vault → storage + containers + lifecycle → RBAC
         → ADF → (Synapse skip on MPN) → Purview (eastus) → (Fabric best-effort) → Databricks → verify
```

---

## 7. Cost & region guardrails

| Rule | Value |
|------|-------|
| Primary region | `uksouth` |
| Failover region | `ukwest` |
| Purview exception | `eastus` — MPN tenant free-tier conflict |
| SKUs | Cheapest viable — enforced by `verify_cost.py` |
| Budget | £1/month guardrail (free resource) |
| Typical MTD | Pennies when lake is empty |

---

## 8. Platform services & MPN limits

| Service | Auto-deploy | MPN note |
|---------|-------------|----------|
| Azure Data Factory | Yes | Idle ~£0 |
| Azure Databricks | Yes | No cluster by default |
| Microsoft Purview | Yes | **eastus** (documented exception) |
| Synapse serverless SQL | Skipped | Blocked on many MPN subs |
| Microsoft Fabric capacity | Best-effort | Quota 0 → Fabric trial |

Details: [docs/GOVERNANCE-DEPLOY.md](docs/GOVERNANCE-DEPLOY.md)

---

## 9. Repo layout

| Path | Purpose |
|------|---------|
| `orchestrate.cmd` | **Start here — single Windows entry point** |
| `scripts/orchestrate.py` | Full lab orchestrator |
| `infra/main.bicep` | Budget, KV, storage, medallion, lifecycle, RBAC |
| `infra/platform-services.bicep` | ADF, Synapse, Purview, Fabric, Databricks |
| `docs/TRAINER-NOTES.md` | Trainer master guide |
| `docs/CLASS-GUIDE.md` | Timed learner blocks |
| `docs/WORKFLOW-AND-CODE.md` | Code + portal walkthrough |
| `COVERAGE-MAP.md` | **Master index** — session, doc, command, portal step-by-step |
| `session-2/` | Session 2 — ADF lab + [manual portal steps](session-2/MANUAL-LAB.md) |

---

## 10. Azure Portal — complete novice practical guide

Use this **after** `orchestrate.cmd` succeeds. Goal: map **what you deployed in code** to **what you see in the UI**, and learn safe hands-on steps every data engineer uses on day one.

**Portal URL:** [https://portal.azure.com](https://portal.azure.com)  
**Sign in:** same account you used for `az login`.

---

### 10.1 Portal layout (5-minute orientation)

| UI area | Where to find it | What it is |
|---------|------------------|------------|
| **Home** | Top-left ☰ → Home | Dashboard; recent resources |
| **Search bar** | Top centre | Type resource name (`rg-jinesh-class1`, `st...`, `kv-...`) |
| **Cloud Shell** | Top bar `>_` icon | Browser terminal (optional — you already use `orchestrate.cmd`) |
| **Notifications** | Bell icon | Deploy success/failure toasts |
| **Settings (gear)** | Your profile area | Directory + subscription switcher |
| **Subscriptions** | Search → *Subscriptions* | Billing boundary; copy **Subscription ID** for `.env` |
| **Resource groups** | Search → *Resource groups* | Folders holding your lab — start here every time |

**Novice rule:** Almost everything in this lab lives in **one** resource group: `rg-<learner>-class1`. Filter there first — never hunt the whole subscription.

---

### 10.2 Walkthrough — Class-1 landing zone (portal clicks)

Do these in order. Replace `<learner>` with your `.env` value.

#### Step 1 — Resource group & tags

1. Search → **Resource groups** → open `rg-<learner>-class1`
2. **Overview** — confirm **Location** = *UK South* or *UK West*
3. **Tags** (left menu) — confirm seven tags: `env`, `owner`, `costcentre`, `data-class`, `course`, `class`, `auto-teardown`
4. **Resource visualizer** (optional) — see budget, KV, storage as a diagram

**Theory:** Tags are metadata for cost, ownership, and automation. Code sets them in `infra/main.bicep` and `orchestrate.py` `_tag_args()`.

#### Step 2 — Budget (cost guardrail)

1. Top search → **Cost Management + Billing** → **Budgets**
2. Open `budget-<learner>-class1`
3. Check **Amount** = £1, **Scope** = your resource group, **Alerts** = 50% / 90%

**Practical exercise:** Note *Current spend* (often £0.00). This is your early-warning system — not a charge.

**Theory:** Budget deploys **before** storage in Bicep so governance exists before data.

#### Step 3 — Key Vault

1. RG → resource `kv-<learner>-<hash>`
2. **Overview** — Status = *Succeeded*
3. **Properties** — SKU = *Standard*, Permission model = **Azure role-based access control**
4. **Access control (IAM)** → **View my access** — you should see *Key Vault Secrets Officer* (after orchestrator RBAC)

**Practical exercise:** Open **Secrets** — list is empty (good). Do **not** store training passwords here unless your exercise requires it.

**Theory:** RBAC vault = even subscription Owner needs an explicit role to read secrets.

#### Step 4 — Storage account (ADLS Gen2)

1. RG → `st<learner><hash>`
2. **Overview** — Performance = *Standard*, Replication = *LRS*
3. **Configuration** — **Hierarchical namespace** = *Enabled* (this makes it Gen2 / data-lake capable)
4. **Containers** — open each: `bronze`, `silver`, `gold`, `audit` (all empty at first)
5. **Lifecycle management** — rule: move to Cool at 30 days, delete at 7 days (training hygiene)
6. **Access control (IAM)** — *Storage Blob Data Contributor* on your user

**Practical exercise — upload a test file (safe):**

1. **Containers** → `bronze` → **Upload**
2. Upload a small `.txt` file (e.g. `hello.txt`)
3. Open the blob → **Properties** → note URL and tier *Hot*

**Practical exercise — prove RBAC matters:**

- Before RBAC: upload may fail with *AuthorizationPermissionMismatch*
- After orchestrator assigns roles: upload succeeds

**Theory:** Medallion layout = bronze (raw) → silver (cleaned) → gold (curated) → audit (lineage/logs).

#### Step 5 — IAM on the resource group (optional deeper look)

1. RG → **Access control (IAM)**
2. **Role assignments** tab — see your roles on child resources (may also appear on KV/storage directly)

**Theory:** Control plane (portal deploy) ≠ data plane (read/write blobs). Owner on subscription does not auto-grant blob access.

---

### 10.3 Walkthrough — Platform services (portal clicks)

#### Azure Data Factory (ADF)

1. RG → `adf-<learner>-<hash>`
2. **Open studio** (or **Author & Monitor**)
3. **Manage** (toolbox icon) → **Linked services** — note ADLS link to your storage account (if deployed by Bicep)
4. **Author** → **Pipelines** — empty at first; factories cost ~£0 at idle

**Novice exercise:** Create an empty pipeline (no trigger) — name it `demo-pipeline` — save. Do **not** publish a self-hosted integration runtime (cost).

**Theory:** ADF = orchestration layer; you pay mainly when pipelines/IRs run.

#### Microsoft Purview

1. Open Purview from RG **or** [web.purview.azure.com](https://web.purview.azure.com)
2. Select account `pview<learner><hash>` (region may show **East US** — documented exception)
3. **Data Map** → **Sources** — empty until you register sources
4. **Data catalog** → **Browse assets** — explore UI; no scan required for Class 1

**Novice exercise:** Click **Register** → choose *Azure Data Lake Storage Gen2* → point at your storage — cancel if you only want a UI tour.

**Theory:** Purview = governance catalog and lineage — not where you store files.

#### Azure Databricks

1. RG → `dbw-<learner>-<hash>` → **Launch workspace**
2. **Compute** — confirm **no cluster** (no DBU charges)
3. **Catalog** / **Workspace** — browse default folders

**Novice exercise:** Do **not** create a cluster in Class 1 unless trainer asks — clusters drive cost.

**Theory:** Workspace = management plane; clusters = spend. Lab deploys workspace only.

#### Microsoft Fabric (if MPN blocked Azure capacity)

1. Open [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. **Start trial** (if prompted) → **Workspaces** → **New workspace**
3. Assign to trial capacity — name `ws-<learner>-class1`

**Theory:** Fabric unifies lakehouse, warehouse, and BI; trial bypasses Azure F-SKU quota on MPN subs.

---

### 10.4 Cost Management UI (novice)

| Task | Portal path |
|------|-------------|
| See spend for your lab only | **Cost Management** → **Cost analysis** → *Add filter* → Resource group = `rg-<learner>-class1` |
| See budget vs actual | **Cost Management** → **Budgets** → `budget-<learner>-class1` |
| See cost by resource | Cost analysis → **Group by** → *Resource* |
| Export for trainer | Cost analysis → **Export** → CSV (optional) |

**Note:** Portal cost can show `-` for 24–48h on new subs — Budget blade + `verify_cost.py` output are more reliable early on.

---

### 10.5 Azure Portal vs code — who does what?

| Action | Use portal when… | Use `orchestrate.cmd` when… |
|--------|------------------|----------------------------|
| First deploy | Never for this lab | Always — reproducible Bicep |
| Upload test blob | Yes — quick visual proof | Optional via `az storage blob upload` |
| Change SKU / region | Avoid — breaks `verify_cost.py` | Re-run orchestrator only |
| Add tags | View in portal; edit in Bicep for consistency | Redeploy via orchestrator |
| Teardown | Can delete RG in portal | Prefer `orchestrate.cmd teardown ... --yes` |
| Understand concepts | Browse blades after deploy | Read `infra/*.bicep` alongside |

---

### 10.6 Safe novice checklist (do / don't)

| Do | Don't |
|----|-------|
| Stay inside `rg-<learner>-class1` | Create random resources in other regions |
| Upload small test files to `bronze` | Upload large datasets or PII |
| Read IAM, tags, lifecycle in portal | Change SKU to Premium / GRS |
| Re-run `orchestrate.cmd` to fix drift | Delete individual resources by hand (breaks Bicep state) |
| Use Cost analysis filtered to your RG | Panic if subscription cost column shows `-` on day 1 |
| Ask AI to explain a blade you see | Paste secrets into AI chat |

---

### 10.7 Suggested 90-minute portal lab (after deploy)

| Min | Activity | Portal location |
|-----|----------|-----------------|
| 0–10 | Orientation | Home, search, subscription, RG overview |
| 10–20 | Governance | RG tags + Budget blade |
| 20–35 | Security | Key Vault properties + IAM |
| 35–55 | Data lake | Storage config, containers, upload to bronze |
| 55–65 | Lifecycle | Storage → Lifecycle management rules |
| 65–75 | Platforms | ADF studio tour, Databricks (no cluster), Purview catalog |
| 75–85 | Cost | Cost analysis filtered to RG |
| 85–90 | Recap | Map each stop back to `infra/main.bicep` / `platform-services.bicep` |

**Deeper line-by-line portal map:** [docs/WORKFLOW-AND-CODE.md](docs/WORKFLOW-AND-CODE.md) §6.

---

### 10.8 External portals quick links

| Service | URL | When to open |
|---------|-----|--------------|
| Azure Portal | [portal.azure.com](https://portal.azure.com) | All ARM resources |
| Purview | [web.purview.azure.com](https://web.purview.azure.com) | Catalog / Data Map |
| Fabric | [app.fabric.microsoft.com](https://app.fabric.microsoft.com) | Trial workspace (MPN) |
| Entra ID (login issues) | [entra.microsoft.com](https://entra.microsoft.com) | MFA / account problems — ask trainer |

