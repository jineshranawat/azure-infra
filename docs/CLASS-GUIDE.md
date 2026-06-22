# Class 1 — Step-by-step teaching guide

Timed flow for trainers and learners. Each block maps to a section of this repo so you can **introduce one concept, deploy or inspect it, then move on**.

**Total classroom time:** ~6 hours (flex to 5–7 depending on Q&A and login issues).

**Golden rule:** Deploy in **guardrail-first** order — never skip ahead to storage before the budget exists.

**Deep dive:** [TRAINER-NOTES.md](TRAINER-NOTES.md) | [WORKFLOW-AND-CODE.md](WORKFLOW-AND-CODE.md)

---

## At a glance

| Block | Time | Concept | Code / artefact | Learner sees in portal |
|-------|------|---------|-----------------|------------------------|
| 0 | 30 min | Machine setup & Azure login | `scripts/setup-windows.ps1`, `.env` | Subscriptions blade |
| 1 | 45 min | UK residency & governance tags | `deploy.sh` (RG), `main.bicep` `tags` | Resource group tags |
| 2 | 45 min | Cost guardrails before spend | `main.bicep` `budget` | Cost Management → Budgets |
| 3 | 60 min | Identity & secrets (Key Vault) | `main.bicep` `keyVault` | Key Vault blade |
| 4 | 75 min | Medallion lake & lifecycle | `main.bicep` storage + containers | Storage → containers, lifecycle |
| 5 | 45 min | Least-privilege data-plane RBAC | `main.bicep` role assignments | Access control (IAM) |
| 6 | 30 min | Verify £0 SKUs & MTD cost | `scripts/verify_cost.py` | Cost analysis |
| 7 | 15 min | Teardown & hygiene | `scripts/teardown.py` | RG delete |

---

## Block 0 — Environment setup (30 min)

**Learning objective:** Every learner can authenticate to Azure CLI without sharing passwords in chat or code.

### Trainer script (5 min)

- Explain: control plane (`az`) vs data plane (SDK / storage APIs).
- Show subscription ID in portal → goes in `.env` only (not committed secrets).
- Role needed: **Contributor** or **Owner** on the training subscription.

### Student TODO

- [ ] Clone this repository
- [ ] Copy `.env.example` → `.env` and fill in subscription ID, learner, email
- [ ] Run **full lab** orchestrator:

  ```powershell
  .\orchestrate.ps1 --skip-setup
  ```

  ```bash
  ./orchestrate.sh --install-cli
  ```

- [ ] Complete `az login` when prompted (browser or `--use-device-code` on headless VMs)
- [ ] Confirm summary output lists Key Vault, storage, and budget

### Checkpoint

- `az account show` returns the correct subscription.
- `.env` is filled in; `.env` is **not** committed to git.

### Common issues

| Symptom | Fix |
|---------|-----|
| `az` not found | New terminal after setup, or re-run `setup-windows.ps1` |
| Portal shows "Unauthorized" for cost | Normal on new subs; budget + verify script still work |
| MFA / device code | `az login --use-device-code --tenant <tenant>` |

---

## Block 1 — UK residency & governance tags (45 min)

**Learning objective:** Data platforms start with **where** data lives and **who owns** it — before any storage exists.

### Concepts to teach (15 min)

1. **UK residency** — BoE training estates use `uksouth` or `ukwest` only.
2. **Resource group** as the blast-radius boundary (one RG = entire Class-1 estate).
3. **Mandatory tags** — chargeback, ownership, teardown policy.

### Code walkthrough (20 min)

| File | What to show |
|------|----------------|
| `deploy.sh` L19–22 | Location guard (`uksouth` / `ukwest` only) |
| `deploy.sh` L26–35 | Tag list applied at RG creation |
| `infra/main.bicep` L7–12 | `@allowed` on `location` param |
| `infra/main.bicep` L28–37 | Same tags on every resource inside Bicep |

**Tag dictionary (have learners recite one use each):**

| Tag | Purpose |
|-----|---------|
| `env=training` | Non-production |
| `owner=<email>` | Human accountable |
| `costcentre=boe-data-enablement` | Chargeback |
| `data-class=training-synthetic` | No real PII |
| `course=azure-etl-boe` | Curriculum traceability |
| `class=class-1` | Session identifier |
| `auto-teardown=nightly` | Automation hint |

### Hands-on (10 min)

**Option A — create RG only (incremental teaching):**

```powershell
az group create --name rg-<learner>-class1 --location uksouth `
  --tags env=training owner=<email> costcentre=boe-data-enablement `
         data-class=training-synthetic course=azure-etl-boe class=class-1 auto-teardown=nightly
```

**Option B — full deploy at end of class:** skip now, revisit tags on deployed RG.

### Student TODO

- [ ] Explain why `eastus` would be rejected by this repo
- [ ] Open RG in portal → **Tags** → verify all seven keys
- [ ] Note naming: `rg-<learner>-class1`

### Checkpoint

Portal: **Resource groups** → `rg-<learner>-class1` → Tags blade matches `deploy.sh`.

---

## Block 2 — Guardrail-first budgeting (45 min)

**Learning objective:** Put **cost alerts before spend-capable resources** — budgets are free; storage is not.

### Concepts to teach (15 min)

1. Consumption budget scoped to **resource group** (not whole subscription).
2. £1/month cap is a **training guardrail**, not the expected bill.
3. Alerts at **50% actual** and **90% forecast** → email to `ownerEmail`.
4. Build order: budget → then Key Vault / storage.

### Code walkthrough (15 min)

| File | What to show |
|------|----------------|
| `infra/main.bicep` L44–86 | `Microsoft.Consumption/budgets` resource |
| `deploy.sh` L49–50 | `budgetStartDate` = 1st of current UTC month |
| `scripts/provision.py` `_ensure_budget` | Same logic in Python SDK path |

Discuss: why `filter.dimensions.ResourceGroupName` matters.

### Hands-on (15 min)

Deploy budget (full deploy or Bicep-only if RG exists):

```powershell
# Full path
bash deploy.sh
# Windows without bash:
az deployment group create --resource-group rg-<learner>-class1 `
  --template-file infra/main.bicep `
  --parameters location=uksouth learner=<learner> ownerEmail=<email> `
               principalObjectId=<object-id> budgetStartDate=<yyyy-mm-01>
```

### Student TODO

- [ ] Portal: **Cost Management** → **Budgets** → open `budget-<learner>-class1`
- [ ] Confirm amount = **1**, time grain = **Monthly**
- [ ] Confirm current spend shows **0** (or near-zero) right after deploy
- [ ] Explain why budget is deployed **before** storage in `main.bicep`

### Checkpoint

Budget exists and is tied to `rg-<learner>-class1` before learners upload any blobs.

---

## Block 3 — Identity & Key Vault (60 min)

**Learning objective:** Secrets live in a **dedicated vault** with **RBAC mode** — not in code, not in git.

### Concepts to teach (20 min)

1. Key Vault **Standard** tier — no fixed monthly fee (pay per operation).
2. **RBAC authorization** vs legacy access policies.
3. Soft-delete (7 days) for recoverability without backup SKUs.
4. Control-plane Owner ≠ ability to read secrets (preview of Block 5).

### Code walkthrough (20 min)

| File | What to show |
|------|----------------|
| `infra/main.bicep` L88–109 | Vault SKU, `enableRbacAuthorization: true` |
| `scripts/provision.py` `_ensure_key_vault` | SDK equivalent |

Naming: `kv-<learner>-<6-char-hash>` from `uniqueString()`.

### Hands-on (20 min)

After deploy:

```powershell
az keyvault show --name kv-<learner>-<hash> --resource-group rg-<learner>-class1 `
  --query "{name:name, sku:properties.sku, rbac:properties.enableRbacAuthorization}" -o table
```

Optional stretch: add a test secret (requires Block 5 RBAC or Owner + RBAC role):

```powershell
az keyvault secret set --vault-name kv-<learner>-<hash> --name demo-secret --value training-only
```

### Student TODO

- [ ] Portal: open Key Vault → **Properties** → RBAC enabled
- [ ] Portal: **Access control (IAM)** — note role assignments (may be empty until Block 5)
- [ ] Articulate why secrets must not go in `.env` or Bicep parameters

### Checkpoint

Vault exists in **uksouth**, SKU **standard**, RBAC **on**.

---

## Block 4 — Medallion lake & lifecycle (75 min)

**Learning objective:** Bronze / silver / gold / audit containers on **ADLS Gen2** with **self-expiring** training data.

### Concepts to teach (25 min)

1. **StorageV2** + **HNS** (`isHnsEnabled`) = ADLS Gen2 / data lake.
2. **Medallion** layering: raw → refined → curated → audit.
3. **Standard_LRS Hot** — no fixed fee; pay per GB and transaction.
4. **Lifecycle management:** Cool @ 30 days, **delete @ 7 days** for training hygiene.
5. **No blob versioning** with HNS (repo disables it deliberately).

### Code walkthrough (25 min)

| File | What to show |
|------|----------------|
| `infra/main.bicep` L111–129 | Storage account SKU + HNS |
| `infra/main.bicep` L148–162 | Container loop: bronze, silver, gold, audit |
| `infra/main.bicep` L164–212 | Lifecycle rules |
| `scripts/provision.py` `MEDALLION_CONTAINERS` | Same four containers in SDK path |

### Hands-on (25 min)

```powershell
az storage account show --name st<learner><hash> --resource-group rg-<learner>-class1 `
  --query "{name:name, kind:kind, sku:sku.name, hns:properties.isHnsEnabled}" -o table

az storage container list --account-name st<learner><hash> --auth-mode login -o table
```

Upload a small test file to `bronze` (portal or CLI) — discuss lifecycle deleting it after 7 days.

### Student TODO

- [ ] Portal: Storage account → **Containers** — four medallion containers present
- [ ] Portal: **Lifecycle management** — two rules visible
- [ ] Draw a one-line diagram: bronze → silver → gold + audit side-channel
- [ ] State why `Standard_GRS` or `Premium` would fail `verify_cost.py`

### Checkpoint

```text
st<learner><hash>  |  StorageV2  |  Standard_LRS  |  HNS=true
Containers: audit, bronze, gold, silver
```

---

## Block 5 — Least-privilege data-plane RBAC (45 min)

**Learning objective:** **Owner** on the subscription does **not** grant blob or secret access — explicit data-plane roles do.

### Concepts to teach (15 min)

1. **Control plane** roles (Contributor, Owner) manage resources.
2. **Data plane** roles operate on data inside resources.
3. Roles used in Class 1:
   - **Key Vault Secrets Officer** on the vault
   - **Storage Blob Data Contributor** on the storage account
4. `principalObjectId` from `az ad signed-in-user show` — not the email address.

### Code walkthrough (15 min)

| File | What to show |
|------|----------------|
| `infra/main.bicep` L214–236 | Scoped role assignments |
| `deploy.sh` L46–47 | Where principal ID is sourced |
| `scripts/provision.py` `_ensure_rbac` | SDK path |

**Trainer note:** Bicep role GUIDs must use `concat()` — string interpolation can corrupt GUIDs containing `586e75` (scientific-notation parsing). If Bicep RBAC fails, assign via CLI:

```powershell
az role assignment create --role "Key Vault Secrets Officer" `
  --assignee-object-id <object-id> --assignee-principal-type User `
  --scope /subscriptions/<sub>/resourceGroups/rg-<learner>-class1/providers/Microsoft.KeyVault/vaults/kv-<learner>-<hash>

az role assignment create --role "Storage Blob Data Contributor" `
  --assignee-object-id <object-id> --assignee-principal-type User `
  --scope /subscriptions/<sub>/resourceGroups/rg-<learner>-class1/providers/Microsoft.Storage/storageAccounts/st<learner><hash>
```

### Hands-on (15 min)

```powershell
az role assignment list --resource-group rg-<learner>-class1 -o table
```

Retry secret upload and blob upload — should succeed after roles propagate (~1–2 min).

### Student TODO

- [ ] Portal: Storage → **Access control (IAM)** → your Blob Data Contributor assignment
- [ ] Portal: Key Vault → **Access control (IAM)** → Secrets Officer assignment
- [ ] Explain in one sentence why Owner alone was insufficient

### Checkpoint

Learner can list/upload blobs with `--auth-mode login` and set a Key Vault secret.

---

## Block 6 — Verify cost & portal tour (30 min)

**Learning objective:** Prove the estate stays on **£0 fixed-fee SKUs** and read **month-to-date** spend.

### Concepts to teach (10 min)

1. SKU allow-list in `verify_cost.py` — automation beats manual portal checks.
2. MTD cost via Cost Management API.
3. Portal cost columns can lag **24–48 hours**; budget + script are immediate.

### Hands-on (20 min)

```powershell
$env:AZURE_SUBSCRIPTION_ID = "<your-guid>"
.\.venv\Scripts\python.exe scripts\verify_cost.py --resource-group rg-<learner>-class1
```

Expected output:

```text
Month-to-date actual cost: £0.0000
All resources pass the £0 SKU allow-list.
```

### Portal tour checklist

- [ ] **Cost Management** → **Cost analysis** → filter RG `rg-<learner>-class1`
- [ ] **Budgets** → `budget-<learner>-class1` → current vs forecast
- [ ] **Resource group** → **Resources** — only expected types (KV, Storage, Budget, role assignments)

### Student TODO

- [ ] Record MTD cost from script output
- [ ] Screenshot budget blade (optional deliverable)
- [ ] List all allowed SKUs from `verify_cost.py` constants

### Checkpoint

Script exits 0; learner can articulate difference between budget cap and actual bill.

---

## Block 7 — Teardown & hygiene (15 min)

**Learning objective:** One resource group delete removes **everything** — the correct pattern for ephemeral training estates.

### Concepts to teach (5 min)

- Single RG blast radius.
- `auto-teardown=nightly` tag meaning for ops automation.
- Never delete subscription — delete RG only.

### Hands-on (10 min)

```powershell
.\.venv\Scripts\python.exe scripts\teardown.py --resource-group rg-<learner>-class1
# or non-interactive:
.\.venv\Scripts\python.exe scripts\teardown.py --resource-group rg-<learner>-class1 --yes
```

### Student TODO

- [ ] Confirm RG gone in portal
- [ ] Confirm budget alerts stopped (RG-scoped budget deleted with RG)
- [ ] Keep `.env` locally for next session; do not commit it

---

## End-of-class deliverables

| # | Deliverable | Evidence |
|---|-------------|----------|
| 1 | Tagged resource group in UK region | Portal screenshot or `az group show` |
| 2 | Budget with £1 cap | Budget blade |
| 3 | Key Vault + storage with 4 containers | Portal or CLI list |
| 4 | RBAC assignments on vault + storage | IAM blade |
| 5 | `verify_cost.py` passing | Terminal output |

---

## Two deployment tracks (pick per cohort)

| Track | When to use | Command |
|-------|-------------|---------|
| **Bicep / IaC** (default) | Infra engineers, GitOps mindset | `bash deploy.sh` |
| **Python SDK** | Data engineers, automation focus | `python scripts/provision.py --subscription-id <guid>` |

Both paths produce the **same estate** in the same build order. Teach Block 2–5 concepts once; demo both entry points if time allows.

---

## Suggested pacing (full day)

```text
09:00 – 09:30   Block 0   Setup & login
09:30 – 10:15   Block 1   Tags & residency
10:15 – 11:00   Block 2   Budget guardrails
11:00 – 12:00   Block 3   Key Vault
12:00 – 13:00   Lunch
13:00 – 14:15   Block 4   Medallion lake & lifecycle
14:15 – 15:00   Block 5   Data-plane RBAC
15:00 – 15:30   Block 6   verify_cost + portal tour
15:30 – 15:45   Block 7   Teardown demo (optional live delete)
```

**Incremental deploy variant:** Run `bash deploy.sh` once at **end of Block 2** (budget only — comment out later resources in Bicep for live stepped deploy), or deploy fully at lunch and use afternoon for portal + RBAC labs. Full template deploy takes ~3–5 minutes.

---

## Trainer quick-reference — build order in code

```text
deploy.sh          →  RG + tags + parameters
main.bicep         →  budget → keyVault → storageAccount → containers → lifecycle → RBAC
provision.py       →  same order in _ensure_* functions (Steps 1–5)
verify_cost.py     →  post-deploy SKU + MTD cost gate
teardown.py        →  delete rg-<learner>-class1
```

---

## Related files

| Path | Role in curriculum |
|------|-------------------|
| `.env.example` | Learner-specific config template |
| `scripts/setup-windows.ps1` | Block 0 automation |
| `infra/main.bicep` | Blocks 1–5 IaC |
| `deploy.sh` | Block 1 entry + full deploy orchestration |
| `scripts/provision.py` | Blocks 1–5 SDK alternative |
| `scripts/verify_cost.py` | Block 6 verification |
| `scripts/teardown.py` | Block 7 cleanup |
