# FinLedger — Databricks secret scope setup (detailed)

**Fastest path (Windows):** add secrets to `.env`, then:

```text
cd session-3
orchestrate.cmd --setup-secrets
```

This uses the **Databricks CLI** on your laptop. Many workspaces **block** `dbutils.secrets.put` in notebooks (your error: `'SecretsHandler' object has no attribute 'put'`).

---

**Purpose:** store storage account key **once** in Databricks scope `finledger`. Each notebook hardcodes `STORAGE_ACCOUNT` and reads `storage-key` from secrets in cell 1.

**Scope name:** `finledger`  
**Secrets stored:**

| Secret key | Contents | Example |
|------------|----------|---------|
| `storage-account` | ADLS storage account name | `stjineshfqdcgg` |
| `storage-key` | Storage account **key1** | *(hidden — from Azure Portal)* |

---

## Part 0 — orchestrate.cmd (recommended for sandbox)

### 0.1 Edit repo-root `.env` (never commit)

```ini
DATABRICKS_HOST=https://adb-7405614287120472.12.azuredatabricks.net
STORAGE_ACCOUNT_KEY=<paste key1 from portal>
DATABRICKS_TOKEN=<paste PAT from Databricks User settings -> Developer -> Access tokens>
```

`DATABRICKS_HOST` is optional — orchestrate can auto-detect from Azure.

### 0.2 Create a Databricks personal access token (one time)

1. Open your workspace: [Databricks workspace](https://adb-7405614287120472.12.azuredatabricks.net/)
2. Top-right → your name → **User settings**
3. **Developer** → **Access tokens** → **Generate new token**
4. Copy token into `.env` as `DATABRICKS_TOKEN`

### 0.3 Run orchestrator

```text
cd d:\azure\session-3
orchestrate.cmd --setup-secrets
```

**Expected:**

```text
INFO — Created secret scope 'finledger'   (or already present)
INFO — Stored secret finledger/storage-account
INFO — Stored secret finledger/storage-key
INFO — Databricks secrets ready — notebooks can use auth_mode=auto
```

### 0.4 Run notebooks

Open `nb_01_read_bronze` → edit `STORAGE_ACCOUNT` in cell 1 if needed → **Run all**.

---

## Who does what

| Role | Task | How often |
|------|------|-----------|
| **Trainer / workspace admin** | Create scope `finledger` + grant READ to learners | Once per Databricks workspace |
| **Each learner** | Run `orchestrate.cmd --setup-secrets` (or `nb_00` if `put` works) | Once (or after key rotation) |

---

## Part A — Trainer: create the secret scope (once)

### A1. Get your Databricks workspace URL

1. Open [portal.azure.com](https://portal.azure.com).
2. Resource group `rg-<learner>-class1` → **Azure Databricks** workspace.
3. Click **Launch workspace**.
4. Copy the browser URL — it looks like:
   ```text
   https://adb-1234567890123456.7.azuredatabricks.net
   ```
   The host is `adb-….azuredatabricks.net` (no path after that).

### A2. Install Databricks CLI (Windows — one time)

On your laptop (Command Prompt or PowerShell):

```text
pip install databricks-cli
```

Or use the newer unified CLI:

```text
pip install databricks-sdk
databricks --version
```

### A3. Log in to the workspace

```text
databricks auth login --host https://adb-<YOUR-WORKSPACE-ID>.azuredatabricks.net
```

A browser opens — sign in with the same Microsoft account you use for Databricks.

**Older CLI alternative:**

```text
databricks configure --token
```

Host: `https://adb-….azuredatabricks.net`  
Token: create under Databricks → **User settings** → **Developer** → **Access tokens** → **Generate new token**.

### A4. Create the scope

```text
databricks secrets create-scope finledger
```

**Verify:**

```text
databricks secrets list-scopes
```

You should see `finledger` in the list.

### A5. Let learners read secrets (important)

By default only the creator may read. Grant the workspace **users** group read access:

```text
databricks secrets put-acl --scope finledger --principal users --permission READ
```

**Verify ACLs:**

```text
databricks secrets list-acls --scope finledger
```

Expected: principal `users` with permission `READ`.

> If your org uses a named group instead of `users`, ask your Databricks admin which principal to use.

---

## Part B — Learner: save credentials (once per person)

Pick **one** method: notebook (easiest) or CLI.

### Method 1 — Notebook (recommended)

**Prerequisites:** Part A complete; import any lab notebook (each file is self-contained).

Optional fallback notebook: `nb_00_setup_credentials.py`

#### B1. Get storage account key from Azure

1. Portal → storage account `st<learner><hash>` (e.g. `stjineshfqdcgg`).
2. Left menu → **Security + networking** → **Access keys**.
3. Click **Show** next to **key1**.
4. Click **Copy** (do not email or commit this value).

#### B2. Run setup notebook

1. Databricks → **Workspace** → open **`nb_00_setup_credentials`**.
2. Set widgets:

| Widget | Value |
|--------|--------|
| `storage_account` | Your storage name, e.g. `stjineshfqdcgg` |
| `storage_account_key` | Paste **key1** from portal |

3. Attach a cluster (Shared or Single-user).
4. **Run all** cells.

#### B3. Expected output

```text
Saved finledger/storage-account = stjineshfqdcgg
Saved finledger/storage-key = **** (hidden)
Loaded key from secret finledger/storage-key
Storage auth OK for stjineshfqdcgg
  loaded/
  incoming/
  ...
SETUP COMPLETE — open nb_01_read_bronze and run
```

#### B4. Clean up

- **Clear** the `storage_account_key` widget (empty string).
- Do **not** save the notebook with the key still in the widget.

---

### Method 2 — CLI (alternative)

After Part A, on your PC:

```text
databricks secrets put --scope finledger --key storage-account --string-value stjineshfqdcgg
```

```text
databricks secrets put --scope finledger --key storage-key --string-value "<paste-key1-here>"
```

CLI prompts for the value if you omit `--string-value` (safer — key not in shell history).

**List keys (values are never shown):**

```text
databricks secrets list --scope finledger
```

Expected keys: `storage-account`, `storage-key`.

---

## Part C — Use secrets in lab notebooks (every session)

Open **`nb_01_read_bronze`** (or nb_02–nb_04, or day6 notebooks):

1. Cell 1 contains `STORAGE_ACCOUNT = "st…"` — change if yours differs.
2. Cell 1 loads `dbutils.secrets.get(scope="finledger", key="storage-key")`.
3. Set widget `run_id` = `session3-lab` (optional paths can stay empty).

**Run all.** You should see:

```text
Storage auth configured for stjineshfqdcgg
```

No key to paste again — even after you **restart the cluster** (secrets persist in Databricks).

---

## Part D — Verify secrets from a notebook

Quick test cell (paste into any notebook):

```python
STORAGE_ACCOUNT = "stjineshfqdcgg"
print(dbutils.secrets.get(scope="finledger", key="storage-key")[:4] + "…")
# Full key is never printed — that is correct
```

If this fails with *Secret does not exist*, run Part B again or ask trainer to check Part A5 (READ ACL).

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `Secret scope finledger does not exist` | Trainer runs `databricks secrets create-scope finledger` (Part A4) |
| `Permission denied` on `dbutils.secrets.get` | Trainer runs `put-acl` for `users` READ (Part A5) |
| `Could not write secrets to scope finledger` | Scope missing or you lack WRITE — use notebook after trainer creates scope |
| `Cannot access ADLS` after secrets saved | Rotate key in portal; re-run `nb_00_setup_credentials` |
| Prefer no secrets at all | **Single-user** cluster (managed identity) — still edit `STORAGE_ACCOUNT` in notebook |

---

## Security notes (lab)

- Storage keys are **sensitive** — treat like passwords.
- Never commit keys to git or save notebooks with keys in widget defaults.
- Rotate key1 in Azure Portal if exposed; update secrets with `nb_00_setup_credentials` again.
- Secret scope `finledger` is for **training only** — production uses managed identity / Unity Catalog.

---

## Quick reference card

```text
# Trainer (once)
databricks auth login --host https://adb-XXXX.azuredatabricks.net
databricks secrets create-scope finledger
databricks secrets put-acl --scope finledger --principal users --permission READ

# Learner (once) — or use nb_00_setup_credentials notebook
databricks secrets put --scope finledger --key storage-account --string-value stYOURACCOUNT
databricks secrets put --scope finledger --key storage-key

# Daily lab
nb_01 → run_id=session3-lab, STORAGE_ACCOUNT correct in cell 1
```

**Related:** [MANUAL-LAB.md](MANUAL-LAB.md) §D · [nb_00_setup_credentials.py](notebooks/nb_00_setup_credentials.py)
