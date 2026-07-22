# Enterprise notify — Jira ticket + attach + email (ADF folder `15-enterprise-notify`)

**Goal:** Show the enterprise pattern: pipeline failure → **raise a Jira ticket** → **attach the ADF run log** → **send email** — as ADF activities (Web + Databricks) and as a one-command local demo you can see in the browser.

**One-command demo (Windows):**

```cmd
enterprise-notify.cmd
```

**Teach HTML:** [`../../docs/enterprise-jira-email-demo.html`](../../docs/enterprise-jira-email-demo.html)

In that HTML open: **§6 Run from UI** · **§7 Build in company** · **§8 Code explained**.  
**20-hour curriculum:** [`../../docs/enterprise-notify-20h.html`](../../docs/enterprise-notify-20h.html) · `enterprise-notify-20h.cmd`

**Code:** `shared-adf-lab/enterprise-notify/` · `scripts/adf_notify.py` · `notebooks/nb_incident_jira_email.py`

---

## A. WHAT & WHY

### Why enterprises do this

When an ADF pipeline fails at 2am, ops needs a **ticket** (Jira/ServiceNow) with evidence attached, and an **email** to the on-call owner — not a Slack guess.

**Analogy:** The kitchen fire alarm (Fail activity) automatically opens a maintenance ticket (Jira), staples the CCTV clip (run log attachment), and texts the manager (email).

### Critical constraint (read this)

Azure Data Factory and Azure Databricks run **in the cloud**. They **cannot** call `http://127.0.0.1` on your laptop.

| Layer | What it proves |
|-------|----------------|
| `enterprise-notify.cmd` | Full visible flow: local Jira UI + mail outbox |
| ADF folder `15-enterprise-notify` | Same activities in Studio (Web / Databricks notebook) |
| Real Jira Cloud + Logic App | Swap `.env` URLs when you have cloud endpoints |

Local services speak **Jira REST** (`POST /rest/api/2/issue`, attachments, comments) so the same Python client works against real Jira later.

---

## B. Components

| Piece | Role |
|-------|------|
| Jira mock `:18080` | Ticket board + REST API |
| Mail sink `:18081` | `/notify` webhook + HTML outbox (Logic App stand-in) |
| `client/jira_client.py` | Create issue, attach file, comment |
| `client/mail_client.py` | POST notify payload |
| `client/run_incident_flow.py` | End-to-end local orchestrator |
| `pl_ntf_01` … `pl_ntf_05` | ADF Studio activities |

### ADF pipelines

| Pipeline | What it does |
|----------|----------------|
| `pl_ntf_01_simulate_work_or_fail` | If `force_fail` → **Fail** activity |
| `pl_ntf_02_web_create_jira` | **Web** POST create issue |
| `pl_ntf_03_web_send_email` | **Web** POST mail webhook |
| `pl_ntf_04_databricks_incident` | **Databricks Notebook** create + attach + email |
| `pl_ntf_05_master_incident` | Execute 01; on **Failed** → Execute 04; optional Web if `use_web_activities=true` |

---

## C. Quick start

```cmd
REM From repo root — visible Jira + mail
enterprise-notify.cmd

REM Optional — deploy ADF pipelines to shared factory
shared-adf-lab\orchestrate.cmd
shared-adf-lab\orchestrate.cmd --run-pipeline pl_ntf_05_master_incident
```

Open:

- http://127.0.0.1:18080/ — Jira board  
- http://127.0.0.1:18081/ — Mail inbox  
- `docs\enterprise-jira-email-demo.html` — teach page  

### `.env` (optional cloud endpoints)

```env
JIRA_BASE_URL=http://127.0.0.1:18080
NOTIFY_MAIL_URL=http://127.0.0.1:18081
OWNER_EMAIL=training@example.com
# JIRA_USER=
# JIRA_API_TOKEN=
```

For real Jira Cloud: set `JIRA_BASE_URL=https://your-org.atlassian.net`, user email + API token.  
For real email: set `NOTIFY_MAIL_URL` to a Logic App HTTP trigger URL and `use_web_activities=true` on the master pipeline.

---

## D. Four re-run scenarios

| Scenario | Expected |
|----------|----------|
| Fresh machine | `enterprise-notify.cmd` creates venv, starts services, creates `FIN-1`, shows mail |
| Re-run after success | Services reused; next ticket `FIN-2` (or next seq); new mail row |
| Re-run after killing mock | Script restarts services on `:18080` / `:18081`, flow succeeds |
| After teardown | Stop Python uvicorn processes or reboot; next `enterprise-notify.cmd` starts clean |

**Re-run command:** `enterprise-notify.cmd`

---

## E. Fix SQL seed warnings from `orchestrate.cmd`

These warnings do **not** block ADF notify deploy.

| Symptom | Cause | Fix |
|---------|--------|-----|
| `40615` / IP not allowed | SQL firewall missing laptop IP | Auto-heal on deploy (`ensure_sql_client_firewall`). Or: `az sql server firewall-rule create -g rg-shared-class1 -s sql-shared-qgr7mj -n AllowClientLaptop --start-ip-address YOUR_IP --end-ip-address YOUR_IP` |
| `IM002` / ODBC driver not found | No ODBC Driver 18 on PC | Install [ODBC Driver 18](https://go.microsoft.com/fwlink/?linkid=2249004) **or** run `shared-adf-lab\orchestrate.cmd --skip-sql` |

Notify-only deploy:

```cmd
shared-adf-lab\orchestrate.cmd --skip-sql
```

---

## F. Now go to the code

| File | Responsibility |
|------|----------------|
| `enterprise-notify/jira_mock/app.py` | Local Jira REST + UI |
| `enterprise-notify/mail_sink/app.py` | Mail webhook + outbox |
| `enterprise-notify/client/*.py` | Shared clients + flow |
| `scripts/adf_notify.py` | Deploy ADF pipelines |
| `scripts/azure_sql.py` | SQL deploy + client firewall heal + soft seed errors |
| `notebooks/nb_incident_jira_email.py` | Databricks twin |
