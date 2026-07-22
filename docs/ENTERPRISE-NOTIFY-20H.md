# Enterprise notify — 20-hour curriculum (ADF + Databricks)

**Projector HTML (full detail):** [enterprise-notify-20h.html](enterprise-notify-20h.html)  
**Quick demo HTML:** [enterprise-jira-email-demo.html](enterprise-jira-email-demo.html)  
**Lab guide:** [../shared-adf-lab/docs/enterprise_jira_email_guide.md](../shared-adf-lab/docs/enterprise_jira_email_guide.md)

**One-command visible demo:**

```cmd
cd /d D:\azure
enterprise-notify.cmd
```

**Open curriculum in browser:**

```cmd
enterprise-notify-20h.cmd
```

---

## Canonical paths (use these — older notes may be wrong)

| Thing | Correct path / name |
|-------|---------------------|
| Entry | `enterprise-notify.cmd` (repo root) |
| Jira UI | `http://127.0.0.1:18080/` |
| Mail UI | `http://127.0.0.1:18081/` |
| ADF folder | `15-enterprise-notify` |
| Pipelines | `pl_ntf_01_simulate_work_or_fail` … `pl_ntf_05_master_incident` |
| SDK | `shared-adf-lab/scripts/adf_notify.py` |
| Notebook SOURCE | `shared-adf-lab/notebooks/nb_incident_jira_email.py` |
| Databricks workspace | `/Shared/shared-adf/nb_incident_jira_email` |
| Clients | `shared-adf-lab/enterprise-notify/client/` |
| Jira mock | `shared-adf-lab/enterprise-notify/jira_mock/app.py` |
| Mail sink | `shared-adf-lab/enterprise-notify/mail_sink/app.py` |
| Factory | `adf-shared-qgr7mj` · RG `rg-shared-class1` |

**Do not use:** bare `scripts/adf_notify.py` without `shared-adf-lab/`; notebook path as filename only.

---

## Timetable

| Hours | Focus | Prove |
|------:|-------|-------|
| 0–1 | Story + localhost constraint | Say 4-step sequence |
| 2–3 | Local cmd + `run_incident_flow.py` + clients | FIN-n + mail |
| 4–5 | Mock REST APIs | Same paths as real Jira |
| 6–8 | `pl_ntf_01` UI + SDK Fail path | PL_NTF-DEMO-FAIL |
| 9–11 | Web `pl_ntf_02` / `03` expressions | Explain URL/body |
| 12–15 | Databricks notebook line-by-line + `pl_ntf_04` | Widgets + soft-fail |
| 16–17 | Master `pl_ntf_05` Failed dependency | Monitor graph |
| 18–19 | Real Jira + Logic App | Company runbook |
| 20 | Capstone | Exit ticket |

---

## Deploy / re-run

```cmd
enterprise-notify.cmd
shared-adf-lab\orchestrate.cmd --skip-sql
shared-adf-lab\orchestrate.cmd --run-only --run-pipeline pl_ntf_05_master_incident
```

Four re-run scenarios: fresh PC; after success; after killing mock (auto-restart); after reboot — all safe via `enterprise-notify.cmd`.
