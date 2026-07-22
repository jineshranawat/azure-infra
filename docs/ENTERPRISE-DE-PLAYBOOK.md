# Enterprise DE playbook — everyday end-to-end patterns

**One command (Windows):** `enterprise-de.cmd`  
**Teach HTML:** [enterprise-de-playbook.html](enterprise-de-playbook.html)  
**Notify-only deep dive:** [enterprise-jira-email-demo.html](enterprise-jira-email-demo.html) · `enterprise-notify.cmd`

## What & why

Six patterns every data engineer uses in production kitchens (pipelines):

| UC | Pattern | Analogy | Azure twin |
|----|---------|---------|------------|
| UC01 | Incident notify | Fire alarm → ticket → evidence → manager SMS | ADF `15-enterprise-notify` |
| UC02 | Data quality gate | Health-check ingredients before plating | Get Metadata + If / expectations |
| UC03 | Incremental watermark | Only new tickets since last service | Lookup watermark + Copy / MERGE |
| UC04 | Quarantine / dead-letter | Spoiled boxes → returns, not the fridge | `reject/` vs `loaded/` paths |
| UC05 | Config-driven ForEach | One timetable, many menu items from clipboard | Lookup config + ForEach |
| UC06 | Run audit trail | Guest book of every service | Monitor + audit JSON + Purview |

## Quick start

```bat
enterprise-de.cmd
enterprise-de.cmd 2
enterprise-de.cmd all
```

**Proof:** `shared-adf-lab\enterprise-de\out\` · UC01 Jira http://127.0.0.1:18080/ · mail http://127.0.0.1:18081/

## Re-run command

`enterprise-de.cmd` — safe on fresh machine, after success, after partial failure, after deleting `out\`.

## Code map

- `enterprise-de.cmd` — thin Windows launcher
- `shared-adf-lab/enterprise-de/run_all.py` — orchestrator
- `shared-adf-lab/enterprise-de/scenarios/uc0N_*.py` — each use case
- `shared-adf-lab/enterprise-de/sample_data/` — CSV / JSON fixtures

## Azure note

ADF/Databricks cannot call laptop `localhost`. Local cmd is the visible E2E proof; Studio folder `15-enterprise-notify` and medallion/watermark pipelines are the cloud shapes.
