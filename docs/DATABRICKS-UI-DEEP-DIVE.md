# Databricks UI — deep dive (100h+)

**Projector page (open in browser):** [databricks-ui-deep-dive.html](databricks-ui-deep-dive.html)

Separate classroom guide for the **Azure Databricks workspace UI**: every major pane, when to use it, nice step-by-step how-to, class workflows, and a **100+ hour** syllabus.

## Covers

Workspace · Notebook editor · Compute · Workflows/Jobs · SQL · Catalog · Volumes · Secrets · Repos · Query history / system tables · Settings awareness · Class `/Shared` notebook map · Daily workflow · UI troubleshooting

## Class anchors

| Item | Value |
|------|--------|
| Workspace | `dbw-shared-qgr7mj` |
| Host | `https://adb-7405613791235979.19.azuredatabricks.net` |
| Folders | `/Shared/day7-day8`, `/Shared/day9`, `/Shared/shared-adf` |
| Cluster | `finledger-lab` · Single user · 30 min auto-terminate |
| Secrets | scope `finledger` |

## Quick start

```cmd
cd D:\azure
.\deploy-shared-lab.cmd
```

Then: Azure Portal → `rg-shared-class1` → `dbw-shared-qgr7mj` → **Launch Workspace** → Compute → start `finledger-lab` → Workspace → `/Shared/day7-day8/master_pyspark_complete`.

## Related

- **[Genie + Agents beginner teach](databricks-genie-agents-teach.html)** · [DATABRICKS-GENIE-AGENTS.md](DATABRICKS-GENIE-AGENTS.md) · `genie-agents-teach.cmd`
- [session-3/UI-OVERVIEW.md](../session-3/UI-OVERVIEW.md) · [MANUAL-LAB.md](../session-3/MANUAL-LAB.md)
- [day9/README.md](../day9/README.md)
- [adf-monitoring-deep-dive.html](adf-monitoring-deep-dive.html) — when ADF calls notebooks
- [infra-cicd-walkthrough.html](infra-cicd-walkthrough.html)
