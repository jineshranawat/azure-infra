# Databricks Genie — 40h+ curriculum (why · what · how)

**Projector HTML (open this):** [databricks-genie-agents-teach.html](databricks-genie-agents-teach.html)  
**Open command:** `genie-agents-teach.cmd`  
**Live create + ask:** `genie-lab.cmd`

## Class Databricks links (click)

| What | URL |
|------|-----|
| Workspace | https://adb-7405613791235979.19.azuredatabricks.net |
| Genie landing | https://adb-7405613791235979.19.azuredatabricks.net/genie |
| **FinLedger Class Genie room** | https://adb-7405613791235979.19.azuredatabricks.net/genie/rooms/01f185e11fc4152bad1eca7e4bbefa05 |
| SQL warehouses | https://adb-7405613791235979.19.azuredatabricks.net/sql/warehouses |
| Catalog | https://adb-7405613791235979.19.azuredatabricks.net/explore/data |

Workspace: `dbw-shared-qgr7mj` · RG: `rg-shared-class1` · Region: `uksouth`

## Create Genie Agent — latest Databricks docs (Jul 2026)

**Official:** [Create and manage a Genie Agent](https://docs.databricks.com/aws/en/genie/set-up) · [Azure MS Learn](https://learn.microsoft.com/en-us/azure/databricks/genie/set-up) · [Genie hub](https://learn.microsoft.com/en-us/azure/databricks/genie/)

Genie Agents were formerly **Genie Spaces**.

### Requirements (docs)

- UC tables/views — max **30** per agent  
- Pro or **serverless** SQL warehouse + **CAN USE** (author credentials embedded)  
- Databricks SQL entitlement · **SELECT** on data · CAN EDIT to edit (creators get CAN MANAGE)  
- Partner-powered AI enabled at account + workspace  

### Exact create steps (from Databricks)

1. Click **Genie Agents** in the sidebar.  
2. Click **New** (upper-right).  
3. Choose data sources → click **Create**.  

Then: Genie Code launches automatically → review/accept suggestions → **Configure → Settings** → set **Default warehouse** (prefer serverless) + Title / Description / Common questions → **Configure → Data** to add/remove tables → tune with [example SQL + instructions](https://docs.databricks.com/aws/en/genie-agents/tune-quality) before sharing.

### Share / clone / export (docs)

- **Share** → users/groups → CAN VIEW/RUN/EDIT/MANAGE → **Copy link**  
- Consumers need SELECT; they do **not** need warehouse CAN USE  
- **Clone:** ⋯ → Clone (copies setup, not chats/Monitor)  
- **Export to metric view:** ⋯ → Export to metric view → path → Create  

Full classroom walkthrough with class links: [databricks-genie-agents-teach.html](databricks-genie-agents-teach.html) §4 and §7.


| Module | Hours | Topic |
|--------|------:|-------|
| M0 | 1 | Open Genie deep links |
| M1 | 3 | Why / What / How + concepts |
| M2–M3 | 5 | Prereqs + SQL warehouse |
| M4 | 4 | UC metadata / comments |
| M5–M6 | 6 | Create agent + Genie Code |
| M7–M8 | 9 | Tune: instructions, examples, trusted assets |
| M9 | 3 | Agent mode |
| M10 | 3 | Share / ACL / RLS |
| M11 | 4 | `genie-lab.cmd` orchestrator |
| M12–M14 | 8 | API, Agent Bricks, cost, FAQ |

Every section uses **Why → What → How (clicks) → Verify → Official link**.

## Official docs

- [Create Genie Agent (MS Learn)](https://learn.microsoft.com/en-us/azure/databricks/genie/set-up)
- [Concepts](https://learn.microsoft.com/en-us/azure/databricks/genie/concepts)
- [Tune quality](https://learn.microsoft.com/en-us/azure/databricks/genie/tune-quality)
- [Agent mode](https://learn.microsoft.com/en-us/azure/databricks/genie/agent-mode)
- [Knowledge Assistant](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-bricks/knowledge-assistant)
- [Genie Agents API](https://docs.databricks.com/aws/en/genie-agents/conversation-api)

## Related

- [databricks-ui-deep-dive.html](databricks-ui-deep-dive.html)
- [enterprise-de-playbook.html](enterprise-de-playbook.html)
