# Purview — what to teach + one ready demo

**Teach page (HTML):** [purview-teach-demo.html](purview-teach-demo.html)  
**One command:** `purview-demo.cmd` (repo root)

## Teach (easy)

| Concept | One-liner |
|---------|-----------|
| Purview | Google for your data estate (catalog + lineage) |
| Catalog / asset | Searchable file/table/path |
| Lineage | Graph: bronze → silver → gold |
| ADF ↔ Purview | Factory pushes Copy/Data Flow lineage automatically |

**Critical:** Classic Purview portal only for ADF lineage (turn **OFF** “New Microsoft Purview portal”). Search paths (`sample_transactions`, `stsharedqgr7mj`) — not the factory name.

## Ready demo component

Pipeline: **`pl_gov_06_master_medallion_governance`** (ADF folder `13-governance-purview`)

```cmd
purview-demo.cmd
```

Then: ADF Monitor → Succeeded → classic Purview → Search catalog → open asset → **Lineage** tab (wait 5–30 min if first sync).

## Deeper guides

- [shared-adf-lab/docs/purview_portal_search_guide.md](../shared-adf-lab/docs/purview_portal_search_guide.md)
- [shared-adf-lab/docs/adf_purview_medallion_governance_guide.md](../shared-adf-lab/docs/adf_purview_medallion_governance_guide.md)
