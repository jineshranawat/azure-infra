# Purview — detailed teach + where the pipelines are

**Open the HTML (full click-by-click):** [purview-teach-demo.html](purview-teach-demo.html)

## Why you see “no Purview pipeline”

There is **no** pipeline named `Purview`. Look in ADF Studio:

1. Factory **`adf-shared-qgr7mj`**
2. **Author** → **Pipelines** → folder **`13-governance-purview`**
3. Pipelines: `pl_gov_01` … `pl_gov_06`

If the folder is missing → deploy first:

```cmd
git pull
infra-walkthrough.cmd --phase 3
```

## One-command demo

```cmd
purview-demo.cmd
```

Deploys governance pipelines if needed, then runs **`pl_gov_06_master_medallion_governance`**.

## Quick pipeline map

| Pipeline | Role |
|----------|------|
| `pl_gov_01` | Bronze Copy |
| `pl_gov_02` | Silver Data Flow |
| `pl_gov_03` | Gold Data Flow |
| `pl_gov_04` | Catalog JSON |
| `pl_gov_05` | Discovery + lineage |
| `pl_gov_06` | **Master demo** (runs 01→05) |

## Purview UI

Classic portal only (New portal **OFF**) → Search `sample_transactions` → **Lineage** tab.
