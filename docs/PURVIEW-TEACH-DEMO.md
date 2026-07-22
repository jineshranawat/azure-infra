# Microsoft Purview — complete guide for new joiners

**Open the full HTML:** [purview-teach-demo.html](purview-teach-demo.html)

After the demo, open **Section 4b — After the demo — Governance** in that HTML
(`#governance`) for the structured explanation students ask for (concepts table, pipeline map, exit tickets).

## Do not miss — OUR lab steps (Section 0)

After understanding concepts, every student must complete:

1. `git pull` + `az login`
2. Pin SDK: `pip install --force-reinstall azure-mgmt-datafactory==9.3.0`
3. `infra-walkthrough.cmd --phase 3` (creates `13-governance-purview` / `pl_gov_*`)
4. ADF UI: open folder **13-governance-purview** → `pl_gov_06`
5. Manage → Microsoft Purview = **Connected** (+ Data Curator if needed)
6. `purview-demo.cmd` → Monitor Succeeded
7. Classic Purview → search `sample_transactions` → **Lineage** tab
8. Optional: open `audit/governance/` on `stsharedqgr7mj`
9. Exit ticket (analogy + folder + commands)

### Troubleshoot (common class errors)

| Error | Fix |
|-------|-----|
| ADF UI: `Cannot read properties of undefined (reading 'variable')` when pasting Purview resource ID | **Cancel** the Connect panel. Do **not** paste by hand. Run `purview-demo.cmd` or phase 3 so code links ADF. Account must be `pviewrohan4hnv7s` (…**nv**…, not …nn…). |
| `ForEachActivity.__init__() got an unexpected keyword argument 'items'` | `pip install --force-reinstall azure-mgmt-datafactory==9.3.0` then re-run phase 3 |

## Official sources

- [Connect Data Factory to Purview](https://learn.microsoft.com/en-us/azure/data-factory/connect-data-factory-to-azure-purview)
- [ADF lineage in Data Map](https://learn.microsoft.com/en-us/purview/data-map-lineage-azure-data-factory)
- [Tutorial: Push lineage to Purview](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-push-lineage-to-purview)
- [Classic lineage user guide](https://learn.microsoft.com/en-us/purview/data-gov-classic-lineage-user-guide)
