# ADF Monitoring — deep dive (defect investigation)

**Projector page (open in browser):** [adf-monitoring-deep-dive.html](adf-monitoring-deep-dive.html)

Separate from the infra/CI walkthrough on purpose. Covers:

- Pipeline runs vs activity runs vs trigger runs  
- Studio Monitor tour (factory `adf-shared-qgr7mj`)  
- Input / Output / Error reading  
- Debug vs Trigger now  
- Parent/child runs  
- Activity retries (`retry=1`, 30s) + orchestrator `--run-all-until-success`  
- Defect investigation playbook  
- FinLedger error catalog (403, 3204, notebook 404, SQL firewall, …)  
- `az datafactory` query examples  
- Workflow Mermaid diagrams  
- **100+ hour** hands-on syllabus (modules M0–M15)

## Quick start

```cmd
cd D:\azure
.\release.cmd adf
cd shared-adf-lab
.\orchestrate.cmd --run-pipeline pl_01_bronze_copy
```

Then open ADF Studio → **Monitor** → **Pipeline runs**, or open the HTML page above and follow §9 playbook.

## Related lab markdown

- [shared-adf-lab/README.md](../shared-adf-lab/README.md) §J/K  
- [pipeline_code_walkthrough.md](../shared-adf-lab/docs/pipeline_code_walkthrough.md) Part 7  
- [debug_print_variables_guide.md](../shared-adf-lab/docs/debug_print_variables_guide.md)  
- [adf_user_properties_guide.md](../shared-adf-lab/docs/adf_user_properties_guide.md)  
- Hub: [infra-cicd-walkthrough.html](infra-cicd-walkthrough.html)
