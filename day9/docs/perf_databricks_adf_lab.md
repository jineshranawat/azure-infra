# Performance & NFR Lab — novice + architect

**Notebook:** https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/perf_databricks_adf_lab  

## Dual audience

| Reader | Gets |
|--------|------|
| **Novice** | Kitchen story, Spark UI words, stopwatch, simple probes |
| **Architect** | NFR table, ADF↔Databricks correlation, scorecard, design-review script |

## Flow

1. Big picture + NFR dictionary  
2. Baseline KPI + `explain`  
3. Spark UI tour  
4. `system.query.history` / billing / compute (soft)  
5. ADF Monitor correlation via `run_id`  
6. Debug playbook + safe probes  
7. Scalability scorecard + wrap  

## Deploy

```cmd
python scripts\deploy_perf_databricks_adf_lab.py
```

Pair with **spark_dag_problems_lab** for smell→fix drills.
