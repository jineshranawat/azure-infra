# 04-01 · Databricks notebook from ADF

> Module 4 · Time budget: 50 min · Source: [Transform data using Databricks notebook](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-using-databricks-notebook)
> Prereqs: Module 3 complete, [`scoring_input.csv`](../data/module-04-external-compute/scoring_input.csv) in `silver/scoring_input/`

## What you'll build

Azure Databricks workspace (training SKU), linked service **`ls_databricks_finledger`**, notebook **`nb_score_high_value_accounts`**, pipeline **`pl_databricks_scoring`** with **Databricks Notebook** activity writing scores to `gold/high_value_accounts/`.

## Why this matters

Data flows handle many transforms; **ML and complex Python** belong in Databricks. ADF orchestrates when the notebook runs and passes parameters (`run_id`, input paths) — FinLedger gold layer customer scoring.

> ⚠️ WARNING: Databricks + cluster runtime is the **highest cost** module. Use smallest cluster; terminate after lab. **MPN quota 0:** portal walkthrough + notebook code review only — trainer sign-off.

## Part A — UI (click by click)

### A0 — Upload scoring input

1. Upload `scoring_input.csv` → `bronze` or `silver/scoring_input/scoring_input.csv` (**6 rows**).

### A1 — Databricks workspace

2. Portal → **Create** → **Azure Databricks** → `dbw-finledger-{learner}` → **UK South** → **Trial** or **Premium** (cheapest lab tier).
3. Open workspace → **Create** → **Notebook** → `nb_score_high_value_accounts`.
4. Python cell:

```python
input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")
df = spark.read.option("header", True).csv(input_path)
scored = df.filter("total_spend_gbp > 1000 OR segment = 'premium'")
scored.write.mode("overwrite").option("header", True).csv(output_path)
```

5. **Widgets** → add `input_path`, `output_path` defaults for manual test.

### A2 — ADF linked service

6. ADF Studio → **Manage** → **Linked services** → **+** → **Azure Databricks**.
7. **Name:** `ls_databricks_finledger`.
8. **Account method:** **From Azure subscription** → select workspace.
9. **Select cluster:** **New job cluster** → **Standard_DS3_v2** (or smallest available) → **Terminate after** 30 min idle.
10. **Test connection** → **Create** → **Publish**.

### A3 — Notebook activity pipeline

11. **Pipelines** → **+** `pl_databricks_scoring`.
12. Drag **Databricks Notebook** activity `Score_accounts`.
13. **Azure Databricks** tab: linked service `ls_databricks_finledger`, notebook path `/nb_score_high_value_accounts`.
14. **Base parameters:** `input_path` = `abfss://bronze@stadfcourse{learner}.dfs.core.windows.net/silver/scoring_input/scoring_input.csv` (adjust), `output_path` = `abfss://bronze@stadfcourse{learner}.dfs.core.windows.net/gold/high_value_accounts/`.
15. **Trigger now** → Monitor **20–40 min** first cluster cold start possible.
16. Verify `gold/high_value_accounts/` — premium + high spend accounts (**4+ rows**).

## Part B — JSON

`pipeline/pl_databricks_scoring.json`

```json
{
  "name": "pl_databricks_scoring",
  "properties": {
    "activities": [
      {
        "name": "Score_accounts",
        "type": "DatabricksNotebook",
        "policy": { "timeout": "0.12:00:00", "retry": 1 },
        "typeProperties": {
          "notebookPath": "/nb_score_high_value_accounts",
          "baseParameters": {
            "input_path": "abfss://bronze@stadfcoursejinesh.dfs.core.windows.net/silver/scoring_input/scoring_input.csv",
            "output_path": "abfss://bronze@stadfcoursejinesh.dfs.core.windows.net/gold/high_value_accounts/"
          }
        },
        "linkedServiceName": {
          "referenceName": "ls_databricks_finledger",
          "type": "LinkedServiceReference"
        }
      }
    ]
  }
}
```

## Part D — Verify

| Check | Expected |
|---|---|
| Notebook activity | Succeeded |
| Gold output | CSV with filtered accounts |
| Cluster | Terminated after idle |

## Cost & tear-down

Delete cluster policy; stop workspace if not needed. Remove Databricks linked service when done.

## Next

[04-02 · HDInsight Spark](04-02-hdinsight-spark-transformation.md)
