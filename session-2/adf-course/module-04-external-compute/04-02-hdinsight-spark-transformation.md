# 04-02 · HDInsight Spark transformation

> Module 4 · Time budget: 50 min · Source: [Transform with Spark on HDInsight](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-transform-data-spark-portal)
> Prereqs: [04-01 · Databricks](04-01-databricks-notebook-activity.md)

## What you'll build

HDInsight **Spark** cluster `hdispark-finledger-{learner}` (smallest head + worker), linked service **`ls_hdinsight_spark`**, pipeline **`pl_spark_aggregate`** running **Spark** activity — aggregate `scoring_input.csv` by `segment` → `gold/segment_totals/`.

## Part A — UI (summary path)

1. Portal → **HDInsight cluster** → Linux, **Spark 3.x**, **UK South**, **1** head + **2** workers (min for lab) — or single worker for cost.
2. Wait 15–30 min cluster create.
3. Upload jar or use **Spark job definition** inline — MS Learn uses `spark-job-definition/spark_aggregate.json`.
4. ADF → **Linked service** → **Azure HDInsight** → cluster + basic auth or MSI.
5. **Pipeline** → **Spark** activity → reference job on ADLS: sum `total_spend_gbp` by `segment`.
6. **Trigger** → output Parquet/CSV to `gold/segment_totals/`.
7. **Delete HDInsight cluster** after verify — **mandatory tear-down**.

## Part B — JSON (Spark activity)

```json
{
  "name": "Spark_aggregate_segments",
  "type": "SparkJob",
  "linkedServiceName": { "referenceName": "ls_hdinsight_spark", "type": "LinkedServiceReference" },
  "typeProperties": {
    "rootPath": "wasb://bronze@stadfcoursejinesh.blob.core.windows.net",
    "entryFilePath": "scripts/spark_aggregate.py",
    "className": "com.finledger.AggregateSegments"
  }
}
```

> ℹ️ NOTE: Follow MS Learn for exact Spark job definition format for your cluster version.

## Part D — Verify

| segment | aggregated total (approx) |
|---|---|
| premium | sum of premium rows |
| standard | sum of standard rows |

## Cost & tear-down

**Delete cluster** — ££ per hour while running.

## Next

[04-03 · Hive transformation](04-03-hive-transformation.md)
