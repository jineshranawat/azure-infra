# 04-03 · Hive transformation

> Module 4 · Time budget: 50 min · Source: [Transform with Hive on HDInsight](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-transform-data-hive-virtual-network-portal)
> Prereqs: [04-02 · Spark](04-02-hdinsight-spark-transformation.md)

## What you'll build

HDInsight **Hadoop** cluster (or reuse Spark cluster with Hive), **Hive** linked service, pipeline **`pl_hive_segment_report`** with **HDInsightHive** activity executing:

```sql
INSERT OVERWRITE DIRECTORY 'abfss://bronze@stadfcourse{learner}.dfs.core.windows.net/gold/hive_segment_report/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT segment, SUM(total_spend_gbp) AS total_spend, COUNT(*) AS account_count
FROM scoring_input_external
GROUP BY segment;
```

External table `scoring_input_external` points at `silver/scoring_input/`.

## Part A — UI (click by click)

1. HDInsight Hadoop cluster OR enable Hive on existing cluster.
2. ADF **Linked service** → **Azure HDInsight** → `ls_hdinsight_hive`.
3. **Pipeline** → **HDInsight Hive** activity `Hive_segment_report`.
4. **Script linked service** → ADLS script path OR inline script in activity.
5. **Hive** script path: `wasbs://bronze@.../scripts/hive_segment_report.hql`.
6. **Trigger** → Monitor Succeeded → `gold/hive_segment_report/` files.
7. **Delete cluster** after lab.

## Part B — JSON

```json
{
  "name": "Hive_segment_report",
  "type": "HDInsightHive",
  "linkedServiceName": { "referenceName": "ls_hdinsight_hive", "type": "LinkedServiceReference" },
  "typeProperties": {
    "scriptPath": "wasb://bronze@stadfcoursejinesh.blob.core.windows.net/scripts/hive_segment_report.hql",
    "defines": { "segmentTable": "scoring_input_external" }
  }
}
```

## Part D — Verify

| Check | Expected |
|---|---|
| Hive activity | Succeeded |
| Output | 2 segment groups (premium, standard) |

## Module 4 recap

External compute: Databricks (notebooks), Spark (jar/job), Hive (SQL). ADF orchestrates; clusters bill separately.

## Next

[05-01 · Managed VNet concepts](../module-05-networking-security/05-01-managed-vnet-private-endpoints-concepts.md)
