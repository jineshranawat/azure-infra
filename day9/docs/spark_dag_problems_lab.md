# Spark DAG Lab (simple) — trainer note

**Notebook:** https://adb-7405613791235979.19.azuredatabricks.net/#workspace/Shared/day9/spark_dag_problems_lab  

v2 = **easy cells**: short code, ASCII diagrams, no matplotlib DAG helper, no repeated unions.

## Pattern every time

1. Read 3–5 lines  
2. Run problem  
3. Glance Spark UI  
4. Run fix  

## Topics (12)

Lazy action · shuffle · tiny/fat partitions · skew salt · broadcast · no collect · cache · prune · cross join · no UDF · no spill lists  

```cmd
python scripts\deploy_spark_dag_problems_lab.py
```
