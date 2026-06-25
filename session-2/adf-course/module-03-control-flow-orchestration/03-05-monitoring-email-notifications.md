# 03-05 · Monitoring & email notifications

> Module 3 · Time budget: 35 min · Source: [Send email from ADF](https://learn.microsoft.com/en-us/azure/data-factory/how-to-send-email)
> Prereqs: [03-04 · Triggers](03-04-triggers-schedule-tumbling-event-custom.md)

## What you'll build

**Failure alert path:** Logic App with **When a HTTP request is received** → **Send email (Office 365 Outlook)** or Gmail connector; pipeline **`pl_finledger_nightly_foreach`** adds **Web** activity on failure branch calling Logic App URL with run metadata. Optional **Azure Monitor alert** on pipeline failed metric.

## Why this matters

FinLedger ops needs `@owner_email` from config notified when nightly ForEach fails — before stores open. Monitor alone requires someone watching; **automated email** closes the loop (Hour 23 morning check pattern in Session 2 `morning_check.py`).

## Part A — Logic App (portal)

1. Portal → **Create** → **Logic App** → `la-finledger-adf-alerts-{learner}` → UK South.
2. **Logic app designer** → **When a HTTP request is received** — copy **HTTP POST URL**.
3. Use sample JSON schema:

```json
{
  "pipelineName": "pl_finledger_nightly_foreach",
  "runId": "guid",
  "status": "Failed",
  "owner_email": "data-eng@example.finledger.uk"
}
```

4. **+ New step** → **Send an email (V2)** → To: dynamic `owner_email` from body → Subject: `FinLedger ADF failure` → Body: include `pipelineName`, `runId`.
5. **Save** Logic App.

### ADF failure Web activity

6. Edit `pl_finledger_nightly_foreach` → top-level **If** after ForEach: `@equals(activity('ForEach_enabled_files').status, 'Failed')` OR add **On failure path** via dependency **Failed** on ForEach.
7. **Web** `Notify_failure`:
   - Method POST
   - URL: Logic App URL
   - Body: `@json(concat('{\"pipelineName\":\"pl_finledger_nightly_foreach\",\"runId\":\"', pipeline().RunId, '\",\"status\":\"Failed\",\"owner_email\":\"', variables('owner_email'), '\"}'))`
8. **Publish** → force failure (bad manifest path) → email received.

### Azure Monitor alert (optional)

9. Factory → **Monitor** in portal → **Alerts** → **New alert rule** → condition: ADF pipeline failed runs &gt; 0 → action group email.

## Part B — JSON (Web body)

```json
{
  "name": "Notify_failure",
  "type": "WebActivity",
  "dependsOn": [
    { "activity": "ForEach_enabled_files", "dependencyConditions": ["Failed"] }
  ],
  "typeProperties": {
    "method": "POST",
    "url": "https://prod-xx.westus.logic.azure.com/workflows/.../triggers/manual/paths/invoke?api-version=2016-10-01&sp=...",
    "body": {
      "value": "@json(concat('{\"pipelineName\":\"', pipeline().Pipeline, '\",\"runId\":\"', pipeline().RunId, '\",\"status\":\"Failed\",\"owner_email\":\"', variables('owner_email'), '\"}'))",
      "type": "Expression"
    },
    "headers": { "Content-Type": "application/json" }
  }
}
```

## Part D — Verify

| Check | Expected |
|---|---|
| Forced failure | Pipeline Failed |
| Web activity | Succeeded (Logic App called) |
| Email | Received within 2 min |
| Monitor | Failed run visible with error detail |

## Common errors

Logic App 401 — SAS URL wrong. Web success but no email — connector auth not configured.

## Cost & tear-down

Logic App consumption minimal. Delete Logic App after course.

## Module 3 recap

Lookup/Set Variable/Web → ForEach manifest → parameters/run_id → schedule trigger → failure email.

## Next

[04-01 · Databricks notebook activity](../module-04-external-compute/04-01-databricks-notebook-activity.md)
