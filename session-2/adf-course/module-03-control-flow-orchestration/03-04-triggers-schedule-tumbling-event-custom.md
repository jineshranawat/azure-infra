# 03-04 · Triggers (schedule, tumbling, event)

> Module 3 · Time budget: 40 min · Source: [Triggers in ADF](https://learn.microsoft.com/en-us/azure/data-factory/concepts-triggers)
> Prereqs: [03-03 · Expressions](03-03-parameters-variables-expressions-dynamic-content.md)

## What you'll build

**Schedule trigger** `tr_finledger_daily_6am` (06:00 UK — set timezone **GMT Standard Time**), **Tumbling window** trigger preview for hourly slices, and **Storage event** trigger on `bronze/incoming/` blob create (optional). All target **`pl_finledger_nightly_foreach`**.

## Why this matters

Manual **Trigger now** does not scale. FinLedger nightly load runs on **schedule**. **Tumbling window** backfills missed hours with self-dependent windows. **Blob events** near-real-time ingest when partner drops files.

## Part A — Schedule trigger (click by click)

1. ADF Studio → **Manage** → **Triggers** → **+ New**.
2. **Trigger type:** **Schedule**.
3. **Name:** `tr_finledger_daily_6am`.
4. **Recurrence:** every **1 Day**, **At:** `06:00:00`.
5. **Time zone:** `(UTC) Dublin, Edinburgh, Lisbon, London` or **GMT Standard Time**.
6. **Start date:** today. **End:** none (or course end date).
7. **Activated:** checked after publish.
8. **Pipeline:** `pl_finledger_nightly_foreach` → parameters `run_id` = `@formatDateTime(utcnow(),'yyyyMMdd-HHmm')`.
9. **OK** → **Publish all**.
10. Triggers blade → **Status** **Started** → **Next trigger** shows tomorrow 06:00.

### Tumbling window (lab preview)

11. **+ New** → **Tumbling window** → `tr_hourly_backfill`.
12. **Interval:** 1 hour, **dependency:** **Sequential** (previous window must succeed).
13. Pipeline receives `windowStart`, `windowEnd` system parameters — pass to copy filter for incremental hours.

### Storage event (optional)

14. **+ New** → **Storage events** → storage `stadfcourse{learner}` → container `bronze` → path begins `incoming/transactions/`.
15. **Event:** Blob created → pipeline with `run_id=event-driven`.

> ⚠️ WARNING: Event triggers require Event Grid subscription — small cost per event.

## Part B — JSON

`trigger/tr_finledger_daily_6am.json`

```json
{
  "name": "tr_finledger_daily_6am",
  "properties": {
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "startTime": "2026-06-10T06:00:00Z",
        "timeZone": "GMT Standard Time",
        "schedule": { "minutes": [0], "hours": [6] }
      }
    },
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "pl_finledger_nightly_foreach",
          "type": "PipelineReference"
        },
        "parameters": {
          "run_id": { "value": "@formatDateTime(utcnow(),'yyyyMMdd')", "type": "Expression" }
        }
      }
    ],
    "runtimeState": "Started"
  }
}
```

## Part D — Verify

| Check | Expected |
|---|---|
| Schedule Started | Green toggle |
| Next trigger | Visible datetime |
| Manual run still works | Trigger now independent |

## Cost & tear-down

**Stop/disable triggers** after course: Manage → trigger → **Stop**.

## Next

[03-05 · Monitoring & email](03-05-monitoring-email-notifications.md)
