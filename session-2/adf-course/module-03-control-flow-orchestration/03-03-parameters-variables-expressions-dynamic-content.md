# 03-03 · Parameters, variables & expressions

> Module 3 · Time budget: 35 min · Source: [Expression builder](https://learn.microsoft.com/en-us/azure/data-factory/how-to-expression-builder-adf)
> Prereqs: [03-02 · ForEach](03-02-control-flow-foreach-if-until-switch.md)

## What you'll build

Enhance **`pl_finledger_nightly_foreach`** with pipeline parameters **`run_id`**, **`environment`**, dynamic sink paths `@concat('loaded/', pipeline().parameters.run_id, '/', item().entity)`, and **system variables** `@pipeline().RunId`, `@utcnow()`.

## Why this matters

Hard-coded paths prevent audit trails. FinLedger ops need **run_id** folders (`loaded/transactions/manual-20260610/`) for reprocessing and support tickets.

## Part A — UI (click by click)

1. Open `pl_finledger_nightly_foreach` → **Parameters** tab:
   - `run_id` (String, default `manual`)
   - `environment` (String, default `dev`)
2. Inside ForEach Copy sink dataset parameters:
   - `entity_name` = `@concat(item().entity, '/', pipeline().parameters.run_id)` or folder expression on sink.
3. **Set Variable** at pipeline start: `run_timestamp` = `@utcnow('yyyy-MM-ddTHH:mm:ss')`.
4. **Append variable** (if using AppendVariable activity): build log message `@concat('FinLedger run ', pipeline().parameters.run_id, ' at ', variables('run_timestamp'))`.
5. **Trigger now** → parameters `run_id=test-run-001` → verify storage path includes `test-run-001`.
6. Use **Add dynamic content** picker — browse functions: `concat`, `substring`, `coalesce`, `if`.

## Part B — Expression reference (FinLedger)

| Expression | Use |
|---|---|
| `@pipeline().parameters.run_id` | Run folder |
| `@pipeline().RunId` | ADF GUID for correlation |
| `@activity('Copy_entity').output.rowsCopied` | Row count in next step |
| `@if(equals(pipeline().parameters.environment,'prod'), 'gold', 'silver')` | Environment branch |

## Part B — JSON

```json
{
  "parameters": {
    "run_id": { "type": "String", "defaultValue": "manual" },
    "environment": { "type": "String", "defaultValue": "dev" }
  },
  "variables": {
    "run_timestamp": { "type": "String" }
  }
}
```

## Part D — Verify

| Check | Expected |
|---|---|
| Custom run_id in path | `loaded/transactions/test-run-001/` |
| RunId in Monitor | Matches notification |

## Common errors

Expression type mismatch — wrap with `string()`. Parameter not passed at trigger — defaults used silently.

## Next

[03-04 · Triggers](03-04-triggers-schedule-tumbling-event-custom.md)
