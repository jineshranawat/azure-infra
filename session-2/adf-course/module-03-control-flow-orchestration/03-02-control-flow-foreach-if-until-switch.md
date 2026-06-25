# 03-02 · ForEach, If, Until, Switch

> Module 3 · Time budget: 40 min · Source: [Control flow tutorial](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-control-flow-portal)
> Prereqs: [03-01 · Activities catalogue](03-01-pipeline-activities-catalogue.md), [`file_manifest.json`](../data/module-03-control-flow/file_manifest.json)

## What you'll build

Pipeline **`pl_finledger_nightly_foreach`**: **Lookup** manifest → **Filter** enabled files → **ForEach** entity → **Copy** using `pl_copy_entity_param` or inline copy → **If** on copy status. **3 copies** run (`stores` skipped — `enabled: false`).

## Why this matters

FinLedger ingests multiple entities nightly from one schedule. **ForEach** iterates JSON array from manifest — add a feed without new pipeline. **Switch** routes by entity type; **Until** polls external job status (pattern for Module 4).

## Part A — UI (click by click)

1. Upload `file_manifest.json` → `bronze/incoming/manifests/file_manifest.json`.
2. Dataset **`ds_file_manifest_json`** → Json path above.
3. Pipeline **`pl_finledger_nightly_foreach`**:
4. **Lookup** `Lookup_manifest` → `ds_file_manifest_json` → **First row only:** OFF (full array in `files` — use dataset structure; for array root use **Set Variable** with expression or store as lines — simplified lab: Lookup returns document, use **ForEach** items: `@activity('Lookup_manifest').output.value[0].files` or filter dataset).
   > 💡 TIP: MS Learn pattern uses Lookup on DB; for JSON array create dataset with file containing array and expression `@activity('Lookup_manifest').output.value` — verify in debug output.
5. **ForEach** `ForEach_enabled_files`:
   - **Items:** `@array(activity('Lookup_manifest').output.value[0].files)` — adjust after inspecting Lookup output JSON shape in Monitor.
   - **Sequential:** OFF (parallel up to `max_parallel_copies: 2`).
6. Inside ForEach, **If Condition** `If_enabled`:
   - Expression: `@equals(item().enabled, true)`
   - **True:** **Copy** activity — source path `@item().source_path`, sink `@item().sink_folder` (dynamic datasets or Execute Pipeline with parameters).
   - **False:** **Wait** 1 second (no-op branch).
7. **Publish** → **Trigger** → Monitor: **3** copy iterations succeed, stores skipped.
8. **Switch** (optional extension): add parallel **Switch** on `@item().entity` for entity-specific pipelines.

### Until demo (conceptual)

9. Add **Until** loop: expression `@equals(variables('job_done'), 'yes')` with **Wait** + **Set Variable** simulating poll — used when waiting for HDInsight job (Module 4).

## Part B — JSON (ForEach fragment)

```json
{
  "name": "ForEach_enabled_files",
  "type": "ForEach",
  "dependsOn": [{ "activity": "Lookup_manifest", "dependencyConditions": ["Succeeded"] }],
  "typeProperties": {
    "items": {
      "value": "@json(string(activity('Lookup_manifest').output.value)).files",
      "type": "Expression"
    },
    "isSequential": false,
    "batchCount": 2,
    "activities": [
      {
        "name": "If_enabled",
        "type": "IfCondition",
        "typeProperties": {
          "expression": { "value": "@equals(item().enabled, true)", "type": "Expression" },
          "ifTrueActivities": [
            {
              "name": "Copy_entity_file",
              "type": "Copy",
              "typeProperties": {
                "source": { "type": "DelimitedTextSource" },
                "sink": { "type": "DelimitedTextSink" }
              },
              "inputs": [{ "referenceName": "ds_bronze_csv_source", "type": "DatasetReference", "parameters": { "entity_name": { "value": "@item().entity", "type": "Expression" }, "file_name": { "value": "@last(split(item().source_path, '/'))", "type": "Expression" } } }],
              "outputs": [{ "referenceName": "ds_bronze_csv_sink", "type": "DatasetReference", "parameters": { "entity_name": { "value": "@item().entity", "type": "Expression" }, "file_name": { "value": "@last(split(item().source_path, '/'))", "type": "Expression" } } }]
            }
          ]
        }
      }
    ]
  }
}
```

## Part D — Verify

| Entity | enabled | Copied |
|---|---|---|
| transactions | true | Yes |
| customers | true | Yes |
| products | true | Yes |
| stores | false | Skipped |

## Common errors

ForEach items expression null — inspect Lookup JSON in Monitor. Batch count — limit parallel copies.

## Next

[03-03 · Parameters & dynamic content](03-03-parameters-variables-expressions-dynamic-content.md)
