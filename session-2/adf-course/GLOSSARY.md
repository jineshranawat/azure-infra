# ADF Course Glossary

Every Azure Data Factory term is defined **once** here and linked from lessons on first appearance. Terms marked *(pending)* will be expanded when their defining lesson is generated.

---

## A

### Activity
A single step inside a pipeline — e.g. Copy, Lookup, Execute Data Flow. Activities are the units of work ADF schedules and monitors.

*First appears:* [00-00 · Overview](module-00-foundations/00-00-overview.md)

### ADLS Gen2 (Azure Data Lake Storage Gen2)
Azure storage with a hierarchical namespace (folders), POSIX-style ACLs, and analytics-optimized access. The course lakehouse landing zone.

*First appears:* [00-01 · Create storage](module-00-foundations/00-01-create-storage-adls-gen2.md)

### ADF (Azure Data Factory)
Microsoft's cloud ETL/ELT orchestration service. Moves and transforms data between hundreds of sources and sinks on a schedule or event.

*First appears:* [00-00 · Overview](module-00-foundations/00-00-overview.md)

### ADF Studio
The browser-based authoring experience for Data Factory — Author, Manage, Monitor, and Learn hubs.

*First appears:* [00-03 · Studio tour](module-00-foundations/00-03-studio-tour-every-pane.md)

### AutoResolveIntegrationRuntime
The default Azure-hosted integration runtime ADF uses for cloud-to-cloud copy when you do not specify another IR.

*First appears:* [00-04 · Linked services & IR](module-00-foundations/00-04-linked-services-and-integration-runtime.md) — deep dive in [00-05](module-00-foundations/00-05-link-adf-to-storage-step-by-step.md)

---

## C

### Container
Top-level blob partition in a storage account (e.g. `bronze`, `silver`, `gold`). Private by default in this course.

*First appears:* [00-01 · Create storage](module-00-foundations/00-01-create-storage-adls-gen2.md)

### Copy activity
Pipeline activity that moves data from a source dataset to a sink dataset using the Copy Data engine (no Spark required).

*First appears:* [01-01 · Copy Data tool](module-01-copy-ingest/01-01-copy-data-tool.md)

### Copy Data tool
ADF Studio wizard that generates a pipeline, datasets, and linked services from a guided UI flow.

*First appears:* [01-01 · Copy Data tool](module-01-copy-ingest/01-01-copy-data-tool.md)

---

## D

### Dataset
Named view of data structure and location in ADF. References a linked service and defines path, schema, and format — not the data itself.

*First appears:* [00-04 · Linked services & IR](module-00-foundations/00-04-linked-services-and-integration-runtime.md) — deep dive in [00-05](module-00-foundations/00-05-link-adf-to-storage-step-by-step.md)

### Data flow (mapping data flow)
Visual, Spark-backed transformation authored in ADF Studio. Compiles to Spark jobs at runtime.

*First appears:* [02-01 · Data flow fundamentals](module-02-data-flows/02-01-data-flow-fundamentals-debug-canvas.md)

### Hierarchical namespace (HNS)
ADLS Gen2 feature enabling real folders and POSIX-style paths on blob storage. Portal: **Enable hierarchical namespace**; ARM: `isHnsEnabled: true`.

*First appears:* [00-01 · Create storage](module-00-foundations/00-01-create-storage-adls-gen2.md)

---

## I

### Integration Runtime (IR)
Compute infrastructure ADF uses to move and transform data. Types: Azure (managed), Self-hosted (on-prem), Azure-SSIS.

*First appears:* [00-04 · Linked services & IR](module-00-foundations/00-04-linked-services-and-integration-runtime.md) — deep dive in [00-05](module-00-foundations/00-05-link-adf-to-storage-step-by-step.md)

---

## L

### Linked service
Connection string in ADF terms — stores *how* to reach an external system (storage, SQL, S3, etc.), not *what* data to read.

*First appears:* [00-04 · Linked services & IR](module-00-foundations/00-04-linked-services-and-integration-runtime.md) — deep dive in [00-05](module-00-foundations/00-05-link-adf-to-storage-step-by-step.md)

---

## M

### Managed identity (system-assigned)
Azure AD identity attached to the Data Factory resource. ADF uses it to authenticate to storage and Key Vault without storing secrets in linked services.

*First appears:* [00-05 · Link ADF to storage](module-00-foundations/00-05-link-adf-to-storage-step-by-step.md)

### Mapping data flow
See **Data flow (mapping data flow)**.

---

## P

### Pipeline
A logical grouping of activities with dependencies, parameters, and optional triggers. The main unit you deploy and monitor.

*First appears:* [00-00 · Overview](module-00-foundations/00-00-overview.md)

### Pipeline run
One execution instance of a pipeline, usually started by a trigger or manual **Trigger now**. Identified by a run ID in Monitor.

*First appears:* [00-00 · Overview](module-00-foundations/00-00-overview.md)

### Parameter (pipeline / dataset)
External input to a pipeline or dataset, supplied at trigger time — enables one pipeline to serve many tables or paths.

*First appears:* [01-03 · Datasets & parameters](module-01-copy-ingest/01-03-datasets-linked-services-parameters.md) *(pending)*

---

## T

### Trigger
Schedules or events that start pipeline runs — schedule, tumbling window, storage event, or custom.

*First appears:* [03-04 · Triggers](module-03-control-flow-orchestration/03-04-triggers-schedule-tumbling-event-custom.md)

---

## W

### Watermark
Stored high-water mark (timestamp or ID) used to copy only new/changed rows on incremental loads.

*First appears:* [01-07 · Incremental copy](module-01-copy-ingest/01-07-incremental-copy-patterns.md) *(pending)*

---

## Naming quick reference

See [SETUP.md](SETUP.md) for full canonical names. Prefixes: `ls_` linked service, `ds_` dataset, `pl_` pipeline, `df_` data flow, `tr_` trigger.

---

*This glossary grows as lessons are generated. Do not duplicate definitions in lesson bodies — link here instead.*
