"""ADF trigger teaching — tumbling window + storage event (blob created)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from azure.core.exceptions import HttpResponseError
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    AzureBlobFSReadSettings,
    AzureBlobFSWriteSettings,
    BlobEventsTrigger,
    CopyActivity,
    DelimitedTextSink,
    DelimitedTextSource,
    Expression,
    ParameterSpecification,
    PipelineReference,
    PipelineResource,
    SetVariableActivity,
    TriggerPipelineReference,
    TriggerResource,
    TumblingWindowTrigger,
)

from adf_pipelines import _client, _ds_ref
from discover import SharedEstate

logger = logging.getLogger(__name__)

TRIGGER_FOLDER = "12-triggers"

TRIGGER_PIPELINE_NAMES = [
    "pl_trig_01_tumbling_window_bronze",
    "pl_trig_02_storage_event_copy",
]

TRIGGER_NAMES = [
    "tr_tumbling_hourly_bronze",
    "tr_storage_bronze_incoming_csv",
]

INCOMING = "incoming/run=session3-lab"
LOADED = "loaded/run=session3-lab"


def _trigger_folder(body: PipelineResource) -> PipelineResource:
    from azure.mgmt.datafactory.models import PipelineFolder

    body.folder = PipelineFolder(name=TRIGGER_FOLDER)
    return body


def _deploy_trigger_pipelines(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
) -> None:
    set_window = SetVariableActivity(
        name="CaptureWindowRange",
        variable_name="window_audit_line",
        value=Expression(
            type="Expression",
            value=(
                "@concat("
                "'tumbling-window | start=', pipeline().parameters.window_start, "
                "' | end=', pipeline().parameters.window_end, "
                "' | factory=', pipeline().DataFactory"
                ")"
            ),
        ),
    )
    copy_in_window = CopyActivity(
        name="CopyFilesInWindow",
        depends_on=[
            ActivityDependency(activity="CaptureWindowRange", dependency_conditions=["Succeeded"])
        ],
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_file",
                folder_path={
                    "value": "@pipeline().parameters.incoming_folder",
                    "type": "Expression",
                },
                file_name={
                    "value": "@pipeline().parameters.expected_file_name",
                    "type": "Expression",
                },
            )
        ],
        outputs=[
            _ds_ref(
                "ds_bronze_loaded_file",
                folder_path={
                    "value": "@pipeline().parameters.loaded_folder",
                    "type": "Expression",
                },
                file_name={
                    "value": "@pipeline().parameters.expected_file_name",
                    "type": "Expression",
                },
            )
        ],
        source=DelimitedTextSource(
            store_settings=AzureBlobFSReadSettings(
                modified_datetime_start={
                    "value": "@pipeline().parameters.window_start",
                    "type": "Expression",
                },
                modified_datetime_end={
                    "value": "@pipeline().parameters.window_end",
                    "type": "Expression",
                },
                recursive=True,
            )
        ),
        sink=DelimitedTextSink(
            store_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")
        ),
    )
    pipelines: dict[str, PipelineResource] = {
        "pl_trig_01_tumbling_window_bronze": PipelineResource(
            activities=[set_window, copy_in_window],
            parameters={
                "window_start": ParameterSpecification(type="String"),
                "window_end": ParameterSpecification(type="String"),
                "incoming_folder": ParameterSpecification(
                    type="String", default_value=INCOMING
                ),
                "loaded_folder": ParameterSpecification(
                    type="String", default_value=LOADED
                ),
                "expected_file_name": ParameterSpecification(
                    type="String", default_value="sample_transactions.csv"
                ),
            },
            variables={"window_audit_line": {"type": "String"}},
            annotations=["trigger-tumbling-window", "concept-scheduled-batch"],
        ),
    }

    log_event = SetVariableActivity(
        name="LogStorageEvent",
        variable_name="event_audit_line",
        value=Expression(
            type="Expression",
            value=(
                "@concat("
                "'storage-event | file=', pipeline().parameters.trigger_file_name, "
                "' | folderPath=', pipeline().parameters.trigger_folder_path, "
                "' | run_id=', pipeline().parameters.run_id"
                ")"
            ),
        ),
    )
    copy_event_file = CopyActivity(
        name="CopyArrivedFile",
        depends_on=[
            ActivityDependency(activity="LogStorageEvent", dependency_conditions=["Succeeded"])
        ],
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_file",
                folder_path={
                    "value": "@pipeline().parameters.incoming_folder",
                    "type": "Expression",
                },
                file_name={
                    "value": "@pipeline().parameters.trigger_file_name",
                    "type": "Expression",
                },
            )
        ],
        outputs=[
            _ds_ref(
                "ds_bronze_loaded_file",
                folder_path={
                    "value": "@pipeline().parameters.loaded_folder",
                    "type": "Expression",
                },
                file_name={
                    "value": "@pipeline().parameters.trigger_file_name",
                    "type": "Expression",
                },
            )
        ],
        source=DelimitedTextSource(
            store_settings=AzureBlobFSReadSettings(recursive=True)
        ),
        sink=DelimitedTextSink(
            store_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")
        ),
    )
    pipelines["pl_trig_02_storage_event_copy"] = PipelineResource(
        activities=[log_event, copy_event_file],
        parameters={
            "trigger_file_name": ParameterSpecification(type="String"),
            "trigger_folder_path": ParameterSpecification(type="String"),
            "incoming_folder": ParameterSpecification(
                type="String", default_value=INCOMING
            ),
            "loaded_folder": ParameterSpecification(type="String", default_value=LOADED),
            "run_id": ParameterSpecification(type="String", default_value="storage-trigger"),
        },
        variables={"event_audit_line": {"type": "String"}},
        annotations=["trigger-storage-event", "concept-event-driven"],
    )

    for name, body in pipelines.items():
        _trigger_folder(body)
        adf.pipelines.create_or_update(rg, factory, name, body)
        logger.info("Trigger pipeline %s", name)


def _storage_account_scope(cfg, estate: SharedEstate) -> str:
    return (
        f"/subscriptions/{cfg.subscription_id}/resourceGroups/{cfg.resource_group}"
        f"/providers/Microsoft.Storage/storageAccounts/{estate.storage_account}"
    )


def _deploy_triggers(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
    cfg,
    estate: SharedEstate,
) -> None:
    """Deploy triggers stopped — trainer starts manually in ADF Studio (class safety)."""
    start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

    tumbling = TumblingWindowTrigger(
        description=(
            "Hourly tumbling window — copies bronze files modified inside each window. "
            "Teaching: batch ETL on a fixed schedule with windowStart/windowEnd."
        ),
        annotations=["lab-tumbling-window"],
        pipeline=TriggerPipelineReference(
            pipeline_reference=PipelineReference(
                reference_name="pl_trig_01_tumbling_window_bronze",
                type="PipelineReference",
            ),
            parameters={
                "window_start": {
                    "value": "@trigger().outputs.windowStartTime",
                    "type": "Expression",
                },
                "window_end": {
                    "value": "@trigger().outputs.windowEndTime",
                    "type": "Expression",
                },
                "incoming_folder": INCOMING,
                "loaded_folder": LOADED,
                "expected_file_name": "sample_transactions.csv",
            },
        ),
        frequency="Hourly",
        interval=1,
        start_time=start,
        end_time=end,
        max_concurrency=1,
    )

    storage_scope = _storage_account_scope(cfg, estate)
    blob_events = BlobEventsTrigger(
        description=(
            "Storage event — runs when a .csv blob is created under "
            "bronze/incoming/run=session3-lab/. Teaching: event-driven ingest."
        ),
        annotations=["lab-storage-event"],
        scope=storage_scope,
        events=["Microsoft.Storage.BlobCreated"],
        blob_path_begins_with="/bronze/incoming/run=session3-lab/",
        blob_path_ends_with=".csv",
        ignore_empty_blobs=True,
        pipelines=[
            TriggerPipelineReference(
                pipeline_reference=PipelineReference(
                    reference_name="pl_trig_02_storage_event_copy",
                    type="PipelineReference",
                ),
                parameters={
                    "trigger_file_name": {
                        "value": "@trigger().outputs.body.fileName",
                        "type": "Expression",
                    },
                    "trigger_folder_path": {
                        "value": "@trigger().outputs.body.folderPath",
                        "type": "Expression",
                    },
                    "incoming_folder": INCOMING,
                    "loaded_folder": LOADED,
                    "run_id": "storage-trigger",
                },
            )
        ],
    )

    specs: list[tuple[str, Any]] = [
        ("tr_tumbling_hourly_bronze", tumbling),
        ("tr_storage_bronze_incoming_csv", blob_events),
    ]

    for name, props in specs:
        adf.triggers.create_or_update(
            rg,
            factory,
            name,
            TriggerResource(properties=props),
        )
        try:
            poller = adf.triggers.begin_stop(rg, factory, name)
            poller.result(timeout=120)
            logger.info("Trigger %s deployed (Stopped)", name)
        except HttpResponseError as exc:
            if "not in Running state" in str(exc) or "NotInRunningState" in str(exc):
                logger.info("Trigger %s already Stopped", name)
            else:
                logger.warning("Could not stop trigger %s: %s", name, exc)


def deploy_triggers(cfg, estate: SharedEstate) -> None:
    adf = _client(cfg)
    rg, factory = cfg.resource_group, estate.data_factory
    _deploy_trigger_pipelines(adf, rg, factory)
    _deploy_triggers(adf, rg, factory, cfg, estate)


def list_trigger_pipeline_names() -> list[str]:
    return list(TRIGGER_PIPELINE_NAMES)


def list_trigger_names() -> list[str]:
    return list(TRIGGER_NAMES)


def default_params(pipeline_name: str) -> dict[str, Any]:
    if pipeline_name == "pl_trig_01_tumbling_window_bronze":
        return {
            "window_start": "2026-07-12T00:00:00Z",
            "window_end": "2026-07-12T01:00:00Z",
            "incoming_folder": INCOMING,
            "loaded_folder": LOADED,
            "expected_file_name": "sample_transactions.csv",
        }
    if pipeline_name == "pl_trig_02_storage_event_copy":
        return {
            "trigger_file_name": "sample_transactions.csv",
            "trigger_folder_path": f"bronze/{INCOMING}",
            "incoming_folder": INCOMING,
            "loaded_folder": LOADED,
            "run_id": "manual-test",
        }
    return {}
