"""Deploy ADF copy pipeline + datasets via SDK (azure.html Day 2 Hours 17–19).

Idempotent: create_or_update on linked service, datasets, pipeline — safe to re-run.
Pipeline: copy bronze/incoming → bronze/loaded (parametrised folder) — no IR cost at idle.
"""

from __future__ import annotations

import logging

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    AzureBlobFSLocation,
    AzureBlobFSReadSettings,
    AzureBlobFSWriteSettings,
    CopyActivity,
    DatasetReference,
    DatasetResource,
    DelimitedTextDataset,
    DelimitedTextSink,
    DelimitedTextSource,
    DelimitedTextWriteSettings,
    AzureBlobFSLinkedService,
    LinkedServiceReference,
    LinkedServiceResource,
    PipelineResource,
    ActivityPolicy,
)

from _config import SessionConfig

logger = logging.getLogger(__name__)

LINKED_SERVICE_NAME = "AdlsBronzeLinkedService"
SOURCE_DATASET = "ds_bronze_incoming_csv"
SINK_DATASET = "ds_bronze_loaded_csv"
PIPELINE_NAME = "pl_bronze_copy"


def _client(cfg: SessionConfig) -> DataFactoryManagementClient:
    return DataFactoryManagementClient(DefaultAzureCredential(), cfg.subscription_id)


def ensure_adf_artifacts(
    cfg: SessionConfig,
    storage_account: str,
    data_factory: str,
) -> None:
    """Create or update linked service (MSI), datasets, and copy pipeline."""
    adf = _client(cfg)
    rg = cfg.resource_group
    dfs_url = f"https://{storage_account}.dfs.core.windows.net"

    # Linked service — MSI auth for factory copy activities
    ls_body = LinkedServiceResource(
        properties=AzureBlobFSLinkedService(
            url=dfs_url,
            account_key=None,
            tenant=None,
            azure_cloud_type=None,
            encrypted_credential=None,
            connect_via=None,
            description="ADLS Gen2 bronze — Session 2 linked service",
            parameters={},
            annotations=[],
        )
    )
    adf.linked_services.create_or_update(rg, data_factory, LINKED_SERVICE_NAME, ls_body)
    logger.info("Linked service '%s' ready", LINKED_SERVICE_NAME)

    ls_ref = LinkedServiceReference(reference_name=LINKED_SERVICE_NAME, type="LinkedServiceReference")

    # Source dataset — parametrised incoming folder
    source_ds = DatasetResource(
        properties=DelimitedTextDataset(
            linked_service_name=ls_ref,
            location=AzureBlobFSLocation(
                file_system="bronze",
                folder_path={"value": "@dataset().incoming_folder", "type": "Expression"},
                file_name="sample_transactions.csv",
            ),
            column_delimiter=",",
            first_row_as_header=True,
            parameters={"incoming_folder": {"type": "String"}},
        )
    )
    adf.datasets.create_or_update(rg, data_factory, SOURCE_DATASET, source_ds)

    # Sink dataset — parametrised loaded folder
    sink_ds = DatasetResource(
        properties=DelimitedTextDataset(
            linked_service_name=ls_ref,
            location=AzureBlobFSLocation(
                file_system="bronze",
                folder_path={"value": "@dataset().loaded_folder", "type": "Expression"},
                file_name="sample_transactions.csv",
            ),
            column_delimiter=",",
            first_row_as_header=True,
            parameters={"loaded_folder": {"type": "String"}},
        )
    )
    adf.datasets.create_or_update(rg, data_factory, SINK_DATASET, sink_ds)
    logger.info("Datasets '%s', '%s' ready", SOURCE_DATASET, SINK_DATASET)

    copy = CopyActivity(
        name="CopyIncomingToLoaded",
        inputs=[DatasetReference(reference_name=SOURCE_DATASET, type="DatasetReference", parameters={"incoming_folder": {"value": "@pipeline().parameters.incoming_folder", "type": "Expression"}})],
        outputs=[DatasetReference(reference_name=SINK_DATASET, type="DatasetReference", parameters={"loaded_folder": {"value": "@pipeline().parameters.loaded_folder", "type": "Expression"}})],
        source=DelimitedTextSource(store_settings=AzureBlobFSReadSettings(recursive=True)),
        sink=DelimitedTextSink(write_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")),
        policy=ActivityPolicy(retry=1, retry_interval_in_seconds=30, timeout="0.12:00:00"),
    )

    pipeline = PipelineResource(
        properties={
            "activities": [copy],
            "parameters": {
                "incoming_folder": {"type": "String"},
                "loaded_folder": {"type": "String"},
            },
            "annotations": ["session-2", "bronze-copy"],
        }
    )
    adf.pipelines.create_or_update(rg, data_factory, PIPELINE_NAME, pipeline)
    logger.info("Pipeline '%s' ready (idle cost ~£0)", PIPELINE_NAME)


def trigger_pipeline_run(
    cfg: SessionConfig,
    data_factory: str,
    incoming_folder: str,
    loaded_folder: str,
) -> str:
    """Start one pipeline run; return run id."""
    adf = _client(cfg)
    run = adf.pipelines.create_run(
        cfg.resource_group,
        data_factory,
        PIPELINE_NAME,
        parameters={
            "incoming_folder": incoming_folder,
            "loaded_folder": loaded_folder,
        },
    )
    logger.info("Pipeline run started: %s", run.run_id)
    return run.run_id
