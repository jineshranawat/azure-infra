"""Deploy all shared ADF linked services + teaching pipelines (idempotent SDK)."""

from __future__ import annotations

import logging
import subprocess
import time
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    AzureBlobFSLinkedService,
    AzureBlobFSLocation,
    AzureBlobFSReadSettings,
    AzureBlobFSWriteSettings,
    AzureDatabricksLinkedService,
    AzureSqlDatabaseLinkedService,
    AzureSqlSink,
    AzureSqlSource,
    AzureSqlTableDataset,
    CopyActivity,
    DatasetReference,
    DatasetResource,
    DatabricksNotebookActivity,
    DelimitedTextDataset,
    DelimitedTextSink,
    DelimitedTextSource,
    ExecutePipelineActivity,
    Expression,
    FilterActivity,
    ForEachActivity,
    GetMetadataActivity,
    IfConditionActivity,
    JsonDataset,
    JsonSource,
    LinkedServiceReference,
    LinkedServiceResource,
    LookupActivity,
    ParameterSpecification,
    PipelineReference,
    PipelineResource,
    SecureString,
    ScriptActivity,
    ScriptActivityScriptBlock,
    SetVariableActivity,
    SqlServerStoredProcedureActivity,
    WaitActivity,
    ActivityPolicy,
)

from _config import SharedAdfConfig, find_az, get_credential
from azure_sql import SqlEstate
from discover import SharedEstate

logger = logging.getLogger(__name__)

# Linked service names
LS_ADLS = "ls_adls_finledger"
LS_DATABRICKS = "ls_databricks_shared"
LS_AZURE_SQL = "ls_azure_sql_westus"

# Notebook imported by run_shared_adf.py
NOTEBOOK_PATH = "/Shared/shared-adf/nb_adf_hello"

_METADATA_BOOTSTRAP_SQL = """
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'adf_job_metadata')
CREATE TABLE dbo.adf_job_metadata (
    job_id NVARCHAR(32) PRIMARY KEY,
    job_name NVARCHAR(64) NOT NULL,
    incoming_folder NVARCHAR(256) NOT NULL,
    loaded_folder NVARCHAR(256) NOT NULL,
    expected_file_name NVARCHAR(128) NOT NULL,
    run_databricks INT NOT NULL DEFAULT 1,
    status NVARCHAR(32) NOT NULL DEFAULT 'READY',
    last_run_id NVARCHAR(64) NULL,
    last_message NVARCHAR(256) NULL,
    updated_utc DATETIME2 NULL
);

IF NOT EXISTS (SELECT 1 FROM dbo.adf_job_metadata WHERE job_id = N'job-01')
INSERT INTO dbo.adf_job_metadata (
    job_id, job_name, incoming_folder, loaded_folder,
    expected_file_name, run_databricks, status
) VALUES
    (N'job-01', N'bronze_metadata_orchestrate', N'incoming/run=session3-lab',
     N'loaded/run=session3-lab', N'sample_transactions.csv', 1, N'READY');

IF NOT EXISTS (SELECT 1 FROM dbo.adf_job_metadata WHERE job_id = N'job-02')
INSERT INTO dbo.adf_job_metadata (
    job_id, job_name, incoming_folder, loaded_folder,
    expected_file_name, run_databricks, status
) VALUES
    (N'job-02', N'disabled_skip_demo', N'incoming/run=session3-lab',
     N'loaded/run=session3-lab', N'sample_transactions.csv', 0, N'HOLD');

UPDATE dbo.adf_job_metadata
SET status = N'READY', last_run_id = NULL, last_message = NULL, updated_utc = NULL
WHERE job_id = N'job-01';
""".strip()

# Wrapped in EXEC so ADF Script activity never prepends session SET statements
# before CREATE/ALTER (error 111: must be first statement in batch).
_METADATA_MARK_STATUS_PROC = """
EXEC(N'
CREATE OR ALTER PROCEDURE dbo.usp_adf_mark_job_status
    @job_id NVARCHAR(32),
    @status NVARCHAR(32),
    @run_id NVARCHAR(64) = NULL,
    @message NVARCHAR(256) = NULL
AS
BEGIN
    UPDATE dbo.adf_job_metadata
    SET status = @status,
        last_run_id = COALESCE(@run_id, last_run_id),
        last_message = COALESCE(@message, last_message),
        updated_utc = GETUTCDATE()
    WHERE job_id = @job_id;
END
');
""".strip()

PIPELINE_NAMES = [
    "pl_01_bronze_copy",
    "pl_02_get_metadata",
    "pl_03_if_then_copy",
    "pl_04_foreach_dates",
    "pl_05_lookup_watermark",
    "pl_06_wait_then_copy",
    "pl_07_databricks_notebook",
    "pl_08_sql_to_lake",
    "pl_09_lake_to_sql",
    "pl_10_parent_chain",
    "pl_11_filename_validate_route",
    "pl_12_sql_metadata_orchestrate",
]


def _client(cfg: SharedAdfConfig) -> DataFactoryManagementClient:
    return DataFactoryManagementClient(get_credential(), cfg.subscription_id)


def _ls_ref(name: str) -> LinkedServiceReference:
    return LinkedServiceReference(reference_name=name, type="LinkedServiceReference")


def _ds_ref(name: str, **params: Any) -> DatasetReference:
    if params:
        return DatasetReference(
            reference_name=name,
            type="DatasetReference",
            parameters=params,
        )
    return DatasetReference(reference_name=name, type="DatasetReference")


def _exists(getter, rg: str, factory: str, name: str) -> bool:
    try:
        getter(rg, factory, name)
        return True
    except ResourceNotFoundError:
        return False
    except Exception as exc:
        if "NotFound" in str(exc) or "ResourceNotFound" in str(exc):
            return False
        raise


def _databricks_workspace_resource_id(cfg: SharedAdfConfig, workspace_name: str) -> str:
    return (
        f"/subscriptions/{cfg.subscription_id}/resourceGroups/{cfg.resource_group}"
        f"/providers/Microsoft.Databricks/workspaces/{workspace_name}"
    )


def _databricks_domain(cfg: SharedAdfConfig, estate: SharedEstate) -> str:
    if cfg.databricks_host:
        host = cfg.databricks_host.replace("https://", "").rstrip("/")
        return f"https://{host}"
    if not estate.databricks_workspace:
        raise SystemExit("Set DATABRICKS_HOST in .env or deploy Databricks workspace.")
    show = subprocess.run(
        [
            find_az(),
            "databricks",
            "workspace",
            "show",
            "-g",
            cfg.resource_group,
            "-n",
            estate.databricks_workspace,
            "--query",
            "workspaceUrl",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return f"https://{show.stdout.strip()}"


def _put_linked_service(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
    name: str,
    properties: dict[str, Any],
) -> None:
    """Upsert linked service with an explicit ``type`` in the payload.

    Some azure-mgmt-datafactory / Python combinations drop the polymorphic
    discriminator when serializing AzureBlobFSLinkedService models, which
    yields: Invalid linked service payload, the 'type' nested in payload is null.
    A plain dict with ``type`` set is reliable across SDK versions.
    """
    if not properties.get("type"):
        raise ValueError(f"Linked service {name} payload missing required 'type'")
    adf.linked_services.create_or_update(
        rg,
        factory,
        name,
        {"properties": properties},
    )


def deploy_linked_services(
    cfg: SharedAdfConfig,
    estate: SharedEstate,
    sql: SqlEstate | None,
) -> None:
    adf = _client(cfg)
    rg, factory = cfg.resource_group, estate.data_factory
    dfs_url = estate.dfs_endpoint

    # ADLS — MSI (overwrites Bicep placeholder without MSI)
    _put_linked_service(
        adf,
        rg,
        factory,
        LS_ADLS,
        {
            "type": "AzureBlobFS",
            "description": "ADLS Gen2 — shared FinLedger lake (eastus)",
            "typeProperties": {"url": dfs_url},
        },
    )
    logger.info("Linked service %s", LS_ADLS)

    # Databricks linked service:
    # - Prefer PAT token from .env (reliable in shared classroom setup)
    # - Fallback to MSI + workspace_resource_id when token is unavailable
    domain = _databricks_domain(cfg, estate)
    # Use the shared all-purpose cluster — avoids eastus DS3_v2 stockouts from
    # spinning up a new job cluster on every ADF notebook activity.
    dbx_type_props: dict[str, Any] = {
        "domain": domain,
        "existingClusterId": cfg.databricks_cluster_id,
    }
    if cfg.databricks_token:
        dbx_type_props["accessToken"] = {
            "type": "SecureString",
            "value": cfg.databricks_token,
        }
    else:
        dbx_type_props["authentication"] = "MSI"
        dbx_type_props["workspaceResourceId"] = _databricks_workspace_resource_id(
            cfg, estate.databricks_workspace
        )
    _put_linked_service(
        adf,
        rg,
        factory,
        LS_DATABRICKS,
        {
            "type": "AzureDatabricks",
            "description": (
                "Shared class Databricks — ADF notebook activities on existing cluster "
                f"({cfg.databricks_cluster_id})"
            ),
            "typeProperties": dbx_type_props,
        },
    )
    logger.info(
        "Linked service %s → %s (%s)",
        LS_DATABRICKS,
        domain,
        "PAT token" if cfg.databricks_token else "MSI",
    )

    if sql:
        conn = (
            f"Server=tcp:{sql.fqdn},1433;"
            f"Database={sql.database_name};"
            f"User ID={sql.admin_login};"
            f"Password={sql.admin_password};"
            "Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
        )
        _put_linked_service(
            adf,
            rg,
            factory,
            LS_AZURE_SQL,
            {
                "type": "AzureSqlDatabase",
                "description": f"Azure SQL westus — {sql.fqdn}",
                "typeProperties": {
                    "connectionString": {
                        "type": "SecureString",
                        "value": conn,
                    }
                },
            },
        )
        logger.info("Linked service %s → %s", LS_AZURE_SQL, sql.fqdn)


def _deploy_datasets(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
    *,
    include_sql: bool,
) -> None:
    ls = _ls_ref(LS_ADLS)

    datasets: list[tuple[str, DatasetResource]] = [
        (
            "ds_bronze_incoming_csv",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="bronze",
                        folder_path={
                            "value": "@dataset().folder_path",
                            "type": "Expression",
                        },
                        file_name="sample_transactions.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                    parameters={"folder_path": {"type": "String"}},
                )
            ),
        ),
        (
            "ds_bronze_loaded_csv",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="bronze",
                        folder_path={
                            "value": "@dataset().folder_path",
                            "type": "Expression",
                        },
                        file_name="sample_transactions.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                    parameters={"folder_path": {"type": "String"}},
                )
            ),
        ),
        (
            "ds_bronze_incoming_folder",
            DatasetResource(
                properties=JsonDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="bronze",
                        folder_path={
                            "value": "@dataset().folder_path",
                            "type": "Expression",
                        },
                    ),
                    parameters={"folder_path": {"type": "String"}},
                )
            ),
        ),
        (
            "ds_audit_watermark_json",
            DatasetResource(
                properties=JsonDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="audit",
                        file_name="watermark.json",
                    ),
                )
            ),
        ),
        (
            "ds_bronze_incoming_file",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="bronze",
                        folder_path={
                            "value": "@dataset().folder_path",
                            "type": "Expression",
                        },
                        file_name={
                            "value": "@dataset().file_name",
                            "type": "Expression",
                        },
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                    parameters={
                        "folder_path": {"type": "String"},
                        "file_name": {"type": "String"},
                    },
                )
            ),
        ),
        (
            "ds_bronze_loaded_file",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="bronze",
                        folder_path={
                            "value": "@dataset().folder_path",
                            "type": "Expression",
                        },
                        file_name={
                            "value": "@dataset().file_name",
                            "type": "Expression",
                        },
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                    parameters={
                        "folder_path": {"type": "String"},
                        "file_name": {"type": "String"},
                    },
                )
            ),
        ),
        (
            "ds_audit_sql_export_csv",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="audit",
                        folder_path="sql_export",
                        file_name="dim_channel.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        ),
    ]

    if include_sql:
        datasets.extend(
            [
                (
                    "ds_sql_dim_channel",
                    DatasetResource(
                        properties=AzureSqlTableDataset(
                            linked_service_name=_ls_ref(LS_AZURE_SQL),
                            table_name="dbo.dim_channel",
                        )
                    ),
                ),
                (
                    "ds_sql_stg_channel",
                    DatasetResource(
                        properties=AzureSqlTableDataset(
                            linked_service_name=_ls_ref(LS_AZURE_SQL),
                            table_name="dbo.stg_channel_from_lake",
                        )
                    ),
                ),
                (
                    "ds_sql_job_metadata",
                    DatasetResource(
                        properties=AzureSqlTableDataset(
                            linked_service_name=_ls_ref(LS_AZURE_SQL),
                            table_name="dbo.adf_job_metadata",
                        )
                    ),
                ),
                (
                    "ds_audit_job_metadata_seed_csv",
                    DatasetResource(
                        properties=DelimitedTextDataset(
                            linked_service_name=ls,
                            location=AzureBlobFSLocation(
                                file_system="audit",
                                folder_path="sql_seed",
                                file_name="job_metadata_seed.csv",
                            ),
                            column_delimiter=",",
                            first_row_as_header=True,
                        )
                    ),
                ),
            ]
        )

    for name, body in datasets:
        adf.datasets.create_or_update(rg, factory, name, body)
    logger.info("Datasets deployed (%d)", len(datasets))


def _copy_bronze_activity(name: str = "CopyBronze") -> CopyActivity:
    return CopyActivity(
        name=name,
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_csv",
                folder_path={
                    "value": "@pipeline().parameters.incoming_folder",
                    "type": "Expression",
                },
            )
        ],
        outputs=[
            _ds_ref(
                "ds_bronze_loaded_csv",
                folder_path={
                    "value": "@pipeline().parameters.loaded_folder",
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
        policy=ActivityPolicy(retry=1, retry_interval_in_seconds=30),
    )


def _copy_validated_file_activity(name: str = "CopyValidatedFile") -> CopyActivity:
    """Copy one validated file (dynamic file_name parameter) incoming → loaded."""
    file_expr = {
        "value": "@pipeline().parameters.expected_file_name",
        "type": "Expression",
    }
    return CopyActivity(
        name=name,
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_file",
                folder_path={
                    "value": "@pipeline().parameters.incoming_folder",
                    "type": "Expression",
                },
                file_name=file_expr,
            )
        ],
        outputs=[
            _ds_ref(
                "ds_bronze_loaded_file",
                folder_path={
                    "value": "@pipeline().parameters.loaded_folder",
                    "type": "Expression",
                },
                file_name=file_expr,
            )
        ],
        source=DelimitedTextSource(
            store_settings=AzureBlobFSReadSettings(recursive=True)
        ),
        sink=DelimitedTextSink(
            store_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")
        ),
        policy=ActivityPolicy(retry=1, retry_interval_in_seconds=30),
    )


def _copy_from_metadata_lookup(name: str = "CopyPerMetadata") -> CopyActivity:
    """Copy one file using paths from LookupJobMetadata SQL row."""
    folder_in = {
        "value": "@activity('LookupJobMetadata').output.firstRow.incoming_folder",
        "type": "Expression",
    }
    folder_out = {
        "value": "@activity('LookupJobMetadata').output.firstRow.loaded_folder",
        "type": "Expression",
    }
    file_expr = {
        "value": "@activity('LookupJobMetadata').output.firstRow.expected_file_name",
        "type": "Expression",
    }
    return CopyActivity(
        name=name,
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_file",
                folder_path=folder_in,
                file_name=file_expr,
            )
        ],
        outputs=[
            _ds_ref(
                "ds_bronze_loaded_file",
                folder_path=folder_out,
                file_name=file_expr,
            )
        ],
        source=DelimitedTextSource(
            store_settings=AzureBlobFSReadSettings(recursive=True)
        ),
        sink=DelimitedTextSink(
            store_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")
        ),
        policy=ActivityPolicy(retry=1, retry_interval_in_seconds=30),
    )


def _mark_job_status_activity(
    name: str,
    *,
    status: str,
    message: str,
    depends_on: list[ActivityDependency] | None = None,
) -> SqlServerStoredProcedureActivity:
    """Write pipeline outcome back to dbo.adf_job_metadata via stored proc."""
    activity = SqlServerStoredProcedureActivity(
        name=name,
        linked_service_name=_ls_ref(LS_AZURE_SQL),
        stored_procedure_name="[dbo].[usp_adf_mark_job_status]",
        stored_procedure_parameters={
            "job_id": {
                "value": "@pipeline().parameters.job_id",
                "type": "String",
            },
            "status": {"value": status, "type": "String"},
            "run_id": {"value": "@pipeline().RunId", "type": "String"},
            "message": {"value": message, "type": "String"},
        },
        policy=ActivityPolicy(retry=1, retry_interval_in_seconds=30),
    )
    if depends_on:
        activity.depends_on = depends_on
    return activity


def _sql_pipelines_enabled(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
    *,
    sql_deployed: bool,
) -> bool:
    """SQL pipelines deploy when SQL was deployed this run OR linked service already exists."""
    if sql_deployed:
        return True
    return _exists(adf.linked_services.get, rg, factory, LS_AZURE_SQL)


def deploy_all_pipelines(
    cfg: SharedAdfConfig,
    estate: SharedEstate,
    *,
    include_sql: bool = True,
    skip_if_present: bool = True,
) -> None:
    adf = _client(cfg)
    rg, factory = cfg.resource_group, estate.data_factory
    sql_enabled = _sql_pipelines_enabled(adf, rg, factory, sql_deployed=include_sql)
    _deploy_datasets(adf, rg, factory, include_sql=sql_enabled)

    pipeline_names = list(PIPELINE_NAMES)
    if not sql_enabled:
        pipeline_names = [
            n
            for n in pipeline_names
            if n
            not in (
                "pl_08_sql_to_lake",
                "pl_09_lake_to_sql",
                "pl_12_sql_metadata_orchestrate",
            )
        ]

    if skip_if_present and all(
        _exists(adf.pipelines.get, rg, factory, n) for n in pipeline_names
    ):
        logger.info("All %d pipelines already present — skip deploy", len(pipeline_names))
        return

    pipelines: dict[str, PipelineResource] = {}

    # 01 — Copy (Session 2 pattern)
    pipelines["pl_01_bronze_copy"] = PipelineResource(
        activities=[_copy_bronze_activity()],
        parameters={
            "incoming_folder": ParameterSpecification(type="String"),
            "loaded_folder": ParameterSpecification(type="String"),
        },
        annotations=["concept-copy", "shared-adf"],
    )

    # 02 — GetMetadata
    pipelines["pl_02_get_metadata"] = PipelineResource(
        activities=[
            GetMetadataActivity(
                name="ListBronzeIncoming",
                dataset=_ds_ref(
                    "ds_bronze_incoming_folder",
                    folder_path={
                        "value": "@pipeline().parameters.incoming_folder",
                        "type": "Expression",
                    },
                ),
                field_list=["childItems", "itemName", "itemType"],
                store_settings=AzureBlobFSReadSettings(recursive=True),
            )
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            )
        },
        annotations=["concept-getmetadata"],
    )

    # 03 — IfCondition → copy only if folder has items
    meta = GetMetadataActivity(
        name="CheckFolder",
        dataset=_ds_ref(
            "ds_bronze_incoming_folder",
            folder_path={
                "value": "@pipeline().parameters.incoming_folder",
                "type": "Expression",
            },
        ),
        field_list=["childItems"],
        store_settings=AzureBlobFSReadSettings(recursive=True),
    )
    copy_if = _copy_bronze_activity("CopyIfFound")
    pipelines["pl_03_if_then_copy"] = PipelineResource(
        activities=[
            meta,
            IfConditionActivity(
                name="IfFilesExist",
                depends_on=[
                    ActivityDependency(activity=meta.name, dependency_conditions=["Succeeded"])
                ],
                expression=Expression(
                    type="Expression",
                    value="@greater(length(activity('CheckFolder').output.childItems), 0)",
                ),
                if_true_activities=[copy_if],
                if_false_activities=[],
            ),
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "loaded_folder": ParameterSpecification(type="String"),
        },
        annotations=["concept-ifcondition"],
    )

    # 04 — ForEach over run_dates array
    foreach_copy = _copy_bronze_activity("CopyEachDate")
    foreach_copy.inputs = [
        _ds_ref(
            "ds_bronze_incoming_csv",
            folder_path={
                "value": "@item().incoming",
                "type": "Expression",
            },
        )
    ]
    foreach_copy.outputs = [
        _ds_ref(
            "ds_bronze_loaded_csv",
            folder_path={
                "value": "@item().loaded",
                "type": "Expression",
            },
        )
    ]
    pipelines["pl_04_foreach_dates"] = PipelineResource(
        activities=[
            ForEachActivity(
                name="ForEachRunDate",
                items=Expression(type="Expression", value="@pipeline().parameters.run_dates"),
                is_sequential=True,
                activities=[foreach_copy],
            )
        ],
        parameters={"run_dates": ParameterSpecification(type="Array")},
        annotations=["concept-foreach"],
    )

    # 05 — Lookup watermark JSON + SetVariable
    lookup = LookupActivity(
        name="ReadWatermark",
        dataset=_ds_ref("ds_audit_watermark_json"),
        source=JsonSource(),
        first_row_only=True,
    )
    setvar = SetVariableActivity(
        name="StoreLastRun",
        depends_on=[ActivityDependency(activity=lookup.name, dependency_conditions=["Succeeded"])],
        variable_name="last_successful_run",
        value=Expression(
            type="Expression",
            value="@activity('ReadWatermark').output.firstRow.last_run",
        ),
    )
    pipelines["pl_05_lookup_watermark"] = PipelineResource(
        activities=[lookup, setvar],
        variables={"last_successful_run": {"type": "String"}},
        annotations=["concept-lookup", "concept-setvariable"],
    )

    # 06 — Wait then copy (retry / throttle pattern)
    wait = WaitActivity(name="WaitBeforeCopy", wait_time_in_seconds=5)
    copy_after_wait = _copy_bronze_activity("CopyAfterWait")
    copy_after_wait.depends_on = [
        ActivityDependency(activity=wait.name, dependency_conditions=["Succeeded"])
    ]
    pipelines["pl_06_wait_then_copy"] = PipelineResource(
        activities=[wait, copy_after_wait],
        parameters={
            "incoming_folder": ParameterSpecification(type="String"),
            "loaded_folder": ParameterSpecification(type="String"),
        },
        annotations=["concept-wait"],
    )

    # 07 — Databricks notebook
    pipelines["pl_07_databricks_notebook"] = PipelineResource(
        activities=[
            DatabricksNotebookActivity(
                name="RunHelloNotebook",
                linked_service_name=_ls_ref(LS_DATABRICKS),
                notebook_path=NOTEBOOK_PATH,
                base_parameters={
                    "run_id": "@pipeline().parameters.run_id",
                    "triggered_by": "adf-pl_07",
                },
                policy=ActivityPolicy(timeout="0.01:00:00", retry=1),
            )
        ],
        parameters={"run_id": ParameterSpecification(type="String", default_value="session3-lab")},
        annotations=["concept-databricks-notebook"],
    )

    # 08 — SQL (westus) → ADLS CSV export
    if sql_enabled:
        pipelines["pl_08_sql_to_lake"] = PipelineResource(
            activities=[
                CopyActivity(
                    name="SqlToLake",
                    inputs=[_ds_ref("ds_sql_dim_channel")],
                    outputs=[_ds_ref("ds_audit_sql_export_csv")],
                    source=AzureSqlSource(
                        sql_reader_query=(
                            "SELECT 'card' AS channel_code, 'Card payments' AS channel_name "
                            "UNION ALL SELECT 'wire', 'Wire transfer' "
                            "UNION ALL SELECT 'UNKNOWN', 'Unknown channel'"
                        )
                    ),
                    sink=DelimitedTextSink(
                        store_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")
                    ),
                )
            ],
            annotations=["concept-sql-source", "cross-region-westus"],
        )

        # 09 — ADLS → SQL staging table
        pipelines["pl_09_lake_to_sql"] = PipelineResource(
            activities=[
                CopyActivity(
                    name="LakeToSql",
                    inputs=[_ds_ref("ds_audit_sql_export_csv")],
                    outputs=[_ds_ref("ds_sql_stg_channel")],
                    source=DelimitedTextSource(
                        store_settings=AzureBlobFSReadSettings(recursive=True)
                    ),
                    sink=AzureSqlSink(
                        pre_copy_script="""
IF OBJECT_ID('dbo.stg_channel_from_lake', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.stg_channel_from_lake (
        channel_code NVARCHAR(32),
        channel_name NVARCHAR(64)
    );
END
""".strip()
                    ),
                )
            ],
            annotations=["concept-sql-sink"],
        )

    # 10 — Parent: ExecutePipeline chain copy → databricks
    pipelines["pl_10_parent_chain"] = PipelineResource(
        activities=[
            ExecutePipelineActivity(
                name="ChildCopy",
                pipeline=PipelineReference(
                    reference_name="pl_01_bronze_copy",
                    type="PipelineReference",
                ),
                parameters={
                    "incoming_folder": {
                        "value": "@pipeline().parameters.incoming_folder",
                        "type": "Expression",
                    },
                    "loaded_folder": {
                        "value": "@pipeline().parameters.loaded_folder",
                        "type": "Expression",
                    },
                },
                wait_on_completion=True,
            ),
            DatabricksNotebookActivity(
                name="ChildNotebook",
                depends_on=[
                    ActivityDependency(activity="ChildCopy", dependency_conditions=["Succeeded"])
                ],
                linked_service_name=_ls_ref(LS_DATABRICKS),
                notebook_path=NOTEBOOK_PATH,
                base_parameters={
                    "run_id": "@pipeline().parameters.run_id",
                    "triggered_by": "adf-pl_10-chain",
                },
            ),
        ],
        parameters={
            "incoming_folder": ParameterSpecification(type="String"),
            "loaded_folder": ParameterSpecification(type="String"),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        annotations=["concept-executepipeline", "e2e-chain"],
    )

    # 11 — File name validation → copy + Databricks (or reject path)
    list_incoming = GetMetadataActivity(
        name="ListIncoming",
        dataset=_ds_ref(
            "ds_bronze_incoming_folder",
            folder_path={
                "value": "@pipeline().parameters.incoming_folder",
                "type": "Expression",
            },
        ),
        field_list=["childItems"],
        store_settings=AzureBlobFSReadSettings(recursive=True),
    )
    filter_valid = FilterActivity(
        name="FilterValidFiles",
        depends_on=[
            ActivityDependency(activity=list_incoming.name, dependency_conditions=["Succeeded"])
        ],
        items=Expression(
            type="Expression",
            value="@activity('ListIncoming').output.childItems",
        ),
        condition=Expression(
            type="Expression",
            value=(
                "@and("
                "equals(item().type, 'File'), "
                "equals(item().name, pipeline().parameters.expected_file_name)"
                ")"
            ),
        ),
    )
    copy_validated = _copy_validated_file_activity()
    databricks_after_valid = DatabricksNotebookActivity(
        name="RunDatabricksAfterValidation",
        depends_on=[
            ActivityDependency(activity=copy_validated.name, dependency_conditions=["Succeeded"])
        ],
        linked_service_name=_ls_ref(LS_DATABRICKS),
        notebook_path=NOTEBOOK_PATH,
        base_parameters={
            "run_id": "@pipeline().parameters.run_id",
            "triggered_by": "adf-pl_11-validate",
            "source_file": "@pipeline().parameters.expected_file_name",
        },
    )
    set_rejected = SetVariableActivity(
        name="MarkValidationFailed",
        variable_name="validation_status",
        value=Expression(
            type="Expression",
            value=(
                "@concat('rejected: no file named ', pipeline().parameters.expected_file_name)"
            ),
        ),
    )
    pipelines["pl_11_filename_validate_route"] = PipelineResource(
        activities=[
            list_incoming,
            filter_valid,
            IfConditionActivity(
                name="IfFileMatched",
                depends_on=[
                    ActivityDependency(
                        activity=filter_valid.name, dependency_conditions=["Succeeded"]
                    )
                ],
                expression=Expression(
                    type="Expression",
                    value="@greater(length(activity('FilterValidFiles').output.value), 0)",
                ),
                if_true_activities=[copy_validated, databricks_after_valid],
                if_false_activities=[set_rejected],
            ),
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "loaded_folder": ParameterSpecification(
                type="String", default_value="loaded/run=session3-lab"
            ),
            "expected_file_name": ParameterSpecification(
                type="String", default_value="sample_transactions.csv"
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        variables={"validation_status": {"type": "String"}},
        annotations=["concept-filter", "concept-filename-validation", "validate-route"],
    )

    # 12 — SQL metadata drives copy + Databricks; write status back to SQL
    if sql_enabled:
        ensure_proc = ScriptActivity(
            name="EnsureMarkStatusProc",
            linked_service_name=_ls_ref(LS_AZURE_SQL),
            scripts=[
                ScriptActivityScriptBlock(
                    type="NonQuery",
                    text=_METADATA_MARK_STATUS_PROC,
                )
            ],
            policy=ActivityPolicy(retry=1, retry_interval_in_seconds=30),
        )
        bootstrap_metadata = ScriptActivity(
            name="BootstrapJobMetadata",
            depends_on=[
                ActivityDependency(
                    activity=ensure_proc.name, dependency_conditions=["Succeeded"]
                )
            ],
            linked_service_name=_ls_ref(LS_AZURE_SQL),
            scripts=[
                ScriptActivityScriptBlock(
                    type="NonQuery",
                    text=_METADATA_BOOTSTRAP_SQL,
                )
            ],
            policy=ActivityPolicy(retry=1, retry_interval_in_seconds=30),
        )
        lookup_job = LookupActivity(
            name="LookupJobMetadata",
            depends_on=[
                ActivityDependency(
                    activity=bootstrap_metadata.name, dependency_conditions=["Succeeded"]
                )
            ],
            dataset=_ds_ref("ds_sql_job_metadata"),
            source=AzureSqlSource(
                sql_reader_query={
                    "value": (
                        "@concat("
                        "'SELECT job_id, job_name, incoming_folder, loaded_folder, "
                        "expected_file_name, run_databricks, status "
                        "FROM dbo.adf_job_metadata WHERE job_id = ''', "
                        "pipeline().parameters.job_id, ''''"
                        ")"
                    ),
                    "type": "Expression",
                }
            ),
            first_row_only=True,
            policy=ActivityPolicy(retry=1, retry_interval_in_seconds=30),
        )
        copy_per_metadata = _copy_from_metadata_lookup()
        databricks_per_metadata = DatabricksNotebookActivity(
            name="RunDatabricksPerMetadata",
            depends_on=[
                ActivityDependency(
                    activity=copy_per_metadata.name, dependency_conditions=["Succeeded"]
                )
            ],
            linked_service_name=_ls_ref(LS_DATABRICKS),
            notebook_path=NOTEBOOK_PATH,
            base_parameters={
                "run_id": "@pipeline().parameters.run_id",
                "triggered_by": "adf-pl_12-metadata",
                "source_file": "@activity('LookupJobMetadata').output.firstRow.expected_file_name",
            },
            policy=ActivityPolicy(timeout="0.01:00:00", retry=1),
        )
        mark_succeeded = _mark_job_status_activity(
            "MarkJobSucceeded",
            status="SUCCEEDED",
            message="metadata-driven copy and notebook completed",
            depends_on=[
                ActivityDependency(
                    activity=databricks_per_metadata.name, dependency_conditions=["Succeeded"]
                )
            ],
        )
        mark_skipped = _mark_job_status_activity(
            "MarkJobSkipped",
            status="SKIPPED",
            message="job status is not READY — no copy or notebook",
        )
        pipelines["pl_12_sql_metadata_orchestrate"] = PipelineResource(
            activities=[
                ensure_proc,
                bootstrap_metadata,
                lookup_job,
                IfConditionActivity(
                    name="IfJobReady",
                    depends_on=[
                        ActivityDependency(
                            activity=lookup_job.name, dependency_conditions=["Succeeded"]
                        )
                    ],
                    expression=Expression(
                        type="Expression",
                        value=(
                            "@equals("
                            "activity('LookupJobMetadata').output.firstRow.status, "
                            "'READY'"
                            ")"
                        ),
                    ),
                    if_true_activities=[
                        copy_per_metadata,
                        databricks_per_metadata,
                        mark_succeeded,
                    ],
                    if_false_activities=[mark_skipped],
                ),
            ],
            parameters={
                "job_id": ParameterSpecification(type="String", default_value="job-01"),
                "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            },
            annotations=[
                "concept-sql-lookup",
                "concept-metadata-driven",
                "concept-stored-procedure",
                "e2e-sql-orchestrate",
            ],
        )

    for name, body in pipelines.items():
        from adf_advanced import apply_pipeline_folder

        apply_pipeline_folder(name, body)
        adf.pipelines.create_or_update(rg, factory, name, body)
        logger.info("Pipeline %s", name)


def trigger_pipeline(
    cfg: SharedAdfConfig,
    data_factory: str,
    pipeline_name: str,
    parameters: dict[str, Any] | None = None,
) -> str:
    adf = _client(cfg)
    run = adf.pipelines.create_run(
        cfg.resource_group,
        data_factory,
        pipeline_name,
        parameters=parameters or {},
    )
    logger.info("Triggered %s → run_id=%s", pipeline_name, run.run_id)
    return run.run_id


def list_pipeline_names(*, include_sql: bool) -> list[str]:
    names = list(PIPELINE_NAMES)
    if not include_sql:
        names = [
            n
            for n in names
            if n
            not in (
                "pl_08_sql_to_lake",
                "pl_09_lake_to_sql",
                "pl_12_sql_metadata_orchestrate",
            )
        ]
    return names


def wait_for_pipeline_run(
    cfg: SharedAdfConfig,
    data_factory: str,
    run_id: str,
    *,
    poll_seconds: int = 10,
    timeout_minutes: int = 20,
) -> str:
    """Poll ADF pipeline run until terminal status; return final status."""
    adf = _client(cfg)
    deadline = time.time() + (timeout_minutes * 60)
    while True:
        run = adf.pipeline_runs.get(cfg.resource_group, data_factory, run_id)
        status = (run.status or "").strip()
        if status in {"Succeeded", "Failed", "Cancelled"}:
            return status
        if time.time() > deadline:
            return "TimedOut"
        time.sleep(poll_seconds)
