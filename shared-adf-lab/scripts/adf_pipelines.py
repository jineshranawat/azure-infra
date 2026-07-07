"""Deploy all shared ADF linked services + teaching pipelines (idempotent SDK)."""

from __future__ import annotations

import logging
import subprocess
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
    SetVariableActivity,
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


def deploy_linked_services(
    cfg: SharedAdfConfig,
    estate: SharedEstate,
    sql: SqlEstate | None,
) -> None:
    adf = _client(cfg)
    rg, factory = cfg.resource_group, estate.data_factory
    dfs_url = estate.dfs_endpoint

    # ADLS — MSI (overwrites Bicep placeholder without MSI)
    adf.linked_services.create_or_update(
        rg,
        factory,
        LS_ADLS,
        LinkedServiceResource(
            properties=AzureBlobFSLinkedService(
                url=dfs_url,
                description="ADLS Gen2 — shared FinLedger lake (eastus)",
            )
        ),
    )
    logger.info("Linked service %s", LS_ADLS)

    # Databricks — job cluster per run (auto-terminate)
    domain = _databricks_domain(cfg, estate)
    adf.linked_services.create_or_update(
        rg,
        factory,
        LS_DATABRICKS,
        LinkedServiceResource(
            properties=AzureDatabricksLinkedService(
                domain=domain,
                authentication="MSI",
                workspace_resource_id=_databricks_workspace_resource_id(
                    cfg, estate.databricks_workspace
                ),
                new_cluster_node_type="Standard_DS3_v2",
                new_cluster_num_of_worker=1,
                new_cluster_version="13.3.x-scala2.12",
                description="Shared class Databricks — ADF notebook activities",
            )
        ),
    )
    logger.info("Linked service %s → %s", LS_DATABRICKS, domain)

    if sql:
        conn = (
            f"Server=tcp:{sql.fqdn},1433;"
            f"Database={sql.database_name};"
            f"User ID={sql.admin_login};"
            f"Password={sql.admin_password};"
            "Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
        )
        adf.linked_services.create_or_update(
            rg,
            factory,
            LS_AZURE_SQL,
            LinkedServiceResource(
                properties=AzureSqlDatabaseLinkedService(
                    connection_string=SecureString(value=conn),
                    description=f"Azure SQL westus — {sql.fqdn}",
                )
            ),
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


def deploy_all_pipelines(
    cfg: SharedAdfConfig,
    estate: SharedEstate,
    *,
    include_sql: bool = True,
    skip_if_present: bool = True,
) -> None:
    adf = _client(cfg)
    rg, factory = cfg.resource_group, estate.data_factory
    _deploy_datasets(adf, rg, factory, include_sql=include_sql)

    pipeline_names = list(PIPELINE_NAMES)
    if not include_sql:
        pipeline_names = [n for n in pipeline_names if n not in ("pl_08_sql_to_lake", "pl_09_lake_to_sql")]

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
                    "ds_bronze_incoming_csv",
                    folder_path={
                        "value": "@pipeline().parameters.incoming_folder",
                        "type": "Expression",
                    },
                ),
                field_list=["childItems", "itemName", "itemType"],
                store_settings=AzureBlobFSReadSettings(recursive=True),
            )
        ],
        parameters={"incoming_folder": ParameterSpecification(type="String")},
        annotations=["concept-getmetadata"],
    )

    # 03 — IfCondition → copy only if folder has items
    meta = GetMetadataActivity(
        name="CheckFolder",
        dataset=_ds_ref(
            "ds_bronze_incoming_csv",
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
            "incoming_folder": ParameterSpecification(type="String"),
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
    if include_sql:
        pipelines["pl_08_sql_to_lake"] = PipelineResource(
            activities=[
                CopyActivity(
                    name="SqlToLake",
                    inputs=[_ds_ref("ds_sql_dim_channel")],
                    outputs=[_ds_ref("ds_audit_sql_export_csv")],
                    source=AzureSqlSource(),
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
                    inputs=[
                        _ds_ref(
                            "ds_bronze_loaded_csv",
                            folder_path={
                                "value": "@pipeline().parameters.source_folder",
                                "type": "Expression",
                            },
                        )
                    ],
                    outputs=[_ds_ref("ds_sql_stg_channel")],
                    source=DelimitedTextSource(
                        store_settings=AzureBlobFSReadSettings(recursive=True)
                    ),
                    sink=AzureSqlSink(),
                )
            ],
            parameters={"source_folder": ParameterSpecification(type="String")},
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

    for name, body in pipelines.items():
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
