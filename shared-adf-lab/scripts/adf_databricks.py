"""Databricks integration teaching pipelines — expressions + notebook activities (no data flows)."""

from __future__ import annotations

import logging
from typing import Any

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    ActivityPolicy,
    AzureBlobFSLocation,
    CopyActivity,
    DatabricksJobActivity,
    DatabricksNotebookActivity,
    DatasetReference,
    DatasetResource,
    DelimitedTextSink,
    DelimitedTextSource,
    ExecutePipelineActivity,
    Expression,
    ForEachActivity,
    GetMetadataActivity,
    IfConditionActivity,
    JsonDataset,
    JsonSource,
    LookupActivity,
    ParameterSpecification,
    PipelineReference,
    PipelineResource,
    SetVariableActivity,
    WaitActivity,
    AzureBlobFSReadSettings,
    AzureBlobFSWriteSettings,
)

from adf_pipelines import (
    LS_ADLS,
    LS_DATABRICKS,
    _client,
    _ds_ref,
    _ls_ref,
)
from expression_folders import apply_expression_builder_folder

logger = logging.getLogger(__name__)

NOTEBOOK_HELLO = "/Shared/shared-adf/nb_adf_hello"
NOTEBOOK_PARAMS = "/Shared/shared-adf/nb_adf_params_echo"
NOTEBOOK_BRONZE = "/Shared/shared-adf/nb_adf_bronze_stats"
NOTEBOOK_INTEGRATION = "/Shared/shared-adf/nb_adf_integration_complete"
NOTEBOOK_JOB_TASK = "/Shared/shared-adf/nb_adf_job_task"

DATABRICKS_PIPELINE_NAMES = [
    "pl_db_01_static_notebook",
    "pl_db_02_pipeline_param_expressions",
    "pl_db_03_system_expressions",
    "pl_db_04_metadata_to_notebook",
    "pl_db_05_copy_then_notebook",
    "pl_db_06_if_then_notebook",
    "pl_db_07_foreach_channels",
    "pl_db_08_lookup_watermark_notebook",
    "pl_db_09_wait_then_notebook",
    "pl_db_10_child_pipeline_notebook",
    "pl_db_10_child_notebook_only",
    "pl_db_11_databricks_job_preview",
    "pl_db_12_end_to_end_integration",
    "pl_db_13_copy_notebook_job_chain",
]


def _notebook(
    name: str,
    *,
    notebook_path: str = NOTEBOOK_HELLO,
    base_parameters: dict[str, str] | None = None,
    depends_on: list[ActivityDependency] | None = None,
    timeout: str = "0.01:00:00",
) -> DatabricksNotebookActivity:
    activity = DatabricksNotebookActivity(
        name=name,
        linked_service_name=_ls_ref(LS_DATABRICKS),
        notebook_path=notebook_path,
        base_parameters=base_parameters or {},
        policy=ActivityPolicy(timeout=timeout, retry=1, retry_interval_in_seconds=30),
    )
    if depends_on:
        activity.depends_on = depends_on
    return activity


def _deploy_databricks_datasets(
    adf,
    rg: str,
    factory: str,
) -> None:
    adf.datasets.create_or_update(
        rg,
        factory,
        "ds_audit_databricks_job_json",
        DatasetResource(
            properties=JsonDataset(
                linked_service_name=_ls_ref(LS_ADLS),
                location=AzureBlobFSLocation(
                    file_system="audit",
                    file_name="adf_databricks_job.json",
                ),
            )
        ),
    )
    logger.info("Dataset ds_audit_databricks_job_json")


def deploy_databricks_pipelines(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
) -> None:
    """Deploy 10 Databricks expression + integration teaching pipelines."""
    pipelines: dict[str, PipelineResource] = {}

    pipelines["pl_db_01_static_notebook"] = PipelineResource(
        activities=[
            _notebook(
                "RunStaticNotebook",
                base_parameters={
                    "run_id": "session3-lab",
                    "triggered_by": "adf-pl_db_01-static",
                    "source_file": "sample_transactions.csv",
                },
            )
        ],
        annotations=["concept-databricks-notebook", "expression-static"],
    )

    pipelines["pl_db_02_pipeline_param_expressions"] = PipelineResource(
        activities=[
            _notebook(
                "RunParamExpressions",
                notebook_path=NOTEBOOK_PARAMS,
                base_parameters={
                    "run_id": "@pipeline().parameters.run_id",
                    "triggered_by": "@concat('adf-', pipeline().parameters.lab_tag)",
                    "source_file": "@pipeline().parameters.expected_file",
                    "lab_tag": "@pipeline().parameters.lab_tag",
                },
            )
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "lab_tag": ParameterSpecification(type="String", default_value="db02-params"),
            "expected_file": ParameterSpecification(
                type="String", default_value="sample_transactions.csv"
            ),
        },
        annotations=["concept-expression", "pipeline-parameters"],
    )

    pipelines["pl_db_03_system_expressions"] = PipelineResource(
        activities=[
            _notebook(
                "RunSystemExpressions",
                notebook_path=NOTEBOOK_PARAMS,
                base_parameters={
                    "run_id": "@pipeline().RunId",
                    "triggered_by": "@concat('factory=', pipeline().DataFactory)",
                    "source_file": "@concat('utc=', utcnow())",
                    "lab_tag": "@guid()",
                },
            )
        ],
        annotations=["concept-expression", "pipeline-system-functions"],
    )

    get_meta = GetMetadataActivity(
        name="GetIncomingMetadata",
        dataset=_ds_ref(
            "ds_bronze_incoming_folder",
            folder_path={
                "value": "@pipeline().parameters.incoming_folder",
                "type": "Expression",
            },
        ),
        field_list=["childItems", "itemName"],
        store_settings=AzureBlobFSReadSettings(recursive=True),
    )
    pipelines["pl_db_04_metadata_to_notebook"] = PipelineResource(
        activities=[
            get_meta,
            _notebook(
                "RunMetadataExpressions",
                notebook_path=NOTEBOOK_PARAMS,
                depends_on=[
                    ActivityDependency(
                        activity=get_meta.name, dependency_conditions=["Succeeded"]
                    )
                ],
                base_parameters={
                    "run_id": "@pipeline().parameters.run_id",
                    "triggered_by": "@concat('childCount=', string(length(activity('GetIncomingMetadata').output.childItems)))",
                    "source_file": "@coalesce(activity('GetIncomingMetadata').output.itemName, 'unknown')",
                    "lab_tag": "db04-metadata",
                },
            ),
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        annotations=["concept-expression", "activity-output", "get-metadata"],
    )

    copy_bronze = CopyActivity(
        name="CopyBronzeForNotebook",
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_file",
                folder_path={
                    "value": "@pipeline().parameters.incoming_folder",
                    "type": "Expression",
                },
                file_name={
                    "value": "@pipeline().parameters.file_name",
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
                    "value": "@pipeline().parameters.file_name",
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
        policy=ActivityPolicy(timeout="0.00:15:00", retry=1),
    )
    pipelines["pl_db_05_copy_then_notebook"] = PipelineResource(
        activities=[
            copy_bronze,
            _notebook(
                "RunBronzeStats",
                notebook_path=NOTEBOOK_BRONZE,
                depends_on=[
                    ActivityDependency(
                        activity=copy_bronze.name, dependency_conditions=["Succeeded"]
                    )
                ],
                base_parameters={
                    "run_id": "@pipeline().parameters.run_id",
                    "triggered_by": "adf-pl_db_05-copy-then-notebook",
                    "bronze_path": "@concat(pipeline().parameters.loaded_folder, '/', pipeline().parameters.file_name)",
                    "storage_account": "@pipeline().parameters.storage_account",
                },
            ),
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "loaded_folder": ParameterSpecification(
                type="String", default_value="loaded/run=session3-lab"
            ),
            "file_name": ParameterSpecification(
                type="String", default_value="sample_transactions.csv"
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "storage_account": ParameterSpecification(type="String", default_value=""),
        },
        annotations=["concept-copy-then-notebook", "expression-concat"],
    )

    check_meta = GetMetadataActivity(
        name="CheckIncomingFiles",
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
    set_file_count = SetVariableActivity(
        name="SetFileCount",
        depends_on=[
            ActivityDependency(activity=check_meta.name, dependency_conditions=["Succeeded"])
        ],
        variable_name="file_count",
        value=Expression(
            type="Expression",
            value="@string(length(activity('CheckIncomingFiles').output.childItems))",
        ),
    )
    notebook_if = _notebook(
        "RunNotebookWhenFilesExist",
        notebook_path=NOTEBOOK_PARAMS,
        base_parameters={
            "run_id": "@pipeline().parameters.run_id",
            "triggered_by": "@concat('files=', variables('file_count'))",
            "source_file": "@pipeline().parameters.expected_file",
            "lab_tag": "db06-if-true",
        },
    )
    skip_note = SetVariableActivity(
        name="MarkSkipped",
        variable_name="run_status",
        value=Expression(type="Expression", value="@string('skipped-no-files')"),
    )
    pipelines["pl_db_06_if_then_notebook"] = PipelineResource(
        activities=[
            check_meta,
            set_file_count,
            IfConditionActivity(
                name="IfFilesExist",
                depends_on=[
                    ActivityDependency(
                        activity=set_file_count.name, dependency_conditions=["Succeeded"]
                    )
                ],
                expression=Expression(
                    type="Expression",
                    value="@greater(int(variables('file_count')), 0)",
                ),
                if_true_activities=[notebook_if],
                if_false_activities=[skip_note],
            ),
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "expected_file": ParameterSpecification(
                type="String", default_value="sample_transactions.csv"
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        variables={
            "file_count": {"type": "String"},
            "run_status": {"type": "String"},
        },
        annotations=["concept-if-condition", "concept-setvariable", "expression-greater"],
    )

    foreach_nb = _notebook(
        "RunNotebookPerChannel",
        notebook_path=NOTEBOOK_PARAMS,
        base_parameters={
            "run_id": "@pipeline().parameters.run_id",
            "triggered_by": "@item()",
            "source_file": "@concat('channel=', item())",
            "lab_tag": "db07-foreach",
        },
    )
    pipelines["pl_db_07_foreach_channels"] = PipelineResource(
        activities=[
            ForEachActivity(
                name="ForEachChannel",
                items=Expression(type="Expression", value="@pipeline().parameters.channels"),
                is_sequential=True,
                activities=[foreach_nb],
            )
        ],
        parameters={
            "channels": ParameterSpecification(
                type="Array",
                default_value=["card", "wire", "fps"],
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        annotations=["concept-foreach", "expression-item"],
    )

    lookup_wm = LookupActivity(
        name="ReadWatermark",
        dataset=_ds_ref("ds_audit_watermark_json"),
        source=JsonSource(),
        first_row_only=True,
    )
    pipelines["pl_db_08_lookup_watermark_notebook"] = PipelineResource(
        activities=[
            lookup_wm,
            _notebook(
                "RunWatermarkNotebook",
                notebook_path=NOTEBOOK_PARAMS,
                depends_on=[
                    ActivityDependency(
                        activity=lookup_wm.name, dependency_conditions=["Succeeded"]
                    )
                ],
                base_parameters={
                    "run_id": "@pipeline().parameters.run_id",
                    "triggered_by": "@concat('lastRun=', activity('ReadWatermark').output.firstRow.last_run)",
                    "source_file": "@activity('ReadWatermark').output.firstRow.last_successful_watermark",
                    "lab_tag": "db08-lookup",
                },
            ),
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        annotations=["concept-lookup", "expression-firstrow"],
    )

    wait_act = WaitActivity(name="WaitForCluster", wait_time_in_seconds=15)
    wait_nb = _notebook(
        "RunAfterWait",
        base_parameters={
            "run_id": "@pipeline().parameters.run_id",
            "triggered_by": "adf-pl_db_09-wait-then-notebook",
            "source_file": "post-wait",
        },
    )
    wait_nb.depends_on = [
        ActivityDependency(activity=wait_act.name, dependency_conditions=["Succeeded"])
    ]
    pipelines["pl_db_09_wait_then_notebook"] = PipelineResource(
        activities=[wait_act, wait_nb],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        annotations=["concept-wait", "cluster-warmup-pattern"],
    )

    pipelines["pl_db_10_child_notebook_only"] = PipelineResource(
        activities=[
            _notebook(
                "ChildNotebookOnly",
                notebook_path=NOTEBOOK_PARAMS,
                base_parameters={
                    "run_id": "@pipeline().parameters.run_id",
                    "triggered_by": "@concat('child-', pipeline().parameters.lab_tag)",
                    "source_file": "child-pipeline",
                    "lab_tag": "@pipeline().parameters.lab_tag",
                },
            )
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "lab_tag": ParameterSpecification(type="String", default_value="db10-child"),
        },
        annotations=["concept-child-pipeline", "internal"],
    )

    pipelines["pl_db_10_child_pipeline_notebook"] = PipelineResource(
        activities=[
            ExecutePipelineActivity(
                name="ExecuteChildNotebook",
                pipeline=PipelineReference(
                    reference_name="pl_db_10_child_notebook_only",
                    type="PipelineReference",
                ),
                parameters={
                    "run_id": {
                        "value": "@pipeline().parameters.run_id",
                        "type": "Expression",
                    },
                    "lab_tag": {
                        "value": "@concat('parent→child-', pipeline().parameters.lab_tag)",
                        "type": "Expression",
                    },
                },
                wait_on_completion=True,
            )
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "lab_tag": ParameterSpecification(type="String", default_value="db10-parent"),
        },
        annotations=["concept-execute-pipeline", "expression-child-params"],
    )

    lookup_job_manifest = LookupActivity(
        name="ReadDatabricksJobManifest",
        dataset=DatasetReference(
            reference_name="ds_audit_databricks_job_json",
            type="DatasetReference",
        ),
        source=JsonSource(),
        first_row_only=True,
    )
    pipelines["pl_db_11_databricks_job_preview"] = PipelineResource(
        activities=[
            lookup_job_manifest,
            DatabricksJobActivity(
                name="RunDatabricksJobPreview",
                depends_on=[
                    ActivityDependency(
                        activity=lookup_job_manifest.name,
                        dependency_conditions=["Succeeded"],
                    )
                ],
                linked_service_name=_ls_ref(LS_DATABRICKS),
                job_id=Expression(
                    type="Expression",
                    value="@string(activity('ReadDatabricksJobManifest').output.firstRow.job_id)",
                ),
                job_parameters={
                    "run_id": {
                        "value": "@pipeline().parameters.run_id",
                        "type": "Expression",
                    },
                    "triggered_by": {
                        "value": "@concat('adf-job-preview-', pipeline().parameters.lab_tag)",
                        "type": "Expression",
                    },
                    "job_note": {
                        "value": "@pipeline().parameters.job_note",
                        "type": "Expression",
                    },
                },
                policy=ActivityPolicy(timeout="0.01:00:00", retry=1),
            ),
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "lab_tag": ParameterSpecification(type="String", default_value="db11-job"),
            "job_note": ParameterSpecification(
                type="String", default_value="adf-databricks-job-activity-preview"
            ),
        },
        annotations=["concept-databricks-job-preview", "databricks-job-activity"],
    )

    copy_for_integration = CopyActivity(
        name="CopyBronzeForIntegration",
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_file",
                folder_path={
                    "value": "@pipeline().parameters.incoming_folder",
                    "type": "Expression",
                },
                file_name={
                    "value": "@pipeline().parameters.file_name",
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
                    "value": "@pipeline().parameters.file_name",
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
        policy=ActivityPolicy(timeout="0.00:15:00", retry=1),
    )
    pipelines["pl_db_12_end_to_end_integration"] = PipelineResource(
        activities=[
            copy_for_integration,
            _notebook(
                "RunCompleteIntegration",
                notebook_path=NOTEBOOK_INTEGRATION,
                depends_on=[
                    ActivityDependency(
                        activity=copy_for_integration.name,
                        dependency_conditions=["Succeeded"],
                    )
                ],
                base_parameters={
                    "run_id": "@pipeline().parameters.run_id",
                    "triggered_by": "adf-pl_db_12-end-to-end",
                    "bronze_path": "@concat(pipeline().parameters.loaded_folder, '/', pipeline().parameters.file_name)",
                    "silver_path": "@pipeline().parameters.silver_path",
                    "storage_account": "@pipeline().parameters.storage_account",
                },
            ),
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "loaded_folder": ParameterSpecification(
                type="String", default_value="loaded/run=session3-lab"
            ),
            "file_name": ParameterSpecification(
                type="String", default_value="sample_transactions.csv"
            ),
            "silver_path": ParameterSpecification(
                type="String", default_value="cleaned/run=session3-lab/adf_integration_delta"
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "storage_account": ParameterSpecification(type="String", default_value=""),
        },
        annotations=["concept-complete-integration", "copy-notebook-delta"],
    )

    copy_chain = CopyActivity(
        name="CopyBronzeForChain",
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_file",
                folder_path={
                    "value": "@pipeline().parameters.incoming_folder",
                    "type": "Expression",
                },
                file_name={
                    "value": "@pipeline().parameters.file_name",
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
                    "value": "@pipeline().parameters.file_name",
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
        policy=ActivityPolicy(timeout="0.00:15:00", retry=1),
    )
    nb_chain = _notebook(
        "RunNotebookInChain",
        notebook_path=NOTEBOOK_BRONZE,
        depends_on=[
            ActivityDependency(activity=copy_chain.name, dependency_conditions=["Succeeded"])
        ],
        base_parameters={
            "run_id": "@pipeline().parameters.run_id",
            "triggered_by": "adf-pl_db_13-chain-notebook",
            "bronze_path": "@concat(pipeline().parameters.loaded_folder, '/', pipeline().parameters.file_name)",
            "storage_account": "@pipeline().parameters.storage_account",
        },
    )
    lookup_chain = LookupActivity(
        name="ReadJobManifestInChain",
        depends_on=[
            ActivityDependency(activity=nb_chain.name, dependency_conditions=["Succeeded"])
        ],
        dataset=DatasetReference(
            reference_name="ds_audit_databricks_job_json",
            type="DatasetReference",
        ),
        source=JsonSource(),
        first_row_only=True,
    )
    job_chain = DatabricksJobActivity(
        name="RunJobPreviewInChain",
        depends_on=[
            ActivityDependency(
                activity=lookup_chain.name, dependency_conditions=["Succeeded"]
            )
        ],
        linked_service_name=_ls_ref(LS_DATABRICKS),
        job_id=Expression(
            type="Expression",
            value="@string(activity('ReadJobManifestInChain').output.firstRow.job_id)",
        ),
        job_parameters={
            "run_id": {"value": "@pipeline().parameters.run_id", "type": "Expression"},
            "triggered_by": {"value": "@string('adf-pl_db_13-chain-job')", "type": "Expression"},
            "job_note": {
                "value": "@concat('after-notebook-', pipeline().RunId)",
                "type": "Expression",
            },
        },
        policy=ActivityPolicy(timeout="0.01:00:00", retry=1),
    )
    pipelines["pl_db_13_copy_notebook_job_chain"] = PipelineResource(
        activities=[copy_chain, nb_chain, lookup_chain, job_chain],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "loaded_folder": ParameterSpecification(
                type="String", default_value="loaded/run=session3-lab"
            ),
            "file_name": ParameterSpecification(
                type="String", default_value="sample_transactions.csv"
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "storage_account": ParameterSpecification(type="String", default_value=""),
        },
        annotations=[
            "concept-complete-integration",
            "copy-notebook-job-preview-chain",
        ],
    )

    for name, body in pipelines.items():
        apply_expression_builder_folder(name, body)
        adf.pipelines.create_or_update(rg, factory, name, body)
        logger.info("Databricks pipeline %s", name)


def deploy_databricks(cfg, estate) -> None:
    """Deploy Databricks integration teaching pipelines."""
    adf = _client(cfg)
    rg, factory = cfg.resource_group, estate.data_factory
    _deploy_databricks_datasets(adf, rg, factory)
    deploy_databricks_pipelines(adf, rg, factory)


def list_databricks_pipeline_names() -> list[str]:
    return list(DATABRICKS_PIPELINE_NAMES)


def default_params(pipeline_name: str) -> dict[str, Any]:
    incoming = "incoming/run=session3-lab"
    loaded = "loaded/run=session3-lab"
    if pipeline_name == "pl_db_02_pipeline_param_expressions":
        return {
            "run_id": "session3-lab",
            "lab_tag": "db02-params",
            "expected_file": "sample_transactions.csv",
        }
    if pipeline_name in (
        "pl_db_04_metadata_to_notebook",
        "pl_db_06_if_then_notebook",
    ):
        params = {"incoming_folder": incoming, "run_id": "session3-lab"}
        if pipeline_name == "pl_db_06_if_then_notebook":
            params["expected_file"] = "sample_transactions.csv"
        return params
    if pipeline_name == "pl_db_05_copy_then_notebook":
        return {
            "incoming_folder": incoming,
            "loaded_folder": loaded,
            "file_name": "sample_transactions.csv",
            "run_id": "session3-lab",
            "storage_account": "",
        }
    if pipeline_name == "pl_db_07_foreach_channels":
        return {"channels": ["card", "wire", "fps"], "run_id": "session3-lab"}
    if pipeline_name in (
        "pl_db_01_static_notebook",
        "pl_db_03_system_expressions",
        "pl_db_08_lookup_watermark_notebook",
        "pl_db_09_wait_then_notebook",
    ):
        return {"run_id": "session3-lab"}
    if pipeline_name == "pl_db_10_child_pipeline_notebook":
        return {"run_id": "session3-lab", "lab_tag": "db10-parent"}
    if pipeline_name == "pl_db_10_child_notebook_only":
        return {"run_id": "session3-lab", "lab_tag": "db10-child"}
    if pipeline_name == "pl_db_11_databricks_job_preview":
        return {
            "run_id": "session3-lab",
            "lab_tag": "db11-job",
            "job_note": "adf-databricks-job-activity-preview",
        }
    if pipeline_name in ("pl_db_12_end_to_end_integration", "pl_db_13_copy_notebook_job_chain"):
        return {
            "incoming_folder": incoming,
            "loaded_folder": loaded,
            "file_name": "sample_transactions.csv",
            "run_id": "session3-lab",
            "storage_account": "",
            **(
                {"silver_path": "cleaned/run=session3-lab/adf_integration_delta"}
                if pipeline_name == "pl_db_12_end_to_end_integration"
                else {}
            ),
        }
    return {}
