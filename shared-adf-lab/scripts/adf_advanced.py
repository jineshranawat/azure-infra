"""Advanced ADF concepts — folders, mapping data flows, extra activities (idempotent)."""

from __future__ import annotations

import logging
from typing import Any

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    AppendVariableActivity,
    AzureBlobFSLocation,
    AzureBlobFSReadSettings,
    AzureBlobFSWriteSettings,
    AzureSqlSource,
    CopyActivity,
    DataFlowFolder,
    DataFlowReference,
    DataFlowResource,
    DataFlowSink,
    DataFlowSource,
    DatasetReference,
    DatasetResource,
    DelimitedTextDataset,
    DelimitedTextSink,
    DelimitedTextSource,
    DeleteActivity,
    ExecuteDataFlowActivity,
    ExecuteDataFlowActivityTypePropertiesCompute,
    Expression,
    FailActivity,
    GetMetadataActivity,
    IfConditionActivity,
    JsonDataset,
    JsonSource,
    LinkedServiceReference,
    LookupActivity,
    MappingDataFlow,
    ParameterSpecification,
    PipelineFolder,
    PipelineResource,
    PowerQuerySource,
    ScriptActivity,
    ScriptActivityScriptBlock,
    SetVariableActivity,
    SwitchActivity,
    SwitchCase,
    UntilActivity,
    ValidationActivity,
    WebActivity,
    WebActivityMethod,
    WranglingDataFlow,
    ActivityPolicy,
)

from adf_pipelines import LS_ADLS, LS_AZURE_SQL, _client, _ds_ref, _ls_ref
from expression_folders import apply_expression_builder_folder

logger = logging.getLogger(__name__)

ADVANCED_PIPELINE_NAMES = [
    "pl_13_df_bronze_cleanse",
    "pl_14_df_channel_aggregate",
    "pl_15_delete_rejected",
    "pl_16_web_ping",
    "pl_17_switch_channel",
    "pl_18_until_file_ready",
    "pl_19_append_audit_log",
    "pl_20_validate_incoming",
    "pl_21_fail_on_bad_param",
    "pl_22_sql_script_count",
    "pl_23_incremental_watermark_copy",
    "pl_24_sql_change_tracking_copy",
    "pl_25_power_query_returns",
    "pl_26_join_returns_txn",
]

DATA_FLOW_NAMES = [
    "df_01_bronze_cleanse",
    "df_02_channel_aggregate",
    "df_03_returns_by_channel",
    "df_04_join_returns_txn",
]

WRANGLING_DATA_FLOW_NAMES = [
    "pq_01_returns_wrangle",
]

PIPELINE_FOLDERS: dict[str, str] = {
    "pl_01_bronze_copy": "01-core-copy",
    "pl_06_wait_then_copy": "01-core-copy",
    "pl_02_get_metadata": "02-discovery-control",
    "pl_10_parent_chain": "03-iteration-orchestration",
    "pl_08_sql_to_lake": "05-integration-sql",
    "pl_09_lake_to_sql": "05-integration-sql",
    "pl_12_sql_metadata_orchestrate": "05-integration-sql",
    "pl_13_df_bronze_cleanse": "06-dataflows",
    "pl_14_df_channel_aggregate": "06-dataflows",
    "pl_15_delete_rejected": "07-advanced-activities",
    "pl_16_web_ping": "07-advanced-activities",
    "pl_20_validate_incoming": "07-advanced-activities",
    "pl_22_sql_script_count": "07-advanced-activities",
    "pl_24_sql_change_tracking_copy": "08-incremental-cdc",
    "pl_25_power_query_returns": "09-power-query",
    "pl_26_join_returns_txn": "06-dataflows",
}

DATA_FLOW_FOLDERS: dict[str, str] = {
    "df_01_bronze_cleanse": "06-dataflows",
    "df_02_channel_aggregate": "06-dataflows",
    "df_03_returns_by_channel": "09-power-query",
    "df_04_join_returns_txn": "06-dataflows",
    "pq_01_returns_wrangle": "09-power-query",
}


def apply_pipeline_folder(name: str, body: PipelineResource) -> PipelineResource:
    apply_expression_builder_folder(name, body)
    if body.folder:
        return body
    folder = PIPELINE_FOLDERS.get(name)
    if folder:
        body.folder = PipelineFolder(name=folder)
    return body


def _deploy_advanced_datasets(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
) -> None:
    ls = _ls_ref(LS_ADLS)
    datasets: list[tuple[str, DatasetResource]] = [
        (
            "ds_df_bronze_source",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="bronze",
                        folder_path="incoming/run=session3-lab",
                        file_name="sample_transactions.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        ),
        (
            "ds_silver_cleaned_csv",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="silver",
                        folder_path="cleaned/run=session3-lab",
                        file_name="cleaned_transactions.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        ),
        (
            "ds_silver_channel_agg_csv",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="silver",
                        folder_path="aggregates/run=session3-lab",
                        file_name="channel_totals.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        ),
        (
            "ds_bronze_rejected_folder",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="bronze",
                        folder_path="incoming/run=session3-lab/rejected",
                        file_name="wrong_file_name.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        ),
        (
            "ds_returns_raw",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="bronze",
                        folder_path="incoming/returns",
                        file_name="returns_raw.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        ),
        (
            "ds_silver_returns_summary",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="silver",
                        folder_path="returns/summary/run=session3-lab",
                        file_name="returns_by_channel.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        ),
        (
            "ds_silver_returns_joined",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="silver",
                        folder_path="returns/joined/run=session3-lab",
                        file_name="returns_with_txn.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        ),
        (
            "ds_audit_sql_cdc_csv",
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="audit",
                        folder_path="sql_cdc_export/run=session3-lab",
                        file_name="dim_channel_changes.csv",
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        ),
        (
            "ds_audit_cdc_watermark_json",
            DatasetResource(
                properties=JsonDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system="audit",
                        file_name="cdc_watermark.json",
                    ),
                )
            ),
        ),
    ]
    for name, body in datasets:
        adf.datasets.create_or_update(rg, factory, name, body)
    logger.info("Advanced datasets deployed (%d)", len(datasets))


def _df_bronze_cleanse_script() -> list[str]:
    return [
        "source(output(",
        "      transaction_id as string,",
        "      account_id as string,",
        "      amount_gbp as double,",
        "      value_date as string,",
        "      channel as string,",
        "      status as string",
        " ),",
        " allowSchemaDrift: true,",
        " validateSchema: false,",
        " ignoreNoFilesFound: false) ~> BronzeSource",
        "BronzeSource derive(ingestion_ts = currentTimestamp()) ~> AddTimestamp",
        "AddTimestamp select(mapColumn(",
        "      transaction_id,",
        "      account_id,",
        "      amount_gbp,",
        "      channel,",
        "      status,",
        "      ingestion_ts",
        " )) ~> SelectCols",
        "SelectCols sink(allowSchemaDrift: true,",
        " validateSchema: false,",
        " truncate: true) ~> SilverSink",
    ]


def _df_channel_aggregate_script() -> list[str]:
    return [
        "source(output(",
        "      transaction_id as string,",
        "      account_id as string,",
        "      amount_gbp as double,",
        "      value_date as string,",
        "      channel as string,",
        "      status as string",
        " ),",
        " allowSchemaDrift: true,",
        " validateSchema: false,",
        " ignoreNoFilesFound: false) ~> TxnSource",
        "TxnSource aggregate(groupBy(channel),",
        " total_amount = sum(amount_gbp),",
        " txn_count = count()) ~> ByChannel",
        "ByChannel sink(allowSchemaDrift: true,",
        " validateSchema: false,",
        " truncate: true) ~> AggSink",
    ]


def _df_returns_by_channel_script() -> list[str]:
    """Mapping flow — same outcome as Power Query group-by in pq_01 (see catalog)."""
    return [
        "source(output(",
        "      return_id as string,",
        "      transaction_id as string,",
        "      return_date as string,",
        "      reason as string,",
        "      refund_gbp as double,",
        "      channel as string",
        " ),",
        " allowSchemaDrift: true,",
        " validateSchema: false,",
        " ignoreNoFilesFound: false) ~> ReturnsSource",
        "ReturnsSource aggregate(groupBy(channel),",
        " total_refund_gbp = sum(refund_gbp),",
        " return_count = count()) ~> ByChannel",
        "ByChannel sink(allowSchemaDrift: true,",
        " validateSchema: false,",
        " truncate: true) ~> SummarySink",
    ]


def _df_join_returns_txn_script() -> list[str]:
    return [
        "source(output(",
        "      return_id as string,",
        "      transaction_id as string,",
        "      return_date as string,",
        "      reason as string,",
        "      refund_gbp as double,",
        "      channel as string",
        " ),",
        " allowSchemaDrift: true,",
        " validateSchema: false,",
        " ignoreNoFilesFound: false) ~> ReturnsSource",
        "source(output(",
        "      transaction_id as string,",
        "      account_id as string,",
        "      amount_gbp as double,",
        "      value_date as string,",
        "      channel as string,",
        "      status as string",
        " ),",
        " allowSchemaDrift: true,",
        " validateSchema: false,",
        " ignoreNoFilesFound: false) ~> TxnSource",
        "ReturnsSource, TxnSource join(transaction_id == transaction_id,",
        " joinType:'inner',",
        " matchType:'exact',",
        " ignoreSpaces: false,",
        " broadcast: 'auto')~> Joined",
        "Joined select(mapColumn(",
        "      return_id,",
        "      transaction_id,",
        "      refund_gbp,",
        "      reason,",
        "      amount_gbp,",
        "      channel = ReturnsSource@channel",
        " )) ~> SelectJoined",
        "SelectJoined sink(allowSchemaDrift: true,",
        " validateSchema: false,",
        " truncate: true) ~> JoinedSink",
    ]


def deploy_data_flows(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
) -> None:
    flows: dict[str, DataFlowResource] = {
        "df_01_bronze_cleanse": DataFlowResource(
            properties=MappingDataFlow(
                description="Bronze CSV → derive timestamp → select columns → silver",
                folder=DataFlowFolder(name=DATA_FLOW_FOLDERS["df_01_bronze_cleanse"]),
                sources=[
                    DataFlowSource(
                        name="BronzeSource",
                        dataset=DatasetReference(
                            reference_name="ds_df_bronze_source",
                            type="DatasetReference",
                        ),
                    )
                ],
                sinks=[
                    DataFlowSink(
                        name="SilverSink",
                        dataset=DatasetReference(
                            reference_name="ds_silver_cleaned_csv",
                            type="DatasetReference",
                        ),
                    )
                ],
                script_lines=_df_bronze_cleanse_script(),
                annotations=["concept-mapping-dataflow", "derive", "select"],
            )
        ),
        "df_02_channel_aggregate": DataFlowResource(
            properties=MappingDataFlow(
                description="Aggregate transaction amounts and counts by channel",
                folder=DataFlowFolder(name=DATA_FLOW_FOLDERS["df_02_channel_aggregate"]),
                sources=[
                    DataFlowSource(
                        name="TxnSource",
                        dataset=DatasetReference(
                            reference_name="ds_df_bronze_source",
                            type="DatasetReference",
                        ),
                    )
                ],
                sinks=[
                    DataFlowSink(
                        name="AggSink",
                        dataset=DatasetReference(
                            reference_name="ds_silver_channel_agg_csv",
                            type="DatasetReference",
                        ),
                    )
                ],
                script_lines=_df_channel_aggregate_script(),
                annotations=["concept-mapping-dataflow", "aggregate"],
            )
        ),
        "df_03_returns_by_channel": DataFlowResource(
            properties=MappingDataFlow(
                description="Returns CSV → aggregate refund totals by channel (PQ lab parity)",
                folder=DataFlowFolder(name=DATA_FLOW_FOLDERS["df_03_returns_by_channel"]),
                sources=[
                    DataFlowSource(
                        name="ReturnsSource",
                        dataset=DatasetReference(
                            reference_name="ds_returns_raw",
                            type="DatasetReference",
                        ),
                    )
                ],
                sinks=[
                    DataFlowSink(
                        name="SummarySink",
                        dataset=DatasetReference(
                            reference_name="ds_silver_returns_summary",
                            type="DatasetReference",
                        ),
                    )
                ],
                script_lines=_df_returns_by_channel_script(),
                annotations=["concept-power-query-parity", "aggregate"],
            )
        ),
        "df_04_join_returns_txn": DataFlowResource(
            properties=MappingDataFlow(
                description="Inner join returns to bronze transactions on transaction_id",
                folder=DataFlowFolder(name=DATA_FLOW_FOLDERS["df_04_join_returns_txn"]),
                sources=[
                    DataFlowSource(
                        name="ReturnsSource",
                        dataset=DatasetReference(
                            reference_name="ds_returns_raw",
                            type="DatasetReference",
                        ),
                    ),
                    DataFlowSource(
                        name="TxnSource",
                        dataset=DatasetReference(
                            reference_name="ds_df_bronze_source",
                            type="DatasetReference",
                        ),
                    ),
                ],
                sinks=[
                    DataFlowSink(
                        name="JoinedSink",
                        dataset=DatasetReference(
                            reference_name="ds_silver_returns_joined",
                            type="DatasetReference",
                        ),
                    )
                ],
                script_lines=_df_join_returns_txn_script(),
                annotations=["concept-mapping-dataflow", "join"],
            )
        ),
    }
    for name, body in flows.items():
        adf.data_flows.create_or_update(rg, factory, name, body)
        logger.info("Data flow %s", name)


def deploy_wrangling_data_flows(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
) -> None:
    """Deploy Power Query wrangling shell — mashup completed in Studio (see catalog)."""
    flows: dict[str, DataFlowResource] = {
        "pq_01_returns_wrangle": DataFlowResource(
            properties=WranglingDataFlow(
                description=(
                    "Power Query returns wrangle — open in Studio to author M mashup; "
                    "automated lab uses df_03_returns_by_channel via pl_25"
                ),
                folder=DataFlowFolder(name=DATA_FLOW_FOLDERS["pq_01_returns_wrangle"]),
                document_locale="en-US",
                script="",
                sources=[
                    PowerQuerySource(
                        name="ReturnsRaw",
                        dataset=DatasetReference(
                            reference_name="ds_returns_raw",
                            type="DatasetReference",
                        ),
                    )
                ],
                annotations=["concept-power-query", "studio-mashup-required"],
            )
        ),
    }
    for name, body in flows.items():
        adf.data_flows.create_or_update(rg, factory, name, body)
        logger.info("Wrangling data flow %s", name)


def _execute_df_activity(name: str, data_flow_name: str) -> ExecuteDataFlowActivity:
    return ExecuteDataFlowActivity(
        name=name,
        data_flow=DataFlowReference(
            reference_name=data_flow_name,
            type="DataFlowReference",
        ),
        compute=ExecuteDataFlowActivityTypePropertiesCompute(
            compute_type="General",
            core_count=8,
        ),
        trace_level="Coarse",
        policy=ActivityPolicy(timeout="0.01:00:00", retry=0),
    )


def deploy_advanced_pipelines(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
    *,
    include_sql: bool,
) -> None:
    pipelines: dict[str, PipelineResource] = {}

    pipelines["pl_13_df_bronze_cleanse"] = PipelineResource(
        activities=[_execute_df_activity("RunBronzeCleanse", "df_01_bronze_cleanse")],
        annotations=["concept-execute-dataflow"],
    )

    pipelines["pl_14_df_channel_aggregate"] = PipelineResource(
        activities=[_execute_df_activity("RunChannelAgg", "df_02_channel_aggregate")],
        annotations=["concept-execute-dataflow", "aggregate"],
    )

    pipelines["pl_15_delete_rejected"] = PipelineResource(
        activities=[
            DeleteActivity(
                name="DeleteRejectedDecoys",
                dataset=DatasetReference(
                    reference_name="ds_bronze_rejected_folder",
                    type="DatasetReference",
                ),
                store_settings=AzureBlobFSReadSettings(recursive=True),
                enable_logging=False,
            )
        ],
        annotations=["concept-delete"],
    )

    pipelines["pl_16_web_ping"] = PipelineResource(
        activities=[
            WebActivity(
                name="PingHttpBin",
                method=WebActivityMethod.GET,
                url="https://jsonplaceholder.typicode.com/todos/1",
                policy=ActivityPolicy(timeout="0.00:05:00", retry=1),
            )
        ],
        annotations=["concept-web-activity"],
    )

    set_wire = SetVariableActivity(
        name="RouteWire",
        variable_name="route_message",
        value=Expression(type="Expression", value="@string('wire path selected')"),
    )
    set_card = SetVariableActivity(
        name="RouteCard",
        variable_name="route_message",
        value=Expression(type="Expression", value="@string('card path selected')"),
    )
    set_default = SetVariableActivity(
        name="RouteDefault",
        variable_name="route_message",
        value=Expression(type="Expression", value="@string('default path selected')"),
    )
    pipelines["pl_17_switch_channel"] = PipelineResource(
        activities=[
            SwitchActivity(
                name="SwitchByChannel",
                on=Expression(
                    type="Expression",
                    value="@pipeline().parameters.channel",
                ),
                cases=[
                    SwitchCase(value="wire", activities=[set_wire]),
                    SwitchCase(value="card", activities=[set_card]),
                ],
                default_activities=[set_default],
            )
        ],
        parameters={
            "channel": ParameterSpecification(type="String", default_value="wire"),
        },
        variables={"route_message": {"type": "String"}},
        annotations=["concept-switch"],
    )

    check_folder = GetMetadataActivity(
        name="CheckIncoming",
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
    mark_found = SetVariableActivity(
        name="MarkFileFound",
        depends_on=[
            ActivityDependency(activity=check_folder.name, dependency_conditions=["Succeeded"])
        ],
        variable_name="file_ready",
        value=Expression(
            type="Expression",
            value=(
                "@if(greater(length(activity('CheckIncoming').output.childItems), 0), "
                "'yes', 'no')"
            ),
        ),
    )
    pipelines["pl_18_until_file_ready"] = PipelineResource(
        activities=[
            UntilActivity(
                name="UntilFileExists",
                expression=Expression(
                    type="Expression",
                    value="@equals(variables('file_ready'), 'yes')",
                ),
                activities=[check_folder, mark_found],
                timeout="0.00:05:00",
            )
        ],
        variables={"file_ready": {"type": "String", "defaultValue": "no"}},
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
        },
        annotations=["concept-until", "concept-polling"],
    )

    append1 = AppendVariableActivity(
        name="AppendStepOne",
        variable_name="audit_steps",
        value=Expression(type="Expression", value="@string('lookup-watermark')"),
    )
    append2 = AppendVariableActivity(
        name="AppendStepTwo",
        depends_on=[ActivityDependency(activity=append1.name, dependency_conditions=["Succeeded"])],
        variable_name="audit_steps",
        value=Expression(type="Expression", value="@string('copy-bronze')"),
    )
    pipelines["pl_19_append_audit_log"] = PipelineResource(
        activities=[append1, append2],
        variables={"audit_steps": {"type": "Array"}},
        annotations=["concept-append-variable"],
    )

    pipelines["pl_20_validate_incoming"] = PipelineResource(
        activities=[
            ValidationActivity(
                name="ValidateFolderHasFiles",
                dataset=_ds_ref(
                    "ds_bronze_incoming_folder",
                    folder_path={
                        "value": "@pipeline().parameters.incoming_folder",
                        "type": "Expression",
                    },
                ),
                timeout="0.00:02:00",
                sleep=10,
                child_items=True,
            )
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
        },
        annotations=["concept-validation"],
    )

    fail_act = FailActivity(
        name="FailBadParam",
        message="force_fail=true — intentional demo failure path",
        error_code="PL21-DEMO-FAIL",
    )
    ok_set = SetVariableActivity(
        name="MarkOk",
        variable_name="run_status",
        value=Expression(type="Expression", value="@string('ok')"),
    )
    pipelines["pl_21_fail_on_bad_param"] = PipelineResource(
        activities=[
            IfConditionActivity(
                name="IfForceFail",
                expression=Expression(
                    type="Expression",
                    value="@equals(pipeline().parameters.force_fail, true)",
                ),
                if_true_activities=[fail_act],
                if_false_activities=[ok_set],
            )
        ],
        parameters={
            "force_fail": ParameterSpecification(type="Bool", default_value=False),
        },
        variables={"run_status": {"type": "String"}},
        annotations=["concept-fail"],
    )

    read_cdc_watermark = LookupActivity(
        name="ReadCdcWatermark",
        dataset=DatasetReference(
            reference_name="ds_audit_cdc_watermark_json",
            type="DatasetReference",
        ),
        source=JsonSource(),
        first_row_only=True,
    )
    copy_incremental = CopyActivity(
        name="CopyFilesModifiedAfterWatermark",
        depends_on=[
            ActivityDependency(
                activity=read_cdc_watermark.name,
                dependency_conditions=["Succeeded"],
            )
        ],
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_file",
                folder_path={
                    "value": "@pipeline().parameters.incoming_folder",
                    "type": "Expression",
                },
                file_name="sample_transactions.csv",
            )
        ],
        outputs=[
            _ds_ref(
                "ds_bronze_loaded_file",
                folder_path={
                    "value": "@pipeline().parameters.loaded_folder",
                    "type": "Expression",
                },
                file_name="sample_transactions.csv",
            )
        ],
        source=DelimitedTextSource(
            store_settings=AzureBlobFSReadSettings(
                modified_datetime_start={
                    "value": (
                        "@activity('ReadCdcWatermark').output.firstRow.last_modified_after"
                    ),
                    "type": "Expression",
                }
            )
        ),
        sink=DelimitedTextSink(
            store_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")
        ),
        policy=ActivityPolicy(timeout="0.00:15:00", retry=1),
    )
    pipelines["pl_23_incremental_watermark_copy"] = PipelineResource(
        activities=[read_cdc_watermark, copy_incremental],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "loaded_folder": ParameterSpecification(
                type="String", default_value="loaded/run=session3-lab"
            ),
        },
        annotations=["concept-incremental-copy", "concept-watermark", "concept-cdc-adjacent"],
    )

    pipelines["pl_25_power_query_returns"] = PipelineResource(
        activities=[
            _execute_df_activity("RunReturnsSummary", "df_03_returns_by_channel")
        ],
        annotations=[
            "concept-power-query-parity",
            "concept-execute-dataflow",
            "see-pq_01-for-studio-mashup",
        ],
    )

    pipelines["pl_26_join_returns_txn"] = PipelineResource(
        activities=[_execute_df_activity("RunReturnsJoin", "df_04_join_returns_txn")],
        annotations=["concept-execute-dataflow", "concept-join"],
    )

    if include_sql:
        pipelines["pl_22_sql_script_count"] = PipelineResource(
            activities=[
                ScriptActivity(
                    name="CountMetadataRows",
                    linked_service_name=_ls_ref(LS_AZURE_SQL),
                    scripts=[
                        ScriptActivityScriptBlock(
                            type="Query",
                            text="SELECT COUNT(*) AS row_count FROM dbo.adf_job_metadata",
                        )
                    ],
                    policy=ActivityPolicy(timeout="0.00:05:00", retry=1),
                )
            ],
            annotations=["concept-script-activity", "sql"],
        )

        read_cdc_version = LookupActivity(
            name="ReadCdcVersion",
            dataset=DatasetReference(
                reference_name="ds_audit_cdc_watermark_json",
                type="DatasetReference",
            ),
            source=JsonSource(),
            first_row_only=True,
        )
        enable_change_tracking = ScriptActivity(
            name="EnableChangeTracking",
            linked_service_name=_ls_ref(LS_AZURE_SQL),
            scripts=[
                ScriptActivityScriptBlock(
                    type="NonQuery",
                    text="""
IF NOT EXISTS (
    SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID()
)
    ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON
        (CHANGE_RETENTION = 2, AUTO_CLEANUP = ON);

IF NOT EXISTS (
    SELECT 1 FROM sys.change_tracking_tables
    WHERE object_id = OBJECT_ID('dbo.dim_channel')
)
    ALTER TABLE dbo.dim_channel ENABLE CHANGE_TRACKING
        WITH (TRACK_COLUMNS_UPDATED = OFF);
""".strip(),
                )
            ],
            policy=ActivityPolicy(timeout="0.00:05:00", retry=1),
        )
        simulate_source_change = ScriptActivity(
            name="SimulateSourceChange",
            depends_on=[
                ActivityDependency(
                    activity=enable_change_tracking.name,
                    dependency_conditions=["Succeeded"],
                )
            ],
            linked_service_name=_ls_ref(LS_AZURE_SQL),
            scripts=[
                ScriptActivityScriptBlock(
                    type="NonQuery",
                    text="""
IF EXISTS (SELECT 1 FROM dbo.dim_channel WHERE channel_code = 'fps')
    UPDATE dbo.dim_channel
    SET channel_name = 'Faster Payments (lab)'
    WHERE channel_code = 'fps';
ELSE
    INSERT INTO dbo.dim_channel (channel_code, channel_name, is_active)
    VALUES ('fps', 'Faster Payments', 1);
""".strip(),
                )
            ],
            policy=ActivityPolicy(timeout="0.00:05:00", retry=1),
        )
        read_cdc_version.depends_on = [
            ActivityDependency(
                activity=simulate_source_change.name,
                dependency_conditions=["Succeeded"],
            )
        ]
        copy_cdc_changes = CopyActivity(
            name="CopySqlCdcChanges",
            depends_on=[
                ActivityDependency(
                    activity=read_cdc_version.name,
                    dependency_conditions=["Succeeded"],
                )
            ],
            inputs=[_ds_ref("ds_sql_dim_channel")],
            outputs=[_ds_ref("ds_audit_sql_cdc_csv")],
            source=AzureSqlSource(
                sql_reader_query=Expression(
                    type="Expression",
                    value=(
                        "@concat("
                        "'SELECT ct.SYS_CHANGE_OPERATION AS change_op, "
                        "c.channel_code, c.channel_name, c.is_active "
                        "FROM CHANGETABLE(CHANGES dbo.dim_channel, ', "
                        "string(activity('ReadCdcVersion').output.firstRow.last_sync_version), "
                        "') AS ct INNER JOIN dbo.dim_channel c "
                        "ON c.channel_code = ct.channel_code'"
                        ")"
                    ),
                )
            ),
            sink=DelimitedTextSink(
                store_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")
            ),
            policy=ActivityPolicy(timeout="0.00:15:00", retry=1),
        )
        pipelines["pl_24_sql_change_tracking_copy"] = PipelineResource(
            activities=[
                enable_change_tracking,
                simulate_source_change,
                read_cdc_version,
                copy_cdc_changes,
            ],
            annotations=["concept-sql-cdc", "concept-change-tracking", "sql"],
        )

    for name, body in pipelines.items():
        if not include_sql and name in (
            "pl_22_sql_script_count",
            "pl_24_sql_change_tracking_copy",
        ):
            continue
        apply_pipeline_folder(name, body)
        adf.pipelines.create_or_update(rg, factory, name, body)
        logger.info("Advanced pipeline %s", name)


def deploy_advanced(
    cfg,
    estate,
    *,
    include_sql: bool = True,
) -> None:
    """Deploy mapping data flows + advanced-activity teaching pipelines."""
    adf = _client(cfg)
    rg, factory = cfg.resource_group, estate.data_factory
    _deploy_advanced_datasets(adf, rg, factory)
    deploy_data_flows(adf, rg, factory)
    deploy_wrangling_data_flows(adf, rg, factory)
    deploy_advanced_pipelines(adf, rg, factory, include_sql=include_sql)


def list_advanced_pipeline_names(*, include_sql: bool) -> list[str]:
    names = list(ADVANCED_PIPELINE_NAMES)
    if not include_sql:
        names = [
            n
            for n in names
            if n not in ("pl_22_sql_script_count", "pl_24_sql_change_tracking_copy")
        ]
    return names


def default_params(pipeline_name: str) -> dict[str, Any]:
    if pipeline_name == "pl_17_switch_channel":
        return {"channel": "wire"}
    if pipeline_name == "pl_21_fail_on_bad_param":
        return {"force_fail": False}
    if pipeline_name == "pl_20_validate_incoming":
        return {"incoming_folder": "incoming/run=session3-lab"}
    if pipeline_name == "pl_18_until_file_ready":
        return {"incoming_folder": "incoming/run=session3-lab"}
    if pipeline_name == "pl_23_incremental_watermark_copy":
        return {
            "incoming_folder": "incoming/run=session3-lab",
            "loaded_folder": "loaded/run=session3-lab",
        }
    return {}
