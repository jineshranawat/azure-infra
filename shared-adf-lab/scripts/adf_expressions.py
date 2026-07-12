"""Pure expression-builder teaching pipelines — no data flows, no Databricks (pl_ex_01–pl_ex_13)."""

from __future__ import annotations

import logging
from typing import Any

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    AppendVariableActivity,
    AzureBlobFSReadSettings,
    AzureBlobFSWriteSettings,
    CopyActivity,
    DelimitedTextSink,
    DelimitedTextSource,
    ExecutePipelineActivity,
    Expression,
    FailActivity,
    ForEachActivity,
    GetMetadataActivity,
    IfConditionActivity,
    JsonSource,
    LookupActivity,
    ParameterSpecification,
    PipelineReference,
    PipelineResource,
    SetVariableActivity,
    SwitchActivity,
    SwitchCase,
    UserProperty,
)

from adf_global_parameters import deploy_global_parameters
from adf_pipelines import _client, _ds_ref
from expression_folders import apply_expression_builder_folder

logger = logging.getLogger(__name__)

EXPRESSION_PIPELINE_NAMES = [
    "pl_ex_01_pipeline_param_variables",
    "pl_ex_02_system_context_variables",
    "pl_ex_03_concat_string_functions",
    "pl_ex_04_metadata_output_expressions",
    "pl_ex_05_lookup_json_expressions",
    "pl_ex_06_if_equals_compare",
    "pl_ex_07_foreach_item_expressions",
    "pl_ex_08_switch_route_expressions",
    "pl_ex_09_logic_and_or_not",
    "pl_ex_10_child_pipeline_expressions",
    "pl_ex_10_child_echo_params",
    "pl_ex_11_debug_print_variables",
    "pl_ex_12_dataset_foreach_item",
    "pl_ex_13_user_properties_monitor",
]


def _user_prop(name: str, expression: str) -> UserProperty:
    """Activity user property with dynamic expression (shows in Monitor columns)."""
    return UserProperty(
        name=name,
        value={"value": expression, "type": "Expression"},
    )


def deploy_expression_pipelines(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
) -> None:
    pipelines: dict[str, PipelineResource] = {}

    pipelines["pl_ex_01_pipeline_param_variables"] = PipelineResource(
        activities=[
            SetVariableActivity(
                name="CaptureRunId",
                variable_name="captured_run_id",
                value=Expression(
                    type="Expression",
                    value="@pipeline().parameters.run_id",
                ),
            ),
            SetVariableActivity(
                name="CaptureLabTag",
                depends_on=[
                    ActivityDependency(activity="CaptureRunId", dependency_conditions=["Succeeded"])
                ],
                variable_name="captured_lab_tag",
                value=Expression(
                    type="Expression",
                    value="@pipeline().parameters.lab_tag",
                ),
            ),
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "lab_tag": ParameterSpecification(type="String", default_value="ex01-params"),
        },
        variables={
            "captured_run_id": {"type": "String"},
            "captured_lab_tag": {"type": "String"},
        },
        annotations=["expression-pipeline-parameters"],
    )

    pipelines["pl_ex_02_system_context_variables"] = PipelineResource(
        activities=[
            SetVariableActivity(
                name="CapturePipelineRunId",
                variable_name="adf_run_id",
                value=Expression(type="Expression", value="@pipeline().RunId"),
            ),
            SetVariableActivity(
                name="CaptureFactoryName",
                depends_on=[
                    ActivityDependency(
                        activity="CapturePipelineRunId", dependency_conditions=["Succeeded"]
                    )
                ],
                variable_name="adf_factory",
                value=Expression(type="Expression", value="@pipeline().DataFactory"),
            ),
            SetVariableActivity(
                name="CaptureUtcNow",
                depends_on=[
                    ActivityDependency(
                        activity="CaptureFactoryName", dependency_conditions=["Succeeded"]
                    )
                ],
                variable_name="captured_utc",
                value=Expression(type="Expression", value="@utcnow()"),
            ),
            SetVariableActivity(
                name="CaptureGuid",
                depends_on=[
                    ActivityDependency(activity="CaptureUtcNow", dependency_conditions=["Succeeded"])
                ],
                variable_name="captured_guid",
                value=Expression(type="Expression", value="@guid()"),
            ),
        ],
        variables={
            "adf_run_id": {"type": "String"},
            "adf_factory": {"type": "String"},
            "captured_utc": {"type": "String"},
            "captured_guid": {"type": "String"},
        },
        annotations=["expression-system-functions"],
    )

    pipelines["pl_ex_03_concat_string_functions"] = PipelineResource(
        activities=[
            SetVariableActivity(
                name="BuildLakePath",
                variable_name="lake_path",
                value=Expression(
                    type="Expression",
                    value=(
                        "@concat("
                        "pipeline().parameters.folder_path, "
                        "'/', pipeline().parameters.file_name"
                        ")"
                    ),
                ),
            ),
            SetVariableActivity(
                name="BuildTaggedMessage",
                depends_on=[
                    ActivityDependency(activity="BuildLakePath", dependency_conditions=["Succeeded"])
                ],
                variable_name="tagged_message",
                value=Expression(
                    type="Expression",
                    value=(
                        "@concat("
                        "'run=', pipeline().parameters.run_id, "
                        "'|path=', variables('lake_path')"
                        ")"
                    ),
                ),
            ),
            SetVariableActivity(
                name="UpperChannel",
                depends_on=[
                    ActivityDependency(
                        activity="BuildTaggedMessage", dependency_conditions=["Succeeded"]
                    )
                ],
                variable_name="channel_upper",
                value=Expression(
                    type="Expression",
                    value="@toUpper(pipeline().parameters.channel)",
                ),
            ),
        ],
        parameters={
            "folder_path": ParameterSpecification(
                type="String", default_value="bronze/incoming/run=session3-lab"
            ),
            "file_name": ParameterSpecification(
                type="String", default_value="sample_transactions.csv"
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "channel": ParameterSpecification(type="String", default_value="card"),
        },
        variables={
            "lake_path": {"type": "String"},
            "tagged_message": {"type": "String"},
            "channel_upper": {"type": "String"},
        },
        annotations=["expression-concat", "expression-string-functions"],
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
    pipelines["pl_ex_04_metadata_output_expressions"] = PipelineResource(
        activities=[
            get_meta,
            SetVariableActivity(
                name="SetFileCount",
                depends_on=[
                    ActivityDependency(
                        activity=get_meta.name, dependency_conditions=["Succeeded"]
                    )
                ],
                variable_name="file_count",
                value=Expression(
                    type="Expression",
                    value="@string(length(activity('GetIncomingMetadata').output.childItems))",
                ),
            ),
            SetVariableActivity(
                name="SetItemName",
                depends_on=[
                    ActivityDependency(activity="SetFileCount", dependency_conditions=["Succeeded"])
                ],
                variable_name="item_name",
                value=Expression(
                    type="Expression",
                    value="@coalesce(activity('GetIncomingMetadata').output.itemName, 'none')",
                ),
            ),
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
        },
        variables={
            "file_count": {"type": "String"},
            "item_name": {"type": "String"},
        },
        annotations=["expression-activity-output", "expression-coalesce"],
    )

    lookup_wm = LookupActivity(
        name="LookupWatermark",
        dataset=_ds_ref("ds_audit_watermark_json"),
        source=JsonSource(),
        first_row_only=True,
    )
    pipelines["pl_ex_05_lookup_json_expressions"] = PipelineResource(
        activities=[
            lookup_wm,
            SetVariableActivity(
                name="SetLastRun",
                depends_on=[
                    ActivityDependency(
                        activity=lookup_wm.name, dependency_conditions=["Succeeded"]
                    )
                ],
                variable_name="last_run",
                value=Expression(
                    type="Expression",
                    value="@activity('LookupWatermark').output.firstRow.last_run",
                ),
            ),
            SetVariableActivity(
                name="SetWatermark",
                depends_on=[
                    ActivityDependency(activity="SetLastRun", dependency_conditions=["Succeeded"])
                ],
                variable_name="watermark",
                value=Expression(
                    type="Expression",
                    value="@activity('LookupWatermark').output.firstRow.last_successful_watermark",
                ),
            ),
        ],
        variables={
            "last_run": {"type": "String"},
            "watermark": {"type": "String"},
        },
        annotations=["expression-lookup", "expression-firstrow"],
    )

    set_status = SetVariableActivity(
        name="SetStatusFromParam",
        variable_name="job_status",
        value=Expression(
            type="Expression",
            value="@pipeline().parameters.status",
        ),
    )
    mark_ready = SetVariableActivity(
        name="MarkReadyPath",
        variable_name="branch_taken",
        value=Expression(type="Expression", value="@string('ready-branch')"),
    )
    mark_hold = SetVariableActivity(
        name="MarkHoldPath",
        variable_name="branch_taken",
        value=Expression(type="Expression", value="@string('hold-branch')"),
    )
    pipelines["pl_ex_06_if_equals_compare"] = PipelineResource(
        activities=[
            set_status,
            IfConditionActivity(
                name="IfStatusReady",
                depends_on=[
                    ActivityDependency(
                        activity=set_status.name, dependency_conditions=["Succeeded"]
                    )
                ],
                expression=Expression(
                    type="Expression",
                    value="@equals(variables('job_status'), 'READY')",
                ),
                if_true_activities=[mark_ready],
                if_false_activities=[mark_hold],
            ),
        ],
        parameters={
            "status": ParameterSpecification(type="String", default_value="READY"),
        },
        variables={
            "job_status": {"type": "String"},
            "branch_taken": {"type": "String"},
        },
        annotations=["expression-if", "expression-equals"],
    )

    append_step = AppendVariableActivity(
        name="AppendChannelStep",
        variable_name="channel_audit",
        value=Expression(
            type="Expression",
            value="@concat('processed-', item())",
        ),
    )
    pipelines["pl_ex_07_foreach_item_expressions"] = PipelineResource(
        activities=[
            ForEachActivity(
                name="ForEachChannel",
                items=Expression(type="Expression", value="@pipeline().parameters.channels"),
                is_sequential=True,
                activities=[append_step],
            )
        ],
        parameters={
            "channels": ParameterSpecification(
                type="Array",
                default_value=["card", "wire", "fps"],
            ),
        },
        variables={"channel_audit": {"type": "Array"}},
        annotations=["expression-foreach", "expression-item"],
    )

    route_a = SetVariableActivity(
        name="RouteA",
        variable_name="route_result",
        value=Expression(type="Expression", value="@string('route-a')"),
    )
    route_b = SetVariableActivity(
        name="RouteB",
        variable_name="route_result",
        value=Expression(type="Expression", value="@string('route-b')"),
    )
    route_default = SetVariableActivity(
        name="RouteDefault",
        variable_name="route_result",
        value=Expression(type="Expression", value="@string('route-default')"),
    )
    pipelines["pl_ex_08_switch_route_expressions"] = PipelineResource(
        activities=[
            SwitchActivity(
                name="SwitchRoute",
                on=Expression(
                    type="Expression",
                    value="@pipeline().parameters.route",
                ),
                cases=[
                    SwitchCase(value="A", activities=[route_a]),
                    SwitchCase(value="B", activities=[route_b]),
                ],
                default_activities=[route_default],
            )
        ],
        parameters={
            "route": ParameterSpecification(type="String", default_value="A"),
        },
        variables={"route_result": {"type": "String"}},
        annotations=["expression-switch"],
    )

    count_meta = GetMetadataActivity(
        name="CountIncomingFiles",
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
    set_files = SetVariableActivity(
        name="SetFileCountForLogic",
        depends_on=[
            ActivityDependency(
                activity=count_meta.name, dependency_conditions=["Succeeded"]
            )
        ],
        variable_name="file_count",
        value=Expression(
            type="Expression",
            value="@string(length(activity('CountIncomingFiles').output.childItems))",
        ),
    )
    mark_proceed = SetVariableActivity(
        name="MarkProceed",
        variable_name="logic_result",
        value=Expression(type="Expression", value="@string('proceed')"),
    )
    mark_skip = SetVariableActivity(
        name="MarkSkip",
        variable_name="logic_result",
        value=Expression(type="Expression", value="@string('skip')"),
    )
    pipelines["pl_ex_09_logic_and_or_not"] = PipelineResource(
        activities=[
            count_meta,
            set_files,
            IfConditionActivity(
                name="IfHasFilesAndNotEmpty",
                depends_on=[
                    ActivityDependency(
                        activity=set_files.name, dependency_conditions=["Succeeded"]
                    )
                ],
                expression=Expression(
                    type="Expression",
                    value=(
                        "@and("
                        "greater(int(variables('file_count')), 0), "
                        "not(equals(pipeline().parameters.force_empty, true))"
                        ")"
                    ),
                ),
                if_true_activities=[mark_proceed],
                if_false_activities=[mark_skip],
            ),
        ],
        parameters={
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "force_empty": ParameterSpecification(type="Bool", default_value=False),
        },
        variables={
            "file_count": {"type": "String"},
            "logic_result": {"type": "String"},
        },
        annotations=["expression-and", "expression-not", "expression-greater"],
    )

    pipelines["pl_ex_10_child_echo_params"] = PipelineResource(
        activities=[
            SetVariableActivity(
                name="EchoRunId",
                variable_name="echo_run_id",
                value=Expression(
                    type="Expression",
                    value="@pipeline().parameters.run_id",
                ),
            ),
            SetVariableActivity(
                name="EchoMessage",
                depends_on=[
                    ActivityDependency(activity="EchoRunId", dependency_conditions=["Succeeded"])
                ],
                variable_name="echo_message",
                value=Expression(
                    type="Expression",
                    value="@pipeline().parameters.message",
                ),
            ),
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "message": ParameterSpecification(type="String", default_value="child-default"),
        },
        variables={
            "echo_run_id": {"type": "String"},
            "echo_message": {"type": "String"},
        },
        annotations=["expression-child-pipeline", "internal"],
    )

    pipelines["pl_ex_10_child_pipeline_expressions"] = PipelineResource(
        activities=[
            ExecutePipelineActivity(
                name="ExecuteChildEcho",
                pipeline=PipelineReference(
                    reference_name="pl_ex_10_child_echo_params",
                    type="PipelineReference",
                ),
                parameters={
                    "run_id": {
                        "value": "@pipeline().parameters.run_id",
                        "type": "Expression",
                    },
                    "message": {
                        "value": "@concat('parent→child-', pipeline().parameters.lab_tag)",
                        "type": "Expression",
                    },
                },
                wait_on_completion=True,
            )
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "lab_tag": ParameterSpecification(type="String", default_value="ex10-parent"),
        },
        annotations=["expression-execute-pipeline", "expression-child-params"],
    )

    print_via_fail = FailActivity(
        name="PrintViaFailMessage",
        message=Expression(
            type="Expression",
            value="@variables('v_debug_line')",
        ),
        error_code="DEBUG-PRINT",
    )
    mark_printed = SetVariableActivity(
        name="MarkPrintedToMonitor",
        variable_name="v_status",
        value=Expression(type="Expression", value="@string('see BuildDebugLine output in Monitor')"),
    )
    pipelines["pl_ex_11_debug_print_variables"] = PipelineResource(
        activities=[
            SetVariableActivity(
                name="CaptureGlobalEnvironment",
                variable_name="v_global_env",
                value=Expression(
                    type="Expression",
                    value="@string(pipeline().globalParameters.g_lab_environment)",
                ),
            ),
            SetVariableActivity(
                name="CaptureGlobalTrainerTag",
                depends_on=[
                    ActivityDependency(
                        activity="CaptureGlobalEnvironment",
                        dependency_conditions=["Succeeded"],
                    )
                ],
                variable_name="v_global_tag",
                value=Expression(
                    type="Expression",
                    value="@string(pipeline().globalParameters.g_trainer_tag)",
                ),
            ),
            SetVariableActivity(
                name="CaptureRunIdParam",
                depends_on=[
                    ActivityDependency(
                        activity="CaptureGlobalTrainerTag",
                        dependency_conditions=["Succeeded"],
                    )
                ],
                variable_name="v_run_id",
                value=Expression(
                    type="Expression",
                    value="@pipeline().parameters.run_id",
                ),
            ),
            SetVariableActivity(
                name="CaptureFactoryName",
                depends_on=[
                    ActivityDependency(
                        activity="CaptureRunIdParam",
                        dependency_conditions=["Succeeded"],
                    )
                ],
                variable_name="v_factory",
                value=Expression(type="Expression", value="@pipeline().DataFactory"),
            ),
            SetVariableActivity(
                name="CapturePipelineRunId",
                depends_on=[
                    ActivityDependency(
                        activity="CaptureFactoryName",
                        dependency_conditions=["Succeeded"],
                    )
                ],
                variable_name="v_pipeline_run_id",
                value=Expression(type="Expression", value="@pipeline().RunId"),
            ),
            SetVariableActivity(
                name="BuildDebugLine",
                depends_on=[
                    ActivityDependency(
                        activity="CapturePipelineRunId",
                        dependency_conditions=["Succeeded"],
                    )
                ],
                variable_name="v_debug_line",
                value=Expression(
                    type="Expression",
                    value=(
                        "@concat("
                        "'DEBUG | global_env=', variables('v_global_env'), "
                        "' | global_tag=', variables('v_global_tag'), "
                        "' | run_id=', variables('v_run_id'), "
                        "' | factory=', variables('v_factory'), "
                        "' | pipeline_run=', variables('v_pipeline_run_id')"
                        ")"
                    ),
                ),
            ),
            AppendVariableActivity(
                name="AppendDebugStep",
                depends_on=[
                    ActivityDependency(
                        activity="BuildDebugLine",
                        dependency_conditions=["Succeeded"],
                    )
                ],
                variable_name="debug_steps",
                value=Expression(
                    type="Expression",
                    value="@variables('v_debug_line')",
                ),
            ),
            IfConditionActivity(
                name="IfShowDebugFail",
                depends_on=[
                    ActivityDependency(
                        activity="AppendDebugStep",
                        dependency_conditions=["Succeeded"],
                    )
                ],
                expression=Expression(
                    type="Expression",
                    value="@equals(pipeline().parameters.show_debug_fail, true)",
                ),
                if_true_activities=[print_via_fail],
                if_false_activities=[mark_printed],
            ),
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "show_debug_fail": ParameterSpecification(type="Bool", default_value=False),
        },
        variables={
            "v_global_env": {"type": "String"},
            "v_global_tag": {"type": "String"},
            "v_run_id": {"type": "String"},
            "v_factory": {"type": "String"},
            "v_pipeline_run_id": {"type": "String"},
            "v_debug_line": {"type": "String"},
            "v_status": {"type": "String"},
            "debug_steps": {"type": "Array"},
        },
        annotations=["expression-debug-print", "expression-global-parameters"],
    )

    # pl_ex_12 — @dataset() parameters fed by @pipeline().parameters AND @item() inside ForEach
    probe_main = GetMetadataActivity(
        name="ProbeMainFolder",
        dataset=_ds_ref(
            "ds_bronze_incoming_folder",
            folder_path={
                "value": "@pipeline().parameters.default_incoming",
                "type": "Expression",
            },
        ),
        field_list=["childItems", "itemName"],
        store_settings=AzureBlobFSReadSettings(recursive=True),
    )
    set_baseline = SetVariableActivity(
        name="SetBaselineFileCount",
        depends_on=[
            ActivityDependency(activity="ProbeMainFolder", dependency_conditions=["Succeeded"])
        ],
        variable_name="baseline_file_count",
        value=Expression(
            type="Expression",
            value="@string(length(activity('ProbeMainFolder').output.childItems))",
        ),
    )
    get_item_folder = GetMetadataActivity(
        name="GetFolderMetadata",
        dataset=_ds_ref(
            "ds_bronze_incoming_folder",
            folder_path={
                "value": "@item().incoming",
                "type": "Expression",
            },
        ),
        field_list=["childItems"],
        store_settings=AzureBlobFSReadSettings(recursive=True),
    )
    log_item = AppendVariableActivity(
        name="LogItemResult",
        depends_on=[
            ActivityDependency(
                activity="GetFolderMetadata", dependency_conditions=["Succeeded"]
            )
        ],
        variable_name="foreach_log",
        value=Expression(
            type="Expression",
            value=(
                "@concat("
                "item().label, "
                "' | @dataset folder_path←item().incoming=', item().incoming, "
                "' | files=', string(length(activity('GetFolderMetadata').output.childItems))"
                ")"
            ),
        ),
    )
    pipelines["pl_ex_12_dataset_foreach_item"] = PipelineResource(
        activities=[
            probe_main,
            set_baseline,
            ForEachActivity(
                name="ForEachFolderJobs",
                depends_on=[
                    ActivityDependency(
                        activity="SetBaselineFileCount", dependency_conditions=["Succeeded"]
                    )
                ],
                items=Expression(
                    type="Expression", value="@pipeline().parameters.folder_jobs"
                ),
                is_sequential=True,
                activities=[get_item_folder, log_item],
            ),
        ],
        parameters={
            "default_incoming": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "folder_jobs": ParameterSpecification(type="Array"),
        },
        variables={
            "baseline_file_count": {"type": "String"},
            "foreach_log": {"type": "Array"},
        },
        annotations=["expression-dataset-parameters", "expression-foreach-item"],
    )

    list_incoming = GetMetadataActivity(
        name="ListIncomingWithTags",
        dataset=_ds_ref(
            "ds_bronze_incoming_folder",
            folder_path={
                "value": "@pipeline().parameters.incoming_folder",
                "type": "Expression",
            },
        ),
        field_list=["childItems"],
        store_settings=AzureBlobFSReadSettings(recursive=True),
        user_properties=[
            _user_prop("phase", "@string('discover')"),
            _user_prop("job_name", "@pipeline().parameters.job_name"),
            _user_prop("source_folder", "@pipeline().parameters.incoming_folder"),
            _user_prop("environment", "@pipeline().globalParameters.g_lab_environment"),
            _user_prop("run_id", "@pipeline().parameters.run_id"),
        ],
    )
    tag_channel = AppendVariableActivity(
        name="TagChannelStep",
        variable_name="channel_audit",
        value=Expression(
            type="Expression",
            value="@concat('tagged-channel=', item())",
        ),
        user_properties=[
            _user_prop("phase", "@string('foreach-tag')"),
            _user_prop("channel", "@item()"),
            _user_prop("job_name", "@pipeline().parameters.job_name"),
            _user_prop("run_id", "@pipeline().parameters.run_id"),
        ],
    )
    copy_tagged = CopyActivity(
        name="CopyBronzeWithTags",
        depends_on=[
            ActivityDependency(
                activity="ForEachChannelTags", dependency_conditions=["Succeeded"]
            )
        ],
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
        user_properties=[
            _user_prop("phase", "@string('load')"),
            _user_prop("job_name", "@pipeline().parameters.job_name"),
            _user_prop(
                "source_path",
                "@concat(pipeline().parameters.incoming_folder, '/sample_transactions.csv')",
            ),
            _user_prop(
                "destination_path",
                "@concat(pipeline().parameters.loaded_folder, '/sample_transactions.csv')",
            ),
            _user_prop("pipeline_run_id", "@pipeline().RunId"),
        ],
    )
    pipelines["pl_ex_13_user_properties_monitor"] = PipelineResource(
        activities=[
            list_incoming,
            ForEachActivity(
                name="ForEachChannelTags",
                depends_on=[
                    ActivityDependency(
                        activity="ListIncomingWithTags",
                        dependency_conditions=["Succeeded"],
                    )
                ],
                items=Expression(
                    type="Expression", value="@pipeline().parameters.channels"
                ),
                is_sequential=True,
                activities=[tag_channel],
            ),
            copy_tagged,
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "job_name": ParameterSpecification(
                type="String", default_value="finledger-bronze-load"
            ),
            "incoming_folder": ParameterSpecification(
                type="String", default_value="incoming/run=session3-lab"
            ),
            "loaded_folder": ParameterSpecification(
                type="String", default_value="loaded/run=session3-lab"
            ),
            "channels": ParameterSpecification(
                type="Array",
                default_value=["card", "wire", "fps"],
            ),
        },
        variables={"channel_audit": {"type": "Array"}},
        annotations=["expression-user-properties", "concept-monitoring"],
    )

    for name, body in pipelines.items():
        apply_expression_builder_folder(name, body)
        adf.pipelines.create_or_update(rg, factory, name, body)
        logger.info("Expression pipeline %s", name)


def deploy_expressions(cfg, estate) -> None:
    adf = _client(cfg)
    deploy_global_parameters(cfg, estate.data_factory, adf=adf)
    deploy_expression_pipelines(adf, cfg.resource_group, estate.data_factory)


def list_expression_pipeline_names() -> list[str]:
    return list(EXPRESSION_PIPELINE_NAMES)


def default_params(pipeline_name: str) -> dict[str, Any]:
    incoming = "incoming/run=session3-lab"
    if pipeline_name == "pl_ex_01_pipeline_param_variables":
        return {"run_id": "session3-lab", "lab_tag": "ex01-params"}
    if pipeline_name == "pl_ex_03_concat_string_functions":
        return {
            "folder_path": "bronze/incoming/run=session3-lab",
            "file_name": "sample_transactions.csv",
            "run_id": "session3-lab",
            "channel": "card",
        }
    if pipeline_name in (
        "pl_ex_04_metadata_output_expressions",
        "pl_ex_09_logic_and_or_not",
    ):
        params = {"incoming_folder": incoming}
        if pipeline_name == "pl_ex_09_logic_and_or_not":
            params["force_empty"] = False
        return params
    if pipeline_name == "pl_ex_06_if_equals_compare":
        return {"status": "READY"}
    if pipeline_name == "pl_ex_07_foreach_item_expressions":
        return {"channels": ["card", "wire", "fps"]}
    if pipeline_name == "pl_ex_08_switch_route_expressions":
        return {"route": "A"}
    if pipeline_name == "pl_ex_10_child_pipeline_expressions":
        return {"run_id": "session3-lab", "lab_tag": "ex10-parent"}
    if pipeline_name == "pl_ex_10_child_echo_params":
        return {"run_id": "session3-lab", "message": "child-default"}
    if pipeline_name == "pl_ex_11_debug_print_variables":
        return {"run_id": "session3-lab", "show_debug_fail": False}
    if pipeline_name == "pl_ex_12_dataset_foreach_item":
        return {
            "default_incoming": incoming,
            "folder_jobs": [
                {"incoming": incoming, "label": "main-incoming"},
                {
                    "incoming": f"{incoming}/rejected",
                    "label": "rejected-decoys",
                },
            ],
        }
    if pipeline_name == "pl_ex_13_user_properties_monitor":
        return {
            "run_id": "session3-lab",
            "job_name": "finledger-bronze-load",
            "incoming_folder": incoming,
            "loaded_folder": "loaded/run=session3-lab",
            "channels": ["card", "wire", "fps"],
        }
    return {}
