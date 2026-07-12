"""ADF folder for all expression-builder teaching pipelines (single Studio group)."""

from __future__ import annotations

from azure.mgmt.datafactory.models import PipelineFolder, PipelineResource

EXPRESSION_BUILDER_FOLDER = "11-expression-builder"
COMPLETE_INTEGRATION_FOLDER = "04-integration-databricks-complete"

# Pure expression labs (pl_ex_*)
_EXPRESSION_EX_PIPELINES = [
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

# Databricks + expression labs (pl_db_*)
_EXPRESSION_DB_PIPELINES = [
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
]

# Core / advanced pipelines that heavily teach expressions (also grouped here)
_EXPRESSION_CORE_PIPELINES = [
    "pl_03_if_then_copy",
    "pl_04_foreach_dates",
    "pl_05_lookup_watermark",
    "pl_11_filename_validate_route",
    "pl_17_switch_channel",
    "pl_18_until_file_ready",
    "pl_19_append_audit_log",
    "pl_21_fail_on_bad_param",
    "pl_23_incremental_watermark_copy",
]

# Complete ADF + Databricks integration (notebook + Job preview + infra)
_COMPLETE_INTEGRATION_PIPELINES = [
    "pl_07_databricks_notebook",
    "pl_10_parent_chain",
    "pl_db_11_databricks_job_preview",
    "pl_db_12_end_to_end_integration",
    "pl_db_13_copy_notebook_job_chain",
]

EXPRESSION_BUILDER_PIPELINE_NAMES = (
    _EXPRESSION_EX_PIPELINES + _EXPRESSION_DB_PIPELINES + _EXPRESSION_CORE_PIPELINES
)

EXPRESSION_BUILDER_FOLDERS: dict[str, str] = dict.fromkeys(
    EXPRESSION_BUILDER_PIPELINE_NAMES,
    EXPRESSION_BUILDER_FOLDER,
)

COMPLETE_INTEGRATION_FOLDERS: dict[str, str] = dict.fromkeys(
    _COMPLETE_INTEGRATION_PIPELINES,
    COMPLETE_INTEGRATION_FOLDER,
)


def apply_expression_builder_folder(name: str, body: PipelineResource) -> PipelineResource:
    """Assign pipeline to teaching folder (complete integration wins over expression builder)."""
    complete = COMPLETE_INTEGRATION_FOLDERS.get(name)
    if complete:
        body.folder = PipelineFolder(name=complete)
        return body
    folder = EXPRESSION_BUILDER_FOLDERS.get(name)
    if folder:
        body.folder = PipelineFolder(name=folder)
    return body
