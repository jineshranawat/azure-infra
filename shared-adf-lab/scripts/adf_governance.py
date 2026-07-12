"""Governance + Purview teaching — medallion bronze/silver/gold and catalog metadata."""

from __future__ import annotations

import json
import logging
from typing import Any

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    AppendVariableActivity,
    AzureBlobFSLocation,
    AzureBlobFSReadSettings,
    AzureBlobFSWriteSettings,
    CopyActivity,
    DatasetResource,
    DelimitedTextDataset,
    DelimitedTextSink,
    DelimitedTextSource,
    ExecutePipelineActivity,
    Expression,
    GetMetadataActivity,
    JsonDataset,
    JsonSource,
    LookupActivity,
    ParameterSpecification,
    PipelineFolder,
    PipelineReference,
    PipelineResource,
    SetVariableActivity,
    UserProperty,
)

from adf_advanced import _execute_df_activity
from adf_pipelines import LS_ADLS, _client, _ds_ref, _ls_ref
from discover import SharedEstate
from purview_link import link_purview_to_adf

logger = logging.getLogger(__name__)

GOVERNANCE_FOLDER = "13-governance-purview"

INCOMING = "incoming/run=session3-lab"
LOADED = "loaded/run=session3-lab"
SILVER_CLEANED = "cleaned/run=session3-lab"
GOLD_AGG = "aggregates/run=session3-lab"

GOVERNANCE_PIPELINE_NAMES = [
    "pl_gov_01_medallion_bronze_land",
    "pl_gov_02_medallion_silver_cleanse",
    "pl_gov_03_medallion_gold_aggregate",
    "pl_gov_04_governance_catalog_metadata",
    "pl_gov_05_purview_discovery_lineage",
    "pl_gov_06_master_medallion_governance",
]

CATALOG_TEMPLATE: dict[str, Any] = {
    "catalog": "finledger-governance",
    "purview_concepts": [
        "data_map",
        "data_assets",
        "lineage",
        "glossary",
        "classifications",
        "ownership",
        "scan",
    ],
    "glossary_terms": [
        {
            "name": "FinLedger.Transaction",
            "definition": "Retail payment transaction ingested from partner files.",
        },
        {
            "name": "Medallion.Bronze",
            "definition": "Raw immutable landing layer — append-only CSV in ADLS bronze.",
        },
        {
            "name": "Medallion.Silver",
            "definition": "Cleansed conforming layer — typed columns, rejected rows removed.",
        },
        {
            "name": "Medallion.Gold",
            "definition": "Business aggregates for reporting and regulatory KPIs.",
        },
    ],
    "classifications": [
        {"name": "FinLedger.Internal", "applies_to": "non-PII operational data"},
        {"name": "MICROSOFT.PERSONAL.NAME", "applies_to": "customer_name column if present"},
    ],
    "data_assets": [
        {
            "qualified_name": "abfss://bronze@stshared.dfs.core.windows.net/incoming/run=session3-lab",
            "layer": "bronze",
            "type": "azure_datalake_gen2_path",
        },
        {
            "qualified_name": "abfss://silver@stshared.dfs.core.windows.net/cleaned/run=session3-lab",
            "layer": "silver",
            "type": "azure_datalake_gen2_path",
        },
        {
            "qualified_name": "abfss://silver@stshared.dfs.core.windows.net/aggregates/run=session3-lab",
            "layer": "gold",
            "type": "azure_datalake_gen2_path",
            "note": "Logical gold layer — physically in silver container for shared lab cost",
        },
    ],
    "lineage_edges": [
        {"from_layer": "bronze", "to_layer": "silver", "pipeline": "pl_gov_02_medallion_silver_cleanse"},
        {"from_layer": "silver", "to_layer": "gold", "pipeline": "pl_gov_03_medallion_gold_aggregate"},
    ],
}


def _gov_folder(body: PipelineResource) -> PipelineResource:
    body.folder = PipelineFolder(name=GOVERNANCE_FOLDER)
    return body


def _gov_prop(name: str, expression: str) -> UserProperty:
    return UserProperty(name=name, value={"value": expression, "type": "Expression"})


def _upload_governance_catalog(cfg, estate: SharedEstate) -> None:
    from _config import get_datalake_credential

    body = json.dumps(CATALOG_TEMPLATE, indent=2)
    body = body.replace("stshared", estate.storage_account)
    try:
        from azure.storage.filedatalake import DataLakeServiceClient

        client = DataLakeServiceClient(
            account_url=f"https://{estate.storage_account}.dfs.core.windows.net",
            credential=get_datalake_credential(estate.storage_account, cfg.subscription_id),
        )
        fs = client.get_file_system_client("audit")
        path = "governance/purview_catalog_template.json"
        file_client = fs.get_file_client(path)
        try:
            file_client.create_file()
        except Exception:
            pass
        file_client.upload_data(body.encode("utf-8"), overwrite=True)
        logger.info("Governance catalog template at audit/%s", path)
    except Exception as exc:
        logger.warning("Governance catalog upload skipped: %s", exc)


def _deploy_governance_datasets(adf: DataFactoryManagementClient, rg: str, factory: str) -> None:
    ls = _ls_ref(LS_ADLS)
    adf.datasets.create_or_update(
        rg,
        factory,
        "ds_audit_governance_catalog_json",
        DatasetResource(
            properties=JsonDataset(
                linked_service_name=ls,
                location=AzureBlobFSLocation(
                    file_system="audit",
                    file_name="governance/purview_catalog_template.json",
                ),
            )
        ),
    )
    for name, container, folder, fname in (
        ("ds_gov_bronze_incoming_csv", "bronze", INCOMING, "sample_transactions.csv"),
        ("ds_gov_silver_cleaned_csv", "silver", SILVER_CLEANED, "cleaned_transactions.csv"),
        ("ds_gov_gold_aggregate_csv", "silver", GOLD_AGG, "channel_totals.csv"),
    ):
        adf.datasets.create_or_update(
            rg,
            factory,
            name,
            DatasetResource(
                properties=DelimitedTextDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system=container,
                        folder_path=folder,
                        file_name=fname,
                    ),
                    column_delimiter=",",
                    first_row_as_header=True,
                )
            ),
        )
    for name, container, folder in (
        ("ds_gov_silver_folder", "silver", SILVER_CLEANED),
        ("ds_gov_gold_folder", "silver", GOLD_AGG),
    ):
        adf.datasets.create_or_update(
            rg,
            factory,
            name,
            DatasetResource(
                properties=JsonDataset(
                    linked_service_name=ls,
                    location=AzureBlobFSLocation(
                        file_system=container,
                        folder_path=folder,
                    ),
                )
            ),
        )
    logger.info("Governance datasets deployed")


def _deploy_governance_pipelines(
    adf: DataFactoryManagementClient,
    rg: str,
    factory: str,
) -> None:
    bronze_copy = CopyActivity(
        name="LandBronzeCanonical",
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_csv",
                folder_path={"value": "@pipeline().parameters.incoming_folder", "type": "Expression"},
            )
        ],
        outputs=[
            _ds_ref(
                "ds_bronze_loaded_csv",
                folder_path={"value": "@pipeline().parameters.loaded_folder", "type": "Expression"},
            )
        ],
        source=DelimitedTextSource(store_settings=AzureBlobFSReadSettings(recursive=True)),
        sink=DelimitedTextSink(store_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")),
        user_properties=[
            _gov_prop("purview_concept", "@string('data_asset')"),
            _gov_prop("medallion_layer", "@string('bronze')"),
            _gov_prop("glossary_term", "@string('Medallion.Bronze')"),
            _gov_prop("classification", "@string('FinLedger.Internal')"),
            _gov_prop("run_id", "@pipeline().parameters.run_id"),
        ],
    )
    pipelines: dict[str, PipelineResource] = {
        "pl_gov_01_medallion_bronze_land": PipelineResource(
            activities=[bronze_copy],
            parameters={
                "incoming_folder": ParameterSpecification(type="String", default_value=INCOMING),
                "loaded_folder": ParameterSpecification(type="String", default_value=LOADED),
                "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            },
            annotations=["governance-medallion-bronze", "purview-lineage"],
        ),
    }

    silver_df = _execute_df_activity("RunSilverCleanse", "df_01_bronze_cleanse")
    silver_df.user_properties = [
        _gov_prop("purview_concept", "@string('lineage')"),
        _gov_prop("medallion_layer", "@string('silver')"),
        _gov_prop("glossary_term", "@string('Medallion.Silver')"),
        _gov_prop("upstream", "@string('bronze/incoming')"),
        _gov_prop("downstream", "@string('silver/cleaned')"),
    ]
    pipelines["pl_gov_02_medallion_silver_cleanse"] = PipelineResource(
        activities=[silver_df],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        annotations=["governance-medallion-silver", "purview-lineage", "dataflow"],
    )

    gold_df = _execute_df_activity("RunGoldAggregate", "df_02_channel_aggregate")
    gold_df.user_properties = [
        _gov_prop("purview_concept", "@string('lineage')"),
        _gov_prop("medallion_layer", "@string('gold')"),
        _gov_prop("glossary_term", "@string('Medallion.Gold')"),
        _gov_prop("upstream", "@string('silver/cleaned')"),
        _gov_prop("downstream", "@string('gold/aggregates')"),
    ]
    pipelines["pl_gov_03_medallion_gold_aggregate"] = PipelineResource(
        activities=[gold_df],
        annotations=["governance-medallion-gold", "purview-lineage", "dataflow"],
    )

    read_catalog = LookupActivity(
        name="ReadGovernanceCatalog",
        dataset=_ds_ref("ds_audit_governance_catalog_json"),
        source=JsonSource(),
        first_row_only=True,
    )
    set_catalog = SetVariableActivity(
        name="CaptureCatalogSummary",
        depends_on=[
            ActivityDependency(activity="ReadGovernanceCatalog", dependency_conditions=["Succeeded"])
        ],
        variable_name="catalog_summary",
        value=Expression(
            type="Expression",
            value=(
                "@concat("
                "'governance | catalog=', activity('ReadGovernanceCatalog').output.firstRow.catalog, "
                "' | concepts=', string(length(activity('ReadGovernanceCatalog').output.firstRow.purview_concepts)), "
                "' | terms=', string(length(activity('ReadGovernanceCatalog').output.firstRow.glossary_terms)), "
                "' | run=', pipeline().parameters.run_id"
                ")"
            ),
        ),
    )
    append_term = AppendVariableActivity(
        name="AppendGlossaryAudit",
        depends_on=[
            ActivityDependency(activity="CaptureCatalogSummary", dependency_conditions=["Succeeded"])
        ],
        variable_name="governance_audit",
        value=Expression(type="Expression", value="@variables('catalog_summary')"),
    )
    pipelines["pl_gov_04_governance_catalog_metadata"] = PipelineResource(
        activities=[read_catalog, set_catalog, append_term],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        variables={
            "catalog_summary": {"type": "String"},
            "governance_audit": {"type": "Array"},
        },
        annotations=["purview-glossary", "purview-classification", "purview-catalog"],
    )

    meta_bronze = GetMetadataActivity(
        name="DiscoverBronzeContainer",
        dataset=_ds_ref(
            "ds_bronze_incoming_folder",
            folder_path={"value": "@pipeline().parameters.incoming_folder", "type": "Expression"},
        ),
        field_list=["childItems"],
        store_settings=AzureBlobFSReadSettings(recursive=True),
        user_properties=[
            _gov_prop("purview_concept", "@string('scan_discovery')"),
            _gov_prop("medallion_layer", "@string('bronze')"),
        ],
    )
    meta_silver = GetMetadataActivity(
        name="DiscoverSilverFolder",
        depends_on=[
            ActivityDependency(activity="DiscoverBronzeContainer", dependency_conditions=["Succeeded"])
        ],
        dataset=_ds_ref("ds_gov_silver_folder"),
        field_list=["childItems"],
        store_settings=AzureBlobFSReadSettings(recursive=True),
        user_properties=[
            _gov_prop("purview_concept", "@string('scan_discovery')"),
            _gov_prop("medallion_layer", "@string('silver')"),
        ],
    )
    meta_gold = GetMetadataActivity(
        name="DiscoverGoldFolder",
        depends_on=[
            ActivityDependency(activity="DiscoverSilverFolder", dependency_conditions=["Succeeded"])
        ],
        dataset=_ds_ref("ds_gov_gold_folder"),
        field_list=["childItems"],
        store_settings=AzureBlobFSReadSettings(recursive=True),
        user_properties=[
            _gov_prop("purview_concept", "@string('scan_discovery')"),
            _gov_prop("medallion_layer", "@string('gold')"),
        ],
    )
    set_discovery = SetVariableActivity(
        name="SummarizeDiscovery",
        depends_on=[
            ActivityDependency(activity="DiscoverGoldFolder", dependency_conditions=["Succeeded"])
        ],
        variable_name="discovery_summary",
        value=Expression(
            type="Expression",
            value=(
                "@concat("
                "'purview-discovery | bronze_files=', string(length(activity('DiscoverBronzeContainer').output.childItems)), "
                "' | silver_files=', string(length(activity('DiscoverSilverFolder').output.childItems)), "
                "' | gold_files=', string(length(activity('DiscoverGoldFolder').output.childItems))"
                ")"
            ),
        ),
    )
    lineage_copy = CopyActivity(
        name="PublishLineageBronzeCopy",
        depends_on=[
            ActivityDependency(activity="SummarizeDiscovery", dependency_conditions=["Succeeded"])
        ],
        inputs=[
            _ds_ref(
                "ds_bronze_incoming_csv",
                folder_path={"value": "@pipeline().parameters.incoming_folder", "type": "Expression"},
            )
        ],
        outputs=[
            _ds_ref(
                "ds_bronze_loaded_csv",
                folder_path={"value": "@pipeline().parameters.loaded_folder", "type": "Expression"},
            )
        ],
        source=DelimitedTextSource(store_settings=AzureBlobFSReadSettings(recursive=True)),
        sink=DelimitedTextSink(store_settings=AzureBlobFSWriteSettings(copy_behavior="MergeFiles")),
        user_properties=[
            _gov_prop("purview_concept", "@string('lineage')"),
            _gov_prop("medallion_layer", "@string('bronze')"),
            _gov_prop("lineage_edge", "@string('bronze_incoming_to_bronze_loaded')"),
        ],
    )
    pipelines["pl_gov_05_purview_discovery_lineage"] = PipelineResource(
        activities=[meta_bronze, meta_silver, meta_gold, set_discovery, lineage_copy],
        parameters={
            "incoming_folder": ParameterSpecification(type="String", default_value=INCOMING),
            "loaded_folder": ParameterSpecification(type="String", default_value=LOADED),
        },
        variables={"discovery_summary": {"type": "String"}},
        annotations=["purview-discovery", "purview-lineage", "purview-scan"],
    )

    exec_bronze = ExecutePipelineActivity(
        name="RunBronzeLand",
        pipeline=PipelineReference(
            reference_name="pl_gov_01_medallion_bronze_land",
            type="PipelineReference",
        ),
        parameters={
            "run_id": {"value": "@pipeline().parameters.run_id", "type": "Expression"},
            "incoming_folder": {"value": "@pipeline().parameters.incoming_folder", "type": "Expression"},
            "loaded_folder": {"value": "@pipeline().parameters.loaded_folder", "type": "Expression"},
        },
        wait_on_completion=True,
    )
    exec_silver = ExecutePipelineActivity(
        name="RunSilverCleanse",
        depends_on=[ActivityDependency(activity="RunBronzeLand", dependency_conditions=["Succeeded"])],
        pipeline=PipelineReference(
            reference_name="pl_gov_02_medallion_silver_cleanse",
            type="PipelineReference",
        ),
        parameters={
            "run_id": {"value": "@pipeline().parameters.run_id", "type": "Expression"},
        },
        wait_on_completion=True,
    )
    exec_gold = ExecutePipelineActivity(
        name="RunGoldAggregate",
        depends_on=[ActivityDependency(activity="RunSilverCleanse", dependency_conditions=["Succeeded"])],
        pipeline=PipelineReference(
            reference_name="pl_gov_03_medallion_gold_aggregate",
            type="PipelineReference",
        ),
        wait_on_completion=True,
    )
    exec_catalog = ExecutePipelineActivity(
        name="RunCatalogMetadata",
        depends_on=[ActivityDependency(activity="RunGoldAggregate", dependency_conditions=["Succeeded"])],
        pipeline=PipelineReference(
            reference_name="pl_gov_04_governance_catalog_metadata",
            type="PipelineReference",
        ),
        parameters={
            "run_id": {"value": "@pipeline().parameters.run_id", "type": "Expression"},
        },
        wait_on_completion=True,
    )
    exec_discovery = ExecutePipelineActivity(
        name="RunPurviewDiscovery",
        depends_on=[ActivityDependency(activity="RunCatalogMetadata", dependency_conditions=["Succeeded"])],
        pipeline=PipelineReference(
            reference_name="pl_gov_05_purview_discovery_lineage",
            type="PipelineReference",
        ),
        parameters={
            "incoming_folder": {"value": "@pipeline().parameters.incoming_folder", "type": "Expression"},
            "loaded_folder": {"value": "@pipeline().parameters.loaded_folder", "type": "Expression"},
        },
        wait_on_completion=True,
    )
    pipelines["pl_gov_06_master_medallion_governance"] = PipelineResource(
        activities=[exec_bronze, exec_silver, exec_gold, exec_catalog, exec_discovery],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "incoming_folder": ParameterSpecification(type="String", default_value=INCOMING),
            "loaded_folder": ParameterSpecification(type="String", default_value=LOADED),
        },
        annotations=["governance-master", "medallion-end-to-end", "purview"],
    )

    for name, body in pipelines.items():
        _gov_folder(body)
        adf.pipelines.create_or_update(rg, factory, name, body)
        logger.info("Governance pipeline %s", name)


def deploy_governance(cfg, estate: SharedEstate, *, link_purview: bool = True) -> None:
    """Deploy governance datasets, catalog template, pipelines; optionally link Purview."""
    adf = _client(cfg)
    rg, factory = cfg.resource_group, estate.data_factory
    _upload_governance_catalog(cfg, estate)
    _deploy_governance_datasets(adf, rg, factory)
    _deploy_governance_pipelines(adf, rg, factory)
    if link_purview:
        try:
            link_purview_to_adf(cfg, factory, adf=adf)
        except Exception as exc:
            logger.warning("Purview ADF link skipped: %s", exc)


def list_governance_pipeline_names() -> list[str]:
    return list(GOVERNANCE_PIPELINE_NAMES)


def default_params(pipeline_name: str) -> dict[str, Any]:
    base = {
        "run_id": "session3-lab",
        "incoming_folder": INCOMING,
        "loaded_folder": LOADED,
    }
    if pipeline_name in GOVERNANCE_PIPELINE_NAMES:
        if pipeline_name in ("pl_gov_02_medallion_silver_cleanse", "pl_gov_03_medallion_gold_aggregate"):
            return {"run_id": "session3-lab"}
        if pipeline_name == "pl_gov_04_governance_catalog_metadata":
            return {"run_id": "session3-lab"}
        if pipeline_name == "pl_gov_05_purview_discovery_lineage":
            return {"incoming_folder": INCOMING, "loaded_folder": LOADED}
        return base
    return {}
