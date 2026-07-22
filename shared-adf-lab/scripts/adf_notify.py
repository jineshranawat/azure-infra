"""Enterprise notify pipelines — Jira ticket + attach + email (folder 15-enterprise-notify)."""

from __future__ import annotations

import logging
from typing import Any

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    ActivityPolicy,
    ExecutePipelineActivity,
    Expression,
    FailActivity,
    IfConditionActivity,
    ParameterSpecification,
    PipelineFolder,
    PipelineReference,
    PipelineResource,
    SetVariableActivity,
    WebActivity,
    WebActivityMethod,
)

from adf_databricks import _notebook
from adf_pipelines import _client

logger = logging.getLogger(__name__)

NOTIFY_FOLDER = "15-enterprise-notify"
NOTEBOOK_INCIDENT = "/Shared/shared-adf/nb_incident_jira_email"

NOTIFY_PIPELINE_NAMES = [
    "pl_ntf_01_simulate_work_or_fail",
    "pl_ntf_02_web_create_jira",
    "pl_ntf_03_web_send_email",
    "pl_ntf_04_databricks_incident",
    "pl_ntf_05_master_incident",
]


def _folder(body: PipelineResource) -> PipelineResource:
    body.folder = PipelineFolder(name=NOTIFY_FOLDER)
    return body


def _deploy_notify_pipelines(adf: DataFactoryManagementClient, rg: str, factory: str) -> None:
    pipelines: dict[str, PipelineResource] = {}

    fail_act = FailActivity(
        name="FailForcedIncident",
        message="PL_NTF-DEMO-FAIL — intentional enterprise failure; raise Jira + email next",
        error_code="PL_NTF-DEMO-FAIL",
    )
    ok_set = SetVariableActivity(
        name="MarkWorkOk",
        variable_name="work_status",
        value=Expression(type="Expression", value="@string('ok')"),
    )
    pipelines["pl_ntf_01_simulate_work_or_fail"] = PipelineResource(
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
            "force_fail": ParameterSpecification(type="Bool", default_value=True),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
        },
        variables={"work_status": {"type": "String"}},
        annotations=["enterprise-notify", "fail-path"],
    )

    # Azure ADF cannot reach localhost — set jira_base_url to a reachable host, or use pl_ntf_04 + local cmd.
    pipelines["pl_ntf_02_web_create_jira"] = PipelineResource(
        activities=[
            WebActivity(
                name="WebCreateJiraIssue",
                method=WebActivityMethod.POST,
                url={
                    "value": "@concat(pipeline().parameters.jira_base_url, '/rest/api/2/issue')",
                    "type": "Expression",
                },
                body={
                    "value": (
                        "@json(concat("
                        "'{\"fields\":{\"project\":{\"key\":\"FIN\"},\"summary\":\"[ADF] ',"
                        "pipeline().parameters.pipeline_name,"
                        "' failed — run_id=', pipeline().parameters.run_id,"
                        "',\"description\":\"Automated FinLedger incident. Run ID: ',"
                        "pipeline().parameters.run_id,"
                        "'\",\"issuetype\":{\"name\":\"Bug\"}}}')"
                        ")"
                    ),
                    "type": "Expression",
                },
                headers={"Content-Type": "application/json"},
                policy=ActivityPolicy(timeout="0.00:05:00", retry=1),
            )
        ],
        parameters={
            "jira_base_url": ParameterSpecification(
                type="String", default_value="http://127.0.0.1:18080"
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "pipeline_name": ParameterSpecification(
                type="String", default_value="pl_ntf_05_master_incident"
            ),
        },
        annotations=["enterprise-notify", "web-jira"],
    )

    pipelines["pl_ntf_03_web_send_email"] = PipelineResource(
        activities=[
            WebActivity(
                name="WebSendEmailNotify",
                method=WebActivityMethod.POST,
                url={
                    "value": (
                        "@if(endswith(pipeline().parameters.notify_mail_url, '/notify'),"
                        " pipeline().parameters.notify_mail_url,"
                        " if(contains(pipeline().parameters.notify_mail_url, 'logic.azure.com'),"
                        "  pipeline().parameters.notify_mail_url,"
                        "  concat(pipeline().parameters.notify_mail_url, '/notify')))"
                    ),
                    "type": "Expression",
                },
                body={
                    "value": (
                        "@json(concat("
                        "'{\"to\":\"', pipeline().parameters.owner_email,"
                        "'\",\"subject\":\"[FinLedger] ADF incident — ',"
                        "pipeline().parameters.run_id,"
                        "'\",\"body\":\"Pipeline ', pipeline().parameters.pipeline_name,"
                        "' failed. Run ID: ', pipeline().parameters.run_id,"
                        "'.\",\"run_id\":\"', pipeline().parameters.run_id, '\"}')"
                        ")"
                    ),
                    "type": "Expression",
                },
                headers={"Content-Type": "application/json"},
                policy=ActivityPolicy(timeout="0.00:05:00", retry=1),
            )
        ],
        parameters={
            "notify_mail_url": ParameterSpecification(
                type="String", default_value="http://127.0.0.1:18081"
            ),
            "owner_email": ParameterSpecification(
                type="String", default_value="training@example.com"
            ),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "pipeline_name": ParameterSpecification(
                type="String", default_value="pl_ntf_05_master_incident"
            ),
        },
        annotations=["enterprise-notify", "web-email"],
    )

    pipelines["pl_ntf_04_databricks_incident"] = PipelineResource(
        activities=[
            _notebook(
                "RaiseJiraAndEmail",
                notebook_path=NOTEBOOK_INCIDENT,
                base_parameters={
                    "run_id": "@pipeline().parameters.run_id",
                    "pipeline_name": "@pipeline().parameters.pipeline_name",
                    "jira_base_url": "@pipeline().parameters.jira_base_url",
                    "notify_mail_url": "@pipeline().parameters.notify_mail_url",
                    "owner_email": "@pipeline().parameters.owner_email",
                    "force_fail": "@string(pipeline().parameters.force_fail)",
                    "jira_user": "@pipeline().parameters.jira_user",
                    "jira_api_token": "@pipeline().parameters.jira_api_token",
                },
                timeout="0.00:20:00",
            )
        ],
        parameters={
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "pipeline_name": ParameterSpecification(
                type="String", default_value="pl_ntf_05_master_incident"
            ),
            "jira_base_url": ParameterSpecification(
                type="String", default_value="http://127.0.0.1:18080"
            ),
            "notify_mail_url": ParameterSpecification(
                type="String", default_value="http://127.0.0.1:18081"
            ),
            "owner_email": ParameterSpecification(
                type="String", default_value="training@example.com"
            ),
            "force_fail": ParameterSpecification(type="Bool", default_value=True),
            "jira_user": ParameterSpecification(type="String", default_value=""),
            "jira_api_token": ParameterSpecification(type="String", default_value=""),
        },
        annotations=["enterprise-notify", "databricks-incident"],
    )

    exec_work = ExecutePipelineActivity(
        name="RunSimulateWork",
        pipeline=PipelineReference(
            reference_name="pl_ntf_01_simulate_work_or_fail",
            type="PipelineReference",
        ),
        parameters={
            "force_fail": {
                "value": "@pipeline().parameters.force_fail",
                "type": "Expression",
            },
            "run_id": {
                "value": "@pipeline().parameters.run_id",
                "type": "Expression",
            },
        },
        wait_on_completion=True,
    )
    exec_incident = ExecutePipelineActivity(
        name="RaiseIncidentOnFailure",
        depends_on=[
            ActivityDependency(
                activity="RunSimulateWork",
                dependency_conditions=["Failed"],
            )
        ],
        pipeline=PipelineReference(
            reference_name="pl_ntf_04_databricks_incident",
            type="PipelineReference",
        ),
        parameters={
            "run_id": {"value": "@pipeline().parameters.run_id", "type": "Expression"},
            "pipeline_name": {
                "value": "@pipeline().parameters.pipeline_name",
                "type": "Expression",
            },
            "jira_base_url": {
                "value": "@pipeline().parameters.jira_base_url",
                "type": "Expression",
            },
            "notify_mail_url": {
                "value": "@pipeline().parameters.notify_mail_url",
                "type": "Expression",
            },
            "owner_email": {
                "value": "@pipeline().parameters.owner_email",
                "type": "Expression",
            },
            "force_fail": {
                "value": "@pipeline().parameters.force_fail",
                "type": "Expression",
            },
            "jira_user": {
                "value": "@pipeline().parameters.jira_user",
                "type": "Expression",
            },
            "jira_api_token": {
                "value": "@pipeline().parameters.jira_api_token",
                "type": "Expression",
            },
        },
        wait_on_completion=True,
    )

    web_create = ExecutePipelineActivity(
        name="WebCreateJiraChild",
        pipeline=PipelineReference(
            reference_name="pl_ntf_02_web_create_jira",
            type="PipelineReference",
        ),
        parameters={
            "jira_base_url": {
                "value": "@pipeline().parameters.jira_base_url",
                "type": "Expression",
            },
            "run_id": {"value": "@pipeline().parameters.run_id", "type": "Expression"},
            "pipeline_name": {
                "value": "@pipeline().parameters.pipeline_name",
                "type": "Expression",
            },
        },
        wait_on_completion=True,
    )
    web_mail = ExecutePipelineActivity(
        name="WebSendEmailChild",
        depends_on=[
            ActivityDependency(
                activity="WebCreateJiraChild",
                dependency_conditions=["Succeeded", "Failed"],
            )
        ],
        pipeline=PipelineReference(
            reference_name="pl_ntf_03_web_send_email",
            type="PipelineReference",
        ),
        parameters={
            "notify_mail_url": {
                "value": "@pipeline().parameters.notify_mail_url",
                "type": "Expression",
            },
            "owner_email": {
                "value": "@pipeline().parameters.owner_email",
                "type": "Expression",
            },
            "run_id": {"value": "@pipeline().parameters.run_id", "type": "Expression"},
            "pipeline_name": {
                "value": "@pipeline().parameters.pipeline_name",
                "type": "Expression",
            },
        },
        wait_on_completion=True,
    )
    if_web = IfConditionActivity(
        name="IfUseWebActivities",
        depends_on=[
            ActivityDependency(
                activity="RaiseIncidentOnFailure",
                dependency_conditions=["Succeeded", "Failed", "Skipped"],
            )
        ],
        expression=Expression(
            type="Expression",
            value="@equals(pipeline().parameters.use_web_activities, true)",
        ),
        if_true_activities=[web_create, web_mail],
        if_false_activities=[
            SetVariableActivity(
                name="SkipWebActivities",
                variable_name="notify_note",
                value=Expression(
                    type="Expression",
                    value="@string('web skipped — run enterprise-notify.cmd for local Jira/mail UI')",
                ),
            )
        ],
    )

    pipelines["pl_ntf_05_master_incident"] = PipelineResource(
        activities=[exec_work, exec_incident, if_web],
        parameters={
            "force_fail": ParameterSpecification(type="Bool", default_value=True),
            "run_id": ParameterSpecification(type="String", default_value="session3-lab"),
            "pipeline_name": ParameterSpecification(
                type="String", default_value="pl_ntf_05_master_incident"
            ),
            "jira_base_url": ParameterSpecification(
                type="String", default_value="http://127.0.0.1:18080"
            ),
            "notify_mail_url": ParameterSpecification(
                type="String", default_value="http://127.0.0.1:18081"
            ),
            "owner_email": ParameterSpecification(
                type="String", default_value="training@example.com"
            ),
            "jira_user": ParameterSpecification(type="String", default_value=""),
            "jira_api_token": ParameterSpecification(type="String", default_value=""),
            "use_web_activities": ParameterSpecification(type="Bool", default_value=False),
        },
        variables={"notify_note": {"type": "String"}},
        annotations=["enterprise-notify", "master-incident"],
    )

    for name, body in pipelines.items():
        _folder(body)
        adf.pipelines.create_or_update(rg, factory, name, body)
        logger.info("Notify pipeline %s", name)


def deploy_notify(cfg, estate) -> None:
    """Deploy enterprise notify folder pipelines (idempotent)."""
    adf = _client(cfg)
    _deploy_notify_pipelines(adf, cfg.resource_group, estate.data_factory)


def list_notify_pipeline_names() -> list[str]:
    return list(NOTIFY_PIPELINE_NAMES)


def default_params(pipeline_name: str) -> dict[str, Any]:
    if pipeline_name not in NOTIFY_PIPELINE_NAMES:
        return {}
    base: dict[str, Any] = {
        "run_id": "session3-lab",
        "force_fail": True,
        "pipeline_name": "pl_ntf_05_master_incident",
        "jira_base_url": "http://127.0.0.1:18080",
        "notify_mail_url": "http://127.0.0.1:18081",
        "owner_email": "training@example.com",
        "use_web_activities": False,
        "jira_user": "",
        "jira_api_token": "",
    }
    if pipeline_name == "pl_ntf_01_simulate_work_or_fail":
        return {"force_fail": True, "run_id": "session3-lab"}
    if pipeline_name == "pl_ntf_02_web_create_jira":
        return {
            "jira_base_url": base["jira_base_url"],
            "run_id": base["run_id"],
            "pipeline_name": base["pipeline_name"],
        }
    if pipeline_name == "pl_ntf_03_web_send_email":
        return {
            "notify_mail_url": base["notify_mail_url"],
            "owner_email": base["owner_email"],
            "run_id": base["run_id"],
            "pipeline_name": base["pipeline_name"],
        }
    return base
