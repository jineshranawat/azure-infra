#!/usr/bin/env python3
"""Shared ADF lab — deploy SQL (westus), linked services, 10 teaching pipelines.

Usage (from shared-adf-lab/orchestrate.cmd):
    python scripts/run_shared_adf.py
    python scripts/run_shared_adf.py --skip-sql
    python scripts/run_shared_adf.py --run-pipeline pl_01_bronze_copy
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from _config import get_datalake_credential, load_config  # noqa: E402
from adf_advanced import default_params as advanced_default_params, deploy_advanced, list_advanced_pipeline_names
from adf_databricks import (  # noqa: E402
    default_params as databricks_default_params,
    deploy_databricks,
    list_databricks_pipeline_names,
)
from adf_expressions import (  # noqa: E402
    default_params as expression_default_params,
    deploy_expressions,
    list_expression_pipeline_names,
)
from adf_triggers import (  # noqa: E402
    default_params as trigger_default_params,
    deploy_triggers,
    list_trigger_names,
    list_trigger_pipeline_names,
)
from adf_pipelines import (  # noqa: E402
    _sql_pipelines_enabled,
    deploy_all_pipelines,
    deploy_linked_services,
    list_pipeline_names,
    trigger_pipeline,
    wait_for_pipeline_run,
    _client,
)
from adf_rbac import ensure_adf_rbac  # noqa: E402
from azure_sql import (  # noqa: E402
    deploy_sql,
    seed_change_tracking,
    seed_job_metadata_table,
    seed_reference_table,
)
from discover import discover_estate  # noqa: E402

logger = logging.getLogger(__name__)
INCOMING = "incoming/run=session3-lab"
LOADED = "loaded/run=session3-lab"
SAMPLE_CSV = Path(__file__).resolve().parent.parent.parent / "day8" / "data" / "sample_transactions.csv"
RETURNS_CSV = Path(__file__).resolve().parent.parent / "data" / "returns_raw.csv"


def _upload_bronze_samples(cfg, estate) -> None:
    """Upload sample CSV to incoming + loaded so copy pipelines have source data."""
    from azure.core.exceptions import HttpResponseError
    from azure.storage.filedatalake import DataLakeServiceClient

    if not SAMPLE_CSV.is_file():
        logger.warning("Sample CSV not found at %s — skip bronze upload", SAMPLE_CSV)
        return
    data = SAMPLE_CSV.read_bytes()
    credential = get_datalake_credential(estate.storage_account, cfg.subscription_id)
    if isinstance(credential, str):
        logger.info("ADLS upload using STORAGE_ACCOUNT_KEY")
    else:
        logger.info("ADLS upload using az login (set STORAGE_ACCOUNT_KEY in .env if 403)")
    try:
        client = DataLakeServiceClient(
            account_url=f"https://{estate.storage_account}.dfs.core.windows.net",
            credential=credential,
        )
    except Exception as exc:
        logger.warning("Bronze upload skipped — could not connect to ADLS: %s", exc)
        return
    try:
        fs = client.get_file_system_client("bronze")
        for folder in (INCOMING, LOADED):
            path = f"{folder}/sample_transactions.csv"
            file_client = fs.get_file_client(path)
            try:
                file_client.create_file()
            except Exception:
                pass
            file_client.upload_data(data, overwrite=True)
        # Decoy files in incoming root — pl_11 rejects; copies also under rejected/ for pl_15 delete
        for decoy_name, decoy_body in (
            ("wrong_file_name.csv", b"transaction_id,amount\n1,9.99\n"),
            ("readme.txt", b"not a csv - should be ignored by pl_11\n"),
        ):
            for sub in ("", "rejected"):
                suffix = f"/{sub}" if sub else ""
                path = f"{INCOMING}{suffix}/{decoy_name}"
                file_client = fs.get_file_client(path)
                try:
                    file_client.create_file()
                except Exception:
                    pass
                file_client.upload_data(decoy_body, overwrite=True)
        if RETURNS_CSV.is_file():
            returns_data = RETURNS_CSV.read_bytes()
            returns_path = "incoming/returns/returns_raw.csv"
            file_client = fs.get_file_client(returns_path)
            try:
                file_client.create_file()
            except Exception:
                pass
            file_client.upload_data(returns_data, overwrite=True)
            logger.info("Returns sample at bronze/%s", returns_path)
        else:
            logger.warning("Returns CSV not found at %s — skip returns upload", RETURNS_CSV)
        logger.info("Bronze sample at incoming + loaded (run=session3-lab)")
    except HttpResponseError as exc:
        logger.warning(
            "Bronze upload failed (%s) — pipelines will still deploy. "
            "Set STORAGE_ACCOUNT_KEY in .env or grant Storage Blob Data Contributor.",
            exc.message if hasattr(exc, "message") else exc,
        )


def _upload_control_files(cfg, estate) -> None:
    """Seed watermark.json on ADLS audit container (idempotent overwrite)."""
    from azure.core.exceptions import HttpResponseError
    from azure.storage.filedatalake import DataLakeServiceClient

    account = estate.storage_account
    watermark = json.dumps(
        {"last_run": "session3-lab", "last_successful_watermark": "2026-07-01"},
        indent=2,
    )
    cdc_watermark = json.dumps(
        {
            "last_modified_after": "1900-01-01T00:00:00Z",
            "last_sync_version": 0,
        },
        indent=2,
    )
    try:
        client = DataLakeServiceClient(
            account_url=f"https://{account}.dfs.core.windows.net",
            credential=get_datalake_credential(account, cfg.subscription_id),
        )
        fs = client.get_file_system_client("audit")
        metadata_seed = (
            "job_id,job_name,incoming_folder,loaded_folder,expected_file_name,run_databricks,status\n"
            "job-01,bronze_metadata_orchestrate,incoming/run=session3-lab,loaded/run=session3-lab,"
            "sample_transactions.csv,1,READY\n"
            "job-02,disabled_skip_demo,incoming/run=session3-lab,loaded/run=session3-lab,"
            "sample_transactions.csv,0,HOLD\n"
        )
        for path, body in [
            ("watermark.json", watermark),
            ("cdc_watermark.json", cdc_watermark),
            ("sql_export/.keep", ""),
            ("sql_seed/job_metadata_seed.csv", metadata_seed),
        ]:
            file_client = fs.get_file_client(path)
            try:
                file_client.create_file()
            except Exception:
                pass
            file_client.upload_data(body.encode("utf-8"), overwrite=True)
        logger.info("Control files on abfss://audit@%s.dfs.core.windows.net/", account)
    except HttpResponseError as exc:
        logger.warning(
            "Audit control-file upload failed (%s) — deploy continues.",
            exc.message if hasattr(exc, "message") else exc,
        )


def _warm_databricks_cluster_for_adf(cfg, *, autotermination_minutes: int = 140) -> bool:
    """Set auto-termination, start shared cluster, wait until RUNNING (for pl_07/pl_10)."""
    if not cfg.databricks_host or not cfg.databricks_token or not cfg.databricks_cluster_id:
        logger.warning(
            "DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_CLUSTER_ID not set — "
            "skip cluster warm-up"
        )
        return False
    import requests

    headers = {"Authorization": f"Bearer {cfg.databricks_token}"}
    cluster_id = cfg.databricks_cluster_id
    base = cfg.databricks_host.rstrip("/")
    get_url = f"{base}/api/2.0/clusters/get"
    resp = requests.get(get_url, headers=headers, params={"cluster_id": cluster_id}, timeout=60)
    if resp.status_code >= 300:
        logger.warning("Could not read cluster %s: %s %s", cluster_id, resp.status_code, resp.text[:200])
        return False
    cluster = resp.json()
    current_auto = cluster.get("autotermination_minutes")
    if current_auto != autotermination_minutes:
        edit_body: dict[str, Any] = {
            "cluster_id": cluster_id,
            "cluster_name": cluster.get("cluster_name"),
            "spark_version": cluster.get("spark_version"),
            "node_type_id": cluster.get("node_type_id"),
            "autotermination_minutes": autotermination_minutes,
        }
        if cluster.get("driver_node_type_id"):
            edit_body["driver_node_type_id"] = cluster["driver_node_type_id"]
        if cluster.get("autoscale"):
            edit_body["autoscale"] = cluster["autoscale"]
        elif cluster.get("num_workers") is not None:
            edit_body["num_workers"] = cluster["num_workers"]
        if cluster.get("azure_attributes"):
            edit_body["azure_attributes"] = cluster["azure_attributes"]
        edit = requests.post(
            f"{base}/api/2.0/clusters/edit",
            headers=headers,
            json=edit_body,
            timeout=60,
        )
        if edit.status_code >= 300:
            logger.warning(
                "Cluster autotermination edit failed: %s %s",
                edit.status_code,
                edit.text[:200],
            )
        else:
            logger.info(
                "Cluster %s autotermination_minutes set to %d (was %s)",
                cluster_id,
                autotermination_minutes,
                current_auto,
            )

    state = (cluster.get("state") or "").upper()
    if state == "RUNNING":
        logger.info("Databricks cluster %s already RUNNING (ADF ready)", cluster_id)
        return True
    if state in {"PENDING", "RESTARTING", "RESIZING"}:
        logger.info("Databricks cluster %s is %s — waiting", cluster_id, state)
    else:
        logger.info("Starting Databricks cluster %s (was %s)", cluster_id, state or "UNKNOWN")
        start = requests.post(
            f"{base}/api/2.0/clusters/start",
            headers=headers,
            json={"cluster_id": cluster_id},
            timeout=60,
        )
        if start.status_code >= 300:
            logger.warning("Cluster start failed: %s %s", start.status_code, start.text[:200])
            return False
    for _ in range(40):
        poll = requests.get(get_url, headers=headers, params={"cluster_id": cluster_id}, timeout=60)
        if poll.status_code >= 300:
            break
        state = (poll.json().get("state") or "").upper()
        if state == "RUNNING":
            logger.info(
                "Databricks cluster %s is RUNNING — ADF pl_07/pl_10 can use it for ~%d min idle",
                cluster_id,
                autotermination_minutes,
            )
            return True
        time.sleep(15)
    logger.warning("Timed out waiting for cluster %s to reach RUNNING", cluster_id)
    return False


def _ensure_databricks_cluster_running(cfg) -> None:
    """Start shared all-purpose cluster if terminated (idempotent)."""
    _warm_databricks_cluster_for_adf(cfg, autotermination_minutes=140)


def _deploy_notebooks(cfg) -> None:
    """Import Databricks notebooks used by ADF notebook activities."""
    if not cfg.databricks_host or not cfg.databricks_token:
        logger.warning(
            "DATABRICKS_HOST / DATABRICKS_TOKEN not set — skip notebook import. "
            "Set in .env for Databricks pipelines."
        )
        return
    import base64
    import requests

    notebooks = [
        ("nb_adf_hello.py", "/Shared/shared-adf/nb_adf_hello"),
        ("nb_adf_params_echo.py", "/Shared/shared-adf/nb_adf_params_echo"),
        ("nb_adf_bronze_stats.py", "/Shared/shared-adf/nb_adf_bronze_stats"),
        ("nb_adf_integration_complete.py", "/Shared/shared-adf/nb_adf_integration_complete"),
        ("nb_adf_job_task.py", "/Shared/shared-adf/nb_adf_job_task"),
    ]
    mkdirs_url = f"{cfg.databricks_host}/api/2.0/workspace/mkdirs"
    mkdirs_resp = requests.post(
        mkdirs_url,
        headers={"Authorization": f"Bearer {cfg.databricks_token}"},
        json={"path": "/Shared/shared-adf"},
        timeout=60,
    )
    if mkdirs_resp.status_code >= 300:
        logger.warning(
            "Notebook folder create failed: %s %s",
            mkdirs_resp.status_code,
            mkdirs_resp.text[:200],
        )
        return

    import_url = f"{cfg.databricks_host}/api/2.0/workspace/import"
    for filename, dbx_path in notebooks:
        nb_path = SCRIPTS.parent / "notebooks" / filename
        if not nb_path.is_file():
            logger.warning("Notebook missing: %s", nb_path)
            continue
        content = nb_path.read_text(encoding="utf-8")
        payload = {
            "path": dbx_path,
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        }
        resp = requests.post(
            import_url,
            headers={"Authorization": f"Bearer {cfg.databricks_token}"},
            json=payload,
            timeout=60,
        )
        if resp.status_code >= 300:
            logger.warning("Notebook import failed %s: %s %s", dbx_path, resp.status_code, resp.text[:200])
        else:
            logger.info("Notebook imported: %s", dbx_path)


def _print_summary(cfg, estate, sql, *, sql_pipelines: bool) -> None:
    sub = cfg.subscription_id
    rg = cfg.resource_group
    adf = estate.data_factory
    portal = (
        f"https://adf.azure.com/en/authoring?"
        f"factory=%2Fsubscriptions%2F{sub}%2FresourceGroups%2F{rg}"
        f"%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2F{adf}"
    )
    print()
    print("=" * 70)
    print("SHARED ADF LAB — DEPLOY COMPLETE")
    print("=" * 70)
    print(f"  Resource group : {rg}")
    print(f"  Data Factory   : {adf} ({cfg.adf_location})")
    print(f"  Storage        : {estate.storage_account} ({cfg.adf_location})")
    if sql:
        print(f"  Azure SQL      : {sql.fqdn} ({sql.location})")
    print(f"  Databricks     : {estate.databricks_workspace}")
    print()
    print("  Pipelines deployed:")
    for name in (
        list_pipeline_names(include_sql=sql_pipelines)
        + list_advanced_pipeline_names(include_sql=sql_pipelines)
        + list_databricks_pipeline_names()
        + list_expression_pipeline_names()
        + list_trigger_pipeline_names()
    ):
        print(f"    - {name}")
    print()
    print("  Triggers deployed (Stopped — start in Studio when teaching):")
    for name in list_trigger_names():
        print(f"    - {name}")
    print()
    print(f"  ADF Studio     : {portal}")
    print()
    print("  Re-run command : shared-adf-lab\\orchestrate.cmd")
    print("=" * 70)


def _default_params(pipeline_name: str) -> dict:
    if pipeline_name in ("pl_01_bronze_copy", "pl_03_if_then_copy", "pl_06_wait_then_copy"):
        return {"incoming_folder": INCOMING, "loaded_folder": LOADED}
    if pipeline_name == "pl_02_get_metadata":
        return {"incoming_folder": INCOMING}
    if pipeline_name == "pl_04_foreach_dates":
        return {"run_dates": [{"incoming": INCOMING, "loaded": LOADED}]}
    if pipeline_name == "pl_07_databricks_notebook":
        return {"run_id": "session3-lab"}
    if pipeline_name == "pl_10_parent_chain":
        return {
            "incoming_folder": INCOMING,
            "loaded_folder": LOADED,
            "run_id": "session3-lab",
        }
    if pipeline_name == "pl_11_filename_validate_route":
        return {
            "incoming_folder": INCOMING,
            "loaded_folder": LOADED,
            "expected_file_name": "sample_transactions.csv",
            "run_id": "session3-lab",
        }
    if pipeline_name == "pl_12_sql_metadata_orchestrate":
        return {"job_id": "job-01", "run_id": "session3-lab"}
    adv = advanced_default_params(pipeline_name)
    if adv:
        return adv
    dbx = databricks_default_params(pipeline_name)
    if dbx:
        return dbx
    ex = expression_default_params(pipeline_name)
    if ex:
        return ex
    trig = trigger_default_params(pipeline_name)
    if trig:
        return trig
    return {}


_DATABRICKS_PIPELINES = frozenset(
    {
        "pl_07_databricks_notebook",
        "pl_10_parent_chain",
        "pl_11_filename_validate_route",
        "pl_12_sql_metadata_orchestrate",
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
    }
)

_DATAFLOW_PIPELINES = frozenset(
    {
        "pl_13_df_bronze_cleanse",
        "pl_14_df_channel_aggregate",
        "pl_25_power_query_returns",
        "pl_26_join_returns_txn",
    }
)


def _pipeline_timeout_minutes(pipeline_name: str) -> int:
    if pipeline_name in _DATAFLOW_PIPELINES:
        return 45
    if pipeline_name in _DATABRICKS_PIPELINES:
        return 30
    return 20


def _pipelines_from(start_name: str, *, include_sql: bool) -> list[str]:
    all_names = (
        list_pipeline_names(include_sql=include_sql)
        + list_advanced_pipeline_names(include_sql=include_sql)
        + list_databricks_pipeline_names()
        + list_expression_pipeline_names()
        + list_trigger_pipeline_names()
    )
    if start_name not in all_names:
        raise SystemExit(f"Unknown pipeline '{start_name}'. Choose one of: {', '.join(all_names)}")
    return all_names[all_names.index(start_name) :]


def _run_pipelines_sequential(
    cfg,
    estate,
    pipeline_names: list[str],
    *,
    poll_seconds: int,
) -> list[str]:
    """Trigger each pipeline in order; return names that did not succeed."""
    failed: list[str] = []
    for pipeline_name in pipeline_names:
        if pipeline_name in _DATABRICKS_PIPELINES:
            _ensure_databricks_cluster_running(cfg)
        params = _default_params(pipeline_name)
        logger.info("Triggering %s ...", pipeline_name)
        run_id = trigger_pipeline(cfg, estate.data_factory, pipeline_name, params)
        status = wait_for_pipeline_run(
            cfg,
            estate.data_factory,
            run_id,
            poll_seconds=poll_seconds,
            timeout_minutes=_pipeline_timeout_minutes(pipeline_name),
        )
        logger.info("  %s -> %s (%s)", pipeline_name, status, run_id)
        if status != "Succeeded":
            failed.append(pipeline_name)
    return failed


def _run_all_until_success(
    cfg,
    estate,
    *,
    include_sql: bool,
    max_rounds: int,
    poll_seconds: int,
) -> bool:
    pending = (
        list_pipeline_names(include_sql=include_sql)
        + list_advanced_pipeline_names(include_sql=include_sql)
        + list_databricks_pipeline_names()
        + list_expression_pipeline_names()
        + list_trigger_pipeline_names()
    )
    logger.info("Run-all mode: %d pipelines", len(pending))
    for round_no in range(1, max_rounds + 1):
        logger.info("Round %d/%d starting...", round_no, max_rounds)
        _upload_bronze_samples(cfg, estate)
        failed: list[str] = []
        for pipeline_name in pending:
            if pipeline_name in _DATABRICKS_PIPELINES:
                _ensure_databricks_cluster_running(cfg)
            params = _default_params(pipeline_name)
            run_id = trigger_pipeline(cfg, estate.data_factory, pipeline_name, params)
            status = wait_for_pipeline_run(
                cfg,
                estate.data_factory,
                run_id,
                poll_seconds=poll_seconds,
                timeout_minutes=_pipeline_timeout_minutes(pipeline_name),
            )
            logger.info("  %s -> %s (%s)", pipeline_name, status, run_id)
            if status != "Succeeded":
                failed.append(pipeline_name)
        if not failed:
            logger.info("All pipelines succeeded in round %d.", round_no)
            return True
        pending = failed
        logger.warning("Round %d failed pipelines: %s", round_no, ", ".join(failed))
    logger.error("Run-all failed after %d rounds. Remaining: %s", max_rounds, ", ".join(pending))
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Shared ADF lab deploy")
    parser.add_argument("--skip-sql", action="store_true", help="Skip westus SQL deploy")
    parser.add_argument("--skip-notebook", action="store_true", help="Skip Databricks notebook import")
    parser.add_argument("--run-pipeline", metavar="NAME", help="Trigger one pipeline after deploy")
    parser.add_argument(
        "--run-from-pipeline",
        metavar="NAME",
        help="Trigger pipelines from NAME through end (e.g. pl_13_df_bronze_cleanse)",
    )
    parser.add_argument(
        "--run-only",
        action="store_true",
        help="Skip deploy/RBAC/upload — only trigger pipeline(s)",
    )
    parser.add_argument(
        "--run-all-until-success",
        action="store_true",
        help="Trigger all concept pipelines, retry failed ones until all succeed",
    )
    parser.add_argument("--max-rounds", type=int, default=3, help="Max retry rounds for --run-all-until-success")
    parser.add_argument("--poll-seconds", type=int, default=10, help="Polling interval for pipeline status")
    parser.add_argument(
        "--warm-cluster-minutes",
        type=int,
        metavar="N",
        help="Set Databricks autotermination to N minutes and start cluster (ADF pl_07/pl_10)",
    )
    parser.add_argument(
        "--warm-cluster-only",
        action="store_true",
        help="Only warm Databricks cluster (skip deploy); use with --warm-cluster-minutes",
    )
    parser.add_argument(
        "--setup-databricks-integration",
        action="store_true",
        help="Bootstrap secrets + Databricks Job + audit manifest (run once per workspace)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s — %(message)s",
    )

    cfg = load_config()

    if args.setup_databricks_integration and args.run_only:
        estate = discover_estate(cfg)
        from databricks_infra import setup_complete_integration  # noqa: E402

        job_id = setup_complete_integration(cfg, estate)
        print(f"Databricks integration ready (job_id={job_id})")
        return 0

    if args.warm_cluster_only:
        minutes = max(10, args.warm_cluster_minutes or 140)
        ok = _warm_databricks_cluster_for_adf(cfg, autotermination_minutes=minutes)
        print()
        print("=" * 70)
        print("ADF DATABRICKS CLUSTER — WARM-UP")
        print("=" * 70)
        print(f"  Cluster ID              : {cfg.databricks_cluster_id}")
        print(f"  Auto-termination (min)  : {minutes}")
        print(f"  Status                  : {'RUNNING' if ok else 'CHECK LOGS'}")
        print(f"  Used by ADF pipelines   : pl_07, pl_10, pl_11, pl_12")
        print("=" * 70)
        return 0 if ok else 2

    estate = discover_estate(cfg)

    if args.warm_cluster_minutes and not args.run_only:
        _warm_databricks_cluster_for_adf(cfg, autotermination_minutes=max(10, args.warm_cluster_minutes))

    sql = None
    sql_pipelines = False
    if args.run_only:
        adf = _client(cfg)
        sql_pipelines = _sql_pipelines_enabled(
            adf, cfg.resource_group, estate.data_factory, sql_deployed=False
        )
    else:
        if args.warm_cluster_minutes:
            pass  # warmed above when not run_only
        if not args.skip_sql:
            sql = deploy_sql(cfg, estate)
            seed_reference_table(sql)
            seed_change_tracking(sql)
            seed_job_metadata_table(sql)

        ensure_adf_rbac(cfg, estate.storage_account, estate.data_factory, estate.databricks_workspace)
        deploy_linked_services(cfg, estate, sql)
        _upload_bronze_samples(cfg, estate)
        _upload_control_files(cfg, estate)
        deploy_all_pipelines(cfg, estate, include_sql=sql is not None, skip_if_present=False)

        adf = _client(cfg)
        sql_pipelines = _sql_pipelines_enabled(
            adf, cfg.resource_group, estate.data_factory, sql_deployed=sql is not None
        )
        deploy_advanced(cfg, estate, include_sql=sql_pipelines)
        deploy_databricks(cfg, estate)
        deploy_expressions(cfg, estate)
        deploy_triggers(cfg, estate)

        if not args.skip_notebook:
            _deploy_notebooks(cfg)

        if args.setup_databricks_integration:
            from databricks_infra import setup_complete_integration  # noqa: E402

            setup_complete_integration(cfg, estate)

    if args.run_from_pipeline:
        _upload_bronze_samples(cfg, estate)
        names = _pipelines_from(args.run_from_pipeline, include_sql=sql_pipelines)
        logger.info("Run-from %s: %d pipelines", args.run_from_pipeline, len(names))
        failed = _run_pipelines_sequential(
            cfg,
            estate,
            names,
            poll_seconds=max(5, args.poll_seconds),
        )
        if failed:
            logger.error("Failed pipelines: %s", ", ".join(failed))
            return 2
        return 0

    if args.run_pipeline or args.run_all_until_success:
        _ensure_databricks_cluster_running(cfg)

    if args.run_pipeline:
        params = _default_params(args.run_pipeline)
        run_id = trigger_pipeline(cfg, estate.data_factory, args.run_pipeline, params)
        status = wait_for_pipeline_run(
            cfg,
            estate.data_factory,
            run_id,
            poll_seconds=15,
            timeout_minutes=_pipeline_timeout_minutes(args.run_pipeline),
        )
        logger.info("Pipeline %s finished: %s", args.run_pipeline, status)

    if args.run_all_until_success:
        ok = _run_all_until_success(
            cfg,
            estate,
            include_sql=sql_pipelines,
            max_rounds=max(1, args.max_rounds),
            poll_seconds=max(5, args.poll_seconds),
        )
        if not ok:
            return 2

    _print_summary(cfg, estate, sql, sql_pipelines=sql_pipelines)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
