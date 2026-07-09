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

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from _config import load_config  # noqa: E402
from adf_pipelines import (  # noqa: E402
    deploy_all_pipelines,
    deploy_linked_services,
    list_pipeline_names,
    trigger_pipeline,
    wait_for_pipeline_run,
)
from adf_rbac import ensure_adf_rbac  # noqa: E402
from azure_sql import deploy_sql, seed_reference_table  # noqa: E402
from discover import discover_estate  # noqa: E402

logger = logging.getLogger(__name__)
INCOMING = "incoming/run=session3-lab"
LOADED = "loaded/run=session3-lab"
SAMPLE_CSV = Path(__file__).resolve().parent.parent.parent / "day8" / "data" / "sample_transactions.csv"


def _upload_bronze_samples(cfg, estate) -> None:
    """Upload sample CSV to incoming + loaded so copy pipelines have source data."""
    from azure.storage.filedatalake import DataLakeServiceClient
    from _config import get_credential

    if not SAMPLE_CSV.is_file():
        logger.warning("Sample CSV not found at %s — skip bronze upload", SAMPLE_CSV)
        return
    data = SAMPLE_CSV.read_bytes()
    client = DataLakeServiceClient(
        account_url=f"https://{estate.storage_account}.dfs.core.windows.net",
        credential=get_credential(),
    )
    fs = client.get_file_system_client("bronze")
    for folder in (INCOMING, LOADED):
        path = f"{folder}/sample_transactions.csv"
        file_client = fs.get_file_client(path)
        try:
            file_client.create_file()
        except Exception:
            pass
        file_client.upload_data(data, overwrite=True)
    logger.info("Bronze sample at incoming + loaded (run=session3-lab)")


def _upload_control_files(cfg, estate) -> None:
    """Seed watermark.json on ADLS audit container (idempotent overwrite)."""
    from azure.storage.filedatalake import DataLakeServiceClient
    from _config import get_credential

    account = estate.storage_account
    watermark = json.dumps(
        {"last_run": "session3-lab", "last_successful_watermark": "2026-07-01"},
        indent=2,
    )
    client = DataLakeServiceClient(
        account_url=f"https://{account}.dfs.core.windows.net",
        credential=get_credential(),
    )
    fs = client.get_file_system_client("audit")
    for path, body in [
        ("watermark.json", watermark),
        ("sql_export/.keep", ""),
    ]:
        file_client = fs.get_file_client(path)
        try:
            file_client.create_file()
        except Exception:
            pass
        file_client.upload_data(body.encode("utf-8"), overwrite=True)
    logger.info("Control files on abfss://audit@%s.dfs.core.windows.net/", account)


def _ensure_databricks_cluster_running(cfg) -> None:
    """Start shared all-purpose cluster if terminated (idempotent)."""
    if not cfg.databricks_host or not cfg.databricks_token or not cfg.databricks_cluster_id:
        logger.warning(
            "DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_CLUSTER_ID not set — "
            "skip cluster warm-up"
        )
        return
    import requests

    headers = {"Authorization": f"Bearer {cfg.databricks_token}"}
    cluster_id = cfg.databricks_cluster_id
    base = cfg.databricks_host.rstrip("/")
    get_url = f"{base}/api/2.0/clusters/get"
    resp = requests.get(get_url, headers=headers, params={"cluster_id": cluster_id}, timeout=60)
    if resp.status_code >= 300:
        logger.warning("Could not read cluster %s: %s %s", cluster_id, resp.status_code, resp.text[:200])
        return
    state = (resp.json().get("state") or "").upper()
    if state == "RUNNING":
        logger.info("Databricks cluster %s already RUNNING", cluster_id)
        return
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
            return
    for _ in range(40):
        poll = requests.get(get_url, headers=headers, params={"cluster_id": cluster_id}, timeout=60)
        if poll.status_code >= 300:
            break
        state = (poll.json().get("state") or "").upper()
        if state == "RUNNING":
            logger.info("Databricks cluster %s is RUNNING", cluster_id)
            return
        time.sleep(15)
    logger.warning("Timed out waiting for cluster %s to reach RUNNING", cluster_id)


def _deploy_notebook(cfg) -> None:
    """Import nb_adf_hello to Databricks workspace."""
    if not cfg.databricks_host or not cfg.databricks_token:
        logger.warning(
            "DATABRICKS_HOST / DATABRICKS_TOKEN not set — skip notebook import. "
            "Set in .env for pl_07 / pl_10 pipelines."
        )
        return
    import requests

    nb_path = SCRIPTS.parent / "notebooks" / "nb_adf_hello.py"
    content = nb_path.read_text(encoding="utf-8")
    # Ensure parent folder exists before import.
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

    # Databricks import API expects base64 or source format — use workspace import 2.0
    url = f"{cfg.databricks_host}/api/2.0/workspace/import"
    payload = {
        "path": "/Shared/shared-adf/nb_adf_hello",
        "format": "SOURCE",
        "language": "PYTHON",
        "overwrite": True,
        "content": __import__("base64").b64encode(content.encode("utf-8")).decode("ascii"),
    }
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {cfg.databricks_token}"},
        json=payload,
        timeout=60,
    )
    if resp.status_code >= 300:
        logger.warning("Notebook import failed: %s %s", resp.status_code, resp.text[:200])
    else:
        logger.info("Notebook imported: /Shared/shared-adf/nb_adf_hello")


def _print_summary(cfg, estate, sql) -> None:
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
    for name in list_pipeline_names(include_sql=sql is not None):
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
    return {}


def _run_all_until_success(
    cfg,
    estate,
    *,
    include_sql: bool,
    max_rounds: int,
    poll_seconds: int,
) -> bool:
    pending = list_pipeline_names(include_sql=include_sql)
    logger.info("Run-all mode: %d pipelines", len(pending))
    for round_no in range(1, max_rounds + 1):
        logger.info("Round %d/%d starting...", round_no, max_rounds)
        failed: list[str] = []
        for pipeline_name in pending:
            params = _default_params(pipeline_name)
            run_id = trigger_pipeline(cfg, estate.data_factory, pipeline_name, params)
            status = wait_for_pipeline_run(
                cfg,
                estate.data_factory,
                run_id,
                poll_seconds=poll_seconds,
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
        "--run-all-until-success",
        action="store_true",
        help="Trigger all concept pipelines, retry failed ones until all succeed",
    )
    parser.add_argument("--max-rounds", type=int, default=3, help="Max retry rounds for --run-all-until-success")
    parser.add_argument("--poll-seconds", type=int, default=10, help="Polling interval for pipeline status")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s — %(message)s",
    )

    cfg = load_config()
    estate = discover_estate(cfg)

    sql = None
    if not args.skip_sql:
        sql = deploy_sql(cfg, estate)
        seed_reference_table(sql)

    ensure_adf_rbac(cfg, estate.storage_account, estate.data_factory, estate.databricks_workspace)
    deploy_linked_services(cfg, estate, sql)
    _upload_bronze_samples(cfg, estate)
    _upload_control_files(cfg, estate)
    # Always apply latest pipeline definitions (idempotent create_or_update).
    deploy_all_pipelines(cfg, estate, include_sql=sql is not None, skip_if_present=False)

    if not args.skip_notebook:
        _deploy_notebook(cfg)

    if args.run_pipeline or args.run_all_until_success:
        _ensure_databricks_cluster_running(cfg)

    if args.run_pipeline:
        trigger_pipeline(cfg, estate.data_factory, args.run_pipeline, _default_params(args.run_pipeline))

    if args.run_all_until_success:
        ok = _run_all_until_success(
            cfg,
            estate,
            include_sql=sql is not None,
            max_rounds=max(1, args.max_rounds),
            poll_seconds=max(5, args.poll_seconds),
        )
        if not ok:
            return 2

    _print_summary(cfg, estate, sql)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
