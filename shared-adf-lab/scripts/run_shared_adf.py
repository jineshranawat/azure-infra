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
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from _config import load_config  # noqa: E402
from adf_pipelines import (  # noqa: E402
    PIPELINE_NAMES,
    deploy_all_pipelines,
    deploy_linked_services,
    trigger_pipeline,
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
    for name in PIPELINE_NAMES:
        print(f"    - {name}")
    print()
    print(f"  ADF Studio     : {portal}")
    print()
    print("  Re-run command : shared-adf-lab\\orchestrate.cmd")
    print("=" * 70)


def main() -> int:
    parser = argparse.ArgumentParser(description="Shared ADF lab deploy")
    parser.add_argument("--skip-sql", action="store_true", help="Skip westus SQL deploy")
    parser.add_argument("--skip-notebook", action="store_true", help="Skip Databricks notebook import")
    parser.add_argument("--run-pipeline", metavar="NAME", help="Trigger one pipeline after deploy")
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
    deploy_all_pipelines(cfg, estate, include_sql=sql is not None)

    if not args.skip_notebook:
        _deploy_notebook(cfg)

    if args.run_pipeline:
        params: dict = {}
        if args.run_pipeline == "pl_01_bronze_copy":
            params = {"incoming_folder": INCOMING, "loaded_folder": LOADED}
        elif args.run_pipeline in ("pl_03_if_then_copy", "pl_06_wait_then_copy"):
            params = {"incoming_folder": INCOMING, "loaded_folder": LOADED}
        elif args.run_pipeline == "pl_07_databricks_notebook":
            params = {"run_id": "session3-lab"}
        elif args.run_pipeline == "pl_10_parent_chain":
            params = {
                "incoming_folder": INCOMING,
                "loaded_folder": LOADED,
                "run_id": "session3-lab",
            }
        elif args.run_pipeline == "pl_04_foreach_dates":
            params = {
                "run_dates": [
                    {"incoming": INCOMING, "loaded": LOADED},
                ]
            }
        trigger_pipeline(cfg, estate.data_factory, args.run_pipeline, params)

    _print_summary(cfg, estate, sql)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
