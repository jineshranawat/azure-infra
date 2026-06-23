"""ADF pipeline run report — 'morning check' (azure.html Day 2 Hour 23).

Queries last 24h pipeline runs without opening the portal.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

from _config import SessionConfig

logger = logging.getLogger(__name__)


def print_morning_check(cfg: SessionConfig, data_factory: str, *, hours: int = 24) -> None:
    """List pipeline runs in the last N hours — failures highlighted."""
    client = DataFactoryManagementClient(DefaultAzureCredential(), cfg.subscription_id)
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)

    runs = client.pipeline_runs.query_by_factory(
        cfg.resource_group,
        data_factory,
        {
            "lastUpdatedAfter": start.isoformat(),
            "lastUpdatedBefore": end.isoformat(),
        },
    )

    values = runs.value or []
    logger.info("=== Morning check: %s (last %dh) ===", data_factory, hours)
    if not values:
        logger.info("No pipeline runs in window — trigger with: orchestrate.cmd --run-pipeline")
        return

    for run in values:
        status = run.status or "Unknown"
        name = run.pipeline_name or "?"
        run_id = run.run_id or "?"
        marker = "FAIL" if status not in ("Succeeded", "InProgress", "Queued") else "OK"
        logger.info("[%s] %s | %s | %s", marker, name, status, run_id)
