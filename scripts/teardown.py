"""Tear down the Class-1 estate by deleting its single resource group.

One resource group holds the entire estate — deleting it removes storage,
Key Vault, budget, and role assignments in one operation.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient

logger = logging.getLogger(__name__)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _confirm(resource_group: str) -> bool:
    prompt = (
        f"Delete resource group '{resource_group}' and ALL contained resources? "
        "Type the resource group name to confirm: "
    )
    answer = input(prompt).strip()
    return answer == resource_group


def teardown(
    subscription_id: str,
    resource_group: str,
    *,
    assume_yes: bool = False,
) -> None:
    if not assume_yes and not _confirm(resource_group):
        logger.warning("Teardown cancelled — confirmation did not match.")
        return

    credential = DefaultAzureCredential()
    client = ResourceManagementClient(credential, subscription_id)

    logger.info("Deleting resource group: %s", resource_group)
    poller = client.resource_groups.begin_delete(resource_group)
    poller.wait()
    logger.info("Resource group deleted: %s", resource_group)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete the Class-1 resource group (full estate teardown)."
    )
    parser.add_argument(
        "--subscription-id",
        default=os.environ.get("AZURE_SUBSCRIPTION_ID"),
        help="Azure subscription ID.",
    )
    parser.add_argument(
        "--resource-group",
        required=True,
        help="Resource group to delete (e.g. rg-demo-class1).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip interactive confirmation.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)

    if not args.subscription_id:
        logger.error("Subscription ID required: --subscription-id or AZURE_SUBSCRIPTION_ID")
        return 1

    try:
        teardown(
            args.subscription_id,
            args.resource_group,
            assume_yes=args.yes,
        )
    except Exception:
        logger.exception("Teardown failed")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
