#!/usr/bin/env python3
"""Deploy Purview, Synapse, and Fabric — delegates to orchestrate.cmd platforms path.

Thin wrapper for trainers who need platform-only re-deploy. Prefer: orchestrate.cmd --platforms-only

Synapse and Fabric often fail on MPN/training subscriptions:
  - Synapse: SqlServerRegionDoesNotAllowProvisioning
  - Fabric: regional quota = 0

Usage:
  orchestrate.cmd --platforms-only
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PLATFORM_BICEP = REPO_ROOT / "infra" / "platform-services.bicep"

logger = logging.getLogger(__name__)


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
    logger.debug("exec: %s", " ".join(cmd))
    return subprocess.run(cmd, text=True, check=False, cwd=REPO_ROOT, **kwargs)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Deploy Purview / Synapse / Fabric")
    parser.add_argument("--subscription-id", default=os.environ.get("AZURE_SUBSCRIPTION_ID"))
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--learner", default=os.environ.get("LEARNER", "demo"))
    parser.add_argument("--owner-email", default=os.environ.get("OWNER_EMAIL"))
    parser.add_argument("--storage-account", help="Class-1 storage account name")
    parser.add_argument("--location", default=os.environ.get("LOCATION", "uksouth"))
    args = parser.parse_args()

    if not args.subscription_id or not args.owner_email:
        logger.error("Need --subscription-id and --owner-email")
        return 1

    # Delegate to orchestrate.py platforms path
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "orchestrate.py"),
        "--platforms-only",
        "--skip-setup",
        "--subscription-id", args.subscription_id,
        "--learner", args.learner,
        "--owner-email", args.owner_email,
        "--location", args.location,
    ]
    result = _run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
