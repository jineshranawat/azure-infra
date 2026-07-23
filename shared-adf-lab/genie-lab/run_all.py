"""One entry: inventory -> create Genie Agent -> run question orchestrator."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from create_genie_agent import create_or_update_agent  # noqa: E402
from grant_self_access import main as grant_access  # noqa: E402
from run_orchestrator import run_suite  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="FinLedger Genie lab")
    parser.add_argument(
        "--phase",
        choices=["all", "create", "ask", "grant"],
        default="all",
        help="all=grant+create+ask (default); create; ask; grant",
    )
    args = parser.parse_args()
    if args.phase in ("all", "grant", "create"):
        # Grants are required for Genie SQL against class schemas
        if args.phase != "ask":
            rc = grant_access()
            if rc != 0 and args.phase == "grant":
                return rc
    if args.phase in ("all", "create"):
        create_or_update_agent()
    if args.phase in ("all", "ask"):
        run_suite()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
