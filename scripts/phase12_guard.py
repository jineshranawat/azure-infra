#!/usr/bin/env python3
"""Phase 12 preflight — personal Class-1 only; never run against shared estate.

Exit codes:
  0 = SKIP (shared estate or misconfigured) — walkthrough treats as success
  1 = OK to call orchestrate.cmd (personal LEARNER + UK location)
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"


def _load_env() -> dict[str, str]:
    values: dict[str, str] = {}
    if not ENV_FILE.is_file():
        return values
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, raw = line.partition("=")
        values[key.strip()] = raw.strip().strip('"').strip("'")
    return values


def main() -> int:
    env = _load_env()
    learner = env.get("LEARNER", "").strip().lower()
    location = env.get("LOCATION", "").strip().lower()
    print(f"  .env LEARNER={learner or '(missing)'}  LOCATION={location or '(missing)'}")

    # Shared-class .env (eastus + LEARNER=shared) — students stop at phase 11
    if learner == "shared" or location == "eastus":
        print()
        print("SKIP Phase 12 — this machine is configured for the SHARED class estate.")
        print("Phase 12 is ONLY for a personal Class-1 lab (separate .env / personal RG).")
        print()
        print("Shared-class students: you are DONE after phase 11. Do not run phase 12.")
        print()
        print("Personal Class-1 (optional later):")
        print("  LEARNER=yourid     # 2-10 lowercase letters/digits, NOT shared")
        print("  LOCATION=uksouth   # UK only — eastus is shared-estate only")
        print("  then: infra-walkthrough.cmd --phase 12")
        return 0

    if location not in ("uksouth", "ukwest"):
        print()
        print("SKIP Phase 12 — LOCATION must be uksouth or ukwest for personal Class-1.")
        print(f"  Current LOCATION={location!r}. Set LOCATION=uksouth in .env and re-run.")
        return 0

    if not learner or learner == "demo":
        print()
        print("SKIP Phase 12 — set a personal LEARNER=yourid in .env first.")
        return 0

    print(f"OK personal Class-1 target: rg-{learner}-class1 in {location}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
