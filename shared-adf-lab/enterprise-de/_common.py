"""Shared helpers for enterprise DE playbook scenarios."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SAMPLE = ROOT / "sample_data"
OUT = ROOT / "out"
OUT.mkdir(parents=True, exist_ok=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def banner(title: str) -> None:
    print()
    print("=" * 68)
    print(title)
    print("=" * 68)
