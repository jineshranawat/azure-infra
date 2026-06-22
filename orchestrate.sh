#!/usr/bin/env bash
# Azure ETL lab — one-command orchestrator (Linux / macOS / Git Bash)
# Full lab (default): ./orchestrate.sh --skip-setup
# Class-1 only:       ./orchestrate.sh --class1-only --skip-setup
# First-time VM:      ./orchestrate.sh --install-cli
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

PY=""
for c in python3 python py; do
  if command -v "$c" >/dev/null 2>&1; then
    PY="$c"
    break
  fi
done
[[ -n "$PY" ]] || { echo "Python 3.11+ required." >&2; exit 1; }

exec "$PY" scripts/orchestrate.py "$@"
