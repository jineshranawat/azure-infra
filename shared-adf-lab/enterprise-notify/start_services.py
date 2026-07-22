"""Start local Jira mock + mail sink if not already healthy (idempotent)."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent
JIRA_DIR = ROOT / "jira_mock"
MAIL_DIR = ROOT / "mail_sink"
JIRA_URL = "http://127.0.0.1:18080"
MAIL_URL = "http://127.0.0.1:18081"
LOG_DIR = ROOT / "out"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def healthy(url: str) -> bool:
    try:
        r = httpx.get(f"{url}/health", timeout=2.0)
        return r.status_code == 200
    except Exception:
        return False


def start(name: str, cwd: Path, port: int, url: str) -> None:
    if healthy(url):
        print(f"  already running: {name} ({url})")
        return
    log = LOG_DIR / f"{name}.log"
    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS  # type: ignore[attr-defined]
    with log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n--- start {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n")
        fh.flush()
        subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(cwd),
            stdout=fh,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
        )
    for _ in range(40):
        if healthy(url):
            print(f"  started: {name} ({url})")
            return
        time.sleep(0.25)
    raise SystemExit(f"Failed to start {name} — see {log}")


def main() -> int:
    print("Ensuring local enterprise services...")
    start("jira-mock", JIRA_DIR, 18080, JIRA_URL)
    start("mail-sink", MAIL_DIR, 18081, MAIL_URL)
    print("OK")
    print(f"  Jira UI : {JIRA_URL}/")
    print(f"  Mail UI : {MAIL_URL}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
