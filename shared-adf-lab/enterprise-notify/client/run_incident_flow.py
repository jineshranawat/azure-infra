"""Complete local incident flow — mirrors ADF enterprise notify activities.

Sequence (same as pl_ntf_05 conceptually):
  1. Simulate ADF pipeline failure (write run log)
  2. Create Jira issue (Web / Python)
  3. Attach ADF run log to the ticket
  4. Send email with ticket key + link
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Allow running as script from repo / enterprise-notify folder
_CLIENT_DIR = Path(__file__).resolve().parent
if str(_CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(_CLIENT_DIR))

from jira_client import JiraClient  # noqa: E402
from mail_client import MailClient  # noqa: E402

OUT_DIR = Path(__file__).resolve().parent.parent / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _default_jira() -> str:
    return os.environ.get("JIRA_BASE_URL", "http://127.0.0.1:18080").rstrip("/")


def _default_mail() -> str:
    return os.environ.get("NOTIFY_MAIL_URL", "http://127.0.0.1:18081").rstrip("/")


def _default_email() -> str:
    return (
        os.environ.get("OWNER_EMAIL")
        or os.environ.get("CLASS_OWNER_EMAIL")
        or "training@example.com"
    )


def wait_healthy(base: str, path: str = "/health", attempts: int = 30) -> None:
    import httpx

    url = f"{base.rstrip('/')}{path}"
    last = ""
    for _ in range(attempts):
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
            last = f"{r.status_code}"
        except Exception as exc:  # noqa: BLE001
            last = str(exc)
        time.sleep(0.5)
    raise RuntimeError(f"Service not healthy at {url}: {last}")


def write_run_log(*, run_id: str, pipeline_name: str, error: str) -> Path:
    path = OUT_DIR / f"adf-run-log-{run_id}.txt"
    path.write_text(
        "\n".join(
            [
                "FinLedger ADF incident run log",
                f"utc_time     : {datetime.now(timezone.utc).isoformat()}",
                f"pipeline     : {pipeline_name}",
                f"run_id       : {run_id}",
                f"status       : Failed",
                f"error        : {error}",
                "activity     : SimulateWork (forced failure for class demo)",
                "",
                "This file is attached to the Jira ticket by the enterprise notify flow.",
            ]
        ),
        encoding="utf-8",
    )
    return path


def run_flow(
    *,
    run_id: str,
    pipeline_name: str,
    force_fail: bool,
    jira_base: str,
    mail_url: str,
    owner_email: str,
    jira_user: str = "",
    jira_token: str = "",
) -> dict:
    print("=" * 64)
    print("ENTERPRISE INCIDENT FLOW (local)")
    print("=" * 64)
    print(f"  run_id        : {run_id}")
    print(f"  pipeline      : {pipeline_name}")
    print(f"  force_fail    : {force_fail}")
    print(f"  jira_base     : {jira_base}")
    print(f"  notify_mail   : {mail_url}")
    print(f"  owner_email   : {owner_email}")
    print()

    wait_healthy(jira_base)
    wait_healthy(mail_url)

    if not force_fail:
        print("force_fail=false — no ticket/email (happy path).")
        return {"status": "ok", "run_id": run_id}

    error = "PL_NTF-DEMO-FAIL: intentional enterprise failure path (force_fail=true)"
    log_path = write_run_log(run_id=run_id, pipeline_name=pipeline_name, error=error)
    print(f"[1/4] Simulated ADF failure - log: {log_path}")

    summary = f"[ADF] {pipeline_name} failed — run_id={run_id}"
    description = (
        f"Automated incident from FinLedger enterprise notify lab.\n\n"
        f"Pipeline: {pipeline_name}\n"
        f"Run ID: {run_id}\n"
        f"Error: {error}\n\n"
        f"See attached adf-run-log for details."
    )

    with JiraClient(jira_base, user=jira_user, api_token=jira_token) as jira:
        created = jira.create_issue(summary=summary, description=description)
        key = created["key"]
        print(f"[2/4] Jira issue created: {key}  ({jira_base}/issue/{key})")
        jira.attach_file(key, log_path)
        print(f"[3/4] Attached run log to {key}")
        jira.add_comment(key, f"Email notification queued to {owner_email}")

    body = (
        f"ADF pipeline failure.\n\n"
        f"Pipeline : {pipeline_name}\n"
        f"Run ID   : {run_id}\n"
        f"Jira     : {key}\n"
        f"Open     : {jira_base}/issue/{key}\n\n"
        f"Error    : {error}\n"
    )
    with MailClient(mail_url) as mail:
        mail_resp = mail.send(
            to=owner_email,
            subject=f"[FinLedger] Incident {key} — {pipeline_name}",
            body=body,
            jira_key=key,
            run_id=run_id,
        )
    print(f"[4/4] Email queued: {mail_resp}")

    result = {
        "status": "incident_raised",
        "run_id": run_id,
        "jira_key": key,
        "jira_url": f"{jira_base}/issue/{key}",
        "mail": mail_resp,
        "log_path": str(log_path),
    }
    result_path = OUT_DIR / f"incident-result-{run_id}.json"
    result_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print()
    print("OPEN IN BROWSER")
    print(f"  Jira board : {jira_base}/")
    print(f"  Ticket     : {jira_base}/issue/{key}")
    print(f"  Mail inbox : {mail_url}/")
    print(f"  Result JSON: {result_path}")
    print("=" * 64)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="FinLedger enterprise incident flow (local)")
    parser.add_argument("--run-id", default=f"local-{int(time.time())}")
    parser.add_argument("--pipeline-name", default="pl_ntf_05_master_incident")
    parser.add_argument("--force-fail", default="true", choices=["true", "false"])
    parser.add_argument("--jira-base", default=_default_jira())
    parser.add_argument("--mail-url", default=_default_mail())
    parser.add_argument("--owner-email", default=_default_email())
    parser.add_argument("--jira-user", default=os.environ.get("JIRA_USER", ""))
    parser.add_argument("--jira-token", default=os.environ.get("JIRA_API_TOKEN", ""))
    args = parser.parse_args()
    run_flow(
        run_id=args.run_id,
        pipeline_name=args.pipeline_name,
        force_fail=args.force_fail.lower() == "true",
        jira_base=args.jira_base,
        mail_url=args.mail_url,
        owner_email=args.owner_email,
        jira_user=args.jira_user,
        jira_token=args.jira_token,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
