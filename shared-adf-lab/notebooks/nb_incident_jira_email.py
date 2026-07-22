# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise incident — Jira ticket + attach + email
# MAGIC
# MAGIC **ADF pipeline:** `pl_ntf_04_databricks_incident` / master `pl_ntf_05_master_incident`
# MAGIC
# MAGIC Same sequence as local `enterprise-notify.cmd`:
# MAGIC 1. Detect failure (`force_fail`)
# MAGIC 2. Create Jira issue (`POST /rest/api/2/issue`)
# MAGIC 3. Attach run-log text
# MAGIC 4. POST email notify (mail sink or Logic App URL)
# MAGIC
# MAGIC **Note:** Azure Databricks cannot reach `127.0.0.1` on your laptop.
# MAGIC For the visible local board/outbox run `enterprise-notify.cmd`.
# MAGIC Point `jira_base_url` / `notify_mail_url` at reachable URLs for cloud runs.

# COMMAND ----------

dbutils.widgets.text("run_id", "session3-lab")
dbutils.widgets.text("pipeline_name", "pl_ntf_05_master_incident")
dbutils.widgets.text("jira_base_url", "http://127.0.0.1:18080")
dbutils.widgets.text("notify_mail_url", "http://127.0.0.1:18081")
dbutils.widgets.text("owner_email", "training@example.com")
dbutils.widgets.text("force_fail", "true")
dbutils.widgets.text("jira_user", "")
dbutils.widgets.text("jira_api_token", "")

run_id = dbutils.widgets.get("run_id")
pipeline_name = dbutils.widgets.get("pipeline_name")
jira_base = dbutils.widgets.get("jira_base_url").rstrip("/")
mail_url = dbutils.widgets.get("notify_mail_url").rstrip("/")
owner_email = dbutils.widgets.get("owner_email")
force_fail = dbutils.widgets.get("force_fail").lower() in ("true", "1", "yes")
jira_user = dbutils.widgets.get("jira_user")
jira_token = dbutils.widgets.get("jira_api_token")

print("Incident params")
print("  run_id         :", run_id)
print("  pipeline_name  :", pipeline_name)
print("  jira_base_url  :", jira_base)
print("  notify_mail_url:", mail_url)
print("  force_fail     :", force_fail)

# COMMAND ----------

import base64
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone

result = {
    "status": "ok",
    "run_id": run_id,
    "pipeline_name": pipeline_name,
    "force_fail": force_fail,
}


def _headers(json_body: bool = True) -> dict:
    h = {"Accept": "application/json"}
    if json_body:
        h["Content-Type"] = "application/json"
    if jira_user and jira_token:
        token = base64.b64encode(f"{jira_user}:{jira_token}".encode()).decode("ascii")
        h["Authorization"] = f"Basic {token}"
    return h


def http_json(method: str, url: str, payload: dict | None = None, timeout: int = 30) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=_headers(json_body=True), method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8") or "{}"
        return json.loads(raw)


def http_multipart(url: str, filename: str, content: bytes, timeout: int = 30) -> object:
    boundary = "----FinLedgerBoundary7MA4YWxkTrZu0gW"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode("utf-8") + content + f"\r\n--{boundary}--\r\n".encode("utf-8")
    headers = _headers(json_body=False)
    headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    headers["X-Atlassian-Token"] = "no-check"
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8") or "[]"
        return json.loads(raw)


if not force_fail:
    result["status"] = "skipped_no_failure"
    dbutils.notebook.exit(json.dumps(result))

error = "PL_NTF-DEMO-FAIL: intentional enterprise failure path (force_fail=true)"
log_text = "\n".join(
    [
        "FinLedger ADF incident run log",
        f"utc_time     : {datetime.now(timezone.utc).isoformat()}",
        f"pipeline     : {pipeline_name}",
        f"run_id       : {run_id}",
        f"status       : Failed",
        f"error        : {error}",
    ]
)

summary = f"[ADF] {pipeline_name} failed — run_id={run_id}"
description = (
    f"Automated incident from FinLedger enterprise notify lab.\n\n"
    f"Pipeline: {pipeline_name}\nRun ID: {run_id}\nError: {error}\n"
)

jira_key = None
jira_error = None
try:
    created = http_json(
        "POST",
        f"{jira_base}/rest/api/2/issue",
        {
            "fields": {
                "project": {"key": "FIN"},
                "summary": summary,
                "description": description,
                "issuetype": {"name": "Bug"},
            }
        },
    )
    jira_key = created.get("key")
    print("Jira created:", jira_key)
    http_multipart(
        f"{jira_base}/rest/api/2/issue/{jira_key}/attachments",
        f"adf-run-log-{run_id}.txt",
        log_text.encode("utf-8"),
    )
    print("Attachment uploaded")
    http_json(
        "POST",
        f"{jira_base}/rest/api/2/issue/{jira_key}/comment",
        {"body": f"Email notification queued to {owner_email}"},
    )
except Exception as exc:  # noqa: BLE001
    jira_error = str(exc)
    print("Jira call failed (expected if URL is localhost from Azure):", jira_error)

mail_resp = None
mail_error = None
notify = mail_url
if "logic.azure.com" not in notify and not notify.endswith("/notify"):
    if "127.0.0.1:18081" in notify or "localhost:18081" in notify:
        notify = f"{notify}/notify"
body = (
    f"ADF pipeline failure.\n\n"
    f"Pipeline : {pipeline_name}\n"
    f"Run ID   : {run_id}\n"
    f"Jira     : {jira_key or '(not created — check jira_base_url)'}\n"
    f"Error    : {error}\n"
)
try:
    mail_resp = http_json(
        "POST",
        notify,
        {
            "to": owner_email,
            "subject": f"[FinLedger] Incident {jira_key or run_id} — {pipeline_name}",
            "body": body,
            "jira_key": jira_key,
            "run_id": run_id,
        },
    )
    print("Mail queued:", mail_resp)
except Exception as exc:  # noqa: BLE001
    mail_error = str(exc)
    print("Mail call failed (expected if URL is localhost from Azure):", mail_error)

result.update(
    {
        "status": "incident_attempted",
        "jira_key": jira_key,
        "jira_error": jira_error,
        "mail": mail_resp,
        "mail_error": mail_error,
        "hint": "For visible local Jira/mail UI run enterprise-notify.cmd on Windows",
    }
)
dbutils.notebook.exit(json.dumps(result))
