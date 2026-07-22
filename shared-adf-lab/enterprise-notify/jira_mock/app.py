"""Local Jira-compatible REST mock + simple ticket UI (class lab — not Atlassian Docker).

API shape matches Jira Cloud enough for teaching:
  POST /rest/api/2/issue
  GET  /rest/api/2/issue/{key}
  POST /rest/api/2/issue/{key}/attachments
  POST /rest/api/2/issue/{key}/comment
  GET  /  (HTML ticket board)
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
ISSUES_FILE = DATA_DIR / "issues.json"
ATTACH_DIR = DATA_DIR / "attachments"
ATTACH_DIR.mkdir(parents=True, exist_ok=True)

_lock = threading.Lock()
app = FastAPI(title="FinLedger Local Jira Mock", version="1.0.0")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load() -> dict:
    if not ISSUES_FILE.is_file():
        return {"seq": 0, "issues": {}}
    return json.loads(ISSUES_FILE.read_text(encoding="utf-8"))


def _save(store: dict) -> None:
    ISSUES_FILE.write_text(json.dumps(store, indent=2), encoding="utf-8")


def _issue_payload(key: str, issue: dict) -> dict:
    return {
        "id": issue["id"],
        "key": key,
        "self": f"http://127.0.0.1:18080/rest/api/2/issue/{key}",
        "fields": {
            "summary": issue["summary"],
            "description": issue.get("description", ""),
            "project": {"key": issue.get("project", "FIN")},
            "issuetype": {"name": issue.get("issuetype", "Bug")},
            "status": {"name": issue.get("status", "Open")},
            "created": issue.get("created"),
            "attachment": issue.get("attachments", []),
            "comment": {"comments": issue.get("comments", [])},
        },
    }


@app.get("/", response_class=HTMLResponse)
def board() -> str:
    with _lock:
        store = _load()
    rows = []
    for key, issue in sorted(store.get("issues", {}).items(), reverse=True):
        n_att = len(issue.get("attachments", []))
        rows.append(
            f"<tr><td><a href='/issue/{key}'>{key}</a></td>"
            f"<td>{issue.get('summary', '')}</td>"
            f"<td>{issue.get('status', 'Open')}</td>"
            f"<td>{n_att}</td>"
            f"<td>{issue.get('created', '')}</td></tr>"
        )
    body = "\n".join(rows) or "<tr><td colspan='5'>No tickets yet — run enterprise-notify.cmd</td></tr>"
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>Local Jira — FinLedger</title>
<style>
 body{{font-family:Segoe UI,sans-serif;margin:2rem;background:#0c1118;color:#e8eef6}}
 a{{color:#4aa3f0}} table{{border-collapse:collapse;width:100%}}
 th,td{{border:1px solid #243247;padding:.55rem .7rem;text-align:left}}
 th{{background:#162032}} .badge{{color:#3ecf8e}}
</style></head><body>
<h1>Local Jira mock <span class="badge">(FinLedger class)</span></h1>
<p>REST compatible with <code>POST /rest/api/2/issue</code>. Not Atlassian Docker — swap
<code>JIRA_BASE_URL</code> to real Jira Cloud later.</p>
<table>
<thead><tr><th>Key</th><th>Summary</th><th>Status</th><th>Attachments</th><th>Created</th></tr></thead>
<tbody>{body}</tbody>
</table>
<p><a href="/rest/api/2/search">JSON search</a> · Mail outbox: <a href="http://127.0.0.1:18081/">http://127.0.0.1:18081/</a></p>
</body></html>"""


@app.get("/issue/{key}", response_class=HTMLResponse)
def issue_page(key: str) -> str:
    with _lock:
        store = _load()
        issue = store.get("issues", {}).get(key)
    if not issue:
        raise HTTPException(404, f"Issue {key} not found")
    atts = "".join(
        f"<li>{a.get('filename')} ({a.get('size', 0)} bytes)</li>" for a in issue.get("attachments", [])
    ) or "<li>(none)</li>"
    comments = "".join(
        f"<li><pre>{c.get('body', '')}</pre></li>" for c in issue.get("comments", [])
    ) or "<li>(none)</li>"
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>{key}</title>
<style>
 body{{font-family:Segoe UI,sans-serif;margin:2rem;background:#0c1118;color:#e8eef6}}
 a{{color:#4aa3f0}} pre{{background:#080d14;padding:1rem;border-radius:8px;white-space:pre-wrap}}
</style></head><body>
<p><a href="/">← Board</a></p>
<h1>{key}</h1>
<p><strong>{issue.get('summary')}</strong></p>
<pre>{issue.get('description', '')}</pre>
<h3>Attachments</h3><ul>{atts}</ul>
<h3>Comments</h3><ul>{comments}</ul>
</body></html>"""


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "jira-mock"}


@app.post("/rest/api/2/issue")
async def create_issue(payload: dict) -> JSONResponse:
    fields = payload.get("fields") or {}
    summary = (fields.get("summary") or "Untitled").strip()
    description = fields.get("description") or ""
    project = (fields.get("project") or {}).get("key") or "FIN"
    issuetype = (fields.get("issuetype") or {}).get("name") or "Bug"
    with _lock:
        store = _load()
        store["seq"] = int(store.get("seq", 0)) + 1
        key = f"{project}-{store['seq']}"
        issue_id = str(uuid.uuid4().int)[:8]
        store.setdefault("issues", {})[key] = {
            "id": issue_id,
            "summary": summary,
            "description": description if isinstance(description, str) else json.dumps(description),
            "project": project,
            "issuetype": issuetype,
            "status": "Open",
            "created": _now(),
            "attachments": [],
            "comments": [],
        }
        _save(store)
    return JSONResponse(
        {
            "id": issue_id,
            "key": key,
            "self": f"http://127.0.0.1:18080/rest/api/2/issue/{key}",
        },
        status_code=201,
    )


@app.get("/rest/api/2/issue/{key}")
def get_issue(key: str) -> dict:
    with _lock:
        store = _load()
        issue = store.get("issues", {}).get(key)
    if not issue:
        raise HTTPException(404, f"Issue {key} not found")
    return _issue_payload(key, issue)


@app.get("/rest/api/2/search")
def search() -> dict:
    with _lock:
        store = _load()
    issues = [_issue_payload(k, v) for k, v in store.get("issues", {}).items()]
    return {"startAt": 0, "maxResults": len(issues), "total": len(issues), "issues": issues}


@app.post("/rest/api/2/issue/{key}/attachments")
async def add_attachment(
    key: str,
    file: UploadFile = File(...),
) -> list:
    with _lock:
        store = _load()
        issue = store.get("issues", {}).get(key)
        if not issue:
            raise HTTPException(404, f"Issue {key} not found")
        raw = await file.read()
        dest = ATTACH_DIR / f"{key}_{file.filename or 'attach.bin'}"
        dest.write_bytes(raw)
        meta = {
            "id": str(uuid.uuid4())[:8],
            "filename": file.filename or dest.name,
            "size": len(raw),
            "created": _now(),
            "content": str(dest),
        }
        issue.setdefault("attachments", []).append(meta)
        _save(store)
    return [meta]


@app.post("/rest/api/2/issue/{key}/comment")
async def add_comment(key: str, payload: dict) -> dict:
    body = payload.get("body") or ""
    with _lock:
        store = _load()
        issue = store.get("issues", {}).get(key)
        if not issue:
            raise HTTPException(404, f"Issue {key} not found")
        comment = {"id": str(uuid.uuid4())[:8], "body": body, "created": _now()}
        issue.setdefault("comments", []).append(comment)
        _save(store)
    return comment


def main() -> None:
    import uvicorn

    uvicorn.run(
        "app:app",
        host="127.0.0.1",
        port=18080,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
