"""Local mail webhook + HTML outbox (Logic App substitute for class).

POST /notify  — ADF Web / Python client posts JSON
GET  /        — list emails
GET  /mail/{id} — view one message
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTBOX_FILE = DATA_DIR / "outbox.json"
EML_DIR = DATA_DIR / "eml"
EML_DIR.mkdir(parents=True, exist_ok=True)

_lock = threading.Lock()
app = FastAPI(title="FinLedger Local Mail Sink", version="1.0.0")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load() -> list:
    if not OUTBOX_FILE.is_file():
        return []
    return json.loads(OUTBOX_FILE.read_text(encoding="utf-8"))


def _save(items: list) -> None:
    OUTBOX_FILE.write_text(json.dumps(items, indent=2), encoding="utf-8")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "mail-sink"}


@app.get("/", response_class=HTMLResponse)
def inbox() -> str:
    with _lock:
        items = _load()
    rows = []
    for m in reversed(items):
        rows.append(
            f"<tr><td><a href='/mail/{m['id']}'>{m['id']}</a></td>"
            f"<td>{m.get('to', '')}</td>"
            f"<td>{m.get('subject', '')}</td>"
            f"<td>{m.get('created', '')}</td></tr>"
        )
    body = "\n".join(rows) or "<tr><td colspan='4'>No mail yet — run enterprise-notify.cmd</td></tr>"
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>Local mail outbox</title>
<style>
 body{{font-family:Segoe UI,sans-serif;margin:2rem;background:#0c1118;color:#e8eef6}}
 a{{color:#4aa3f0}} table{{border-collapse:collapse;width:100%}}
 th,td{{border:1px solid #243247;padding:.55rem .7rem;text-align:left}}
 th{{background:#162032}}
</style></head><body>
<h1>Local mail outbox</h1>
<p>POST JSON to <code>/notify</code> (same shape as a Logic App HTTP trigger body).</p>
<table>
<thead><tr><th>Id</th><th>To</th><th>Subject</th><th>Created</th></tr></thead>
<tbody>{body}</tbody>
</table>
<p>Jira board: <a href="http://127.0.0.1:18080/">http://127.0.0.1:18080/</a></p>
</body></html>"""


@app.get("/mail/{mail_id}", response_class=HTMLResponse)
def view_mail(mail_id: str) -> str:
    with _lock:
        items = _load()
    mail = next((m for m in items if m["id"] == mail_id), None)
    if not mail:
        raise HTTPException(404, "Mail not found")
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>{mail.get('subject')}</title>
<style>
 body{{font-family:Segoe UI,sans-serif;margin:2rem;background:#0c1118;color:#e8eef6}}
 a{{color:#4aa3f0}} pre{{background:#080d14;padding:1rem;border-radius:8px;white-space:pre-wrap}}
</style></head><body>
<p><a href="/">← Inbox</a></p>
<h1>{mail.get('subject')}</h1>
<p><strong>To:</strong> {mail.get('to')}<br/>
<strong>From:</strong> {mail.get('from', 'adf-notify@finledger.local')}<br/>
<strong>Created:</strong> {mail.get('created')}</p>
<pre>{mail.get('body', '')}</pre>
</body></html>"""


@app.post("/notify")
async def notify(payload: dict) -> JSONResponse:
    """Accept Logic-App-like JSON: to, subject, body, plus optional jira_key / pipeline_run_id."""
    to_addr = payload.get("to") or payload.get("owner_email") or "training@example.com"
    subject = payload.get("subject") or "ADF incident notification"
    body = payload.get("body") or payload.get("message") or json.dumps(payload, indent=2)
    mail_id = str(uuid.uuid4())[:8]
    item = {
        "id": mail_id,
        "to": to_addr,
        "from": payload.get("from", "adf-notify@finledger.local"),
        "subject": subject,
        "body": body if isinstance(body, str) else json.dumps(body, indent=2),
        "jira_key": payload.get("jira_key"),
        "pipeline_run_id": payload.get("pipeline_run_id") or payload.get("run_id"),
        "created": _now(),
        "raw": payload,
    }
    eml = (
        f"From: {item['from']}\r\n"
        f"To: {item['to']}\r\n"
        f"Subject: {item['subject']}\r\n"
        f"Date: {item['created']}\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        f"{item['body']}\r\n"
    )
    (EML_DIR / f"{mail_id}.eml").write_text(eml, encoding="utf-8")
    with _lock:
        items = _load()
        items.append(item)
        _save(items)
    return JSONResponse(
        {"status": "sent", "id": mail_id, "outbox": f"http://127.0.0.1:18081/mail/{mail_id}"},
        status_code=202,
    )


def main() -> None:
    import uvicorn

    uvicorn.run(
        "app:app",
        host="127.0.0.1",
        port=18081,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
