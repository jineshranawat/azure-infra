"""Mail notify client — posts to local mail sink or Logic App HTTP URL."""

from __future__ import annotations

from typing import Any

import httpx


class MailClient:
    def __init__(self, notify_url: str, *, timeout: float = 30.0) -> None:
        self.notify_url = notify_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> MailClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def send(
        self,
        *,
        to: str,
        subject: str,
        body: str,
        jira_key: str | None = None,
        run_id: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "to": to,
            "owner_email": to,
            "subject": subject,
            "body": body,
            "message": body,
            "jira_key": jira_key,
            "run_id": run_id,
            "pipeline_run_id": run_id,
        }
        if extra:
            payload.update(extra)
        # Local sink uses /notify; Logic App URL is the full trigger URL (POST as-is)
        url = self.notify_url
        if "logic.azure.com" not in url and not url.rstrip("/").endswith("/notify"):
            if "127.0.0.1:18081" in url or "localhost:18081" in url:
                url = f"{url.rstrip('/')}/notify"
        resp = self._client.post(url, json=payload)
        resp.raise_for_status()
        if resp.content:
            try:
                return resp.json()
            except Exception:
                return {"status": "sent", "raw": resp.text[:500]}
        return {"status": "sent"}
