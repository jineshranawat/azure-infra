"""Jira REST client — works against local mock or real Jira Cloud."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any

import httpx


class JiraClient:
    def __init__(
        self,
        base_url: str,
        *,
        user: str = "",
        api_token: str = "",
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if user and api_token:
            token = base64.b64encode(f"{user}:{api_token}".encode()).decode("ascii")
            headers["Authorization"] = f"Basic {token}"
        self._client = httpx.Client(base_url=self.base_url, headers=headers, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> JiraClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def create_issue(
        self,
        *,
        summary: str,
        description: str,
        project_key: str = "FIN",
        issue_type: str = "Bug",
    ) -> dict[str, Any]:
        payload = {
            "fields": {
                "project": {"key": project_key},
                "summary": summary,
                "description": description,
                "issuetype": {"name": issue_type},
            }
        }
        resp = self._client.post("/rest/api/2/issue", json=payload)
        resp.raise_for_status()
        return resp.json()

    def attach_file(self, issue_key: str, file_path: Path) -> list | dict:
        path = Path(file_path)
        # Multipart upload — must NOT send application/json Content-Type.
        headers = {
            "X-Atlassian-Token": "no-check",
            "Accept": "application/json",
        }
        if "Authorization" in self._client.headers:
            headers["Authorization"] = self._client.headers["Authorization"]
        with path.open("rb") as fh:
            files = {"file": (path.name, fh, "application/octet-stream")}
            resp = httpx.post(
                f"{self.base_url}/rest/api/2/issue/{issue_key}/attachments",
                files=files,
                headers=headers,
                timeout=30.0,
            )
        resp.raise_for_status()
        return resp.json()

    def add_comment(self, issue_key: str, body: str) -> dict[str, Any]:
        resp = self._client.post(
            f"/rest/api/2/issue/{issue_key}/comment",
            json={"body": body},
        )
        resp.raise_for_status()
        return resp.json()

    def get_issue(self, issue_key: str) -> dict[str, Any]:
        resp = self._client.get(f"/rest/api/2/issue/{issue_key}")
        resp.raise_for_status()
        return resp.json()
