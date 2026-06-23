#!/usr/bin/env python3
"""Create a Microsoft Fabric workspace and assign it to a Fabric capacity.

Idempotent: lists existing workspaces first; if displayName matches, skips create.

Requires:
  - Fabric capacity already deployed (infra/platform-services.bicep)
  - Signed-in user with Fabric admin / workspace create rights
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from azure.identity import AzureCliCredential

logger = logging.getLogger(__name__)

# Fabric REST API base — workspace create is control-plane, not Azure ARM.
FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s — %(message)s")


def _fabric_token() -> str:
    """Acquire OAuth token via az login session (DefaultAzureCredential chain)."""
    credential = AzureCliCredential()
    return credential.get_token(FABRIC_SCOPE).token


def _request(
    method: str,
    path: str,
    token: str,
    body: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any] | None, dict[str, str]]:
    """Low-level Fabric API call; returns (status, json_body, response_headers)."""
    url = f"{FABRIC_API}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode()
            headers = dict(resp.headers)
            parsed = json.loads(raw) if raw else None
            return resp.status, parsed, headers
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        logger.error("Fabric API %s %s failed (%s): %s", method, path, exc.code, raw)
        try:
            return exc.code, json.loads(raw), dict(exc.headers)
        except json.JSONDecodeError:
            return exc.code, None, dict(exc.headers)


def _find_workspace_by_display_name(display_name: str, token: str) -> dict[str, Any] | None:
    """List workspaces (paginated) and return the first match on displayName — idempotent skip."""
    continuation: str | None = None
    while True:
        path = "/workspaces"
        if continuation:
            path = f"/workspaces?continuationToken={urllib.parse.quote(continuation)}"
        status, body, _ = _request("GET", path, token)
        if status != 200 or not body:
            return None
        for ws in body.get("value", []):
            if ws.get("displayName") == display_name:
                return ws
        continuation = body.get("continuationToken")
        if not continuation:
            return None


def _wait_workspace(token: str, workspace_id: str, timeout_s: int = 120) -> dict[str, Any] | None:
    """Poll async workspace provisioning until GET returns 200."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status, body, _ = _request("GET", f"/workspaces/{workspace_id}", token)
        if status == 200 and body:
            return body
        time.sleep(3)
    return None


def create_fabric_workspace(
    display_name: str,
    capacity_id: str,
    *,
    description: str = "Class-1 training workspace",
) -> dict[str, Any]:
    """Create workspace on capacity, or return existing workspace if displayName already present."""
    token = _fabric_token()

    existing = _find_workspace_by_display_name(display_name, token)
    if existing:
        logger.info("Fabric workspace already present: %s (%s)", display_name, existing.get("id", ""))
        return existing

    payload = {
        "displayName": display_name,
        "description": description,
        "capacityId": capacity_id,
    }
    status, body, headers = _request("POST", "/workspaces", token, payload)

    if status in (200, 201) and body:
        logger.info("Fabric workspace created: %s", body.get("id", body))
        return body

    if status == 202:
        location = headers.get("Location") or headers.get("location", "")
        workspace_id = location.rstrip("/").split("/")[-1] if location else ""
        if workspace_id:
            logger.info("Fabric workspace provisioning (async): %s", workspace_id)
            result = _wait_workspace(token, workspace_id)
            if result:
                return result
        raise RuntimeError(f"Async workspace create accepted but poll failed (Location={location})")

    raise RuntimeError(f"Fabric workspace create failed with HTTP {status}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Microsoft Fabric workspace on a capacity.")
    parser.add_argument("--workspace-name", required=True, help="Fabric workspace display name")
    parser.add_argument("--capacity-id", required=True, help="Fabric capacity ARM resource ID")
    parser.add_argument("--description", default="Class-1 training workspace")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)

    try:
        ws = create_fabric_workspace(
            args.workspace_name,
            args.capacity_id,
            description=args.description,
        )
    except Exception:
        logger.exception("Fabric workspace creation failed")
        return 1

    ws_id = ws.get("id", "")
    logger.info("Workspace ID: %s", ws_id)
    logger.info("Open Fabric: https://app.fabric.microsoft.com/")
    if ws_id:
        logger.info("Workspace URL: https://app.fabric.microsoft.com/groups/%s", ws_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
