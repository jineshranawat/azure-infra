"""Create finledger Unity Catalog + bronze/silver/gold schemas via REST API.

When an access connector exists: creates storage credential, external locations,
catalog, and medallion schemas (idempotent).

Without access connector: skips API (notebooks use CREATE CATALOG ... MANAGED LOCATION '').
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

from _config import ENV_FILE, _load_dotenv

from unity_catalog_prep import MEDALLION_LAYERS, layer_uc_abfss

logger = logging.getLogger(__name__)

CATALOG = "finledger"
CREDENTIAL_NAME = "finledger-storage"
_API_TIMEOUT = 60


def resolve_databricks_token() -> str:
    token = (
        os.environ.get("DATABRICKS_TOKEN") or _load_dotenv(ENV_FILE).get("DATABRICKS_TOKEN", "")
    ).strip()
    if not token:
        raise SystemExit(
            "DATABRICKS_TOKEN missing in .env — required for Unity Catalog API bootstrap.\n"
            "Databricks → User settings → Developer → Access tokens"
        )
    return token


def _api(
    host: str,
    token: str,
    method: str,
    path: str,
    body: dict | None = None,
) -> tuple[int, dict | str]:
    url = f"{host.rstrip('/')}{path}"
    payload = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=payload,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=_API_TIMEOUT) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw


def _already_exists(status: int, body: dict | str) -> bool:
    if status in (409, 400):
        text = json.dumps(body) if isinstance(body, dict) else str(body)
        lower = text.lower()
        return "already exists" in lower or "already_exist" in lower
    return False


def _ensure_storage_credential(host: str, token: str, access_connector_id: str) -> None:
    status, _ = _api(host, token, "GET", f"/api/2.1/unity-catalog/storage-credentials/{CREDENTIAL_NAME}")
    if status == 200:
        logger.info("Storage credential '%s' already present", CREDENTIAL_NAME)
        return

    status, body = _api(
        host,
        token,
        "POST",
        "/api/2.1/unity-catalog/storage-credentials",
        {
            "name": CREDENTIAL_NAME,
            "azure_managed_identity": {"access_connector_id": access_connector_id},
        },
    )
    if status in (200, 201) or _already_exists(status, body):
        logger.info("Storage credential '%s' ready", CREDENTIAL_NAME)
        return
    raise SystemExit(f"Failed storage credential: HTTP {status} {body}")


def _ensure_external_location(host: str, token: str, name: str, url: str) -> None:
    status, _ = _api(host, token, "GET", f"/api/2.1/unity-catalog/external-locations/{name}")
    if status == 200:
        logger.info("External location '%s' already present", name)
        return

    status, body = _api(
        host,
        token,
        "POST",
        "/api/2.1/unity-catalog/external-locations",
        {
            "name": name,
            "url": url,
            "credential_name": CREDENTIAL_NAME,
            "read_only": False,
        },
    )
    if status in (200, 201) or _already_exists(status, body):
        logger.info("External location '%s' → %s", name, url)
        return
    raise SystemExit(f"Failed external location '{name}': HTTP {status} {body}")


def _ensure_catalog(host: str, token: str, storage_root: str) -> None:
    status, _ = _api(host, token, "GET", f"/api/2.1/unity-catalog/catalogs/{CATALOG}")
    if status == 200:
        logger.info("Catalog '%s' already present", CATALOG)
        return

    status, body = _api(
        host,
        token,
        "POST",
        "/api/2.1/unity-catalog/catalogs",
        {"name": CATALOG, "storage_root": storage_root},
    )
    if status in (200, 201) or _already_exists(status, body):
        logger.info("Catalog '%s' → %s", CATALOG, storage_root)
        return
    raise SystemExit(f"Failed catalog '{CATALOG}': HTTP {status} {body}")


def _ensure_schema(host: str, token: str, layer: str, storage_root: str) -> None:
    full_name = f"{CATALOG}.{layer}"
    status, _ = _api(host, token, "GET", f"/api/2.1/unity-catalog/schemas/{full_name}")
    if status == 200:
        logger.info("Schema '%s' already present", full_name)
        return

    status, body = _api(
        host,
        token,
        "POST",
        "/api/2.1/unity-catalog/schemas",
        {"catalog_name": CATALOG, "name": layer, "storage_root": storage_root},
    )
    if status in (200, 201) or _already_exists(status, body):
        logger.info("Schema '%s' → %s", full_name, storage_root)
        return
    raise SystemExit(f"Failed schema '{full_name}': HTTP {status} {body}")


def bootstrap_finledger_catalog(
    host: str,
    storage_account: str,
    *,
    token: str | None = None,
    access_connector_id: str | None = None,
) -> bool:
    """Create UC infrastructure when access connector exists. Returns True if bootstrapped."""
    if not access_connector_id:
        logger.info(
            "Unity Catalog API skipped — no access connector on workspace.\n"
            "  Run notebook cell 2: CREATE CATALOG finledger MANAGED LOCATION ''\n"
            "  Or redeploy Class-1 (git pull + repo root orchestrate.cmd) for access connector."
        )
        return False

    pat = token or resolve_databricks_token()
    logger.info("Unity Catalog API bootstrap (access connector present)")

    _ensure_storage_credential(host, pat, access_connector_id)

    for layer in MEDALLION_LAYERS:
        container_url = f"abfss://{layer}@{storage_account}.dfs.core.windows.net/"
        _ensure_external_location(host, pat, f"finledger-{layer}", container_url)

    catalog_root = layer_uc_abfss(storage_account, "silver")
    _ensure_catalog(host, pat, catalog_root)

    for layer in MEDALLION_LAYERS:
        _ensure_schema(host, pat, layer, layer_uc_abfss(storage_account, layer))

    logger.info("Unity Catalog ready — Catalog → %s → bronze | silver | gold", CATALOG)
    return True
