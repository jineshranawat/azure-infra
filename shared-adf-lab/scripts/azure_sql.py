"""Deploy Azure SQL in westus + seed reference table (idempotent)."""

from __future__ import annotations

import json
import logging
import secrets
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError

from _config import SharedAdfConfig, find_az, generate_sql_password, get_credential
from discover import SharedEstate

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SQL_BICEP = REPO_ROOT / "infra" / "shared-sql-westus.bicep"
SQL_ADMIN_LOGIN = "finledgeradmin"


@dataclass(frozen=True)
class SqlEstate:
    server_name: str
    fqdn: str
    database_name: str
    admin_login: str
    admin_password: str
    location: str = "westus"


def _get_sql_password_from_kv(cfg: SharedAdfConfig, key_vault: str) -> str | None:
    if not key_vault:
        return None
    try:
        from azure.keyvault.secrets import SecretClient

        vault_url = f"https://{key_vault}.vault.azure.net"
        client = SecretClient(vault_url=vault_url, credential=get_credential())
        return client.get_secret("sql-admin-password").value
    except ImportError:
        pass
    except ResourceNotFoundError:
        return None
    except Exception as exc:
        logger.warning("Key Vault SDK read failed: %s", exc)

    # Fallback: Azure CLI (no azure-keyvault-secrets package required)
    try:
        import json
        import subprocess

        result = subprocess.run(
            [
                find_az(),
                "keyvault",
                "secret",
                "show",
                "--vault-name",
                key_vault,
                "--name",
                "sql-admin-password",
                "--query",
                "value",
                "-o",
                "tsv",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        )
        value = result.stdout.strip()
        return value or None
    except subprocess.CalledProcessError:
        return None
    except Exception as exc:
        logger.warning("Could not read SQL password from Key Vault: %s", exc)
        return None


def deploy_sql(cfg: SharedAdfConfig, estate: SharedEstate) -> SqlEstate:
    """Incremental Bicep deploy for westus SQL — safe to re-run."""
    if not estate.key_vault:
        raise SystemExit("Key Vault required. Run provision-shared.cmd first.")

    password = _get_sql_password_from_kv(cfg, estate.key_vault)
    if not password:
        password = generate_sql_password()
        logger.info("Generated new SQL admin password (stored in Key Vault on deploy)")

    if not estate.name_hash:
        raise SystemExit("Could not derive name hash from storage/ADF names.")

    params = {
        "location": {"value": cfg.sql_location},
        "nameHash": {"value": estate.name_hash},
        "keyVaultName": {"value": estate.key_vault},
        "sqlAdminLogin": {"value": SQL_ADMIN_LOGIN},
        "sqlAdminPassword": {"value": password},
        "ownerEmail": {"value": cfg.owner_email},
    }
    logger.info("Deploying SQL to %s (Basic tier)...", cfg.sql_location)
    deployment_name = "shared-sql-westus"
    subprocess.run(
        [
            find_az(),
            "deployment",
            "group",
            "create",
            "--resource-group",
            cfg.resource_group,
            "--name",
            deployment_name,
            "--template-file",
            str(SQL_BICEP),
            "--parameters",
            json.dumps(params),
            "--mode",
            "Incremental",
            "--only-show-errors",
        ],
        check=True,
        text=True,
    )

    show = subprocess.run(
        [
            find_az(),
            "deployment",
            "group",
            "show",
            "-g",
            cfg.resource_group,
            "-n",
            deployment_name,
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    outputs = json.loads(show.stdout).get("properties", {}).get("outputs", {})
    server_name = outputs.get("sqlServerName", {}).get("value", f"sql-shared-{estate.name_hash}")
    fqdn = outputs.get("sqlServerFqdn", {}).get("value", f"{server_name}.database.windows.net")
    database_name = outputs.get("sqlDatabaseName", {}).get("value", "finledger")

    sql = SqlEstate(
        server_name=server_name,
        fqdn=fqdn,
        database_name=database_name,
        admin_login=SQL_ADMIN_LOGIN,
        admin_password=password,
    )
    logger.info("SQL ready: %s / %s (%s)", sql.fqdn, sql.database_name, sql.location)
    return sql


def seed_reference_table(sql: SqlEstate) -> None:
    """Create dim_channel table if missing — idempotent DDL."""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={sql.fqdn};DATABASE={sql.database_name};"
        f"UID={sql.admin_login};PWD={sql.admin_password};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    ddl = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_channel')
    CREATE TABLE dbo.dim_channel (
        channel_code NVARCHAR(32) PRIMARY KEY,
        channel_name NVARCHAR(64) NOT NULL,
        is_active BIT NOT NULL DEFAULT 1
    );
    IF NOT EXISTS (SELECT 1 FROM dbo.dim_channel)
    INSERT INTO dbo.dim_channel (channel_code, channel_name) VALUES
        ('card', 'Card payments'),
        ('wire', 'Wire transfer'),
        ('UNKNOWN', 'Unknown channel');

    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'stg_channel_from_lake')
    CREATE TABLE dbo.stg_channel_from_lake (
        channel_code NVARCHAR(32),
        channel_name NVARCHAR(64)
    );
    """
    try:
        try:
            import pyodbc  # type: ignore
        except ImportError:
            logger.info("pyodbc missing — installing into current Python environment...")
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "pyodbc"],
                check=True,
                text=True,
            )
            import pyodbc  # type: ignore

        with pyodbc.connect(conn_str, timeout=30) as conn:
            conn.autocommit = True
            conn.cursor().execute(ddl)
        logger.info("SQL seed table dbo.dim_channel ready")
    except ImportError:
        logger.warning(
            "pyodbc not installed — skip SQL seed. Run: pip install pyodbc"
        )
    except subprocess.CalledProcessError as exc:
        logger.warning("pyodbc install failed — skip SQL seed (%s)", exc)
    except Exception as exc:
        logger.warning("SQL seed skipped (run manually in SSMS): %s", exc)
