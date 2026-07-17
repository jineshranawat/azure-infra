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

_ODBC_DRIVERS = (
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server",
)


def _connect_pyodbc(sql: SqlEstate):
    """Open pyodbc connection — tries installed SQL Server ODBC drivers."""
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

    errors: list[str] = []
    for driver in _ODBC_DRIVERS:
        encrypt = "yes" if "ODBC Driver" in driver else "no"
        trust = "yes" if driver == "SQL Server" else "no"
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={sql.fqdn};DATABASE={sql.database_name};"
            f"UID={sql.admin_login};PWD={sql.admin_password};"
            f"Encrypt={encrypt};TrustServerCertificate={trust};"
        )
        try:
            return pyodbc.connect(conn_str, timeout=30)
        except Exception as exc:
            errors.append(f"{driver}: {exc}")
    raise RuntimeError("; ".join(errors))


def _execute_sql_scripts(sql: SqlEstate, *scripts: str, label: str) -> None:
    """Run one or more SQL batches idempotently."""
    try:
        with _connect_pyodbc(sql) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            for script in scripts:
                cursor.execute(script)
        logger.info("SQL seed %s ready", label)
    except ImportError:
        logger.warning("pyodbc not installed — skip SQL seed (%s)", label)
    except subprocess.CalledProcessError as exc:
        logger.warning("pyodbc install failed — skip SQL seed %s (%s)", label, exc)
    except Exception as exc:
        logger.warning("SQL seed %s skipped: %s", label, exc)


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
    _execute_sql_scripts(sql, ddl, label="dbo.dim_channel")


def seed_change_tracking(sql: SqlEstate) -> None:
    """Enable SQL change tracking on dim_channel — idempotent (CDC teaching path)."""
    ddl = """
    IF NOT EXISTS (
        SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID()
    )
    BEGIN
        ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON
            (CHANGE_RETENTION = 2, AUTO_CLEANUP = ON);
    END;

    IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables
                   WHERE object_id = OBJECT_ID('dbo.dim_channel'))
    BEGIN
        ALTER TABLE dbo.dim_channel ENABLE CHANGE_TRACKING
            WITH (TRACK_COLUMNS_UPDATED = OFF);
    END;

    -- Seed a row that pl_24 can mutate on each run to produce CDC output.
    IF NOT EXISTS (SELECT 1 FROM dbo.dim_channel WHERE channel_code = 'fps')
        INSERT INTO dbo.dim_channel (channel_code, channel_name, is_active)
        VALUES ('fps', 'Faster Payments', 1);
    """
    _execute_sql_scripts(sql, ddl, label="change_tracking.dim_channel")


def seed_job_metadata_table(sql: SqlEstate) -> None:
    """Create adf_job_metadata control table + stored proc — idempotent DDL/DML."""
    ddl = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'adf_job_metadata')
    CREATE TABLE dbo.adf_job_metadata (
        job_id NVARCHAR(32) PRIMARY KEY,
        job_name NVARCHAR(64) NOT NULL,
        incoming_folder NVARCHAR(256) NOT NULL,
        loaded_folder NVARCHAR(256) NOT NULL,
        expected_file_name NVARCHAR(128) NOT NULL,
        run_databricks INT NOT NULL DEFAULT 1,
        status NVARCHAR(32) NOT NULL DEFAULT 'READY',
        last_run_id NVARCHAR(64) NULL,
        last_message NVARCHAR(256) NULL,
        updated_utc DATETIME2 NULL
    );

    IF NOT EXISTS (SELECT 1 FROM dbo.adf_job_metadata WHERE job_id = 'job-01')
    INSERT INTO dbo.adf_job_metadata (
        job_id, job_name, incoming_folder, loaded_folder,
        expected_file_name, run_databricks, status
    ) VALUES (
        'job-01', 'bronze_metadata_orchestrate',
        'incoming/run=session3-lab', 'loaded/run=session3-lab',
        'sample_transactions.csv', 1, 'READY'
    );

    IF NOT EXISTS (SELECT 1 FROM dbo.adf_job_metadata WHERE job_id = 'job-02')
    INSERT INTO dbo.adf_job_metadata (
        job_id, job_name, incoming_folder, loaded_folder,
        expected_file_name, run_databricks, status
    ) VALUES (
        'job-02', 'disabled_skip_demo',
        'incoming/run=session3-lab', 'loaded/run=session3-lab',
        'sample_transactions.csv', 0, 'HOLD'
    );

    -- Reset runnable job so re-runs start from READY (job-02 stays HOLD for skip demo).
    UPDATE dbo.adf_job_metadata
    SET status = 'READY', last_run_id = NULL, last_message = NULL, updated_utc = NULL
    WHERE job_id = 'job-01';
    """
    proc = """
    CREATE OR ALTER PROCEDURE dbo.usp_adf_mark_job_status
        @job_id NVARCHAR(32),
        @status NVARCHAR(32),
        @run_id NVARCHAR(64) = NULL,
        @message NVARCHAR(256) = NULL
    AS
    BEGIN
        UPDATE dbo.adf_job_metadata
        SET status = @status,
            last_run_id = COALESCE(@run_id, last_run_id),
            last_message = COALESCE(@message, last_message),
            updated_utc = GETUTCDATE()
        WHERE job_id = @job_id;
    END
    """
    _execute_sql_scripts(sql, ddl, proc, label="dbo.adf_job_metadata + usp_adf_mark_job_status")


def seed_de_governance_tables(sql: SqlEstate) -> None:
    """SQL metadata model for medallion + 50-problems governance registry."""
    ddl = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'de_table_registry')
    CREATE TABLE dbo.de_table_registry (
        registry_id INT IDENTITY(1,1) PRIMARY KEY,
        run_id NVARCHAR(64) NOT NULL,
        problem_num NVARCHAR(16) NOT NULL,
        qualified_name NVARCHAR(512) NOT NULL,
        medallion_layer NVARCHAR(32) NOT NULL,
        classification NVARCHAR(64) NOT NULL,
        glossary_term NVARCHAR(128) NOT NULL,
        status NVARCHAR(32) NOT NULL DEFAULT 'READY',
        updated_utc DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );

    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'de_pipeline_run_log')
    CREATE TABLE dbo.de_pipeline_run_log (
        run_id NVARCHAR(64) PRIMARY KEY,
        bronze_rows INT NULL,
        silver_rows INT NULL,
        gold_channels INT NULL,
        completed_utc DATETIME2 NULL,
        status NVARCHAR(32) NOT NULL,
        source_notebook NVARCHAR(128) NULL
    );

    IF NOT EXISTS (SELECT 1 FROM dbo.adf_job_metadata WHERE job_id = 'job-03')
    INSERT INTO dbo.adf_job_metadata (
        job_id, job_name, incoming_folder, loaded_folder,
        expected_file_name, run_databricks, status
    ) VALUES (
        'job-03', 'medallion_governance_databricks',
        'incoming/run=session3-lab', 'loaded/run=session3-lab',
        'sample_transactions.csv', 1, 'READY'
    );
    """
    _execute_sql_scripts(sql, ddl, label="dbo.de_table_registry + de_pipeline_run_log + job-03")
