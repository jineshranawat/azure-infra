"""Setup cell + write_demo_table / read_demo_table helpers for 50-problems / teach-all.

Handles PERMISSION_DENIED on workspace catalog (dbw_shared_qgr7mj.demo_problems):
prefers finledger/main demo_problems, tests write access, falls back to ADLS then DBFS.

Serverless tip: never hard-fail on abfss mkdirs when account-key config is blocked.
"""

DEMO_TABLE_HELPERS = '''# =============================================================================
# SETUP — writable Unity Catalog schema OR ADLS / DBFS fallback
# =============================================================================
# WHAT: Find a schema you are allowed to write to (finledger/main.demo_problems).
# WHY:  Shared workspace catalog dbw_shared_* often denies USE SCHEMA to learners.
# HOW:  Probe UC write; if denied, try ADLS Delta; if Serverless blocks key/RBAC → DBFS.
# =============================================================================
from pyspark.sql import functions as F

PREFERRED_CATALOG = "finledger"
FALLBACK_CATALOG = "main"
SCHEMA = "demo_problems"
DEMO_BASE_ADLS = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/demo_50_problems"
DEMO_BASE_DBFS = "dbfs:/FileStore/demo_50_problems"
CATALOG_ROOT = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_unity_catalog/{PREFERRED_CATALOG}"

# Prefer ADLS when RBAC works; otherwise DBFS (Serverless-safe)
DEMO_BASE = DEMO_BASE_ADLS
UC_WRITE_ENABLED = False
WRITTEN_TABLES: list[str] = []


def _try_mkdirs(path: str) -> bool:
    """Create folder; return False if Spark/dbutils cannot talk to that filesystem."""
    try:
        dbutils.fs.mkdirs(path)
        return True
    except Exception as exc:
        print(f"  mkdirs skip ({path[:60]}…):", str(exc)[:120])
        return False


def _try_catalog_write(catalog: str) -> bool:
    """Return True if current user can CREATE TABLE in catalog.demo_problems."""
    full_schema = f"{catalog}.{SCHEMA}"
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    except Exception as exc:
        print(f"  CREATE SCHEMA {full_schema} failed:", str(exc)[:160])
        return False
    try:
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"USE SCHEMA {SCHEMA}")
    except Exception as exc:
        print(f"  USE SCHEMA {full_schema} failed:", str(exc)[:160])
        return False
    test_table = f"{full_schema}._write_probe"
    try:
        spark.createDataFrame([("ok",)], ["ping"]).write.format("delta").mode("overwrite").saveAsTable(
            test_table
        )
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")
        return True
    except Exception as exc:
        print(f"  Write probe on {full_schema} failed:", str(exc)[:160])
        return False


def _probe_delta_path(base: str) -> bool:
    """Confirm we can write+read a tiny Delta folder under base."""
    probe = f"{base}/_write_probe"
    try:
        spark.createDataFrame([("ok",)], ["ping"]).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(probe)
        spark.read.format("delta").load(probe).count()
        return True
    except Exception as exc:
        print(f"  Delta probe failed ({base[:50]}…):", str(exc)[:140])
        return False


def _init_storage():
    global CATALOG, UC_TABLE, UC_WRITE_ENABLED, DEMO_BASE
    names = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    print("Catalogs visible:", names)

    candidates = []
    for c in (PREFERRED_CATALOG, FALLBACK_CATALOG):
        if c in names:
            candidates.append(c)
    if PREFERRED_CATALOG not in names:
        # Creating an external catalog needs a managed location — soft if ADLS blocked
        if _try_mkdirs(CATALOG_ROOT):
            try:
                spark.sql(
                    f"CREATE CATALOG IF NOT EXISTS {PREFERRED_CATALOG} MANAGED LOCATION '{CATALOG_ROOT}'"
                )
                candidates.insert(0, PREFERRED_CATALOG)
            except Exception as exc:
                print("CREATE CATALOG finledger skipped:", str(exc)[:160])
        else:
            print("CREATE CATALOG finledger skipped: no ADLS path for managed location")

    for catalog in candidates:
        print(f"Testing write access: {catalog}.{SCHEMA} ...")
        if _try_catalog_write(catalog):
            CATALOG = catalog
            UC_TABLE = f"{catalog}.{SCHEMA}"
            UC_WRITE_ENABLED = True
            print("UC write mode: ENABLED →", UC_TABLE)
            # Still pick a DEMO_BASE for path demos (topics that write Delta folders)
            if _try_mkdirs(DEMO_BASE_ADLS) and _probe_delta_path(DEMO_BASE_ADLS):
                DEMO_BASE = DEMO_BASE_ADLS
            else:
                _try_mkdirs(DEMO_BASE_DBFS)
                DEMO_BASE = DEMO_BASE_DBFS
                print("Path demos use DBFS:", DEMO_BASE)
            return

    # No UC — try ADLS, then DBFS
    CATALOG = "adls"
    UC_TABLE = SCHEMA
    UC_WRITE_ENABLED = False
    if _try_mkdirs(DEMO_BASE_ADLS) and _probe_delta_path(DEMO_BASE_ADLS):
        DEMO_BASE = DEMO_BASE_ADLS
        print("UC write mode: DISABLED — Delta under ADLS:", DEMO_BASE)
    else:
        _try_mkdirs(DEMO_BASE_DBFS)
        DEMO_BASE = DEMO_BASE_DBFS
        print("UC write mode: DISABLED — Delta under DBFS (Serverless-safe):", DEMO_BASE)


_init_storage()


def write_demo_table(df, table_name: str, mode: str = "overwrite") -> str:
    """Save demo output — Unity Catalog if permitted, else DEMO_BASE Delta path."""
    path = f"{DEMO_BASE}/{table_name}"
    writer = df.write.format("delta").mode(mode)
    if mode.lower() == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if UC_WRITE_ENABLED:
        full_name = f"{UC_TABLE}.{table_name}"
        try:
            writer.saveAsTable(full_name)
            print("UC table :", full_name)
            WRITTEN_TABLES.append(full_name)
            return full_name
        except Exception as exc:
            err = str(exc)
            if "PERMISSION_DENIED" not in err and "UNAUTHORIZED" not in err:
                print("UC write error — trying path fallback:", err[:120])
            else:
                print("UC write denied — falling back to path")
    try:
        writer.save(path)
        print("Delta path:", path)
        WRITTEN_TABLES.append(path)
        return path
    except Exception as exc:
        fallback = f"{DEMO_BASE_DBFS}/{table_name}"
        print(f"Path write failed ({str(exc)[:100]}) — DBFS:", fallback)
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(fallback)
        WRITTEN_TABLES.append(fallback)
        return fallback


def read_demo_table(table_name: str):
    """Read demo table from UC or Delta path (ADLS or DBFS)."""
    if UC_WRITE_ENABLED:
        try:
            return spark.table(f"{UC_TABLE}.{table_name}")
        except Exception:
            pass
    for base in (DEMO_BASE, DEMO_BASE_DBFS, DEMO_BASE_ADLS):
        try:
            return spark.read.format("delta").load(f"{base}/{table_name}")
        except Exception:
            continue
    raise FileNotFoundError(f"Demo table not found: {table_name}")


def demo_table_ref(table_name: str) -> str:
    """SQL-safe name for COMMENT / DESCRIBE (UC only)."""
    return f"{UC_TABLE}.{table_name}" if UC_WRITE_ENABLED else f"delta.`{DEMO_BASE}/{table_name}`"


print("=" * 60)
print("SETUP COMPLETE")
print("  Catalog       :", CATALOG)
print("  Schema / mode :", UC_TABLE, "| UC writes:", UC_WRITE_ENABLED)
print("  Demo base     :", DEMO_BASE)
print("  Bronze rows   :", bronze.count())
print("  Auth mode     :", BRONZE_AUTH_MODE)
print("=" * 60)
'''
